/**
 * Copyright 2015 Netsend.
 *
 * This file is part of PersDB.
 *
 * PersDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PersDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PersDB. If not, see <https://www.gnu.org/licenses/>.
 */

/* jshint -W116 */

'use strict';

var async = require('async');
var BSONStream = require('bson-stream');
var LDJSONStream = require('ld-jsonstream');
var keyFilter = require('object-key-filter');
var level = require('level-browserify');
var websocket = require('websocket-stream');

var proxy = require('./proxy');
var dataRequest = require('../lib/data_request');
var MergeTree = require('../lib/merge_tree');
var parsePersConfigs = require('../lib/parse_pers_configs');

/**
 * Instantiates a merge tree, handles indexed DB updates via ES6 Proxy and
 * initiates WebSockets.
 *
 * 1. setup all import and export hooks, filters etc.
 * 2. transparently proxy indexed DB updates
 * 3. create authenticated WebSockets and start transfering BSON
 *
 * This module should be executed as a Web Worker. A message is sent when it enters
 * a certain state and to signal the parent it's ready to receive messages.
 *
 * Full FSM: init --> listen
 *
 * The first message emitted is "init" which signals that this worker is ready to
 * receive configuration data, which consists of the db config, including
 * perspective configs.
 *
 * {
 *   [name]:         {String}      // name of this database, defaults to "_pdb"
 *   [perspectives]: {Array}       // array of other perspectives with urls
 *   [mergeTree]:    {Object}      // any MergeTree options
 * }
 *
 * After the database is opened and hooks are loaded, this worker emits a message
 * named "listen", signalling that it's ready to initiate data requests.
 *
 * Any subsequent messages sent to this worker should be a list of WebSockets to
 * initiate. This worker will open the socket and send an authenticated auth
 * request.
 *
 * A data request is sent on the incoming connection and one is expected from the
 * other side.
 * {
 *   start:          {String|Boolean}  whether or not to receive data as well. Either a
 *                                     boolean or a base64 encoded version number.
 * }
 */

var noop = function() {};
var log; // used after receiving the log configuration
var pm;  // postMessage handler to signal state

// filter password out request
function debugReq(req) {
  return keyFilter(req, ['password'], true);
}

function connErrorHandler(conn, connId, err) {
  log.err('dbe connection error: %s %s', err, connId);
  conn.close();
}

// globals
var db;
var connections = {};

/**
 * - expect one data request
 * - send one data request
 * - maybe export data
 * - maybe import data
 */
function connHandler(conn, mt, pers) {
  var connId = conn.uri;
  log.info('client connected %s', connId);

  if (connections[connId]) {
    connErrorHandler(conn, connId, new Error('connection already exists'));
    return;
  }

  connections[connId] = conn;

  // after data request is sent and received, setup reader and writer
  function finalize(req) {
    // open reader if there is an export config and data is requested
    if (pers.export && req.start) {
      var readerOpts = {
        log: log,
        tail: true,
        tailRetry: 10000,
        raw: true
      };
      if (typeof req.start === 'string') {
        readerOpts.first = req.start;
        readerOpts.excludeFirst = true;
      }
      if (typeof pers.export === 'object') {
        readerOpts.filter    = pers.export.filter;
        readerOpts.hooks     = pers.export.hooks;
        readerOpts.hooksOpts = pers.export.hooksOpts;
      }

      var mtr = mt.createReadStream(readerOpts);

      mtr.on('error', function(err) {
        log.err('db mtr error %s %s', connId, err);
      });

      mtr.on('end', function() {
        log.notice('db mtr end %s', connId);
      });

      mtr.pipe(conn);

      conn.on('error', function(err) {
        log.err('%s: %s', connId, err);
        mtr.close();
      });
      conn.on('close', function() {
        log.info('%s: close', connId);
        mtr.close();
        delete connections[connId];
      });
    }

    // handle data from remote if requested and allowed
    if (pers.import) {
      // create remote transform to ensure h.pe is set to this remote and run all hooks
      var writerOpts = {
        db:         db,
        filter:     pers.import.filter || {},
        hooks:      pers.import.hooks || [],
        hooksOpts:  pers.import.hooksOpts || {},
        log:        log
      };
      // some export hooks need the name of the database
      writerOpts.hooksOpts.to = db;

      // set hooksOpts with all keys but the pre-configured ones
      Object.keys(pers.import).forEach(function(key) {
        if (!~['filter', 'hooks', 'hooksOpts'].indexOf(key)) {
          writerOpts.hooksOpts[key] = pers.import[key];
        }
      });

      var mtw = mt.createRemoteWriteStream(pers.name, writerOpts);

      mtw.on('error', function(err) {
        log.err('db merge tree "%s"', err);
        conn.end();
      });

      // expect bson
      var bs = new BSONStream();

      bs.on('error', function(err) {
        log.err('db bson %s', err);
        conn.end();
      });

      // receive data from remote
      conn.pipe(bs).pipe(mtw);
    }
  }

  // expect one data request
  var ls = new LDJSONStream({ flush: false, maxDocs: 1, maxBytes: 512 });

  conn.pipe(ls).once('readable', function() {
    var req = ls.read();
    log.info('req received %j', req);

    conn.unpipe(ls);

    if (!dataRequest.valid(req)) {
      log.err('invalid data request %j', req);
      connErrorHandler(conn, connId, 'invalid data request');
      return;
    }

    // send data request with last offset if there are any import rules
    if (pers.import) {
      // find last local item of this remote
      mt.lastByPerspective(pers.name, 'base64', function(err, last) {
        if (err) {
          log.err('db connHandler lastByPerspective %s %s', err, pers.name);
          connErrorHandler(conn, connId, err);
          return;
        }

        log.info('db last %s', pers.name, last);

        // send data request with last offset
        var dataReq = { start: true };
        if (last) {
          dataReq.start = last;
        }

        log.notice('db setup pipes and send back data request', dataReq);
        conn.write(JSON.stringify(dataReq) + '\n');

        finalize(req);
      });
    } else {
      log.notice('db signal that no data is expected');
      conn.write(JSON.stringify({ start: false }) + '\n');

      finalize(req);
    }
  });
}

/**
 * Instantiates a merge tree, handles indexed DB updates via ES6 Proxy and
 * initiates WebSockets.
 *
 * 1. setup all import and export hooks, filters etc.
 * 2. transparently proxy indexed DB updates
 * 3. create authenticated WebSockets and start transfering BSON
 */
function start(cfg) {
  if (typeof cfg !== 'object') { throw new TypeError('cfg must be an object'); }

  // setup list of connections to initiate and create an index by perspective name
  var persCfg = parsePersConfigs(cfg.perspectives || []);
  log.info('db persCfg', debugReq(persCfg));

  // 1. setup all import and export hooks, filters etc.

  // return hooksOpts with all but the pre-configured keys
  function createHooksOpts(cfg) {
    var hooksOpts = cfg.hooksOpts || {};

    Object.keys(cfg).forEach(function(key) {
      if (!~['filter', 'hooks', 'hooksOpts', 'hide'].indexOf(key)) {
        hooksOpts[key] = cfg[key];
      }
    });

    return hooksOpts;
  }

  // if hooksOpts has a hide key, push a new hook in hooks
  function ensureHideHook(hooksOpts, hooks) {
    if (hooksOpts && hooksOpts.hide) {
      // create a hook for keys to hide
      var keysToHide = hooksOpts.hide;
      hooks.push(function(db, item, opts, cb) {
        keysToHide.forEach(function(key) {
          delete item[key];
        });
        cb(null, item);
      });
    }
  }

  // replace hide keys with actual hook implementations
  Object.keys(persCfg.pers).forEach(function(name) {
    var pers = persCfg.pers[name];
    if (pers.import) {
      if (pers.import.hooksOpts) {
        ensureHideHook(pers.import.hooksOpts, pers.import.hooks);
        pers.import.hooksOpts = createHooksOpts(pers.import.hooksOpts);
      }
    }
    if (pers.export) {
      if (pers.export.hooksOpts) {
        ensureHideHook(pers.export.hooksOpts, pers.export.hooks);
        pers.export.hooksOpts = createHooksOpts(pers.export.hooksOpts);
      }
    }
  });

  var mtOpts = cfg.mergeTree || {};
  mtOpts.perspectives = Object.keys(persCfg.pers);
  mtOpts.log = log;
  mtOpts.autoMergeInterval = mtOpts.autoMergeInterval || 10000;

  var mt = new MergeTree(db, mtOpts);

  // 2. transparently proxy indexed DB updates
  var writer = mt.createLocalWriteStream();
  proxy(cfg.name || '_pdb', writer);

  // 3. create authenticated WebSockets and start transfering BSON
  function openConn(cfg, cb) {
    if (typeof cfg !== 'object') { throw new TypeError('cfg must be an object'); }
    if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

    if (typeof cfg.username !== 'string') { throw new TypeError('cfg.username must be a string'); }
    if (typeof cfg.password !== 'string') { throw new TypeError('cfg.password must be a string'); }
    if (typeof cfg.connect !== 'object') { throw new TypeError('cfg.connect must be an object'); }
    if (typeof cfg.connect.pathname !== 'string') { throw new TypeError('cfg.connect.pathname must be a string'); }
    if (typeof cfg.connect.protocol !== 'string') { throw new TypeError('cfg.connect.protocol must be a string'); }
    if (typeof cfg.connect.hostname !== 'string') { throw new TypeError('cfg.connect.hostname must be a string'); }

    // currently only wss is supported as a valid protocol
    if (cfg.connect.protocol !== 'wss:') { throw new TypeError('cfg.connect.protocol must be "wss:"'); }
    if (cfg.connect.port != null && typeof cfg.connect.port !== 'string') { throw new TypeError('cfg.connect.port must be a string'); }

    log.notice('db setup WebSocket', debugReq(cfg));

    var authReq = {
      username: cfg.username,
      password: cfg.password,
      db: cfg.connect.pathname.substr(1) // skip leading "/"
    };

    var port = cfg.connect.port || '3344';

    var uri = 'wss://' + cfg.connect.hostname + ':' + port;
    var ws = websocket(uri, 1); // support protocol 1 only
    ws.uri = uri;

    // send the auth request and pass the connection to connHandler
    ws.write(JSON.stringify(authReq) + '\n');
    connHandler(ws, mt, cfg);
    cb();
  }

  // open WebSocket connections
  async.each(persCfg.connect, function(perspective, cb) {
    openConn(persCfg.pers[perspective], cb);
  }, function(err) {
    if (err) { throw err; }
    log.notice('db all WebSockets initiated');
  });

  // send a "listen" signal
  //self.postMessage('listen');
  pm('listen');
}

//if (typeof self.postMessage !== 'function') {
  //throw new Error('this module should be invoked as a Web Worker');
//}

//self.postMessage('init');

/**
 * Start versioning the database transparently.
 *
 * @param {Function} ipc  function called with current state
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   name {String, default "_pdb"}  name of this database
 *   perspectives {Array}           array of other perspectives with url
 *   mergeTree {Object}             any MergeTree options
 */
function init(ipc, opts) {
  if (typeof ipc !== 'function') { throw new TypeError('ipc must be a function'); }
  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }

  if (opts.name != null && typeof opts.name !== 'string') { throw new TypeError('opts.name must be a string'); }
  if (opts.perspectives != null && !Array.isArray(opts.perspectives)) { throw new TypeError('opts.perspectives must be an array'); }
  if (opts.mergeTree != null && typeof opts.mergeTree !== 'object') { throw new TypeError('opts.mergeTree must be an object'); }

  pm = ipc;

  //self.onmessage = null;

  // open log
  log = {
    emerg:   console.error.bind(console),
    alert:   console.error.bind(console),
    crit:    console.error.bind(console),
    err:     console.error.bind(console),
    warning: console.log.bind(console),
    notice:  console.log.bind(console),
    info:    console.log.bind(console),
    debug:   console.log.bind(console),
    debug2:  console.log.bind(console),
    getFileStream: noop,
    getErrorStream: noop,
    close: noop
  };

  var name = opts.name || 'pdb';
  var prefix = '_';

  // open db and call start or exit
  function openDbAndProceed() {
    level(name, { keyEncoding: 'binary', valueEncoding: 'binary', storePrefix: prefix }, function(err, dbc) {
      if (err) {
        log.err('db opening db %s', err);
        throw new Error(9);
      }
      log.info('db opened db %s', name);

      db = dbc;

      start(opts);
    });
  }

  pm('init');

  // assume database exists
  openDbAndProceed();
}

//self.onmessage = init;
module.exports = init;
