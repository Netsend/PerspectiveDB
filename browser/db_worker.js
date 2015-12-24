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

/* global self */
/* jshint -W116 */

'use strict';

var async = require('async');
var BSONStream = require('bson-stream');
var LDJSONStream = require('ld-jsonstream');
var keyFilter = require('object-key-filter');
var level = require('level');
var websocket = require('websocket-stream');

var proxy = require('./proxy');
var dataRequest = require('../lib/data_request');
var logger = require('../lib/logger');
var MergeTree = require('../lib/merge_tree');
var parsePersConfigs = require('../lib/parse_pers_configs');

/**
 * Instantiates a merge tree, handles indexed DB updates via ES6 Proxy and
 * initiates WebSockets.
 *
 * 1. transparently proxy indexed DB updates
 * 2. setup all import and export hooks, filters etc.
 * 3. create authenticated WebSockets and start transfering BSON
 *
 * This module should be executed as a Web Worker. A message is sent when it enters
 * a certain state and to signal the parent it's ready to receive messages.
 *
 * Full FSM: init --> listen
 *
 * The first message emitted is "init" which signals that this worker is ready to
 * receive configuration data, which consists of the db config, including
 * perspective configs and log configuration.
 *
 * {
 *   log:            {Object}      // log configuration
 *   [name]:         {String}      // name of this database, defaults to "persdb"
 *   [debug]:        {Boolean}     // defaults to false
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

var log; // used after receiving the log configuration

var programName = 'dbw';

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
  var connId = conn.url;
  log.info('client connected %s', connId);

  if (connections[connId]) {
    connErrorHandler(conn, connId, new Error('connection already exists'));
    return;
  }

  var mtr, mtw, dataReqReceived, dataReqSent;

  conn.on('error', function(err) {
    log.err('%s: %s', connId, err);
    if (mtr) { mtr.close(); }
  });
  conn.on('close', function() {
    log.info('%s: close', connId);
    if (mtr) { mtr.close(); }
    delete connections[connId];
  });

  connections[connId] = conn;

  // expect one data request
  // create line delimited json stream
  var ls = new LDJSONStream({ maxBytes: 512 });

  // setup mtr and mtw if data requests have been received and sent
  function finalize() {
    if (dataReqReceived && dataReqSent) {
      // send data to remote if requested and allowed
      if (mtr) { mtr.pipe(conn); }

      // receive data from remote if requested and allowed
      if (mtw) {
        // expect bson
        var bs = new BSONStream();

        bs.on('error', function(err) {
          log.err('dbw bson %s', err);
          conn.end();
        });

        // receive data from remote
        conn.pipe(bs).pipe(mtw);
      }
    }
  }

  conn.pipe(ls).once('data', function(req) {
    log.info('req received %j', req);

    conn.unpipe(ls);

    if (!dataRequest.valid(req)) {
      log.err('invalid data request %j', req);
      connErrorHandler(conn, connId, 'invalid data request');
      return;
    }

    dataReqReceived = true;

    // determine start and whether there is an export key
    // if so, open merge tree reader
    if (req.start && pers.export) {
      var readerOpts = {
        log: log,
        first: req.offset,
        tail: true,
        raw: true
      };
      if (typeof pers.export === 'object') {
        readerOpts.filter    = pers.export.filter;
        readerOpts.hooks     = pers.export.hooks;
        readerOpts.hooksOpts = pers.export.hooksOpts;
      }

      mtr = mt.createReadStream(readerOpts);

      mtr.on('error', function(err) {
        log.err('dbw mtr error %s %s', connId, err);
      });

      mtr.on('end', function() {
        log.notice('dbw mtr end %s', connId);
      });
    }
    finalize();
  });

  // send data request with last offset if there are any import rules
  if (pers.import) {
    // find last local item of this remote
    mt.lastByPerspective(pers.name, 'base64', function(err, last) {
      if (err) {
        log.err('dbw connHandler lastByPerspective %s %s', err, pers.name);
        connErrorHandler(conn, connId, err);
        return;
      }

      log.info('dbw last %s %j', pers.name, last);

      // create remote transform to ensure h.pe is set to this remote and run all hooks
      var wsOpts = {
        db:         db,
        filter:     pers.import.filter || {},
        hooks:      pers.import.hooks || [],
        hooksOpts:  pers.import.hooksOpts || {},
        log:        log
      };
      // some export hooks need the name of the database
      wsOpts.hooksOpts.to = db;

      // set hooksOpts with all keys but the pre-configured ones
      Object.keys(pers.import).forEach(function(key) {
        if (!~['filter', 'hooks', 'hooksOpts'].indexOf(key)) {
          wsOpts.hooksOpts[key] = pers.import[key];
        }
      });

      mtw = mt.createRemoteWriteStream(pers.name, wsOpts);

      mtw.on('error', function(err) {
        log.err('dbw merge tree "%s"', err);
        conn.end();
      });

      // send data request with last offset
      var dataReq = { start: true };
      if (last) {
        dataReq.start = last;
      }

      log.notice('dbw setup pipes and send back data request %j', dataReq);
      conn.write(JSON.stringify(dataReq) + '\n');

      dataReqSent = true;
      finalize();
    });
  } else {
    log.notice('dbw signal that no data is expected');
    conn.write(JSON.stringify({ start: false }) + '\n');

    dataReqSent = true;
    finalize();
  }
}

/**
 * Instantiates a merge tree, handles indexed DB updates via ES6 Proxy and
 * initiates WebSockets.
 *
 * 1. transparently proxy indexed DB updates, automatically done by including
 *    proxy.js
 * 2. setup all import and export hooks, filters etc.
 * 3. create authenticated WebSockets and start transfering BSON
 */
function start(cfg) {
  if (typeof cfg !== 'object') { throw new TypeError('cfg must be an object'); }

  // setup list of connections to initiate and create an index by perspective name
  var persCfg = parsePersConfigs(cfg.perspectives || []);
  log.info('dbw persCfg %j', debugReq(persCfg));

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
  mtOpts.autoMergeInterval = mtOpts.autoMergeInterval || 1000;

  // set global, used in connHandler
  var mt = new MergeTree(db, mtOpts);

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
    if (cfg.connect.protocol !== 'wss') { throw new TypeError('cfg.connect.protocol must be "wss"'); }
    if (cfg.connect.port != null && typeof cfg.connect.port !== 'number') { throw new TypeError('cfg.connect.port must be a number'); }

    log.notice('dbw setup WebSocket %j', debugReq(cfg));

    var authReq = {
      username: cfg.username,
      password: cfg.password,
      db: cfg.connect.pathname.substr(1) // skip leading "/"
    };

    var port = cfg.connect.port || 3344;

    var ws = websocket('wss://' + cfg.connect.hostname + ':' + port, 1); // support protocol 1 only

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
    log.notice('dbw all WebSockets initiatesd');
  });

  // handle shutdown
  function shutdown() {
    log.notice('dbw shutting down');

    async.each(Object.keys(connections), function(connId, cb) {
      log.info('closing %s', connId);
      var conn = connections[connId];
      conn.once('close', cb);
      conn.end();
    }, function(err) {
      if (err) { log.err('dbw error closing connection: %s', err); }

      mt.close(function(err) {
        if (err) { log.err('dbw error closing mt: %s', err); }

        db.close(function(err) {
          if (err) { log.err('dbw error closing db: %s', err); }

          log.notice('dbw closed');

          self.close();
        });
      });
    });
  }

  // send a "listen" signal
  self.postMessage('listen');
}

if (typeof self.postMessage !== 'function') {
  throw new Error('this module should be invoked as a Web Worker');
}

self.postMessage('init');

/**
 * Expect a db config, log config and any merge tree options.
 *
 * {
 *   log:            {Object}      // log configuration
 *   [name]:         {String}      // name of this database, defaults to "persdb"
 *   [debug]:        {Boolean}     // defaults to false
 *   [perspectives]: {Array}       // array of other perspectives with urls
 *   [mergeTree]:    {Object}      // any MergeTree options
 * }
 */
function init(msg) {
  if (typeof msg !== 'object') { throw new TypeError('msg must be an object'); }
  if (typeof msg.log !== 'object') { throw new TypeError('msg.log must be an object'); }

  if (msg.name != null && typeof msg.name !== 'string') { throw new TypeError('msg.name must be a string'); }
  if (msg.debug != null && typeof msg.debug !== 'boolean') { throw new TypeError('msg.debug must be a string'); }
  if (msg.perspectives != null && !Array.isArray(msg.perspectives)) { throw new TypeError('msg.perspectives must be an array'); }
  if (msg.mergeTree != null && typeof msg.mergeTree !== 'object') { throw new TypeError('msg.mergeTree must be an object'); }

  self.onmessage = null;

  programName = 'dbw ' + msg.name || 'persdb';

  msg.log.ident = programName;

  // open log
  logger(msg.log, function(err, l) {
    if (err) { l.err(err); throw err; }

    log = l; // use this logger in the mt's as well

    // open db and call start or exit
    function openDbAndProceed() {
      level(msg.name, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, dbc) {
        if (err) {
          log.err('dbw opening db %s', err);
          throw new Error(9);
        }
        log.info('dbw opened db %s', msg.name);

        db = dbc;

        start(msg);
      });
    }

    // assume database exists
    openDbAndProceed();
  });
}

self.onmessage = init;
