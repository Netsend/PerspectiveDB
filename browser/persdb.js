/**
 * Copyright 2015, 2016 Netsend.
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

var EE = require('events');
var util = require('util');

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

// filter password out request
function debugReq(req) {
  return keyFilter(req, ['password'], true);
}

/**
 * Start versioning the database transparently.
 *
 * @param {Object} idb     indexedDB instance
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   name {String, default "_pdb"}  name of this database
 *   iterator {Function}            called with every new item from a remote
 *   perspectives {Array}           array of other perspectives with url
 *   mergeInterval {Number, default 5000} time in ms to wait and repeat a merge, 0
 *                                  means off.
 *   mergeHandler {Function}        function to handle newly created merges
 *                                  signature: function (merged, lhead, next). If
 *                                  not provided a transparent ES6 Proxy is used.
 *   mergeTree {Object}             any MergeTree options
 */
function PersDB(idb, opts) {
  if (idb == null || typeof idb !== 'object') { throw new TypeError('idb must be an object'); }

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }

  if (opts.name != null && typeof opts.name !== 'string') { throw new TypeError('opts.name must be a string'); }
  if (opts.iterator != null && typeof opts.iterator !== 'function') { throw new TypeError('opts.iterator must be a function'); }
  if (opts.perspectives != null && !Array.isArray(opts.perspectives)) { throw new TypeError('opts.perspectives must be an array'); }
  if (opts.mergeInterval != null && typeof opts.mergeInterval !== 'number') { throw new TypeError('opts.mergeInterval must be a number'); }
  if (opts.mergeHandler != null && typeof opts.mergeHandler !== 'function') { throw new TypeError('opts.mergeHandler must be a function'); }
  if (opts.mergeTree != null && typeof opts.mergeTree !== 'object') { throw new TypeError('opts.mergeTree must be an object'); }

  EE.call(this, opts);

  // open log
  this._log = {
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

  this._idb = idb;
  this._opts = opts;
  this._connections = {};

  this._db = level(name, { keyEncoding: 'binary', valueEncoding: 'binary', storePrefix: prefix });

  // setup list of connections to initiate and create an index by perspective name
  this._persCfg = parsePersConfigs(this._opts.perspectives || []);
  this._log.info('db persCfg', debugReq(this._persCfg));

  // setup all import and export hooks, filters and a MergeTree

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

  var that = this;

  // if hooksOpts has a hide key, push a new hook in hooks
  function ensureHideHook(hooksOpts, hooks) {
    if (hooksOpts && hooksOpts.hide) {
      // create a hook for keys to hide
      var keysToHide = hooksOpts.hide;
      hooks.push(function(db, item, opts2, cb) {
        keysToHide.forEach(function(key) {
          delete item[key];
        });
        cb(null, item);
      });
    }
  }

  // replace hide keys with actual hook implementations
  Object.keys(this._persCfg.pers).forEach(function(name) {
    var pers = that._persCfg.pers[name];
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

  var mtOpts = this._opts.mergeTree || {};
  mtOpts.perspectives = Object.keys(this._persCfg.pers);
  mtOpts.log = this._log;

  this._mt = new MergeTree(that._db, mtOpts);

  var writer = this._mt.createLocalWriteStream();

  if (this._opts.mergeHandler) {
    // use mergeHandler
    this._mt.mergeAll({
      mergeHandler: function(newVersion, prevVersion, cb2) {
        that._opts.mergeHandler(newVersion, prevVersion, function(err) {
          if (err) { cb2(err); return; }
          writer.write(cb2);
        });
        that.emit('merge', newVersion);
      },
      interval: that._opts.mergeInterval || 5000
    });
  } else {
    // transparently proxy indexed DB updates via ES6 Proxy
    var reader = proxy(this._idb, writer.write.bind(writer));

    // start auto-merging
    this._mt.mergeAll({
      mergeHandler: function(newVersion, prevVersion, cb2) {
        reader(newVersion, prevVersion, function(err) {
          if (err) { cb2(err); return; }
          that.emit('merge', newVersion);
          cb2();
        });
      },
      interval: that._opts.mergeInterval || 5000
    });
  }
}

util.inherits(PersDB, EE);

module.exports = global.PersDB = PersDB;

PersDB.prototype._connErrorHandler = function _connErrorHandler(conn, connId, err) {
  this._log.err('dbe connection error: %s %s', err, connId);
  conn.close();
};

PersDB.prototype.createReadStream = function createReadStream(opts) {
  if (this._mt == null || typeof this._mt !== 'object') { throw new TypeError('mt must be instantiated'); }
  return this._mt.createReadStream(opts);
};

/**
 * Initiate WebSockets.
 *
 * Create authenticated WebSockets and start transfering BSON
 *
 * @param {Function} cb    function called when everything is setup
 */
PersDB.prototype.connect = function connect(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  function openConn(cfg, cb2) {
    if (typeof cfg !== 'object') { throw new TypeError('cfg must be an object'); }
    if (typeof cb2 !== 'function') { throw new TypeError('cb2 must be a function'); }

    if (typeof cfg.username !== 'string') { throw new TypeError('cfg.username must be a string'); }
    if (typeof cfg.password !== 'string') { throw new TypeError('cfg.password must be a string'); }
    if (typeof cfg.connect !== 'object') { throw new TypeError('cfg.connect must be an object'); }
    if (typeof cfg.connect.pathname !== 'string') { throw new TypeError('cfg.connect.pathname must be a string'); }
    if (typeof cfg.connect.protocol !== 'string') { throw new TypeError('cfg.connect.protocol must be a string'); }
    if (typeof cfg.connect.hostname !== 'string') { throw new TypeError('cfg.connect.hostname must be a string'); }

    // currently only wss is supported as a valid protocol
    if (cfg.connect.protocol !== 'wss:') { throw new TypeError('cfg.connect.protocol must be "wss:"'); }
    if (cfg.connect.port != null && typeof cfg.connect.port !== 'string') { throw new TypeError('cfg.connect.port must be a string'); }

    that._log.notice('db setup WebSocket', debugReq(cfg));

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
    that._connHandler(ws, that._mt, cfg);
    cb2();
  }

  // open WebSocket connections
  async.each(this._persCfg.connect, function(perspective, cb2) {
    openConn(that._persCfg.pers[perspective], cb2);
  }, function(err) {
    if (err) { throw err; }
    that._log.notice('db all WebSockets initiated');
  });

  cb();
};

/**
 * - expect one data request
 * - send one data request
 * - maybe export data
 * - maybe import data
 */
PersDB.prototype._connHandler = function _connHandler(conn, pers) {
  var connId = conn.uri;
  this._log.info('client connected %s', connId);

  var that = this;

  if (this._connections[connId]) {
    that._connErrorHandler(conn, connId, new Error('connection already exists'));
    return;
  }

  this._connections[connId] = conn;

  // after data request is sent and received, setup reader and writer
  function finalize(req) {
    // open reader if there is an export config and data is requested
    if (pers.export && req.start) {
      var readerOpts = {
        log: that._log,
        tail: true,
        tailRetry: 5000,
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

      var mtr = that._mt.createReadStream(readerOpts);

      mtr.on('error', function(err) {
        that._log.err('db mtr error %s %s', connId, err);
      });

      mtr.on('end', function() {
        that._log.notice('db mtr end %s', connId);
      });

      mtr.pipe(conn);

      conn.on('error', function(err) {
        that._log.err('%s: %s', connId, err);
        mtr.close();
      });
      conn.on('close', function() {
        that._log.info('%s: close', connId);
        mtr.close();
        delete that._connections[connId];
      });
    }

    // handle data from remote if requested and allowed
    if (pers.import) {
      // create remote transform to ensure h.pe is set to this remote and run all hooks
      var writerOpts = {
        db:         that._db,
        filter:     pers.import.filter || {},
        hooks:      pers.import.hooks || [],
        hooksOpts:  pers.import.hooksOpts || {},
        log:        that._log
      };
      // some export hooks need the name of the database
      writerOpts.hooksOpts.to = that._db;

      // set hooksOpts with all keys but the pre-configured ones
      Object.keys(pers.import).forEach(function(key) {
        if (!~['filter', 'hooks', 'hooksOpts'].indexOf(key)) {
          writerOpts.hooksOpts[key] = pers.import[key];
        }
      });

      var mtw = that._mt.createRemoteWriteStream(pers.name, writerOpts);

      mtw.on('error', function(err) {
        that._log.err('db merge tree "%s"', err);
        conn.end();
      });

      // expect bson
      var bs = new BSONStream();

      bs.on('error', function(err) {
        that._log.err('db bson %s', err);
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
    that._log.info('req received %j', req);

    conn.unpipe(ls);

    if (!dataRequest.valid(req)) {
      that._log.err('invalid data request %j', req);
      that._connErrorHandler(conn, connId, 'invalid data request');
      return;
    }

    // send data request with last offset if there are any import rules
    if (pers.import) {
      // find last local item of this remote
      that._mt.lastReceivedFromRemote(pers.name, 'base64', function(err, last) {
        if (err) {
          that._log.err('db _connHandler lastReceivedFromRemote %s %s', err, pers.name);
          that._connErrorHandler(conn, connId, err);
          return;
        }

        that._log.info('db last %s', pers.name, last);

        // send data request with last offset
        var dataReq = { start: true };
        if (last) {
          dataReq.start = last;
        }

        that._log.notice('db setup pipes and send back data request', dataReq);
        conn.write(JSON.stringify(dataReq) + '\n');

        finalize(req);
      });
    } else {
      that._log.notice('db signal that no data is expected');
      conn.write(JSON.stringify({ start: false }) + '\n');

      finalize(req);
    }
  });
};
