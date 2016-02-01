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
var level = require('level-js');
var websocket = require('websocket-stream');

var proxy = require('./proxy');
var dataRequest = require('../lib/data_request');
var MergeTree = require('../lib/merge_tree');
var parsePersConfigs = require('../lib/parse_pers_configs');

var noop = function() {};

// filter password out request
function debugReq(req) {
  return keyFilter(req, ['password'], true);
}

/**
 * Start a PersDB instance.
 *
 * Instantiates a merge tree, hooks, filters and handles IndexedDB updates. Use
 * "connections()" for an overview of connected systems and "connect()" to
 * initiate WebSocket connections.
 *
 * This class emits "merge" events when new merges have been saved.
 *
 * 1. setup all import and export hooks, filters etc.
 * 2. use mergeHandler or use ES6 Proxy to transparently proxy IndexedDB updates
 * 3. connect() creates authenticated WebSockets and starts transfering BSON
 *
 * @param {Object} idb     IndexedDB instance
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
 *   conflictHandler {Function}     function to handle merges with conflicts
 *                                  signature: function (attrs, shead, lhead, next)
 *                                  call next with a new item or null if not
 *                                  resolved.
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
  if (opts.conflictHandler != null && typeof opts.conflictHandler !== 'function') { throw new TypeError('opts.conflictHandler must be a function'); }
  if (opts.mergeTree != null && typeof opts.mergeTree !== 'object') { throw new TypeError('opts.mergeTree must be an object'); }

  EE.call(this, opts);

  var that = this;

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
  this._mergeInterval = this._opts.mergeInterval;
  if (this._mergeInterval == null) {
    this._mergeInterval = 5000;
  }

  this._db = level(name, { keyEncoding: 'binary', valueEncoding: 'binary', storePrefix: prefix });

  // setup list of connections to initiate and create an index by perspective name
  this._persCfg = parsePersConfigs(this._opts.perspectives || []);
  this._log.info('db persCfg', debugReq(this._persCfg));

  // inspect protocols, currently only wss is supported as a valid protocol
  this._persCfg.connect.forEach(function(pe) {
    var cfg = that._persCfg.pers[pe];
    if (cfg.connect.protocol !== 'wss:') {
      var err = new Error('only possible to connect to perspectives via wss');
      that._log.err('perspective config error: %s %s', err, cfg.connect);
      throw err;
    }
  });

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

  // set options
  // merge handler needs a writable stream, pass it later to mergeAll
  var mtOpts = this._opts.mergeTree || {};
  mtOpts.perspectives = Object.keys(this._persCfg.pers);
  mtOpts.conflictHandler = this._opts.conflictHandler;
  mtOpts.log = this._log;

  this._mt = new MergeTree(that._db, mtOpts);

  var writer = this._mt.createLocalWriteStream();
  var w = function(item, cb) {
    cb = cb || noop;
    writer.write(item, function(err) {
      if (err) { cb(err); return; }
      cb();
      that.emit('data', item);
    });
  };

  var mergeHandler;
  if (this._opts.mergeHandler) {
    // use given mergeHandler
    mergeHandler = function(newVersion, prevVersion, cb) {
      that._opts.mergeHandler(newVersion, prevVersion, function(err) {
        if (err) { cb(err); return; }
        w(newVersion, cb);
      });
    };
  } else {
    // transparently proxy IndexedDB updates using proxy module
    var reader = proxy(this._idb, w);
    mergeHandler = function(newVersion, prevVersion, cb) {
      reader(newVersion, prevVersion, cb);
    };
  }

  // start auto-merging
  this._mt.mergeAll({
    mergeHandler: mergeHandler,
    interval: that._mergeInterval
  });
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
 * Create an array with perspectives.
 *
 * @return {Array} names of all perspectives
 */
PersDB.prototype.getPerspectives = function getPerspectives() {
  return Object.keys(this._persCfg.pers);
};

/**
 * Create an overview of the status of the connection of each perspective.
 *
 * @return {Object} status of each connection with the perspective name as key
 */
PersDB.prototype.connections = function connections() {
  var that = this;
  var result = {};
  Object.keys(this._persCfg.pers).forEach(function(name) {
    var pers = that._persCfg.pers[name];
    var uri = pers.connect.href;
    var obj = {
      name: name,
      uri: uri
    };
    if (that._connections[uri]) {
      obj.status = 'connected';
    } else {
      obj.status = 'disconnected';
    }
    result[name] = obj;
  });

  return result;
};

/**
 * Initiate a WebSocket to all perspectives.
 *
 * @param {Function} cb  function called when all connections are setup
 */
PersDB.prototype.connectAll = function connectAll(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  // open WebSocket connections
  async.each(this._persCfg.connect, function(pe, cb2) {
    that.connect(that._persCfg.pers[pe].name, cb2);
  }, cb);
};

/**
 * Initiate a WebSocket connection.
 *
 * Create authenticated WebSockets and start transfering BSON
 *
 * @param {String} pe  name of the perspective to connect to
 * @param {Function} cb  function called when all connections are setup
 */
PersDB.prototype.connect = function connect(pe, cb) {
  if (typeof pe !== 'string') { throw new TypeError('pe must be a string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var cfg = this._persCfg.pers[pe];
  if (cfg == null || typeof cfg !== 'object') { throw new Error('perspective configuration not found'); }

  this._log.notice('db setup WebSocket', debugReq(cfg));

  var that = this;

  var authReq = {
    username: cfg.username,
    password: cfg.password,
    db: cfg.connect.pathname.substr(1) // skip leading "/"
  };

  var port = cfg.connect.port || '3344';

  var uri = 'wss://' + cfg.connect.hostname + ':' + port;
  var conn = websocket(uri, 1); // support protocol 1 only

  var cbCalled = false;
  conn.on('error', function(err) {
    that._log.err('ws error', err);
    if (!cbCalled) { cb(err); }
    cbCalled = true;
  });

  // send the auth request and pass the connection to connHandler
  conn.write(JSON.stringify(authReq) + '\n', function(err) {
    if (err) {
      that._log.err('ws write error', err);
      if (!cbCalled) { cb(err); }
      cbCalled = true;
      return;
    }

    that._connHandler(conn, cfg);
    cb();
  });
};

PersDB.prototype.disconnect = function disconnect(cb) {
  var that = this;
  async.each(Object.keys(this._connections), function(connId, cb2) {
    that._log.info('closing %s', connId);
    var conn = that._connections[connId];
    conn.once('close', cb2);
    conn.end();
  }, cb);
};

/**
 * - expect one data request
 * - send one data request
 * - maybe export data
 * - maybe import data
 */
PersDB.prototype._connHandler = function _connHandler(conn, pers) {
  var connId = pers.name;
  this._log.info('client connected %s', connId);

  var that = this;

  if (this._connections[connId]) {
    that._connErrorHandler(conn, connId, new Error('connection already exists'));
    return;
  }

  this._connections[connId] = conn;

  this.emit('connection', connId);
  this.emit('connection:connect', connId);

  conn.on('close', function() {
    that._log.info('%s: close', connId);
    delete that._connections[connId];
    that.emit('connection', connId);
    that.emit('connection:disconnect', connId);
  });

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
