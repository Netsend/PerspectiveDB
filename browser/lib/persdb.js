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
var stream = require('stream');
var util = require('util');

var async = require('async');
var BSONStream = require('bson-stream');
var LDJSONStream = require('ld-jsonstream');
var keyFilter = require('object-key-filter');
var level = require('level-packager')(require('level-js'));
var websocket = require('websocket-stream');
var xtend = require('xtend');

var proxy = require('./proxy');
var dataRequest = require('../../lib/data_request');
var MergeTree = require('../../lib/merge_tree');
var parsePersConfigs = require('../../lib/parse_pers_configs');

var noop = function() {};

// filter password out request
function debugReq(req) {
  return keyFilter(req, ['password'], true);
}

/**
 * Start a PersDB instance.
 *
 * Instantiates a merge tree, hooks, filters and handles IndexedDB updates. Use
 * "connect()" to initiate WebSocket connections to other peers.
 *
 * This class emits "data" events when new local or merges with a remote have been
 * saved.
 *
 * 1. setup all import and export hooks, filters etc.
 * 2. uses ES6 Proxy to transparently proxy IndexedDB updates
 * 3. connect() creates authenticated WebSockets and starts transfering BSON
 *
 * @param {Object} idb  opened IndexedDB database
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   watch {Boolean, default false}  watch changes to all object stores via ES6
 *                                   Proxy
 *   snapshots {String, default "_pdb"}  internal object store that contains each
 *                                       version
 *   conflicts {String, default "_conflicts"}  internal object store where merge
 *                                             conflicts should be saved
 *   iterator {Function}            called with every new item from a remote
 *   perspectives {Array}           array of other perspectives with url
 *   mergeTree {Object}             any MergeTree options
 */
function PersDB(idb, opts) {
  if (idb == null || typeof idb !== 'object') { throw new TypeError('idb must be an object'); }

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }

  if (opts.watch != null && typeof opts.watch !== 'boolean') { throw new TypeError('opts.watch must be a boolean'); }
  if (opts.snapshots != null && typeof opts.snapshots !== 'string') { throw new TypeError('opts.snapshots must be a string'); }
  if (opts.iterator != null && typeof opts.iterator !== 'function') { throw new TypeError('opts.iterator must be a function'); }
  if (opts.perspectives != null && !Array.isArray(opts.perspectives)) { throw new TypeError('opts.perspectives must be an array'); }
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
    debug2:  noop,
    getFileStream: noop,
    getErrorStream: noop,
    close: noop
  };

  var snapshots = opts.snapshots || '_pdb';

  this._idb = idb;
  this._opts = opts;
  this._connections = {};

  this._db = level('_pdb', { keyEncoding: 'binary', valueEncoding: 'none', asBuffer: false, reopenOnTimeout: true, storeName: snapshots });

  // setup list of connections to initiate and create an index by perspective name
  //this._persCfg = parsePersConfigs(this._opts.perspectives || []);
  //this._log.info('db persCfg', debugReq(this._persCfg));

  // inspect protocols, currently only wss is supported as a valid protocol
  /*
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
  */

  this._stores = {}; // enable fast store lookup
  for (var i = 0; i < idb.objectStoreNames.length; i++) {
    var store = idb.objectStoreNames[i];
    that._stores[store] = true;
  }
  this._keyPaths = {}; // enable fast key path lookup

  // set options
  var mtOpts = xtend(this._opts.mergeTree);
  mtOpts.log = this._log;

  this._mt = new MergeTree(that._db, mtOpts);

  this._localWriter = this._mt.createLocalWriteStream();

  // whether or not a proxy is used, make sure the original IndexedDB transaction method is available
  this._idbTransaction = idb.transaction.bind(idb);

  if (this._opts.watch) {
    // transparently track IndexedDB updates using proxy module

    // only proxy readwrite transactions that are not on the snapshot object store
    function doProxy(mode, osName) {
      return mode === 'readwrite' && osName !== snapshots;
    }

    proxy(this._idb, doProxy, that._getHandlers());
  }

  // auto-merge remote versions (use a writable stream to enable backpressure)
  this._mt.startMerge().pipe(new stream.Writable({
    objectMode: true,
    write: that._writeMerge.bind(that)
  }));
}

util.inherits(PersDB, EE);

module.exports = global.PersDB = PersDB;

PersDB.prototype.put = function put(objectStore, key, item) {
  // should use something very much like this.writeMerge
  throw new Error('not implemented yet');

  var id = PersDB._generateId(objectStore, key);
  this._localWriter.write({
    n: {
      h: { id: id },
      b: item
    }
  }, (err) =>  {
    if (err) { this._log.err('put error:', err); return; }
    // TODO do atmoic update of object store
  });
};

PersDB.prototype.del = function del(objectStore, key) {
  // should use something very much like this.writeMerge
  throw new Error('not implemented yet');

  var id = PersDB._generateId(objectStore, key);
  this._localWriter.write({
    n: {
      h: { id: id, d: true }
    }
  }, (err) =>  {
    if (err) { this._log.err('del error:', err); return; }
    // TODO do atmoic update of object store
  });
};

PersDB.prototype._connErrorHandler = function _connErrorHandler(conn, connId, err) {
  this._log.err('dbe connection error: %s %s', err, connId);
  conn && conn.close();
};

PersDB.prototype.createReadStream = function createReadStream(opts) {
  if (this._mt == null || typeof this._mt !== 'object') { throw new TypeError('mt must be instantiated'); }
  return this._mt.createReadStream(opts);
};

/**
 * Add a new perspective.
 *
 * @param {String} name  name of the perspective
 * @param {Object} cfg  the perspective configuration
 */
 /*
PersDB.prototype.addPerspective = function addPerspective(name, cfg) {
  if (!name || typeof name !== 'string') { throw new TypeError('name must be a string'); }
  if (cfg == null || typeof cfg !== 'object') { throw new TypeError('cfg must be an object'); }
  if (this._persCfg.pers[name]) { throw new Error('perspective already exists'); }

  this._persCfg.connect.push(name);
  this._persCfg.pers[name] = parsePersConfigs([cfg]).pers[name];
  this._mt.addPerspective(name);
};
 */

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
 * Create an authenticated secure WebSocket connection to a remote peer and start
 * transfering BSON.
 *
 * @param {Object} remote  configuration details of the remote, see below.
 * @return {Promise}
 *
 * required:
 *   name {String}  local reference to the remote
 *   host {String}  address of the remote
 *   db {String}  name of the database on the remote
 *   username {String}  username of remote account
 *   password {String}  password of remote account
 *
 * opts:
 *   import {Boolean, default true}  import changes from remote
 *   export {Boolean, default true}  export changes to remote
 *   port {Number, default 3344}  port of the remote WebSocket server
 */
PersDB.prototype.connect = function connect(remote) {
  if (remote == null || typeof remote !== 'object') { throw new TypeError('remote must be an object'); }
  if (!remote.name || typeof remote.name !== 'string') { throw new TypeError('remote.name must be a string'); }
  if (!remote.host || typeof remote.host !== 'string') { throw new TypeError('remote.host must be a string'); }
  if (!remote.db || typeof remote.db !== 'string') { throw new TypeError('remote.db must be a string'); }
  if (!remote.username || typeof remote.username !== 'string') { throw new TypeError('remote.username must be a string'); }
  if (!remote.password || typeof remote.password !== 'string') { throw new TypeError('remote.password must be a string'); }

  // options
  if (remote.port && typeof remote.port !== 'number') { throw new TypeError('remote.port must be a number'); }

  var that = this;

  var authReq = {
    username: remote.username,
    password: remote.password,
    db: remote.db
  };

  var port = remote.port || '3344';

  var uri = 'wss://' + remote.host + ':' + port;

  return new Promise((resolve, reject) => {
    var conn = websocket(uri, 1); // support protocol 1 only

    conn.on('error', reject);

    // send the auth request and pass the connection to connHandler
    conn.write(JSON.stringify(authReq) + '\n', function(err) {
      if (err) { reject(err); return; }

      that._mt.addPerspective(remote.name);
      that._connHandler(conn, xtend({
        import: true,
        export: true,
      }, remote));
      resolve();
    });
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
        bson: true
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

// create an id
PersDB._generateId = function _generateId(objectStore, key) {
  return objectStore + '\x01' + key;
};

// extract an id
PersDB._idFromId = function _idFromId(id) {
  // expect only one 0x01
  return id.split('\x01', 2)[1];
};

// extract an object store from an id
PersDB._objectStoreFromId = function _objectStoreFromId(id) {
  // expect only one 0x01
  return id.split('\x01', 1)[0];
};

/**
 * Return handlers for proxying modifications on the object stores to the snapshot
 * object store. Used if the watch option is set.
 *
 * @return {Object}
 */
PersDB.prototype._getHandlers = function _getHandlers() {
  var that = this;

  // pre and post handlers for objectStore.add, put, delete and clear
  function postAdd(os, value, key, ret) {
    // wait for return with key
    ret.onsuccess = function(ev) {
      var obj = {
        n: {
          h: { id: PersDB._generateId(ev.target.source.name, ev.target.result) },
          b: value
        }
      };
      that._localWriter.write(obj);
    };
  }

  function postPut(os, value, key, ret) {
    // wait for return with key
    ret.onsuccess = function(ev) {
      var obj = {
        n: {
          h: { id: PersDB._generateId(ev.target.source.name, ev.target.result) },
          b: value
        }
      };
      that._localWriter.write(obj);
    };
  }

  function postDelete(os, key, ret) {
    // wait for return with key
    ret.onsuccess = function(ev) {
      var obj = {
        n: {
          h: {
            id: PersDB._generateId(ev.target.source.name, key),
            d: true
          }
        }
      };
      that._localWriter.write(obj);
    };
  }

  return { postAdd, postPut, postDelete };
};

/**
 * Handle conflicting merges or other recoverable write errors.
 *
 * TODO: save conflicts in the conflict object store
 */
PersDB.prototype._handleConflict = function _handleConflict(obj, cb) {
  this.emit('conflict', obj);
  process.nextTick(cb);
};

/**
 * Write new merges from remotes to the object store and the snapshot object store.
 *
 * See MergeTree.startMerge for object syntax.
 *
 * TODO: verify version in object store to prevent data loss because of race
 * conditions between local and remote updates.
 * TODO: make the operations atomic.
 */
PersDB.prototype._writeMerge = function _writeMerge(obj, enc, cb) {
  if (obj.c && obj.c.length) {
    that._handleConflict(obj, cb);
    return;
  }

  var that = this;

  var newVersion = obj.n;
  var prevVersion = obj.l;

  var osName = PersDB._objectStoreFromId(newVersion.h.id);
  if (!this._stores[osName]) {
    this._log.notice('_writeMerge object from an unknown object store received', osName, newVersion.h);
    process.nextTick(cb);
    return;
  }
  var id = PersDB._idFromId(newVersion.h.id);

  this._log.debug('_writeMerge', osName, newVersion.h);

  // open a rw transaction on the object store
  var tr = this._idbTransaction([osName], 'readwrite');
  var os = tr.objectStore(osName);

  // make sure the keypath of this object store is known
  if (!this._keyPaths.hasOwnProperty(osName)) {
    this._keyPaths[osName] = os.keyPath;
    if (typeof this._keyPaths[osName] !== 'string') {
      console.error('warning: keypath is not a string, converting: %s, store: %s', this._keyPaths[osName], osName);
      this._keyPaths[osName] = Object.prototype.toString(this._keyPaths[osName]);
    }
  }

  // ensure keypath is set if the store has a keypath
  if (this._keyPaths[osName] && !newVersion.b[this._keyPaths[osName]]) {
    newVersion.b[this._keyPaths[osName]] = id;
  }

  tr.oncomplete = function(ev) {
    that._log.debug('_writeMerge success', ev);
    that._localWriter.write(obj, function(err) {
      if (err) { cb(err); return; }
      that.emit('merge', obj);
      cb();
    });
  };

  tr.onabort = function(ev) {
    console.error('_writeMerge abort', ev);
    cb(ev.target);
    that._handleConflict(obj, cb);
  };

  tr.onerror = function(ev) {
    console.error('_writeMerge error', ev);
    cb(ev.target);
    that._handleConflict(obj, cb);
  };

  if (newVersion.h.d) {
    that._log.debug('delete', newVersion.h);
    os.delete(id);
  } else {
    that._log.debug('put', newVersion.b);
    os.put(newVersion.b);
  }
};
