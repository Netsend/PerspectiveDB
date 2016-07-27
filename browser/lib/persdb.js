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

'use strict';

var EE = require('events');
var stream = require('stream');
var util = require('util');

var async = require('async');
var level = require('level-packager')(require('level-js'));
var websocket = require('websocket-stream');
var xtend = require('xtend');

var proxy = require('./proxy');
var MergeTree = require('../../lib/merge_tree');
var remoteConnHandler = require('../../lib/remote_conn_handler');

var noop = function() {};

/**
 * @event PersDB#data
 * @param {Object} obj
 * @param {String} obj.os - name of the object store
 * @param {?Object} obj.n - new version, is null on delete
 * @param {?Object} obj.p - previous version, is null on insert
 */
/**
 * @event PersDB#conflict
 * @param {Object} obj
 * @param {String} obj.os - name of the object store
 * @param {?Object} obj.n - new version, is null on delete
 * @param {?Object} obj.p - previous version, is null on insert
 * @param {?Array} obj.c - array with conflicting key names
 */

/**
 * Creates a new PersDB instance. Make sure idb is opened (and upgradeneeded is
 * handled) before passing it to this constructor. If opts.watch is used, then add,
 * put and delete operations on any of the object stores are automatically
 * detected. Note that for watch to work, ES6 Proxy must be supported by the
 * browser (i.e. Firefox 18+ or Chrome 49+). If watch is not used, {@link PersDB#put}
 * and {@link PersDB#del} must be used in order to modify the contents of any
 * object store.
 *
 * @class PersDB
 *
 * @param {IDBDatabase} idb  opened IndexedDB database
 * @param {Object} [opts]
 * @param {Boolean} opts.watch=false  automatically watch changes to all object stores using ES6 Proxy
 *
 * @fires PersDB#data
 * @fires PersDB#conflict
 *
 * @todo support loading hooks
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

/**
 * Insert or update an item in a store by key.
 *
 * @todo make atomic
 *
 * @param {String} objectStore - name of the store
 * @param {mixed} key - key of the item to update
 * @param {mixed} item - item contents
 * @return {Promise}
 */
PersDB.prototype.put = function put(objectStore, key, item) {
  // should use something very much like this.writeMerge
  throw new Error('not implemented yet');

  var id = PersDB._generateId(objectStore, key);
  return new Promise((resolve, reject) => {
    this._localWriter.write({
      n: {
        h: { id: id },
        b: item
      }
    }, (err) => {
      err ? reject(err) : resolve();
    });
  });
};

/**
 * Delete an item by key.
 *
 * @todo make atomic
 *
 * @param {String} objectStore - name of the store
 * @param {mixed} key - key of the item to update
 * @return {Promise}
 */
PersDB.prototype.del = function del(objectStore, key) {
  // should use something very much like this.writeMerge
  throw new Error('not implemented yet');

  var id = PersDB._generateId(objectStore, key);
  return new Promise((resolve, reject) => {
    this._localWriter.write({
      n: {
        h: { id: id, d: true }
      }
    }, (err) =>  {
      err ? reject(err) : resolve();
    });
  });
};

/**
 * Get a readable stream over each and every version in chronological order.
 *
 * @param {Object} [opts]
 * @param {Boolean} opts.bson=false - whether to return a BSON serialized or
 *   deserialized object (false).
 * @param {Object} opts.filter - conditions a document should hold
 * @param {String} opts.first - base64 first version, offset
 * @param {String} opts.last - base64 last version
 * @param {Boolean} opts.excludeFirst=false - whether or not first item should
 *   be excluded
 * @param {Boolean} opts.excludeLast=false - whether or not the last item should
 *   be excluded
 * @param {Boolean} opts.reverse=false - if true, starts with last version
 * @param {Array} opts.hooks - array of asynchronous functions to execute, each
 *   hook has the following signature: db, object, options, callback and should
 *   callback with an error object, the new item and possibly extra data.
 * @param {Object} opts.hooksOpts - options to pass to a hook
 * @param {Boolean} opts.tail=false - if true, keeps the stream open
 * @param {Number} opts.tailRetry=1000 - reopen readers every tailRetry ms
 * @return {stream.Readable}
 * @see {@link https://nodejs.org/api/stream.html#stream_class_stream_readable}
 */
PersDB.prototype.createReadStream = function createReadStream(opts) {
  return this._mt.createReadStream(opts);
};

/**
 * Create an authenticated secure WebSocket connection to a remote peer and start
 * transfering BSON.
 *
 * @param {Object} remote
 * @param remote.name {String}  local reference to the remote
 * @param remote.host {String}  address of the remote
 * @param remote.db {String}  name of the database on the remote
 * @param {String} remote.username - username of remote account
 * @param {String} remote.password - password of remote account
 * @param {Boolean} [remote.import=true] - import changes from remote
 * @param {Boolean} [remote.export=true] - export changes to remote
 * @param {Number} [remote.port=3344] - port of the remote WebSocket server
 * @return {Promise}
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
  var error;

  return new Promise((resolve, reject) => {
    var conn = new WebSocket(uri);

    var connId = remote.name;

    conn.onerror = reject;

    // cleanup closed connections
    conn.onclose = function() {
      reject(); // ensure promise is called when the connection is closed prematurely
      delete that._connections[connId];
    };

    // register connection
    if (that._connections[connId]) {
      error = new Error('connection already exists');
      that._connErrorHandler(conn, connId, error);
      reject(error);
      return;
    }

    that._connections[connId] = conn;

    // send the auth request
    conn.onopen = function() {
      conn.send(JSON.stringify(authReq) + '\n');

      // start merging
      that._mt.addPerspective(remote.name);

      var opts = xtend({
        import: true,
        export: true,
      }, remote);

      // do the data request handshake and setup readers and writers.
      // pass the socket with stream capabilities
      remoteConnHandler(websocket(conn), that._mt, opts, true, remote.name, function(err) {
        if (err) {
          that._connErrorHandler(conn, connId, err);
          reject(err);
          return;
        }

        resolve();
      });
    };
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

PersDB.prototype._connErrorHandler = function _connErrorHandler(conn, connId, err) {
  this._log.err('connection error with %s: %s', connId, err);
  conn && conn.close();
};

/**
 * Return handlers for proxying modifications on the object stores to the snapshot
 * object store. Used if the watch option is set.
 *
 * @private
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
 * Write new merges from remotes to the object store and the snapshot object store.
 *
 * See MergeTree.startMerge for object syntax.
 *
 * @todo verify version in object store to prevent data loss because of race
 * conditions between local and remote updates.
 * @todo make the operations atomic.
 *
 * @private
 * @fires PersDB#data
 * @fires PersDB#conflict
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

  this._log.debug2('_writeMerge', osName, newVersion.h);

  // open a rw transaction on the object store
  var tr = this._idbTransaction([osName], 'readwrite');
  var os = tr.objectStore(osName);

  // make sure the keypath of this object store is known
  if (!this._keyPaths.hasOwnProperty(osName)) {
    this._keyPaths[osName] = os.keyPath;
    if (typeof this._keyPaths[osName] !== 'string') {
      that._log.warning('warning: keypath is not a string, converting: %s, store: %s', this._keyPaths[osName], osName);
      this._keyPaths[osName] = Object.prototype.toString(this._keyPaths[osName]);
    }
  }

  // ensure keypath is set if the store has a keypath and this is not a delete
  if (this._keyPaths[osName] && !newVersion.h.d && !newVersion.b[this._keyPaths[osName]]) {
    newVersion.b[this._keyPaths[osName]] = id;
  }

  tr.oncomplete = function(ev) {
    that._log.debug2('_writeMerge success', ev);
    that._localWriter.write(obj, function(err) {
      if (err) { cb(err); return; }
      that.emit('data', { os: osName, n: obj.n && obj.n.b, p: obj.l && obj.l.b });
      cb();
    });
  };

  tr.onabort = function(ev) {
    that._log.err('_writeMerge abort', ev);
    cb(ev.target);
    that._handleConflict(obj, cb);
  };

  tr.onerror = function(ev) {
    that._log.err('_writeMerge error', ev);
    cb(ev.target);
    that._handleConflict(obj, cb);
  };

  if (newVersion.h.d) {
    that._log.debug2('delete', newVersion.h);
    os.delete(id);
  } else {
    that._log.debug2('put', newVersion.b);
    os.put(newVersion.b);
  }
};
