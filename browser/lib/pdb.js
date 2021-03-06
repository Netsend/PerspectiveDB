/**
 * Copyright 2015, 2016 Netsend.
 *
 * This file is part of PerspectiveDB.
 *
 * PerspectiveDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PerspectiveDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PerspectiveDB. If not, see <https://www.gnu.org/licenses/>.
 */

'use strict';

var EE = require('events');
var stream = require('stream');
var util = require('util');

var async = require('async');
var idbReadableStream = require('idb-readable-stream');
var level = require('level-packager')(require('level-js'));
var websocket = require('websocket-stream');
var xtend = require('xtend');

var proxy = require('./proxy');

var idbIdOps = require('../../lib/idb_id_ops');
var isEqual = require('../../lib/is_equal');
var MergeTree = require('../../lib/merge_tree');
var noop = require('../../lib/noop');
var remoteConnHandler = require('../../lib/remote_conn_handler');

// hooks
var filterIdbStore = require('../../hooks/core/filter_idb_store');

var Writable = stream.Writable;

/**
 * @typedef {Object} PerspectiveDB~snapshot
 * @property {Object} h - header
 * @property {mixed} h.id - id
 * @property {String} h.v - version
 * @property {String[]} h.pa - parent versions
 * @property {Number} [h.i] - local increment
 * @property {String} [h.pe] - perspective, remote this version originated from
 * @property {Object} b - body
 * @property {Object} [m] - meta info for application specific purposes
 */

/**
 * @typedef {Object} PerspectiveDB~conflictObject
 * @property {Number} id - conflict id
 * @property {String} store - name of the object store
 * @property {mixed} key - key of the object in the object store
 * @property {?Object} new - new version, undefined on delete
 * @property {?Object} prev - previous version, undefined on insert
 * @property {String} remote - remote the new version originated from
 * @property {String[]} conflict - array with conflicting key names
 * @property {String} [error] - error message if something else occurred
 */

/**
 * @event PerspectiveDB#data
 * @param {Object} obj
 * @param {String} obj.store - name of the object store
 * @param {?Object} obj.new - new version, undefined on delete
 * @param {?Object} obj.prev - previous version, undefined on insert
 */
/**
 * @event PerspectiveDB#conflict
 * @param {PerspectiveDB~conflictObject} obj - conflict object
 */

/**
 * @callback PerspectiveDB~stderrCb
 * @param {Error} [err]
 */

/**
 * Use {@link PerspectiveDB.createNode} to create a new instance. Don't use new PerspectiveDB directly.
 *
 * @class PerspectiveDB
 *
 * @fires PerspectiveDB#data
 * @fires PerspectiveDB#conflict
 */
function PerspectiveDB(idb, ldb, opts) {
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

  this._idb = idb;
  this._db = ldb;
  this._opts = opts;
  this._connections = {};
  this._snapshotStore = opts.snapshotStore || '_pdb';
  this._conflictStore = opts.conflictStore || 'conflict';

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
}

util.inherits(PerspectiveDB, EE);

module.exports = global.PerspectiveDB = PerspectiveDB;

/**
 * Create a new {@link PerspectiveDB} instance. Make sure idb is opened and upgradeneeded
 * is handled and both the conflict and snapshot store exist. The conflict store
 * must have autoIncrement set. Furthermore, don't start writing to any object
 * store until the call back is called with the pdb instance.
 *
 * If opts.watch is used, then add, put and delete operations on any of the object
 * stores are automatically detected. Note that for watch to work, ES6 Proxy must
 * be supported by the browser (i.e. Firefox 18+ or Chrome 49+). If watch is not
 * used, {@link PerspectiveDB#put} and {@link PerspectiveDB#del} must be used in order to modify
 * the contents of any object store.
 *
 * @example:
 * indexedDB.open('MyDB').onsuccess = ev => {
 *   var db = ev.target.result
 *   var opts = { watch: true }
 *
 *   PerspectiveDB.createNode(db, opts).then(pdb => {
 *     pdb.connect({ ... })
 *   }).catch(err => console.error(err))
 * })
 *
 * @param {IDBDatabase} idb  opened IndexedDB database
 * @param {Object} [opts]
 * @param {Boolean} opts.watch=false  automatically watch changes to all object
 *   stores using ES6 Proxy
 * @param {String} opts.snapshotStore=_pdb  name of the object store used
 *   internally for saving new versions
 * @param {Boolean} opts.startMerge=true  automatically start merging remotes
 *   internally for saving new versions
 * @param {String} opts.conflictStore=conflict  name of the object store used
 *   internally for saving a conflict
 * @param {Object} opts.mergeTree  MergeTree options
 * @param {Function} cb  first paramter will be an error or null, second paramter
 *  will be the PerspectiveDB instance
 */
PerspectiveDB.createNode = function createNode(idb, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = {};
  }
  opts = xtend({
    startMerge: true
  }, opts);

  if (idb == null || typeof idb !== 'object') { throw new TypeError('idb must be an object'); }
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (opts.hasOwnProperty('watch') && opts.watch != null && typeof opts.watch !== 'boolean') { throw new TypeError('opts.watch must be a boolean'); }
  if (opts.snapshotStore != null && typeof opts.snapshotStore !== 'string') { throw new TypeError('opts.snapshotStore must be a string'); }
  if (opts.conflictStore != null && typeof opts.conflictStore !== 'string') { throw new TypeError('opts.conflictStore must be a string'); }

  var snapshotStore = opts.snapshotStore || '_pdb';
  var conflictStore = opts.conflictStore || 'conflict';

  // ensure the snapshot and conflict stores exist
  var snapshotStoreExists = idb.objectStoreNames.contains(snapshotStore);
  var conflictStoreExists = idb.objectStoreNames.contains(conflictStore);

  if (!snapshotStoreExists) {
    throw new Error('snapshot store does not exist');
  }
  if (!conflictStoreExists) {
    throw new Error('conflict store does not exist');
  }

  // check if the conflict store has the auto increment property set
  if (!idb.transaction(conflictStore).objectStore(conflictStore).autoIncrement) {
    throw new Error('conflict store must have auto increment set');
  }

  // pass the opened database
  var ldb = level(idb.name, {
    storeName: snapshotStore,
    idb: idb, // pass the opened database instance
    keyEncoding: 'binary',
    valueEncoding: 'none',
    asBuffer: false,
    snapshot: false
  });

  var pdb;
  try {
    pdb = new PerspectiveDB(idb, ldb, opts);
  } catch(err) {
    cb(err);
    return;
  }

  // auto-merge remote versions (use a writable stream to enable backpressure)
  if (opts.startMerge) {
    pdb.startMerge();
  }

  if (opts.hasOwnProperty('watch') && opts.watch) {
    // transparently track IndexedDB updates using proxy module
    var reservedStores = [snapshotStore, conflictStore];
    proxy(idb, pdb._getHandlers(), { exclude: reservedStores });
  }

  cb(null, pdb);
};

// start merge
PerspectiveDB.prototype.startMerge = function startMerge() {
  this._mt.startMerge().pipe(new Writable({
    objectMode: true,
    write: this._writeMerge.bind(this)
  }));
};

// stop merge
PerspectiveDB.prototype.stopMerge = function stopMerge(cb) {
  this._mt.stopMerge(cb);
};

PerspectiveDB.prototype.close = function close(cb) {
  this.stopMerge(cb);
};

/**
 * @callback PerspectiveDB~getConflictCb
 * @param {Error} [err]
 * @param {PerspectiveDB~conflictObject} conflict - conflict object
 * @param {mixed} current - current local head
 */
/**
 * Get a conflict by conflict key, includes the last known version.
 *
 * Note: discrepancies between the local head and the version in the object store,
 * must be treated as bugs. If using the "watch" option, then please report them
 * back. Otherwise make sure updates to the object stores are done exclusively
 * using PerspectiveDB~put and PerspectiveDB~get.
 *
 * @param {Number} conflictId - key of the conflict in the conflict store
 * @param {PerspectiveDB~getConflictCb} cb - gets three parameters
 */
PerspectiveDB.prototype.getConflict = function getConflict(conflictId, cb) {
  var that = this;

  // fetch the conflict object
  this._getItem(this._conflictStore, conflictId, function(err, conflict) {
    if (err) { cb(err); return; }
    if (!conflict) { cb(new Error('conflict not found')); return; }

    // get the current local head
    that._mt.getLocalHead(conflict.n.h.id, function(err, head) {
      if (err) { cb(err); return; }
      cb(null, PerspectiveDB._convertConflict(conflictId, conflict), head && head.b);
    });
  });
};

/**
 * Resolve a conflict by conflict key. "resolved" is treated as a new version and
 * is written to the object store if the current local head matches "toBeResolved".
 *
 * Note: discrepancies between the local head and the version in the object store,
 * must be treated as bugs. If using the "watch" option, then please report them
 * back. Otherwise make sure updates to the object stores are done exclusively
 * using PerspectiveDB~put and PerspectiveDB~get.
 *
 * @param {Number} conflictId - key of the conflict in the conflict store
 * @param {mixed} toBeResolved - object currently in the object store that should
 *   be overwritten
 * @param {Object} resolved - new item that solves given conflict and should
 *   overwrite the object store
 * @param {Boolean} [del] - whether or not resolving means deleting from the object
 *   store
 * @param {PerspectiveDB~stderrCb} cb
 */
PerspectiveDB.prototype.resolveConflict = function resolveConflict(conflictId, toBeResolved, resolved, del, cb) {
  if (typeof del === 'function') {
    cb = del;
    del = false;
  }
  if (typeof conflictId !== 'number') { throw new TypeError('conflictId must be a number'); }
  if (resolved == null || typeof resolved !== 'object') { throw new TypeError('resolved must be an object'); }
  if (del != null && typeof del !== 'boolean') { throw new TypeError('del must be a boolean'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  var error;

  // fetch the conflict object
  this._getItem(this._conflictStore, conflictId, function(err, conflict) {
    if (err) { cb(err); return; }
    if (!conflict) { cb(new Error('conflict not found')); return; }

    var id = conflict.n.h.id;

    // get the current local head, if any, and make sure it matches toBeResolved
    that._mt.getLocalHead(id, function(err, lhead) {
      if (err) { cb(err); return; }
      var lheadBody = lhead && lhead.b;

      if (!isEqual(lheadBody, toBeResolved)) {
        error = new Error('local head and toBeResolved don\'t match');
        that._log.err('resolveConflict %s: %j %j while processing', error, lheadBody, toBeResolved, conflict);
        cb(error);
        return;
      }

      // determine parents (local and remote versions)
      var parents = {};
      if (lhead) {
        parents[lhead.h.v] = true;
      }
      // the conflicting merge was either locally merged or directly from a remote (fast-forward)
      that._mt.getRemoteTree(conflict.pe).getByVersion(conflict.n.h.v, function(err, found) {
        if (err) { cb(err); return; }

        if (found) { // then this is the remote parent
          parents[found.h.v] = true;
        } else { // this is a locally created merge and at least one of the two parents must be the remote parent
          // check some invariants
          if (conflict.n.h.pa.length < 2) { throw new Error('expected conflict to be a merge'); }
          if (conflict.n.h.pa.length > 2) { throw new Error('only supports merges with two parents'); }

          // determine which of the parents was from the remote at the time of conflict
          var headIdx = conflict.n.h.pa.indexOf(conflict.l.h.v);
          if (headIdx === -1) {
            // invalid merge
            error = new Error('parent in local tree could not be determined');
            that._log.err('resolveConflict encountered unexpected conflict object: %s', error, conflict);
            cb(error);
            return;
          } else {
            if (headIdx === 1) {
              parents[conflict.n.h.pa[0]] = true;
            } else {
              parents[conflict.n.h.pa[1]] = true;
            }
          }
        }

        var newVersion = {
          h: {
            id: id,
            v: MergeTree.generateRandomVersion(),
            pa: Object.keys(parents)
          },
          b: resolved
        }
        if (del) {
          newVersion.h.d = true;
        }

        var newMerge = {
          n: newVersion,
          l: lhead,
          lcas: conflict.lcas,
          pe: conflict.pe,
          c: null
        };

        that._writeMerge(newMerge, null, function(err) {
          if (err) { cb(err); return; }
          that._removeItem(that._conflictStore, conflictId, cb);
        });
      });
    });
  });
};

/**
 * Remove an item from store by key.
 *
 * @private
 * @param {String} store - name of the object store
 * @param {key} key  key of the conflict object in the conflict store
 * @param {Function} cb - first paramter will be an error or null
 */
PerspectiveDB.prototype._removeItem = function _removeItem(store, key, cb) {
  if (typeof store !== 'string') { throw new TypeError('store must be a string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var tx = this._idbTransaction(store, 'readwrite');
  tx.objectStore(store).delete(key);

  tx.onabort = () => {
    cb(tx.error);
  };

  tx.onerror = () => {
    cb(tx.error);
  };

  tx.oncomplete = () => {
    cb();
  }
};

/**
 * Get an item from store by key.
 *
 * @private
 * @param {String} storeName - name of the object store
 * @param {key} key - key of the object in the store
 * @param {Function} cb - first paramter will be an error or null, second paramter
 *  will be the item.
 */
PerspectiveDB.prototype._getItem = function _getItem(storeName, key, cb) {
  if (typeof storeName !== 'string') { throw new TypeError('storeName must be a string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var tx = this._idbTransaction(storeName);
  var req = tx.objectStore(storeName).get(key);

  tx.onabort = () => {
    cb(tx.error);
  };

  tx.onerror = () => {
    cb(tx.error);
  };

  tx.oncomplete = () => {
    cb(null, req.result);
  }
};

/**
 * @callback PerspectiveDB~getConflictsProceedCb
 * @param {Boolean|Error} [continue=true] - call this handler optionally with a
 *   boolean or error to indicate whether to proceed to the next item or stop
 *   iterating
 */
/**
 * @callback PerspectiveDB~getConflictsCb
 * @param {PerspectiveDB~conflictObject} conflictObject - conflict object
 * @param {PerspectiveDB~getConflictsProceedCb} next - callback to proceed to the next
 *   conflict or stop iterating
 */
/**
 * Get an iterator over all unresolved conflicts.
 *
 * @param {Object} [opts] - @see {@link https://github.com/timkuijsten/node-idb-readable-stream} options
 * @param {PerspectiveDB~getConflictsCb} next - iterator called with two parameters
 * @param {PerspectiveDB~stderrCb} [done] - final callback called when done iterating or
 *   when iterating is discontinued
 */
PerspectiveDB.prototype.getConflicts = function getConflicts(opts, next, done) {
  if (typeof opts === 'function') {
    done = next;
    next = opts;
    opts = {};
  }
  opts = xtend({
    direction: 'next',
    snapshot: false // don't iterate over a snapshot so that backpressure can be used
  }, opts);
  done = done || noop;
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (typeof next !== 'function') { throw new TypeError('next must be a function'); }
  if (typeof done !== 'function') { throw new TypeError('done must be a function'); }

  var reader = idbReadableStream(this._idb, this._conflictStore, opts);
  var writer = new Writable({
    objectMode: true,
    write: function(obj, enc, cb) {
      next(PerspectiveDB._convertConflict(obj.key, obj.value), function(cont) {
        if (cont instanceof Error) {
          cont = false;
        }
        if (cont == null || cont) {
          cb();
        } else {
          reader.pause();
          writer.end();
          cb();
        }
      });
    }
  });
  reader.pipe(writer).on('finish', done).on('error', done);
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
PerspectiveDB.prototype.createReadStream = function createReadStream(opts) {
  return this._mt.createReadStream(opts);
};

/**
 * Create an authenticated secure WebSocket connection to a remote peer and start
 * transfering BSON.
 *
 * @param {Object} remote
 * @param remote.name {String}  local reference to the remote
 * @param remote.host {String}  address of the remote
 * @param {String} remote.username - username of remote account
 * @param {String} remote.password - password of remote account
 * @param {Array} [remote.stores] - limit synchronization to given stores
 * @param {String} [remote.db=remote.name] - name of the database on the remote
 * @param {Boolean|Object} [remote.import=true] - import changes from remote
 * @param {Boolean|Object} [remote.export=true] - export changes to remote
 * @param {Number} [remote.port=3344] - port of the remote WebSocket server
 * @return {Promise}
 */
PerspectiveDB.prototype.connect = function connect(remote) {
  if (remote == null || typeof remote !== 'object') { throw new TypeError('remote must be an object'); }
  if (!remote.name || typeof remote.name !== 'string') { throw new TypeError('remote.name must be a string'); }
  if (!remote.host || typeof remote.host !== 'string') { throw new TypeError('remote.host must be a string'); }
  if (!remote.username || typeof remote.username !== 'string') { throw new TypeError('remote.username must be a string'); }
  if (!remote.password || typeof remote.password !== 'string') { throw new TypeError('remote.password must be a string'); }

  // options
  if (remote.port && typeof remote.port !== 'number') { throw new TypeError('remote.port must be a number'); }

  var that = this;

  var authReq = {
    username: remote.username,
    password: remote.password,
    db: remote.db || remote.name
  };

  var port = remote.port || '3344';
  var uri = 'wss://' + remote.host + ':' + port;
  var error;

  // prevent side effects
  remote = xtend(remote);

  // see if imported and exported stores must be limited
  if (remote.stores && remote.stores.length) {
    remote.import = xtend(remote.import);
    remote.export = xtend(remote.export);

    // for import
    if (remote.import == null || typeof remote.import === 'boolean') {
      remote.import = {};
    }

    remote.import.hooks = remote.import.hooks || [];
    remote.import.hooksOpts = remote.import.hooksOpts || {};
    remote.import.hooksOpts.stores = remote.stores;
    remote.import.hooks.push(filterIdbStore);

    // same for export
    if (remote.export == null || typeof remote.export === 'boolean') {
      remote.export = {};
    }

    remote.export.hooks = remote.export.hooks || [];
    remote.export.hooksOpts = remote.export.hooksOpts || {};
    remote.export.hooksOpts.stores = remote.stores;
    remote.export.hooks.push(filterIdbStore);
  }

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

      conn.binaryType = 'arraybuffer';

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

PerspectiveDB.prototype.disconnect = function disconnect(cb) {
  var that = this;
  async.each(Object.keys(this._connections), function(connId, cb2) {
    that._log.info('closing %s', connId);
    var conn = that._connections[connId];
    conn.once('close', cb2);
    conn.end();
  }, cb);
};

PerspectiveDB.prototype._connErrorHandler = function _connErrorHandler(conn, connId, err) {
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
PerspectiveDB.prototype._getHandlers = function _getHandlers() {
  var that = this;

  // pre and post handlers for objectStore.add, put, delete and clear
  function addSuccess(ev, store, key, value) {
    var obj = {
      n: {
        h: { id: idbIdOps.generateId(ev.target.source.name, ev.target.result) },
        b: value
      }
    };
    that._localWriter.write(obj);
  }
  function putSuccess(ev, store, key, value) {
    var obj = {
      n: {
        h: { id: idbIdOps.generateId(ev.target.source.name, ev.target.result) },
        b: value
      }
    };
    that._localWriter.write(obj);
  }
  function deleteSuccess(ev, store, key) {
    var obj = {
      n: {
        h: {
          id: idbIdOps.generateId(ev.target.source.name, key),
          d: true
        }
      }
    };
    that._localWriter.write(obj);
  }

  function addError(...args) {
    that._log.err('add error', args);
  }
  function putError(...args) {
    that._log.err('put error', args);
  }
  function deleteError(...args) {
    that._log.err('delete error', args);
  }

  return { addSuccess, putSuccess, deleteSuccess, addError, putError, deleteError };
};

/**
 * Write new merges from remotes to the object store and the snapshot object store.
 *
 * See MergeTree.startMerge for object syntax.
 *
 * @todo make the operations atomic.
 *
 * @private
 * @fires PerspectiveDB#data
 * @fires PerspectiveDB#conflict
 */
PerspectiveDB.prototype._writeMerge = function _writeMerge(obj, enc, cb) {
  if (obj.c && obj.c.length) {
    this._handleConflict(null, obj, cb);
    return;
  }

  var that = this;

  var newVersion = obj.n;
  var prevVersion = obj.l;

  var storeName = idbIdOps.objectStoreFromId(newVersion.h.id);
  if (!this._stores[storeName]) {
    this._log.notice('_writeMerge object from an unknown object store received', storeName, newVersion.h);
    process.nextTick(cb);
    return;
  }
  var id = idbIdOps.idFromId(newVersion.h.id);

  this._log.debug2('_writeMerge', storeName, newVersion.h);

  // open a rw transaction on the object store
  var tx = this._idbTransaction([storeName], 'readwrite');
  var store = tx.objectStore(storeName);

  // make sure the keypath of this object store is known
  if (!this._keyPaths.hasOwnProperty(storeName)) {
    this._keyPaths[storeName] = store.keyPath;
    if (this._keyPaths[storeName] != null && typeof this._keyPaths[storeName] !== 'string') {
      that._log.warning('warning: keypath is not a string, converting: %s, store: %s', this._keyPaths[storeName], storeName);
      this._keyPaths[storeName] = Object.prototype.toString(this._keyPaths[storeName]);
    }
  }

  // ensure keypath is set if the store has a keypath and this is not a delete
  if (this._keyPaths[storeName] && !newVersion.h.d && !newVersion.b[this._keyPaths[storeName]]) {
    newVersion.b[this._keyPaths[storeName]] = id;
  }

  tx.oncomplete = function(ev) {
    that._log.debug2('_writeMerge success', ev);
    that._localWriter.write(obj, function(err) {
      if (err) { cb(err); return; }
      that.emit('data', { store: storeName, key: id, new: obj.n && obj.n.b, prev: obj.l && obj.l.b });
      cb();
    });
  };

  tx.onabort = function(ev) {
    that._log.warning('_writeMerge abort', ev);
    that._handleConflict(tx.error, obj, cb);
  };

  tx.onerror = function(ev) {
    that._log.warning('_writeMerge error', ev);
    that._handleConflict(tx.error, obj, cb);
  };

  // fetch current version and ensure there are no local changes
  var lookup = store.get(id);
  lookup.onsuccess = function() {
    // The current item in the store is expected to match the previous version,
    // since this is an update.
    if (isEqual(lookup.result, prevVersion && prevVersion.b)) {
      if (newVersion.h.d) {
        that._log.debug2('delete', newVersion.h);
        store.delete(id);
        // handle errors with tx.onabort
      } else {
        that._log.debug2('put', newVersion.b);
        store.put(newVersion.b, id);
        // handle errors with tx.onabort
      }
    } else if (isEqual(lookup.result, newVersion)) {
      // In some cases, i.e. if the user reloaded in the middle of this routine, a
      // new version might have been saved in the store but not yet in the DAG
      // (since this routine is not atomic and first saves in the store, then in
      // the DAG). The next time data is asked from the remote it asks for the
      // last version in the DAG and thus gets a version that is already in the
      // store. Therefore double check that if the store version is not the
      // expected previous version, maybe it already equals the new version.
      that._log.notice('merge already in store', newVersion.h);
    } else {
      // save in conflict store (let this transaction timeout)
      that._handleConflict(new Error('unexpected local version'), obj);
    }
  };

  // handle lookup errors with tx.onabort
};

// save an object in the conflict store
PerspectiveDB.prototype._handleConflict = function _handleConflict(err, obj, cb) {
  cb = cb || noop;

  if (err) {
    obj.err = err.message;
  }
  // open a write transaction on the conflict store
  var tx = this._idbTransaction([this._conflictStore], 'readwrite');

  tx.onabort = () => {
    this._log.err('save conflict abort', tx.error, obj.n.h, err);
    cb(tx.error);
  };

  tx.onerror = () => {
    this._log.err('save conflict error', tx.error, obj.n.h, err);
    cb(tx.error);
  };

  var req = tx.objectStore(this._conflictStore).put(obj);

  tx.oncomplete = () => {
    this._log.notice('conflict saved', obj);
    cb();
    this.emit('conflict', PerspectiveDB._convertConflict(req.result, obj));
  };
};

// convert a conflict object to the publicly documented structure
PerspectiveDB._convertConflict = function _convertConflict(id, obj) {
  var storeName = idbIdOps.objectStoreFromId(obj.n.h.id);
  var storeId = idbIdOps.idFromId(obj.n.h.id);

  var conflictObject = {
    id: id,
    store: storeName,
    key: storeId,
    new: obj.n && obj.n.b,
    prev: obj.l && obj.l.b,
    conflict: obj.c || [],
    remote: obj.pe
  };

  if (obj.err) {
    conflictObject.error = obj.err;
  }

  return conflictObject;
};
