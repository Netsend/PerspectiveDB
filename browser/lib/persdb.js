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
 * @event PersDB#data
 * @param {Object} obj
 * @param {String} obj.store - name of the object store
 * @param {?Object} obj.new - new version, is null on delete
 * @param {?Object} obj.prev - previous version, is null on insert
 */
/**
 * @event PersDB#conflict
 * @param {Object} obj
 * @param {String} obj.store - name of the object store
 * @param {?Object} obj.new - new version, is null on delete
 * @param {?Object} obj.prev - previous version, is null on insert
 * @param {?Array} obj.conflicts - array with conflicting key names
 */

/**
 * Use {@link PersDB.createNode} to create a new instance. Don't use new PersDB directly.
 *
 * @class PersDB
 *
 * @fires PersDB#data
 * @fires PersDB#conflict
 */
function PersDB(idb, ldb, opts) {
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
  this._conflictStore = opts.conflictStore || '_conflicts';

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

util.inherits(PersDB, EE);

module.exports = global.PersDB = PersDB;

/**
 * Create a new {@link PersDB} instance. Make sure idb is opened and upgradeneeded is
 * handled. The snapshot and conflict store must exist or otherwise the
 * upgradeIfNeeded option must be used. The conflict store must have autoIncrement
 * set. Furthermore, don't start writing to any object store until the call back is
 * called with the pdb instance.
 *
 * If opts.watch is used, then add, put and delete operations on any of the object
 * stores are automatically detected. Note that for watch to work, ES6 Proxy must
 * be supported by the browser (i.e. Firefox 18+ or Chrome 49+). If watch is not
 * used, {@link PersDB#put} and {@link PersDB#del} must be used in order to modify
 * the contents of any object store.
 *
 * @example:
 * indexedDB.open('MyDB').onsuccess = ev => {
 *   var db = ev.target.result
 *   var opts = { watch: true }
 *
 *   PersDB.createNode(db, opts).then(pdb => {
 *     pdb.connect({ ... })
 *   }).catch(err => console.error(err))
 * })
 *
 * @param {IDBDatabase} idb  opened IndexedDB database
 * @param {Object} [opts]
 * @param {Boolean} opts.watch=false  automatically watch changes to all object
 *   stores using ES6 Proxy
 * @param {Boolean} opts.upgradeIfNeeded=false  automatically create the snapshot
 *  and conflict store if they don't exist. This will increment the database
 *  version and closes the passed in idb database. A newly opened idb instance will
 *  be provided to the callback.
 * @param {String} opts.snapshotStore=_pdb  name of the object store used
 *   internally for saving new versions
 * @param {String} opts.conflictStore=_conflicts  name of the object store used
 *   internally for saving conflicts
 * @param {Function} cb  first paramter will be an error or null, second paramter
 *  will be the PersDB instance, third parameter will be a new IndexedDB instance
 *  if snapshot and conflict stores have been created (see opts.upgradeIfNeeded).
 */
PersDB.createNode = function createNode(idb, opts, cb) {
  if (idb == null || typeof idb !== 'object') { throw new TypeError('idb must be an object'); }

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }

  if (opts.watch != null && typeof opts.watch !== 'boolean') { throw new TypeError('opts.watch must be a boolean'); }
  if (opts.upgradeIfNeeded != null && typeof opts.upgradeIfNeeded !== 'boolean') { throw new TypeError('opts.upgradeIfNeeded must be a boolean'); }
  if (opts.snapshotStore != null && typeof opts.snapshotStore !== 'string') { throw new TypeError('opts.snapshotStore must be a string'); }
  if (opts.conflictStore != null && typeof opts.conflictStore !== 'string') { throw new TypeError('opts.conflictStore must be a string'); }

  var snapshotStore = opts.snapshotStore || '_pdb';
  var conflictStore = opts.conflictStore || '_conflicts';

  // ensure the snapshot and conflict stores exist
  var snapshotStoreExists = idb.objectStoreNames.contains(snapshotStore);
  var conflictStoreExists = idb.objectStoreNames.contains(conflictStore);

  if (!opts.upgradeIfNeeded) {
    if (!snapshotStoreExists) {
      throw new Error('snapshot store does not exist and opts.upgradeIfNeeded is false');
    }
    if (!conflictStoreExists) {
      throw new Error('conflict store does not exist and opts.upgradeIfNeeded is false');
    }
  }

  var tasks = [];
  if (opts.upgradeIfNeeded && !snapshotStoreExists) {
    tasks.push(function(cb2) {
      idb.close();
      var req = indexedDB.open(idb.name, ++idb.version);

      req.onerror = function(ev) {
        cb2(ev.target.error);
      };

      req.onupgradeneeded = function() {
        req.result.createObjectStore(snapshotStore);
      };

      req.onsuccess = function() {
        // set new idb object
        idb = req.result;
        cb2();
      };
    });
  }

  if (opts.upgradeIfNeeded && !conflictStoreExists) {
    tasks.push(function(cb2) {
      idb.close();
      var req = indexedDB.open(idb.name, ++idb.version);

      req.onerror = function(ev) {
        cb2(ev.target.error);
      };

      req.onupgradeneeded = function() {
        req.result.createObjectStore(conflictStore, { autoIncrement: true });
      };

      req.onsuccess = function() {
        // set new idb object
        idb = req.result;
        cb2();
      };
    });
  }

  // pass the opened database
  var pdb;
  tasks.push(function(cb2) {
    var ldb = level(idb.name, {
      storeName: snapshotStore,
      idb: idb, // pass the opened database instance
      keyEncoding: 'binary',
      valueEncoding: 'none',
      asBuffer: false,
      reopenOnTimeout: true
    });
    pdb = new PersDB(idb, ldb, opts);

    // auto-merge remote versions (use a writable stream to enable backpressure)
    pdb._mt.startMerge().pipe(new Writable({
      objectMode: true,
      write: pdb._writeMerge.bind(pdb)
    }));

    if (opts.watch) {
      // transparently track IndexedDB updates using proxy module
      var reservedStores = [snapshotStore, conflictStore];
      proxy(idb, pdb._getHandlers(), { exclude: reservedStores });
    }

    // check if the conflict store has the auto increment property set
    if (!idb.transaction(conflictStore).objectStore(conflictStore).autoIncrement) {
      throw new Error('conflict store must have auto increment set');
    }
    process.nextTick(cb2);
  });

  async.series(tasks, function(err) {
    if (err) { cb(err); return; }
    cb(null, pdb, idb);
  });
};

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

  var id = idbIdOps.generateId(objectStore, key);
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

  var id = idbIdOps.generateId(objectStore, key);
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
 * Get a conflict by conflict key.
 *
 * @param {Number} conflictKey - key of the conflict in the conflict store
 * @param {Function} cb - first argument will be an error or null, second argument
 *   will be the conflict object. Third argument will be the current version in the
 *   object store.
 */
PersDB.prototype.getConflict = function getConflict(conflictKey, cb) {
  var that = this;

  // fetch the conflict object
  this._getItem(this._conflictStore, conflictKey, function(err, conflict) {
    if (err) { cb(err); return; }
    if (!conflict) { cb(new Error('conflict not found')); return; }

    var storeName = idbIdOps.objectStoreFromId(conflict.n.h.id);
    var storeId = idbIdOps.idFromId(conflict.n.h.id);

    that._getItem(storeName, storeId, function(err, storeItem) {
      if (err) { cb(err); return; }

      cb(null, conflict, storeItem);
    });
  });
};

/**
 * Resolve a conflict by conflict key.
 *
 * If the current local head is the same as toBeResolved, write resolved to the
 * store and delete from conflict store.
 *
 * @param {Number} conflictKey - key of the conflict in the conflict store
 * @param {Object} toBeResolved - object currently in the object store that should
 *   be overwritten
 * @param {Object} resolved - new item that solves given conflict and should
 *   overwrite the object store
 * @param {Boolean} [del] - whether or not resolving means deleting from the object
 *   store
 * @param {Function} [cb] - first argument will be an error or null
 */
PersDB.prototype.resolveConflict = function resolveConflict(conflictKey, toBeResolved, resolved, del, cb) {
  if (typeof del === 'function') {
    cb = del;
    del = false;
  }
  if (typeof conflictKey !== 'number') { throw new TypeError('conflictKey must be a number'); }
  if (toBeResolved == null || typeof toBeResolved !== 'object') { throw new TypeError('toBeResolved must be an object'); }
  if (resolved == null || typeof resolved !== 'object') { throw new TypeError('resolved must be an object'); }
  if (del != null && typeof del !== 'boolean') { throw new TypeError('del must be a boolean'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  // fetch the conflict object
  this._getItem(conflictKey, function(err, conflict) {
    if (err) { cb(err); return; }
    if (!conflict) { cb(new Error('conflict not found')); return; }

    var id = conflict.n.h.id;

    // make sure the local head matches toBeResolved
    var heads = [];
    that._mt.getLocalTree().getHeadVersions(id, function(head, next) {
      heads.push(head);
      next();
    }, function(err) {
      if (err) { cb(err); return; }
      if (heads.length !== 1) {
        that._log.err('getConflicts encountered invalid snapshot state: %d heads while one was expected', heads.length);
        cb(new Error('expected one head'));
        return;
      }
      var head = heads[0];

      var newVersion = {
        h: {
          id: id,
          v: MergeTree.generateRandomVersion(),
          pa: [head.h.v] // other parents are determined later on
        },
        b: resolved
      }

      if (del) {
        newVersion.h.d = true;
      }

      if (!isEqual(head.b, toBeResolved)) {
        that._log.err('getConflicts mismatch between local head and toBeResolved %j %j while processing', head.b, toBeResolved, conflict);
        cb(new Error('local head and toBeResolved don\'t match'));
        return;
      }

      // differentiate between real merge conflicts on conflicting keys, versus other errors
      if (conflict.c && conflict.c.length) {
        // this was a real merge conflict, so use the remote version as a parent
        // note: merge conflicts never yield a merged item so conflict.n is always the remote version
        newVersion.h.pa.push(conflict.n.h.v);

        var newMerge = {
          n: newVersion,
          l: head,
          lcas: conflict.lcas,
          pe: conflict.pe,
          c: null
        };

        that._writeMerge(newMerge, null, function(err) {
          if (err) { cb(err); return; }
          that._removeItem(that._conflictStore, conflictKey, cb);
        });
      } else {
        if (!conflict.err) {
          that._log.warning('conflict objects should either have a "c" key or an "err" key', conflict);
          throw new Error('expected error key');
        }

        // check if the conflict version was a locally created merge or already existed in the remote tree
        // this info is needed for determining the right parents of the resolved item
        that._mt.getRemoteTree(conflict.pe).getByVersion(conflict.n.h.v, function(err, found) {
          if (err) { cb(err); return; }

          if (found) { // then this is the other parent
            newVersion.h.pa.push(conflict.n.h.v);
          } else { // discard the conflict and create a new merge with the same remote parent
            // check some invariants
            if (conflict.n.h._pa.length < 2) { throw new Error('expected conflict to be a merge'); }
            if (conflict.n.h._pa.length > 2) { throw new Error('only supports merges with two parents'); }
            if (!conflict.l) { throw new Error('expected a local head'); }

            // determine which of the parents was the local head at the time of conflict
            var lheadIdx = conflict.n.h.pa.indexOf(conflict.l.h.v);
            if (lheadIdx === -1) {
              // invalid merge
              throw new Error('parent in local tree not found');
            } else {
              if (lheadIdx === 1) {
                newVersion.h.pa.push(conflict.n.h.pa[0]);
              } else {
                newVersion.h.pa.push(conflict.n.h.pa[1]);
              }
            }
          }

          var newMerge = {
            n: newVersion,
            l: head,
            lcas: conflict.lcas,
            pe: conflict.pe,
            c: null
          };

          that._writeMerge(newMerge, null, function(err) {
            if (err) { cb(err); return; }
            that._removeItem(that._conflictStore, conflictKey, cb);
          });
        });
      }
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
PersDB.prototype._removeItem = function _removeItem(store, key, cb) {
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
PersDB.prototype._getItem = function _getItem(storeName, key, cb) {
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
 * Get an iterator over all unresolved conflicts.
 *
 * @param {Object} [opts] - idb-readable-stream options
 * @param {Function} next - iterator called with three arguments
 * @param {Number} next.conflictKey - first parameter is the  conflict key
 * @param {Object} next.conflictObject - second parameter is the conflict object
 * @param {Function} next.proceed - third parameter is a callback to proceed to
 *   the next conflict or stop iterating
 * @param {Boolean} [next.proceed.continue=true] - call this handler optionally
 *   with a boolean to indicate whether to proceed with the next conflict or stop
 *   iterating and call the done handler.
 * @param {Function} [done] - Final callback called when done iterating. Called
 *   with one optional argument.
 * @param {Error} [done.err] - first parameter will be an error or null
 */
PersDB.prototype.getConflicts = function getConflicts(opts, next, done) {
  if (typeof opts === 'function') {
    done = next;
    next = opts;
    opts = {};
  }
  opts = xtend({
    direction: 'prev'
  }, opts);
  done = done || noop;
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (typeof next !== 'function') { throw new TypeError('next must be a function'); }
  if (typeof done !== 'function') { throw new TypeError('done must be a function'); }

  var reader = idbReadableStream(this._idb, this._conflictStore, opts);
  reader.pipe(new Writable({
    objectMode: true,
    write: function(obj, enc, cb) {
      var writer = this;
      // pass conflict to caller for resolving
      next(obj.key, obj.value, function(cont) {
        if (cont == null || cont) {
          cb();
        } else {
          writer.end(cb);
        }
      });
    }
  })).on('finish', done).on('error', done);
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
 * @param {Array} [remote.stores] - limit synchronization to given stores
 * @param {Boolean|Object} [remote.import=true] - import changes from remote
 * @param {Boolean|Object} [remote.export=true] - export changes to remote
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

PersDB.prototype.disconnect = function disconnect(cb) {
  var that = this;
  async.each(Object.keys(this._connections), function(connId, cb2) {
    that._log.info('closing %s', connId);
    var conn = that._connections[connId];
    conn.once('close', cb2);
    conn.end();
  }, cb);
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
 * @fires PersDB#data
 * @fires PersDB#conflict
 */
PersDB.prototype._writeMerge = function _writeMerge(obj, enc, cb) {
  if (obj.c && obj.c.length) {
    that._handleConflict(new Error('upstream merge conflict'), obj, cb);
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
    if (isEqual(lookup.result, prevVersion)) {
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
      that._log.debug2('merge already in store', newVersion.h);
    } else {
      // save in conflicts (let this transaction timeout)
      that._handleConflict(new Error('unexpected local version'), obj);
    }
  };

  // handle lookup errors with tx.onabort
};

// save an object in the conflict store
PersDB.prototype._handleConflict = function _handleConflict(origErr, obj, cb) {
  cb = cb || noop;

  var that = this;

  obj.err = origErr.message;

  // open a write transaction on the conflict store
  var tx = this._idbTransaction([this._conflictStore], 'readwrite');

  tx.onabort = function(ev) {
    that._log.err('save conflict abort', ev, obj.n.h, origErr);
    cb(tx.error);
  };

  tx.onerror = function(ev) {
    that._log.err('save conflict error', ev, obj.n.h, origErr);
    cb(tx.error);
  };

  tx.oncomplete = function() {
    that._log.notice('conflict saved %j', obj.n.h);
    cb();
  };

  tx.objectStore(this._conflictStore).put(obj);
};
