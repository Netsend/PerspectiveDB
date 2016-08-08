/**
 * Copyright 2016 Netsend.
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

/**
 * Get an item from store by key.
 *
 * @param {IDBDatabase} db - opened indexedDB
 * @param {String} storeName - name of the object store
 * @param {key} key - key of the object in the store
 * @param {Function} cb - first paramter will be an error or null, second paramter
 *  will be the item.
 */
function get(db, storeName, key, cb) {
  if (db == null || typeof db !== 'object') { throw new TypeError('db must be an object'); }
  if (typeof storeName !== 'string') { throw new TypeError('storeName must be a string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var tx = db.transaction(storeName);
  var req = tx.objectStore(storeName).get(key);

  tx.onabort = () => cb(tx.error);
  tx.onerror = () => cb(tx.error);
  tx.oncomplete = () => cb(null, req.result);
}

/**
 * Insert an item in store by key and value.
 *
 * @param {IDBDatabase} db - opened indexedDB
 * @param {String} storeName - name of the object store
 * @param {mixed} value - value to store
 * @param {key} [key] - key of the object in the store
 * @param {Function} cb - first paramter will be an error or null, second paramter
 *  will be the item.
 */
function put(db, storeName, value, key, cb) {
  if (typeof key === 'function') {
    cb = key;
    key = null;
  }
  if (db == null || typeof db !== 'object') { throw new TypeError('db must be an object'); }
  if (typeof storeName !== 'string') { throw new TypeError('storeName must be a string'); }
  // value can be anything
  // key is optional
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var tx = db.transaction(storeName);
  var req = tx.objectStore(storeName).put(value, key);

  tx.onabort = () => cb(tx.error);
  tx.onerror = () => cb(tx.error);
  tx.oncomplete = () => cb(null, req.result);
}

/**
 * Delete an item from store by key.
 *
 * @param {IDBDatabase} db - opened indexedDB
 * @param {String} storeName - name of the object store
 * @param {key} key - key of the object in the store
 * @param {mixed} value - value to store
 * @param {Function} cb - first paramter will be an error or null, second paramter
 *  will be the item.
 */
function del(db, storeName, key, cb) {
  if (db == null || typeof db !== 'object') { throw new TypeError('db must be an object'); }
  if (typeof storeName !== 'string') { throw new TypeError('storeName must be a string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var tx = db.transaction(storeName);
  var req = tx.objectStore(storeName).delete(key);

  tx.onabort = () => cb(tx.error);
  tx.onerror = () => cb(tx.error);
  tx.oncomplete = () => cb(null, req.result);
}

function dropDb(name, cb) {
  if (typeof name !== 'string') { throw new TypeError('name must be a string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var req = indexedDB.deleteDatabase(name);

  req.onsuccess = () => cb();
  req.onerror = () => cb(req.error);
}

// opts.stores => { storeName: storeCreationOpts } }
// opts.data => { storeName: []|{} } }
// cb(err, db)
function createDb(name, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = {};
  }
  opts = opts || {};
  if (typeof name !== 'string') { throw new TypeError('name must be a string'); }
  if (opts == null || typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var req = indexedDB.open(name);

  // create stores if any
  var stores = Object.keys(opts.stores || {});
  if (stores.length) {
    req.onupgradeneeded = () => {
      var db = req.result;

      stores.forEach(function(storeName) {
        db.createObjectStore(storeName, opts.stores[storeName]);
      });
    }
  }

  req.onsuccess = () => {
    var db = req.result;

    // load data if any
    var data = Object.keys(opts.data || {});
    if (data.length) {
      var tx = db.transaction(data, 'readwrite');

      data.forEach(function(storeName) {
        var store = tx.objectStore(storeName);
        if (Array.isArray(opts.data[storeName])) {
          opts.data[storeName].forEach(function(obj) {
            store.put(obj);
          });
        } else { // assume this is an object, use the object keys
          Object.keys(opts.data[storeName]).forEach(function(key) {
            var obj = opts.data[storeName][key];
            store.put(obj, key);
          });
        }
      });

      tx.onabort = () => cb(tx.error);
      tx.onerror = () => cb(tx.error);
      tx.oncomplete = () => cb(null, db);
    } else {
      cb(null, db);
    }
  };
  req.onerror = () => cb(req.error);
}

// opts.stores => { storeName: storeCreationOpts } }
// cb(err, db)
function recreateDb(name, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = null;
  }
  if (typeof name !== 'string') { throw new TypeError('name must be a string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  dropDb(name, function(err) {
    if (err) { cb(err); return; }
    createDb(name, opts, cb);
  });
}

module.exports = { dropDb, createDb, recreateDb, get, put, del };
