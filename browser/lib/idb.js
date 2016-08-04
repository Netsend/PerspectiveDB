/**
 * Copyright 2016 Netsend.
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

  var tx = db(storeName);
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

  var tx = db(storeName);
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

  var tx = db(storeName);
  var req = tx.objectStore(storeName).delete(key);

  tx.onabort = () => cb(tx.error);
  tx.onerror = () => cb(tx.error);
  tx.oncomplete = () => cb(null, req.result);
}

module.exports = { get, put, del };
