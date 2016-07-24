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

if (typeof Proxy !== 'function') {
  throw new Error('missing ES6 Proxy');
}

function noop() {}

/**
 * Proxies the transaction method on idb so that pre- and post handlers for all
 * update methods can be registered and selectively activated for each transaction
 * and object store.
 *
 * @param {IndexedDB} idb  an opened IndexedDB database to proxy
 * @param {Function} doProxy  function that gets the mode and name of each
 *                            transaction and object store and should return either
 *                            true or false about whether or not to invoke the
 *                            handlers. function(mode, name) => boolean
 * @param {Object} handlers  pre- and post handlers for add, put, delete and clear.
 * @return {Function} original transaction method in order to bypass the proxy.
 */
function proxy(idb, doProxy, handlers) {
  if (idb == null || typeof idb !== 'object') { throw new TypeError('idb must be an object'); }
  if (typeof doProxy !== 'function') { throw new TypeError('doProxy must be a function'); }
  if (handlers == null || typeof handlers !== 'object') { throw new TypeError('handlers must be an object'); }

  // pre and post handlers for objectStore.add, put, delete and clear
  var preAdd    = handlers.preAdd || noop;
  var prePut    = handlers.prePut || noop;
  var preDelete = handlers.preDelete || noop;
  var preClear  = handlers.preClear || noop;

  var postAdd    = handlers.postAdd || noop;
  var postPut    = handlers.postPut || noop;
  var postDelete = handlers.postDelete || noop;
  var postClear  = handlers.postClear || noop;

  // proxy db.transaction.objectStore function to catch new object store modification commands
  function proxyObjectStore(target, mode) {
    return new Proxy(target, {
      apply: function(target, that, args) {
        console.log('proxyObjectStore', target, args);

        var obj = target.apply(that, args);

        // only proxy if doProxy returns true
        if (!doProxy(mode, target.name)) {
          return obj;
        }

        // proxy add, put, delete and clear
        var origAdd = obj.add;
        var origPut = obj.put;
        var origDelete = obj.delete;
        var origClear = obj.clear;

        // proxy add
        function proxyAdd(value, key) {
          preAdd(obj, value, key);
          var req = origAdd.apply(obj, arguments);
          postAdd(obj, value, key, req);
          return req;
        }

        // proxy put
        function proxyPut(value, key) {
          prePut(obj, value, key);
          var req = origPut.apply(obj, arguments);
          postPut(obj, value, key, req);
          return req;
        }

        // proxy delete by adding a delete item
        function proxyDelete(key) {
          preDelete(obj, key);
          var req = origDelete.apply(obj, arguments);
          postDelete(obj, key, req);
          return req;
        }

        // proxy clear by adding one delete per item
        function proxyClear() {
          preClear(obj);
          var req = origClear.apply(obj, arguments);
          postClear(obj, req);
          return req;
        }

        obj.add = proxyAdd;
        obj.put = proxyPut;
        obj.delete = proxyDelete;
        obj.clear = proxyClear;

        return obj;
      }
    });
  }

  // proxy db.transaction object to catch new transactions and requests and add the snapshot collection
  function proxyTransaction(target) {
    return new Proxy(target, {
      apply: function(target, that, args) {
        // proxy the opening of object stores for the target transaction
        var obj = target.apply(that, args);
        obj.objectStore = proxyObjectStore(obj.objectStore, obj.mode);
        return obj;
      }
    });
  }

  var origTransaction = idb.transaction.bind(idb);
  idb.transaction = proxyTransaction(idb.transaction);
  return origTransaction;
}

module.exports = proxy;
