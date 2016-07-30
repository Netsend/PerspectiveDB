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

function noop() {}

/**
 * Proxies the transaction method on idb so that pre- and post handlers for all
 * update methods can be registered and selectively activated for each transaction
 * and object store.
 *
 * @param {IndexedDB} idb  an opened IndexedDB database to proxy
 * @param {Object} handlers  pre- and post handlers for add, put, delete and clear.
 * @param {Function} [handlers.preAdd] - pre add handler
 * @param {Function} [handlers.postAdd] - post add handler
 * @param {Function} [handlers.prePut] - pre put handler
 * @param {Function} [handlers.postPut] - post put handler
 * @param {Function} [handlers.preDelete] - pre delete handler
 * @param {Function} [handlers.postDelete] - post delete handler
 * @param {Function} [handlers.preClear] - pre clear handler
 * @param {Function} [handlers.postClear] - post clear handler
 * @param {Object} opts
 * @param {Array} [opts.exclude] - exclude given stores from proxying
 * @return {Function} original transaction method in order to bypass the proxy.
 */
function proxy(idb, handlers, opts) {
  if (idb == null || typeof idb !== 'object') { throw new TypeError('idb must be an object'); }
  if (handlers == null || typeof handlers !== 'object') { throw new TypeError('handlers must be an object'); }

  opts = opts || {};
  if (opts.exclude && !Array.isArray(opts.exclude)) { throw new TypeError('opts.exclude must be an array'); }

  var exclude = opts.exclude || [];

  // pre and post handlers for objectStore.add, put, delete and clear
  var preAdd    = handlers.preAdd || noop;
  var prePut    = handlers.prePut || noop;
  var preDelete = handlers.preDelete || noop;
  var preClear  = handlers.preClear || noop;

  var postAdd    = handlers.postAdd || noop;
  var postPut    = handlers.postPut || noop;
  var postDelete = handlers.postDelete || noop;
  var postClear  = handlers.postClear || noop;

  // proxy onsuccess and onerror handlers, supports multiple onsuccess and onerror handlers
  function proxyRequest(target) {
    var successHandlers = [];
    var errorHandlers = [];

    var req = new Proxy(target, {
      get: function(target, property) {
        return target[property];
      },
      set: function(target, property, value) {
        if (property === 'onsuccess') {
          successHandlers.push(value);
        } else if (property === 'onerror') {
          errorHandlers.push(value);
        } else {
          target[property] = value;
        }
        return true;
      }
    });

    target.onsuccess = function() {
      var args = arguments;
      successHandlers.forEach(function(h) {
        h.apply(req, args);
      });
    }
    target.onerror = function() {
      var args = arguments;
      errorHandlers.forEach(function(h) {
        h.apply(req, args);
      });
    }
    return req;
  }

  // proxy db.transaction.objectStore function to catch new object store modification commands
  function proxyObjectStore(target) {
    return new Proxy(target, {
      apply: function(target, that, args) {
        var obj = target.apply(that, args);

        // proxy add
        obj.add = new Proxy(obj.add, {
          apply: function(target, that, args) {
            var value = args[0];
            var key = args[1];

            preAdd(obj, value, key);
            var req = proxyRequest(target.apply(that, args));
            postAdd(obj, value, key, req);
            return req;
          }
        });

        // proxy put
        obj.put = new Proxy(obj.put, {
          apply: function(target, that, args) {
            var value = args[0];
            var key = args[1];

            prePut(obj, value, key);
            var req = proxyRequest(target.apply(that, args));
            postPut(obj, value, key, req);
            return req;
          }
        });

        // proxy delete
        obj.delete = new Proxy(obj.delete, {
          apply: function(target, that, args) {
            var key = args[0];

            preDelete(obj, key);
            var req = proxyRequest(target.apply(that, args));
            postDelete(obj, key, req);
            return req;
          }
        });

        // proxy clear
        obj.clear = new Proxy(obj.clear, {
          apply: function(target, that, args) {
            preClear(obj);
            var req = proxyRequest(target.apply(that, args));
            postClear(obj, req);
            return req;
          }
        });

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

        // only proxy if readwrite and not excluded
        if (obj.mode === 'readonly' || ~exclude.indexOf(obj.objectStore)) {
          return obj;
        }

        obj.objectStore = proxyObjectStore(obj.objectStore);
        return obj;
      }
    });
  }

  var origTransaction = idb.transaction.bind(idb);
  idb.transaction = proxyTransaction(idb.transaction);
  return origTransaction;
}

module.exports = proxy;
