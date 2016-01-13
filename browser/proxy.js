/**
 * Copyright 2015 Netsend.
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

var errors = [];
if (typeof indexedDB !== 'object') {
  errors.push('missing Indexed Database API');
}
if (typeof crypto !== 'object') {
  errors.push('missing Web Cryptography API');
}
if (typeof Proxy !== 'function') {
  errors.push('missing ES6 Proxy');
}

if (errors.length) {
  console.error(errors);
  throw new Error(errors.join(', '));
}

/**
 * @param {String} [name]  name of the snapshot object store, defaults to _pdb
 * @param {Object} writer  writable stream to the snapshot object store
 */
module.exports = function(name, writer) {
  if (typeof name === 'object') {
    writer = name;
    name = null;
  }
  if (name != null && typeof name !== 'string') { throw new TypeError('name must be a string'); }
  if (writer == null || typeof writer !== 'object') { throw new TypeError('writer must be an object'); }

  function _generateId(ev, cb) {
    return ev.target.source.name + '\x01' + ev.target.result;
  }

  // pre and post handlers for objectStore.add, put, delete and clear
  function preAdd(os, value, key) {
    console.log('preAdd');
  }

  function postAdd(os, value, key, ret) {
    console.log('postAdd', os.name, key, value);
    // wait for return with key
    ret.onsuccess = function(ev) {
      var obj = {
        h: { id: _generateId(ev) },
        b: value
      };
      console.log(obj);
      writer.write(obj);
    };
  }

  function prePut(os, value, key) {
    console.log('prePut');
  }

  function postPut(os, value, key, ret) {
    console.log('postPut', os.name);
    // wait for return with key
    ret.onsuccess = function(ev) {
      var obj = {
        h: { id: _generateId(ev) },
        b: value
      };
      console.log(obj);
      writer.write(obj);
    };
  }

  function preDelete(os, key, ret) {
    console.log('preDelete');
  }

  function postDelete(os, key, ret) {
    console.log('postDelete');
    // wait for return with key
    ret.onsuccess = function(ev) {
      var obj = {
        h: {
          id: _generateId(ev),
          d: true
        }
      };
      console.log(obj);
      writer.add(obj);
    };
  }

  function preClear() {
    console.log('preClear');
    // TODO: should be translated in a delete for every object
  }

  function postClear() {
    console.log('postClear');
  }

  // proxy db.transaction.objectStore function to catch new object store modification commands
  function proxyObjectStore(target) {
    return new Proxy(target, {
      apply: function(target, that, args) {
        // proxy all modification calls that are not on the snapshot collection itself
        /*
        TODO: incorporate object store differentiation in level
        if (args[0] === SNAPSHOT_COLLECTION) {
          console.log('target', target, 'that', that, 'args', args);
          return target.apply(that, args);
        }
        */

        console.log('proxyObjectStore', target, args);

        var obj = target.apply(that, args);

        // proxy add, put, delete and clear
        var origAdd = obj.add;
        var origPut = obj.put;
        var origDelete = obj.delete;
        var origClear = obj.clear;

        // proxy add
        function proxyAdd(value, key) {
          preAdd(obj, value, key);
          var ret = origAdd.apply(obj, arguments);
          postAdd(obj, value, key, ret);
          return ret;
        }

        // proxy put
        function proxyPut(value, key) {
          prePut(obj, value, key);
          var ret = origPut.apply(obj, arguments);
          postPut(obj, value, key, ret);
          return ret;
        }

        // proxy delete by adding a delete item
        function proxyDelete(key) {
          preDelete(obj, key);
          var ret = origDelete.apply(obj, arguments);
          postDelete(obj, key, ret);
          return ret;
        }

        // proxy clear by adding one delete per item
        function proxyClear() {
          preClear(obj);
          var ret = origClear.apply(obj, arguments);
          postClear(obj, ret);
          return ret;
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
        // add snapshot collection to readwrite transactions
        if (args && args[1] && args[1] === 'readwrite') {
          // TODO: incorporate object store differentiation in level
          //args[0].push(SNAPSHOT_COLLECTION);
          var obj = target.apply(that, args);
          obj.addEventListener('success', function(ev) {
            var transaction = ev.target.result;
            console.log('proxyTransaction success');
          });

          console.log('proxyTransaction', target, args, obj.mode);

          // proxy the opening of object stores for the target transaction
          obj.objectStore = proxyObjectStore(obj.objectStore);
          return obj;
        }

        console.log('proxyTransaction not a readwrite transaction', target.mode);
        return target.apply(that, args);
      }
    });
  }

  // proxy indexedDB.open method and catch the creation of new database instances
  function proxyIdbOpen(target) {
    return new Proxy(target, {
      apply: function(target, that, args) {
        var obj = target.apply(that, args);

        obj.addEventListener('upgradeneeded', function(ev) {
          console.log('proxyIdbOpen upgradeneeded', ev);

          var db = ev.target.result;

          try {
            // TODO: incorporate object store differentiation in level
            //db.createObjectStore(SNAPSHOT_COLLECTION, { keyPath: 'h.i', autoIncrement: true });
          } catch (err) {
            if (err.name !== 'ConstraintError') {
              throw err;
            }
          }
        });

        obj.addEventListener('success', function(ev) {
          console.log('proxyIdbOpen success');

          var db = ev.target.result;

          db.transaction = proxyTransaction(db.transaction);
        });

        console.log('proxyIdbOpen', target, args, obj);
        return obj;
      }
    });
  }

  indexedDB.open = proxyIdbOpen(indexedDB.open);
};
