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
  // wait for https://bugzilla.mozilla.org/show_bug.cgi?id=842818
  //errors.push('missing Web Cryptography API');
  console.log('WARNING: missing crypto interface, using a cryptographically INSECURE PRNG');
  var crypto = {
    // expect buffer to be an Uint8Array
    getRandomValues: function(buffer) {
      for (var i = 0; i < buffer.length; i++) {
        buffer[i] = Math.floor(Math.random() * 256);
      }
    }
  };
  //errors.push('missing Web Cryptography API');
}
if (typeof Proxy !== 'function') {
  errors.push('missing ES6 Proxy');
}

if (errors.length) {
  console.error(errors);
  throw new Error(errors.join(', '));
}

var SNAPSHOT_COLLECTION = '_pdb';

// from: https://gist.github.com/jonleighton/958841
// Converts a TypedArray directly to base64, without any intermediate 'convert to string then
// use window.btoa' step. According to my tests, this appears to be a faster approach:
// http://jsperf.com/encoding-xhr-image-data/5
function base64TypedArray(bytes) {
  var base64    = '';
  var encodings = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';

  var byteLength    = bytes.byteLength;
  var byteRemainder = byteLength % 3;
  var mainLength    = byteLength - byteRemainder;

  var a, b, c, d;
  var chunk;

  // Main loop deals with bytes in chunks of 3
  for (var i = 0; i < mainLength; i = i + 3) {
    // Combine the three bytes into a single integer
    chunk = (bytes[i] << 16) | (bytes[i + 1] << 8) | bytes[i + 2];

    // Use bitmasks to extract 6-bit segments from the triplet
    a = (chunk & 16515072) >> 18; // 16515072 = (2^6 - 1) << 18
    b = (chunk & 258048)   >> 12; // 258048   = (2^6 - 1) << 12
    c = (chunk & 4032)     >>  6; // 4032     = (2^6 - 1) << 6
    d = chunk & 63;               // 63       = 2^6 - 1

    // Convert the raw binary segments to the appropriate ASCII encoding
    base64 += encodings[a] + encodings[b] + encodings[c] + encodings[d];
  }

  // Deal with the remaining bytes and padding
  if (byteRemainder === 1) {
    chunk = bytes[mainLength];

    a = (chunk & 252) >> 2; // 252 = (2^6 - 1) << 2

    // Set the 4 least significant bits to zero
    b = (chunk & 3)   << 4; // 3   = 2^2 - 1

    base64 += encodings[a] + encodings[b] + '==';
  } else if (byteRemainder === 2) {
    chunk = (bytes[mainLength] << 8) | bytes[mainLength + 1];

    a = (chunk & 64512) >> 10; // 64512 = (2^6 - 1) << 10
    b = (chunk & 1008)  >>  4; // 1008  = (2^6 - 1) << 4

    // Set the 2 least significant bits to zero
    c = (chunk & 15)    <<  2; // 15    = 2^4 - 1

    base64 += encodings[a] + encodings[b] + encodings[c] + '=';
  }

  return base64;
}

/**
 * Generate a random byte string.
 *
 * By default generates a 48 bit base64 id (string of 8 characters)
 *
 * @param {Number, default: 6} [size]  number of random bytes te generate
 * @return {String} the random bytes encoded in base64
 */
function _generateRandomVersion(size) {
  var data = new Uint8Array(size || 6);
  crypto.getRandomValues(data);
  return base64TypedArray(data);
}

// pre and post handlers for objectStore.add, put, delete and clear
function preAdd(sc, os, value, key) {
  console.log('preAdd');
}

function postAdd(sc, os, value, key) {
  console.log('postAdd');
  var obj = {
    h: {
      id: key,
      v: _generateRandomVersion(),
      pa: []
    },
    b: value
  };
  console.log(obj);
  sc.add(obj);
}

function prePut(sc, os, value, key) {
  console.log('prePut');
}

function postPut(sc, os, value, key) {
  console.log('postPut', os.name);
  var obj = {
    h: {
      id: key,
      v: _generateRandomVersion(),
      pa: []
    },
    b: value
  };
  console.log(obj);
  sc.add(obj);
}

function preDelete(sc, os, key) {
  console.log('preDelete');
}

function postDelete(sc, os, key) {
  console.log('postDelete');
  var obj = {
    h: {
      id: key,
      v: _generateRandomVersion(),
      pa: [],
      d: true
    }
  };
  console.log(obj);
  sc.add(obj);
}

function preClear(sc) {
  console.log('preClear');
  // todo: should be translated in a delete for every object
}

function postClear(sc) {
  console.log('postClear');
}

// proxy db.transaction.objectStore function to catch new object store modification commands
function proxyObjectStore(target) {
  return new Proxy(target, {
    apply: function(target, that, args) {
      // proxy all modification calls that are not on the snapshot collection itself
      if (args[0] === SNAPSHOT_COLLECTION) {
        console.log('target', target, 'that', that, 'args', args);
        return target.apply(that, args);
      }

      console.log('proxyObjectStore', target, args);

      var obj = target.apply(that, args);

      // handle to the snapshot collection
      var sc = obj.transaction.objectStore(SNAPSHOT_COLLECTION);

      // proxy add, put, delete and clear
      var origAdd = obj.add;
      var origPut = obj.put;
      var origDelete = obj.delete;
      var origClear = obj.clear;

      // proxy add
      function proxyAdd(value, key) {
        preAdd(sc, obj, value, key);
        var ret = origAdd.apply(obj, arguments);
        postAdd(sc, obj, value, key);
        return ret;
      }

      // proxy put
      function proxyPut(value, key) {
        prePut.apply(obj, [sc, obj, value, key]);
        var ret = origPut.apply(obj, arguments);
        postPut.apply(obj, [sc, obj, value, key]);
        return ret;
      }

      // proxy delete by adding a delete item
      function proxyDelete(key) {
        preDelete.apply(obj, [sc, obj, key]);
        var ret = origDelete.apply(obj, arguments);
        postDelete.apply(obj, [sc, obj, key]);
        return ret;
      }

      // proxy clear by adding one delete per item
      function proxyClear() {
        preClear.apply(obj, [sc, obj]);
        var ret = origClear.apply(obj, arguments);
        postClear.apply(obj, [sc, obj]);
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
        args[0].push(SNAPSHOT_COLLECTION);
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
          db.createObjectStore(SNAPSHOT_COLLECTION, { keyPath: 'h.i', autoIncrement: true });
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
