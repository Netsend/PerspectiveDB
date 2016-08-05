'use strict';

function dropDb(name, cb) {
  if (typeof name !== 'string') { throw new TypeError('name must be a string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var req = indexedDB.deleteDatabase(name);

  req.onsuccess = () => cb();
  req.onerror = () => cb(req.error);
}

// opts.stores => { storeName: storeCreationOpts } }
// opts.fixtures => { storeName: []|{} } }
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

    // load fixtures if any
    var fixtures = Object.keys(opts.fixtures || {});
    if (fixtures.length) {
      var tx = db.transaction(fixtures, 'readwrite');

      fixtures.forEach(function(storeName) {
        var store = tx.objectStore(storeName);
        if (Array.isArray(opts.fixtures[storeName])) {
          opts.fixtures[storeName].forEach(function(obj) {
            store.put(obj);
          });
        } else { // assume this is an object, use the object keys
          Object.keys(opts.fixtures[storeName]).forEach(function(key) {
            var obj = opts.fixtures[storeName][key];
            console.log(obj, key);
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

module.exports = { dropDb, createDb, recreateDb };
