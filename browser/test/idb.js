'use strict';

function dropDb(name, cb) {
  if (typeof name !== 'string') { throw new TypeError('name must be a string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var req = indexedDB.deleteDatabase(name);

  req.onsuccess = () => cb();
  req.onerror = () => cb(req.error);
}

// opts.stores => { storeName: storeCreationOpts } }
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

  req.onsuccess = () => cb(null, req.result);
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
