'use strict';

var objectStoreFromId = require('../../lib/idb_id_ops').objectStoreFromId;

// filter objects that don't belong to one of the given stores
// use opts.include and opts.exclude to specify which stores to include
module.exports = function(db, item, opts, cb) {
  var os = objectStoreFromId(item.h.id);
  if (~opts.stores.indexOf(os)) {
    process.nextTick(function() {
      cb(null, item);
    });
    return;
  }

  process.nextTick(function() {
    cb(null, null);
  });
};
