'use strict';

// filter objects that are older than a certain offset compared to the current time.
// use opts.field to specify which field name to check and opts.maxAge in seconds to set the max offset time 
module.exports = function(db, item, opts, cb) {
  if (typeof item[opts.field] !== 'undefined') {
    var now = (new Date()).getTime();
    var maxAge = now - (opts.maxAge * 1000);
    maxAge = new Date(maxAge);
    if (item[opts.field] >= maxAge) {
      process.nextTick(function() {
        cb(null, item);
      });
      return;
    }
  }

  process.nextTick(function() {
    cb(null, null);
  });
};
