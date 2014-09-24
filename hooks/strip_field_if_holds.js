'use strict';

// use opts.field and opts.fieldFilter to check if a field should be stripped
var match = require('./../lib/match');
module.exports = function(db, item, opts, cb) {
  if (typeof item[opts.field] !== 'undefined') {
    if (match(opts.fieldFilter, item)) {
      delete item[opts.field];
    }
  }

  cb(null, item);
};
