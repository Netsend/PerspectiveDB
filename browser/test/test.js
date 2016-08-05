'use strict';

var test = require('tape');

var idb = require('./idb.js');
var PersDB = require('../lib/persdb.js');

//require('./basic')(test, idb, PersDB, function(err) {
  //if (err) throw err;
require('./merge_conflict1')(test, idb, PersDB, function(err) {
  if (err) throw err;
});
//});
