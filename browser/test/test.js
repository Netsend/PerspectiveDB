'use strict';

var async = require('async');

var idb = require('./idb.js');
var PersDB = require('../lib/persdb.js');

var tasks = [];
var tasks2 = [];
tasks2.push(function(cb) {
  require('./basic')(idb, PersDB, cb);
});
tasks2.push(function(cb) {
  require('./merge_conflict1')(idb, PersDB, cb);
});
tasks.push(function(cb) {
  require('./merge_conflict2')(idb, PersDB, cb);
});

async.series(tasks, function(err) {
  if (err) throw err;
});
