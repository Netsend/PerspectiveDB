'use strict';

var test = require('tape');

var idbTools = require('../lib/idb_tools.js');
var PersDB = require('../lib/persdb.js');

require('./basic').run(test, idbTools, PersDB);
require('./merge_conflict1').run(test, idbTools, PersDB);
require('./merge_conflict2').run(test, idbTools, PersDB);
