'use strict';

var test = require('tape');

var idbTools = require('../lib/idb_tools.js');
var PerspectiveDB = require('../lib/persdb.js');

require('./basic').run(test, idbTools, PerspectiveDB);
require('./merge_conflict1').run(test, idbTools, PerspectiveDB);
require('./merge_conflict2').run(test, idbTools, PerspectiveDB);
