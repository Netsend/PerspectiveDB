/* jshint -W097 */

'use strict';

var Proxy = require('./proxy');
var connect = require('./connect');
var MergeTree = require('../lib/merge_tree');

var config = require('./config.json');

connect(config.url, config.auth, function(msg) {
  console.log('ready');
}, function() {
  console.log('closed');
});
