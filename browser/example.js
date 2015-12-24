/* jshint -W097 */

'use strict';

require('./proxy'); // loading proxy will transparently proxy indexedDB

var connect = require('./connect');

var config = require('./config.json');

connect(config.url, config.auth, function() {
  console.log('ready');
}, function() {
  console.log('closed');
});
