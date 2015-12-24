/* jshint -W097 */

'use strict';

var config = require('./config.json');
var worker = new Worker('build/worker.js');
worker.onmessage = function (event) {
  console.log(event.data);
};
console.log("OK");
