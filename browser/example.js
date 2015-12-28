/* jshint -W097 */

'use strict';

var config = require('./config.json');
var worker = new Worker('build/worker.js');
worker.onmessage = function(ev) {
  if (ev.data !== 'init') {
    throw new Error('expected init');
  }

  // send config
  worker.postMessage({
    perspectives: config
  });
};
console.log('OK');
