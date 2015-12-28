/* jshint -W097 */

'use strict';

var config = require('./config.json');
var worker = new Worker('build/worker.js');

const START  = 1;
const INIT   = 2;
const LISTEN = 3;

var phase = START;

worker.onmessage = function(ev) {
  switch (phase) {
  case START:
    if (ev.data !== 'init') {
      throw new Error('expected init');
    }
    phase = INIT;

    // send config
    worker.postMessage({
      perspectives: config
    });
    break;
  case INIT:
    if (ev.data !== 'listen') {
      throw new Error('expected listen');
    }
    phase = LISTEN;
    break;
  default:
    console.error('unexpected state: ' + ev.data);
    throw new Error('no next state expected');
  }
};
