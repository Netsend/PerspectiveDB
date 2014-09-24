#!/usr/bin/env node

/**
 * Copyright 2014 Netsend.
 *
 * This file is part of Mastersync.
 *
 * Mastersync is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * Mastersync is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with Mastersync. If not, see <https://www.gnu.org/licenses/>.
 */

'use strict';

var readline = require('readline');

var program = require('commander');
var async = require('async');

program
  .version('0.0.2')
  .usage('[-v] attr [attr2 ...]')
  .description('read object from stdin and output given attributes only')
  .option('-v, --verbose', 'verbose logging')
  .parse(process.argv);

var attrs = program.args;
if (!attrs.length) { program.help(); }

function start() {
  var rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false
  });

  var rlClosed = false;

  var queue = async.queue(function(item, cb) {
    if (program.verbose) { console.log('worker'); }
    attrs.forEach(function(attr) {
      if (typeof item[attr] !== 'undefined') {
        console.log(item[attr]);
      }
    });
    cb();
  }, 1);

  queue.saturated = function() {
    if (program.verbose) { console.log('saturated'); }
    if (!rlClosed) { rl.pause(); }
  };

  queue.empty = function() {
    if (program.verbose) { console.log('empty'); }
    if (!rlClosed) { rl.resume(); }
  };

  queue.drain = function() {
    if (program.verbose) { console.log('drain'); }
  };

  rl.on('line', function(line) {
    if (program.verbose) { console.log('line'); }
    try {
      queue.push(JSON.parse(line));
    } catch(err) {
      console.error(err, line);
    }
  });

  rl.on('close', function() {
    if (program.verbose) { console.log('readline closed'); }
    rlClosed = true;
  });
}

start();
