/**
 * Copyright 2016 Netsend.
 *
 * This file is part of PersDB.
 *
 * PersDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PersDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PersDB. If not, see <https://www.gnu.org/licenses/>.
 */

'use strict';

var assert = require('assert');
var cp = require('child_process');

var xtend = require('xtend');

function noop() {}

function spawn(args, options, spawnOpts) {
  if (typeof args === 'string') { args = [args]; }

  if (typeof args !== 'string' && !Array.isArray(args)) { throw new TypeError('args must be a string or an array'); }

  var opts = xtend({
    echoOut: false,
    echoErr: true,
    onSpawn: noop,
    onMessage: null,                                          // handle ipc messages
    onExit: noop,
    exitCode: 0,                                              // test if exit code is 0
    exitSignal: null,                                         // test if exit signal is empty
    testStdout: null,
    testStderr: function(s) {                                 // test if stderr is empty
      if (s.length) { throw new Error(s); }
    }
  }, options);

  var sOpts = xtend({
    args: args,
    stdio: ['pipe', 'pipe', 'pipe', 'ipc']
  }, spawnOpts);

  // print line number
  console.log('test #%d', new Error().stack.split('\n')[2].match(/\.js:([0-9]+):[0-9]+/)[1]);

  var extraArgs = [];
  var child = cp.spawn(process.execPath, sOpts.args.concat(extraArgs), sOpts);

  if (opts.echoOut) { child.stdout.pipe(process.stdout); }
  if (opts.echoErr) { child.stderr.pipe(process.stderr); }

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    if (opts.testStdout) {
      opts.testStdout(stdout);
    }
    if (opts.testStderr) {
      opts.testStderr(stderr);
    }
    assert.strictEqual(code, opts.exitCode);
    assert.strictEqual(sig, opts.exitSignal);
    opts.onExit(null, code, sig, stdout, stderr);
  });

  if (opts.onMessage) {
    child.on('message', function(msg) {
      opts.onMessage(msg, child);
    });
  }
  opts.onSpawn(child);
}

module.exports = spawn;
