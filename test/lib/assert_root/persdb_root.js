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

if (process.getuid() !== 0) { 
  console.error('run tests as root');
  process.exit(1);
}

var assert = require('assert');
var childProcess = require('child_process');
var fs = require('fs');
var spawn = childProcess.spawn;

var async = require('async');
var level = require('level');
var rimraf = require('rimraf');

var logger = require('../../../lib/logger');

var tasks = [];
var tasks2 = [];

// print line number
function lnr() {
  return new Error().stack.split('\n')[2].match(/persdb_root.js:([0-9]+):[0-9]+/)[1];
}

var cons, silence;
var chroot = '/var/persdb/test_persdb_root';
var user = 'nobody';
var group = 'nobody';
var dbPath = '/data';

// open loggers
tasks.push(function(done) {
  logger({ console: true, mask: logger.DEBUG2 }, function(err, l) {
    if (err) { throw err; }
    cons = l;
    logger({ silence: true }, function(err, l) {
      if (err) { throw err; }
      silence = l;
      // ensure chroot
      fs.mkdir(chroot, 0o755, function(err) {
        if (err && err.code !== 'EEXIST') { throw err; }

        // remove any pre-existing dbPath
        rimraf(chroot + dbPath, done);
      });
    });
  });
});

// should start a server and listen on port 1234, login and get some data from the server
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = spawn(__dirname + '/../../../bin/persdb.js', [__dirname + '/test_persdb.hjson']);

  child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert(/error loading hooks: "foo,bar" /.test(stderr));
    assert.strictEqual(code, 2);
    assert.strictEqual(sig, null);
    done();
  });

  child.on('message', function(msg) {
    console.log("message");
    switch(msg) {
    case 'init':
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

async.series(tasks, function(err) {
  if (err) {
    console.error(err);
  } else {
    console.log('ok');
  }

  // cleanup after
  cons.close(function(err) {
    if (err) { throw err; }
    silence.close(function(err) {
      if (err) { throw err; }
      rimraf(chroot + dbPath, function(err) {
        if (err) { console.error(err); }
      });
    });
  });
});
