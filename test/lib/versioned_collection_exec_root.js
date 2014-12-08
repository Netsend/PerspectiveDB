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

var async = require('async');

var tasks = [];
var tasks2 = [];

// should require chrootUser to have a valid username
tasks.push(function(done) {
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/user not found: test/.test(buff.toString()));
    assert.strictEqual(code, 1);
    assert.strictEqual(sig, null);
    done();
  });

  child.send({
    dbName: 'test',
    dbPort: 27019,
    collectionName: 'test',
    chrootUser: 'test',
    chrootNewRoot: '/var/empty'
  });
});

// should require chrootNewRoot to be a valid path
tasks.push(function(done) {
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/changing root failed: ENOENT, No such file or directory/.test(buff.toString()));
    assert.strictEqual(code, 1);
    assert.strictEqual(sig, null);
    done();
  });

  child.send({
    dbName: 'test',
    dbPort: 27019,
    collectionName: 'test',
    chrootUser: 'nobody',
    chrootNewRoot: '/some'
  });
});

// should not fail with valid configurations
tasks.push(function(done) {
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  var buff = new Buffer(0);
  //child.stdout.pipe(process.stdout);

  child.stderr.setEncoding('utf8');
  child.stderr.pipe(process.stderr);
  child.stderr.on('data', function(data) {
    buff += data;
  });

  child.on('exit', function(code, sig) {
    assert.strictEqual(buff.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  child.send({
    dbName: 'test',
    dbPort: 27019,
    collectionName: 'test',
    chrootUser: 'nobody',
    chrootNewRoot: '/var/empty'
  });

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      break;
    case 'listen':
      child.kill();
      break;
    }
  });
});

async.series(tasks, function(err) {
  if (err) {
    console.error(err);
  } else {
    console.log('ok');
  }
});
