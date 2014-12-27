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
var spawn = childProcess.spawn;

var async = require('async');

var tasks = [];

var dbServer, dbClient;
var databaseNames = ['testserver', 'test_client'];
var Database = require('../_database');

// open database connection
var database = new Database(databaseNames);
tasks.push(function(done) {
  database.connect(function(err, dbs) {
    dbServer = dbs[0];
    dbClient = dbs[1];
    done(err);
  });
});

var child1, child2;

// should insert some dummies in the collection to version on the server side
tasks.push(function(done) {
  var coll = dbServer.collection('someColl');
  coll.insert([{ foo: 'bar' },{ quz: 'zab' }], done);
});

// should start a server and a client, the client should login and get some data from the server
tasks.push(function(done) {
  child1 = spawn(__dirname + '/../../server2.js', ['-d', 'test/lib/test_server.ini']);

  child1.stdout.setEncoding('utf8');
  child1.stdout.pipe(process.stdout);

  child1.stderr.setEncoding('utf8');
  child1.stderr.pipe(process.stderr);

  child1.on('close', function(code, sig) {
    console.log('close');
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    console.log('child1 dead');
  });

  setTimeout(done, 1000);
});

tasks.push(function(done) {
  child2 = spawn(__dirname + '/../../server2.js', ['-d', 'test/lib/test_client.ini']);

  child2.stdout.setEncoding('utf8');
  child2.stdout.pipe(process.stdout);

  child2.stderr.setEncoding('utf8');
  child2.stderr.pipe(process.stderr);

  child2.on('close', function(code, sig) {
    console.log('close');
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    console.log('child2 dead');
    child1.kill();
  });

  setTimeout(function() {
    child1.on('close', done);
    child2.kill();
  }, 1200);
});

tasks.push(database.disconnect.bind(database));

async.series(tasks, function(err) {
  if (err) {
    console.error(err);
  } else {
    console.log('ok');
  }
});
