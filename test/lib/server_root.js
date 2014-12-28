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
var Timestamp = require('mongodb').Timestamp;

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
  var item1 = { foo: 'bar', someKey: 'someVal', someOtherKey: 'B' };
  var item2 = { foo: 'baz', someKey: 'someVal' };
  var item3 = { quz: 'zab', zab: 'bar' };

  var items = [item1, item2, item3];
  coll.insert(items, done);
});

// should start a server and a client, the client should login and get some data from the server
tasks.push(function(done) {
  child1 = spawn(__dirname + '/../../server2.js', ['-d', 'test/lib/test_server.ini']);

  child1.stdout.setEncoding('utf8');

  child1.stderr.setEncoding('utf8');
  child1.stderr.pipe(process.stderr);

  child1.on('close', function(code, sig) {
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
  });

  setTimeout(done, 1000);
});

tasks.push(function(done) {
  child2 = spawn(__dirname + '/../../server2.js', ['-d', 'test/lib/test_client.ini']);

  child2.stdout.setEncoding('utf8');

  child2.stderr.setEncoding('utf8');
  child2.stderr.pipe(process.stderr);

  child2.on('close', function(code, sig) {
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    child1.kill();
  });

  setTimeout(function() {
    child1.on('close', done);
    child2.kill();
  }, 1200);
});

// should have sent, saved and ackd dummies
tasks.push(function(done) {
  dbServer.collection('someColl').find().toArray(function(err, collServerItems) {
    if (err) { done(err); }
    assert.strictEqual(collServerItems.length, 3);

    var item1id = collServerItems[0]._id;
    var item1v = collServerItems[0]._v;

    var item2id = collServerItems[1]._id;
    var item2v = collServerItems[1]._v;

    dbServer.collection('m3.someColl').find().toArray(function(err, snapshotServerItems) {
      if (err) { done(err); }
      assert.strictEqual(snapshotServerItems.length, 3);

      dbClient.collection('someColl').find().toArray(function(err, collClientItems) {
        if (err) { done(err); }
        assert.strictEqual(collClientItems.length, 2);
        assert.deepEqual(collClientItems[0], { _id: item1id, foo: 'bar', someKey: 'someVal', someOtherKey: 'B', _v: item1v });
        assert.deepEqual(collClientItems[1], { _id: item2id, foo: 'baz', someKey: 'someVal', _v: item2v });

        dbClient.collection('m3.someColl').find().toArray(function(err, snapshotClientItems) {
          if (err) { done(err); }
          assert.strictEqual(snapshotClientItems.length, 4);
          assert.deepEqual(snapshotClientItems[0], {
            _id: { _co: 'someColl', _id: item1id, _v: item1v, _pe: 'testserver', _pa: [] },
            _m3: { _op: new Timestamp(0, 0), _ack: false },
            foo: 'bar',
            someKey: 'someVal',
            someOtherKey: 'B'
          });
          assert.deepEqual(snapshotClientItems[1], {
            _id: { _co: 'someColl', _id: item2id, _v: item2v, _pe: 'testserver', _pa: [] },
            _m3: { _op: new Timestamp(0, 0), _ack: false },
            foo: 'baz',
            someKey: 'someVal'
          });

          assert.strictEqual(snapshotClientItems[2]._m3._op.greaterThan(new Timestamp(0, 0)), true);
          delete snapshotClientItems[2]._m3._op;
          assert.deepEqual(snapshotClientItems[2], {
            _id: { _co: 'someColl', _id: item1id, _v: item1v, _pe: '_local', _pa: [], _i: 1 },
            _m3: { _ack: true },
            foo: 'bar',
            someKey: 'someVal',
            someOtherKey: 'B'
          });

          assert.strictEqual(snapshotClientItems[3]._m3._op.greaterThan(new Timestamp(0, 0)), true);
          delete snapshotClientItems[3]._m3._op;
          assert.deepEqual(snapshotClientItems[3], {
            _id: { _co: 'someColl', _id: item2id, _v: item2v, _pe: '_local', _pa: [], _i: 2 },
            _m3: { _ack: true },
            foo: 'baz',
            someKey: 'someVal'
          });
          done();
        });
      });
    });
  });
});

tasks.push(database.disconnect.bind(database));

async.series(tasks, function(err) {
  if (err) {
    console.error(err);
  } else {
    console.log('ok');
  }
});
