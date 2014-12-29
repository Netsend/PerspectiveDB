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

var db;
var databaseNames = ['test_local_only'];
var Database = require('../_database');

// open database connection
var database = new Database(databaseNames);
tasks.push(function(done) {
  database.connect(function(err, dbs) {
    db = dbs[0];
    done(err);
  });
});

var child;

// should insert some dummies in the collection to version on the server side
tasks.push(function(done) {
  var coll = db.collection('someColl');
  var item1 = { foo: 'bar', someKey: 'someVal', someOtherKey: 'B' };
  var item2 = { foo: 'baz', someKey: 'someVal' };
  var item3 = { quz: 'zab', zab: 'bar' };

  var items = [item1, item2, item3];
  coll.insert(items, done);
});

// should start a server and a client, the client should login and get some data from the server
tasks.push(function(done) {
  child = spawn(__dirname + '/../../server2.js', ['-d', 'test/lib/test_local_only.ini']);

  child.on('close', function() {
    child.kill();
    done();
  });

  child.stdout.setEncoding('utf8');

  child.stderr.setEncoding('utf8');
  child.stderr.pipe(process.stderr);
  child.stdout.pipe(process.stdout);

  child.on('close', function(code, sig) {
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
  });

  var i = 0;
  child.stdout.on('data', function(data) {
    if (/_applyOplogUpdateFullDoc, set ackd/.test(data)) {
      i++;
      if (i === 2) {
        child.kill();
      }
    }
  });
});

// should have saved and ackd dummies
tasks.push(function(done) {
  db.collection('someColl').find().toArray(function(err, collClientItems) {

    var item1id = collClientItems[0]._id;
    var item2id = collClientItems[1]._id;
    var item3id = collClientItems[2]._id;

    var item1v = collClientItems[0]._v;
    var item2v = collClientItems[1]._v;
    var item3v = collClientItems[2]._v;

    if (err) { done(err); }
    assert.strictEqual(collClientItems.length, 3);
    assert.deepEqual(collClientItems[0], { _id: item1id, foo: 'bar', someKey: 'someVal', someOtherKey: 'B', _v: item1v });
    assert.deepEqual(collClientItems[1], { _id: item2id, foo: 'baz', someKey: 'someVal', _v: item2v });
    assert.deepEqual(collClientItems[2], { _id: item3id, quz: 'zab', zab: 'bar', _v: item3v });

    db.collection('m3.someColl').find().toArray(function(err, snapshotClientItems) {
      if (err) { done(err); }
      assert.strictEqual(snapshotClientItems.length, 3);
      assert.strictEqual(snapshotClientItems[0]._m3._op.greaterThan(new Timestamp(0, 0)), true);
      delete snapshotClientItems[0]._m3._op;
      assert.deepEqual(snapshotClientItems[0], {
        _id: { _co: 'someColl', _id: item1id, _v: item1v, _pe: '_local', _pa: [], _lo: true, _i: 1 },
        _m3: { _ack: true },
        foo: 'bar',
        someKey: 'someVal',
        someOtherKey: 'B'
      });

      assert.strictEqual(snapshotClientItems[1]._m3._op.greaterThan(new Timestamp(0, 0)), true);
      delete snapshotClientItems[1]._m3._op;
      assert.deepEqual(snapshotClientItems[1], {
        _id: { _co: 'someColl', _id: item2id, _v: item2v, _pe: '_local', _pa: [], _lo: true, _i: 2 },
        _m3: { _ack: true },
        foo: 'baz',
        someKey: 'someVal'
      });
      done();
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
