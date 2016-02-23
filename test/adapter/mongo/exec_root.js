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

if (process.getuid() !== 0) { 
  console.error('run tests as root');
  process.exit(1);
}

var assert = require('assert');
var childProcess = require('child_process');

var async = require('async');
var bson = require('bson');
var BSONStream = require('bson-stream');
var LDJSONStream = require('ld-jsonstream');
var mongodb = require('mongodb');

var config = require('./config.json');
var logger = require('../../../lib/logger');

var BSON = new bson.BSONPure.BSON();
var MongoClient = mongodb.MongoClient;
var Timestamp = mongodb.Timestamp;

var tasks = [];
var tasks2 = [];

// print line number
function lnr() {
  return new Error().stack.split('\n')[2].match(/exec_root.js:([0-9]+):[0-9]+/)[1];
}

var logger = require('../../../lib/logger');

var cons, silence, db;
var databaseName = 'test_exec_root';
var collectionName = 'test_exec_root';

// open loggers and a connection to the database
tasks.push(function(done) {
  logger({ console: true, mask: logger.DEBUG2 }, function(err, l) {
    if (err) { throw err; }
    cons = l;
    logger({ silence: true }, function(err, l) {
      if (err) { throw err; }
      silence = l;
      MongoClient.connect(config.url, function(err, dbc) {
        if (err) { throw err; }
        db = dbc.db(databaseName);
        db.collection(collectionName).deleteMany({}, done);
      });
    });
  });
});

// should require db string
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../adapter/mongo/exec', { silent: true });

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert(/msg.db must be a non-empty string/.test(stderr));
    assert.strictEqual(code, 1);
    assert.strictEqual(sig, null);
    done();
  });

  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true }
      });
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

// should ask for last version over version control channel (fd 7)
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.spawn(process.execPath, [__dirname + '/../../../adapter/mongo/exec'], {
    cwd: '/',
    env: {},
    stdio: ['pipe', 'pipe', 'pipe', 'ipc', null, null, 'pipe', 'pipe']
  });

  var versionControl = child.stdio[7];

  // expect version requests in ld-json format
  var ls = new LDJSONStream();

  versionControl.pipe(ls);

  var validRequest = false;
  ls.on('data', function(data) {
    assert.deepEqual(data, {
      id: null
    });
    validRequest = true;

    child.kill();
  });

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    assert.strictEqual(validRequest, true);
    done();
  });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG2 },
        db: databaseName,
        coll: collectionName,
        url: config.url
      });
      break;
    case 'listen':
      break;
    default:
      console.error(msg);
      throw new Error('unknown state');
    }
  });
});

// should send a new version based on an oplog update
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var offset = new Timestamp(0, (new Date()).getTime() / 1000);

  var child = childProcess.spawn(process.execPath, [__dirname + '/../../../adapter/mongo/exec'], {
    cwd: '/',
    env: {},
    stdio: ['pipe', 'pipe', 'pipe', 'ipc', null, null, 'pipe', 'pipe']
  });

  var dataChannel = child.stdio[6];
  var versionControl = child.stdio[7];

  var bs = new BSONStream();
  dataChannel.pipe(bs).on('readable', function() {
    var obj = bs.read();
    if (!obj) { return; }

    var ts = obj.m._op;
    assert.strictEqual(ts.greaterThan(offset), true);
    assert.deepEqual(obj, {
      h: { id: 'foo' },
      m: { _op: ts },
      b: { _id: 'foo', bar: 'baz' }
    });
    child.kill();
  });

  // expect version requests in ld-json format
  var ls = new LDJSONStream();

  versionControl.pipe(ls);

  var i = 0;
  ls.on('data', function(data) {
    i++;

    // expect a request for the last version in the DAG
    if (i === 1) {
      assert.deepEqual(data, { id: null });

      var offset = new Timestamp(0, (new Date()).getTime() / 1000);

      // send response with current timestamp in oplog
      versionControl.write(BSON.serialize({
        h: { id: 'foo' },
        m: { _op: offset }, 
        b: {}
      }));

      // and insert an item into the collection
      var coll = db.collection(collectionName);
      coll.insertOne({ _id: 'foo', bar: 'baz' }, function(err) {
        if (err) { throw err; }
      });
    }

    // expect a request for the last version of the inserted item
    if (i === 2) {
      assert.deepEqual(data, { id: 'foo' });

      // send response with a fake version
      versionControl.write(BSON.serialize({
        h: { id: 'foo', v: 'Aaaaaa' },
        b: {
          some: 'data'
        }
      }));

      // this should make the process emit a new version on the data channel
    }
  });

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    assert.strictEqual(i, 1);
    done();
  });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG2 },
        db: databaseName,
        coll: collectionName,
        url: config.url
      });
      break;
    case 'listen':
      break;
    default:
      console.error(msg);
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
      if (err) { console.error(err); }
      db.close();
    });
  });
});
