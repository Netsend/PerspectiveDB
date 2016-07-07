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
var spawn = require('../../lib/spawn');

var BSON = new bson.BSONPure.BSON();
var MongoClient = mongodb.MongoClient;
var Timestamp = mongodb.Timestamp;

var tasks = [];
var tasks2 = [];

// print line number
function lnr() {
  return new Error().stack.split('\n')[2].match(/exec_root.js:([0-9]+):[0-9]+/)[1];
}

var cons, silence, db, oplogColl;
var databaseName = 'test_exec_root';
var collectionName = 'test_exec_root';
var collectionName2 = 'test_exec_root2';
var oplogDbName = 'local';
var oplogCollName = 'oplog.$main';
var coll1, coll2, conflictColl;

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
        coll1 = db.collection(collectionName);
        coll2 = db.collection(collectionName2);
        oplogColl = dbc.db(oplogDbName).collection(oplogCollName);
        conflictColl = db.collection('conflicts'); // default conflict collection

        // ensure empty collections
        coll1.remove(function(err) {
          if (err) { throw err; }
          coll2.remove(function(err) {
            if (err) { throw err; }
            conflictColl.remove(function(err) {
              if (err) { throw err; }
              done();
            });
          });
        });
      });
    });
  });
});

// should require url string
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../adapter/mongodb/exec', { silent: true });

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert(/msg.url must be a non-empty string/.test(stderr));
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

  var child = childProcess.spawn(process.execPath, [__dirname + '/../../../adapter/mongodb/exec'], {
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

    child.send({ type: 'kill' });
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

  var child = childProcess.spawn(process.execPath, [__dirname + '/../../../adapter/mongodb/exec'], {
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

    var ts = obj.n.m._op;
    var now = new Timestamp(0, (new Date()).getTime() / 1000);
    assert.strictEqual(ts.greaterThan(now), true);
    assert.deepEqual(obj, {
      n: {
        h: { id: collectionName + '\x01foo' },
        m: { _op: ts, _id: 'foo' },
        b: { bar: 'baz' } // should not send _id to pdb in the body, only in .h and .m
      }
    });
    child.send({ type: 'kill' });
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

      // send response with last timestamp in oplog
      oplogColl.find({}).sort({ '$natural': -1 }).limit(1).project({ ts: 1 }).next(function(err, oplogItem) {
        if (err) { throw err; }

        versionControl.write(BSON.serialize({
          h: { id: collectionName + '\x01foo' },
          m: { _op: oplogItem.ts, _id: 'foo' },
          b: {}
        }));

        // and insert an item into the collection
        coll1.insertOne({ _id: 'foo', bar: 'baz' }, function(err) {
          if (err) { throw err; }
        });
      });
    }

    // expect a request for the last version of the inserted item
    if (i === 2) {
      assert.deepEqual(data, { id: 'foo' });

      // send response with a fake version
      versionControl.write(BSON.serialize({
        h: { id: collectionName + '\x01foo', v: 'Aaaaaa' },
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
        coll: collectionName,
        url: config.url,
        oplogTransformOpts: {
          awaitData: false // speedup tests
        }
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

// should save a merge in the conflict collection if the local head does not match the item in the collection
// conflict = delete merge item with nothing in the collection
tasks.push(function(done) {
  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG2 },
        coll: collectionName2,
        url: config.url,
        oplogTransformOpts: {
          awaitData: false // speedup tests
        }
      });
      break;
    case 'listen':
      var dataChannel = child.stdio[6];
      var versionControl = child.stdio[7];
      var pe = 'baz';

      // expect one version request (for the last version in the DAG)
      var i = 0;
      versionControl.pipe(new LDJSONStream()).on('data', function(data) {
        i++;
        assert.equal(i, 1);
        assert.deepEqual(data, { id: null });

        // send something to prevent bootstrapping an empty tree
        var future = new Timestamp(0, (new Date()).getTime() / 1000 + 1); // in the future so no oplog items exist yet
        versionControl.write(BSON.serialize({
          h: { id: collectionName2 + '\x01qux' },
          m: { _op: future, _id: 'qux' },
          b: {}
        }), function(err) {
          if (err) { throw err; }

          // real test starts here
          // the presumed merge/delete item
          dataChannel.write(BSON.serialize({
            n: {
              h: { id: collectionName2 + '\x01qux', v: 'Aaaa', pe: pe, pa: [], d: true },
              b: { bar: 'baz' } // should not send _id to pdb in the body, only in .h and .m
            },
            l: null,
            lcas: [],
            pe: pe,
            c: null
          }), function(err) {
            // wait a while and inspect collections
            setTimeout(function() {
              if (err) { throw err; }

              // collection itself should be empty
              coll2.find({}).toArray(function(err, items) {
                if (err) { throw err; }
                assert.strictEqual(items.length, 0);

                // conflict collection should contain the just sent item with a delete error
                conflictColl.find({}, { sort: { _id: 1 } }).toArray(function(err, items) {
                  if (err) { throw err; }

                  assert.strictEqual(items.length, 1);
                  delete items[0]._id; // delete object id
                  assert.deepEqual(items, [{
                    n: {
                      h: { id: collectionName2 + '\x01qux', v: 'Aaaa', pe: pe, pa: [], d: true },
                      b: { bar: 'baz' }
                    },
                    l: null,
                    lcas: [],
                    pe: pe,
                    c: null,
                    err: 'local head expected' // delete error
                  }]);
                  child.send({ type: 'kill' });
                });
              });
            }, 100);
          });
        });
      });
      break;
    default:
      console.error(msg);
      throw new Error('unknown state');
    }
  }

  var opts = {
    onMessage: onMessage,
    onExit: done
  };

  var spawnOpts = {
    cwd: '/',
    env: {},
    stdio: ['pipe', 'pipe', 'pipe', 'ipc', null, null, 'pipe', 'pipe']
  };

  spawn([__dirname + '/../../../adapter/mongodb/exec', __dirname + '/test1_persdb_source_mongo.hjson'], opts, spawnOpts);
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
