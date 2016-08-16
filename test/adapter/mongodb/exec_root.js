/**
 * Copyright 2016 Netsend.
 *
 * This file is part of PerspectiveDB.
 *
 * PerspectiveDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PerspectiveDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PerspectiveDB. If not, see <https://www.gnu.org/licenses/>.
 */

'use strict';

if (process.getuid() !== 0) { 
  console.error('run tests as root');
  process.exit(1);
}

var assert = require('assert');

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
  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true }
      });
      break;
    default:
      throw new Error('unknown state');
    }
  }

  var opts = {
    onMessage: onMessage,
    onExit: done,
    echoErr: false,
    exitCode: 1,
    testStderr: function(stderr) {
      assert(/msg.url must be a non-empty string/.test(stderr));
    }
  };

  spawn([__dirname + '/../../../adapter/mongodb/exec', __dirname + '/test1_pdb_source_mongo.hjson'], opts);
});

// should ask for last version in general and per collection over version control channel (fd 7)
tasks.push(function(done) {

  var i = 0;
  function onSpawn(child) {
    var versionControl = child.stdio[7];

    // expect version requests in ld-json format
    var ls = new LDJSONStream();

    versionControl.pipe(ls);

    ls.on('data', function(data) {
      switch (i++) {
      case 0:
        assert.deepEqual(data, {});
        versionControl.write(BSON.serialize({}));
        break;
      case 1:
        assert.deepEqual(data, { prefixFirst: collectionName + '\x01' });
        versionControl.write(BSON.serialize({}));
        child.send({ type: 'kill' });
        break;
      default:
        throw new Error('unexpected');
      }
    });
  }

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG2 },
        collections: [collectionName],
        url: config.url
      });
      break;
    case 'listen':
      break;
    default:
      console.error(msg);
      throw new Error('unknown state');
    }
  }

  function onExit() {
    assert.strictEqual(i, 2);
    done();
  }

  var opts = {
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: onExit
  };

  var spawnOpts = {
    cwd: '/',
    env: {},
    stdio: ['pipe', 'pipe', 'pipe', 'ipc', null, null, 'pipe', 'pipe']
  };

  spawn([__dirname + '/../../../adapter/mongodb/exec', __dirname + '/test1_pdb_source_mongo.hjson'], opts, spawnOpts);
});

// should send a new version based on an oplog update
tasks.push(function(done) {
  var i = 0;
  var j = 0;
  function onSpawn(child) {
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
      j++;
      child.send({ type: 'kill' });
    });

    // expect version requests in ld-json format
    var lastTs;
    versionControl.pipe(new LDJSONStream()).on('data', function(data) {
      switch (i++) {
      case 0:
        // expect a request for the last version in the DAG
        assert.deepEqual(data, {});
        // send response with last timestamp in oplog
        oplogColl.find({}).sort({ '$natural': -1 }).limit(1).project({ ts: 1 }).next(function(err, oplogItem) {
          if (err) { throw err; }

          lastTs = oplogItem.ts;

          versionControl.write(BSON.serialize({
            h: { id: collectionName + '\x01foo' },
            m: { _op: lastTs, _id: 'foo' },
            b: {}
          }));

          // and insert an item into the collection
          // this should make the process emit a new version on the data channel
          coll1.insertOne({ _id: 'foo', bar: 'baz' }, function(err) {
            if (err) { throw err; }
          });
        });
        break;
      case 1:
        // expect a request for any head of this collection
        assert.deepEqual(data, { prefixFirst: collectionName + '\x01' });

        versionControl.write(BSON.serialize({
          h: { id: collectionName + '\x01foo' },
          m: { _op: lastTs, _id: 'foo' },
          b: {}
        }));

        break;
      default:
        throw new Error('unexpected');
      }
    });
  }

  function onExit() {
    assert.strictEqual(i, 2);
    assert.strictEqual(j, 1);
    done();
  }

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG2 },
        collections: [collectionName],
        url: config.url
      });
      break;
    case 'listen':
      break;
    default:
      console.error(msg);
      throw new Error('unknown state');
    }
  }

  var opts = {
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: onExit
  };

  var spawnOpts = {
    cwd: '/',
    env: {},
    stdio: ['pipe', 'pipe', 'pipe', 'ipc', null, null, 'pipe', 'pipe']
  };

  spawn([__dirname + '/../../../adapter/mongodb/exec', __dirname + '/test1_pdb_source_mongo.hjson'], opts, spawnOpts);
});

// should save a merge in the conflict collection if the local head does not match the item in the collection
// conflict = delete merge item with nothing in the collection
tasks.push(function(done) {
  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG2 },
        collections: [collectionName2],
        url: config.url
      });
      break;
    case 'listen':
      var dataChannel = child.stdio[6];
      var versionControl = child.stdio[7];
      var pe = 'baz';

      // send something to prevent bootstrapping an empty tree
      var future = new Timestamp(0, (new Date()).getTime() / 1000 + 1); // in the future so no oplog items exist yet
      var lastItem = BSON.serialize({
        h: { id: collectionName2 + '\x01qux' },
        m: { _op: future, _id: 'qux' },
        b: {}
      });

      // expect a global and a prefixed head lookup
      var i = 0;
      versionControl.pipe(new LDJSONStream()).on('data', function(data) {
        switch (i++) {
        case 0:
          assert.deepEqual(data, {});
          versionControl.write(lastItem);
          break;
        case 1:
          assert.deepEqual(data, { prefixFirst: collectionName2 + '\x01' });
          versionControl.write(lastItem, function(err) {
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
          break;
        default:
          throw new Error('unexpected');
        }
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

  spawn([__dirname + '/../../../adapter/mongodb/exec', __dirname + '/test1_pdb_source_mongo.hjson'], opts, spawnOpts);
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
