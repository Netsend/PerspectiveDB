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
var fs = require('fs');
var net = require('net');

var async = require('async');
var bson = require('bson');
var BSONStream = require('bson-stream');
var LDJSONStream = require('ld-jsonstream');
var level = require('level-packager')(require('leveldown'));
var mongodb = require('mongodb');
var rimraf = require('rimraf');

var MergeTree = require('../../../lib/merge_tree');
var logger = require('../../../lib/logger');
var spawn = require('../../lib/spawn');

var BSON = new bson.BSONPure.BSON();
var MongoClient = mongodb.MongoClient;

// must match with the hjson config files
var collectionName1 = 'test1';
var collectionName2 = 'test2';
var collectionName3 = 'test3';

var tasks = [];
var tasks2 = [];

var db, coll1, coll2, coll3, conflictColl;
var cons, silence;
var dbroot = '/var/pdb/test';

// open loggers and setup db connection
tasks.push(function(done) {
  logger({ console: true, mask: logger.DEBUG2 }, function(err, l) {
    if (err) { throw err; }
    cons = l;
    logger({ silence: true }, function(err, l) {
      if (err) { throw err; }
      silence = l;
      // remove any pre-existing db and ensure dbroot
      rimraf(dbroot, function(err) {
        if (err) { throw err; }
        fs.mkdir(dbroot, 0o755, function(err) {
          if (err && err.code !== 'EEXIST') { throw err; }
          MongoClient.connect('mongodb://127.0.0.1:27019/pdb', function(err, dbc) {
            if (err) { throw err; }

            db = dbc;

            // ensure an empty database and recreate collections
            db.dropDatabase(function(err) {
              if (err) { throw err; }

              db.createCollection(collectionName1, function(err, coll) {
                if (err) { throw err; }
                coll1 = coll;
                db.createCollection(collectionName2, function(err, coll) {
                  if (err) { throw err; }
                  coll2 = coll;
                  db.createCollection(collectionName3, function(err, coll) {
                    if (err) { throw err; }
                    coll3 = coll;
                    db.createCollection('conflicts', function(err, coll) { // default conflict collection
                      if (err) { throw err; }
                      conflictColl = coll;
                      done();
                    });
                  });
                });
              });
            });
          });
        });
      });
    });
  });
});

// test with empty collection and empty leveldb. after pdb is spawned, save new object in collection and update
tasks.push(function(done) {
  var opts = {
    onSpawn: function(child) {
      // give some time to setup
      setTimeout(function() {
        var authReq = {
          username: 'someClient',
          password: 'somepass',
          db: 'test1_pdb_source_mongo'
        };
        var dataReq = {
          start: true
        };

        var ls = new LDJSONStream({ flush: false, maxDocs: 1 });

        var i = 0;
        var client = net.createConnection(1234, function() {
          // send auth request
          client.write(JSON.stringify(authReq) + '\n');

          client.pipe(ls).once('data', function(data) {
            // expect data request
            assert.deepEqual(data, { start: true });

            // send data request
            client.write(JSON.stringify(dataReq) + '\n');

            // expect BSON data
            client.unpipe(ls);
            // push back any data in ls
            if (ls.buffer.length) {
              client.unshift(ls.buffer);
            }

            // do some inserts and an update in the mongo collection
            coll1.insert({ _id: 'foo' });
            coll1.insert({ _id: 'bar' });
            process.nextTick(function() {
              coll1.update({ _id: 'foo' }, { $set: { test: true } });
            });

            // expect data to echo back
            var versions = [];
            client.pipe(new BSONStream()).on('data', function(item) {
              versions.push(item.h.v);

              if (i === 0) {
                assert.deepEqual(Object.keys(item.m), ['_op', '_id']);
                assert.equal(item.m._id, 'foo');
                delete item.m;
                assert.deepEqual(item, { h: { id: collectionName1 + '\x01foo', v: versions[i], pa: [] }, b: {} });
              }
              if (i === 1) {
                assert.deepEqual(Object.keys(item.m), ['_op', '_id']);
                assert.equal(item.m._id, 'bar');
                delete item.m;
                assert.deepEqual(item, { h: { id: collectionName1 + '\x01bar', v: versions[i], pa: [] }, b: {} });
              }
              if (i > 1) {
                assert.deepEqual(Object.keys(item.m), ['_op', '_id']);
                assert.equal(item.m._id, 'foo');
                delete item.m;
                assert.deepEqual(item, { h: { id: collectionName1 + '\x01foo', v: versions[i], pa: [versions[i - 2]] }, b: { test: true } });
                client.end();
              }
              i++;
            });
          });

          client.on('close', function(err) {
            assert(!err);
            assert.strictEqual(i, 3);
            child.kill();
          });
        });
      }, 1200);
    },
    onExit: done,
    testStdout: function(stdout) {
      assert(/TCP server bound 127.0.0.1:1234/.test(stdout));
      assert(/client connected 127.0.0.1-/.test(stdout));
    }
  };
  spawn([__dirname + '/../../../bin/pdb', __dirname + '/test1_pdb_source_mongo.hjson'], opts);
});

// test with non-empty collection and empty leveldb. save new object in collection, update and then spawn db
tasks.push(function(done) {
  var opts = {
    onSpawn: function(child) {
      // give some time to setup
      setTimeout(function() {
        var authReq = {
          username: 'someClient',
          password: 'somepass',
          db: 'test2_pdb_source_mongo'
        };
        var dataReq = {
          start: true
        };

        var ls = new LDJSONStream({ flush: false, maxDocs: 1 });

        var client = net.createConnection(1234, function() {
          // send auth request
          client.write(JSON.stringify(authReq) + '\n');

          client.pipe(ls).once('data', function(data) {
            // expect data request
            assert.deepEqual(data, { start: true });

            // send data request
            client.write(JSON.stringify(dataReq) + '\n');

            // expect BSON data
            client.unpipe(ls);
            // push back any data in ls
            if (ls.buffer.length) {
              client.unshift(ls.buffer);
            }

            // expect data to echo back
            var i = 0;
            client.on('readable', function() {
              var item = client.read();
              if (!item) { return; }

              item = BSON.deserialize(item);
              if (i === 0) {
                assert.deepEqual(Object.keys(item.m), ['_op', '_id']);
                assert.equal(item.m._id, 'bar');
                delete item.m;
                assert.deepEqual(item, { h: { id: collectionName2 + '\x01bar', v: item.h.v, pa: [] }, b: { } });
              }
              if (i > 0) {
                assert.deepEqual(Object.keys(item.m), ['_op', '_id']);
                assert.equal(item.m._id, 'foo');
                delete item.m;
                assert.deepEqual(item, { h: { id: collectionName2 + '\x01foo', v: item.h.v, pa: [] }, b: { test: true } });
                client.end();
              }
              i++;
            });

            client.on('close', function(err) {
              assert(!err);
              assert.strictEqual(i, 2);
              child.kill();
            });
          });
        });
      }, 1200);
    },
    onExit: done,
    echoOut: false,
    testStdout: function(stdout) {
      assert(/TCP server bound 127.0.0.1:1234/.test(stdout));
      assert(/client connected 127.0.0.1-/.test(stdout));
    }
  };

  // do some inserts and an update in the mongo collection
  coll2.insert({ _id: 'foo' }, function(err) {
    if (err) { throw err; }
    coll2.insert({ _id: 'bar' }, function(err) {
      if (err) { throw err; }
      coll2.update({ _id: 'foo' }, { $set: { test: true } }, function(err) {
        if (err) { throw err; }
        spawn([__dirname + '/../../../bin/pdb', __dirname + '/test2_pdb_source_mongo.hjson'], opts);
      });
    });
  });
});

// test with empty collection and empty leveldb. spawn db, then save new objects in level via remote
// because the merge handler only passes locally confirmed objects, the fast-forward from foo Aaaa to Bbbb
// should be saved in conflicts.
tasks.push(function(done) {
  var opts = {
    onSpawn: function(child) {
      // give some time to setup
      setTimeout(function() {
        var authReq = {
          username: 'someClient',
          password: 'somepass',
          db: 'test3_pdb_source_mongo'
        };
        var dataReq = {
          start: true
        };

        var ls = new LDJSONStream({ flush: false, maxDocs: 1 });

        var client = net.createConnection(1234, function() {
          // send auth request
          client.write(JSON.stringify(authReq) + '\n');

          client.pipe(ls).once('data', function(data) {
            // expect data request
            assert.deepEqual(data, { start: true });

            // send data request
            client.write(JSON.stringify(dataReq) + '\n');

            // expect BSON data
            client.unpipe(ls);
            // push back any data in ls
            if (ls.buffer.length) {
              client.unshift(ls.buffer);
            }

            // write some objects
            client.write(BSON.serialize({ h: { id: collectionName3 + '\x01foo', v: 'Aaaa', pa: [] },       b: { } }));
            client.write(BSON.serialize({ h: { id: collectionName3 + '\x01bar', v: 'Xxxx', pa: [] },       b: { } }));
            client.write(BSON.serialize({ h: { id: collectionName3 + '\x01foo', v: 'Bbbb', pa: ['Aaaa'] }, b: { test: true } }));

            // give the process some time
            setTimeout(function() {
              child.kill();
            }, 800);
          });
        });
      }, 1200);
    },
    onExit: function(err) {
      if (err) { throw err; }

      // inspect leveldb
      // open and search in database if items are written, stage is cleared up to Bbbb, versions are correct etc.
      level(dbroot + '/test3_pdb_source_mongo/data', { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, db) {
        if (err) { throw err; }

        var pe = 'someClient'; // from config file
        var mt = new MergeTree(db, {
          perspectives: [pe],
          vSize: 3,
          log: silence
        });

        // inspect perspective tree
        var i = 0;
        mt._pe[pe].createReadStream().on('data', function(item) {
          i++;
          switch (i) {
          case 1:
            assert.deepEqual(item, { h: { id: collectionName3 + '\x01foo', v: 'Aaaa', pa: [], pe: pe , i: 1 }, b: { } });
            break;
          case 2:
            assert.deepEqual(item, { h: { id: collectionName3 + '\x01bar', v: 'Xxxx', pa: [], pe: pe , i: 2 }, b: { } });
            break;
          case 3:
            assert.deepEqual(item, { h: { id: collectionName3 + '\x01foo', v: 'Bbbb', pa: ['Aaaa'], pe: pe, i: 3 }, b: { test: true } });
            break;
          }
        }).on('end', function() {
          assert.strictEqual(i, 3);

          // inspect local tree
          i = 0;
          mt.getLocalTree().createReadStream().on('data', function(item) {
            i++;

            switch (i) {
            case 1:
              assert.deepEqual(Object.keys(item.m), ['_op', '_id']);
              assert.equal(item.m._id, 'foo');
              delete item.m;
              assert.deepEqual(item, { h: { id: collectionName3 + '\x01foo', v: 'Aaaa', pa: [], pe: pe, i: 1 }, b: { } });
              break;
            case 2:
              assert.deepEqual(Object.keys(item.m), ['_op', '_id']);
              assert.equal(item.m._id, 'bar');
              delete item.m;
              assert.deepEqual(item, { h: { id: collectionName3 + '\x01bar', v: 'Xxxx', pa: [], pe: pe, i: 2 }, b: { } });
              break;
            }
          }).on('end', function() {
            assert.strictEqual(i, 2);

            db.close();

            // inspect the mongodb collection
            coll3.find({}, { sort: { _id: 1 } }).toArray(function(err, items) {
              if (err) { throw err; }

              assert.strictEqual(items.length, 2);
              assert.deepEqual(items[0], { _id: 'bar' });
              assert.deepEqual(items[1], { _id: 'foo' });

              // inspect the conflict collection, should only contain Bbbb (since the mongo adapter got that before confirming Aaaa so the merge handler sent { n: Bbbb, l: null }
              conflictColl.find({}, { sort: { _id: 1 } }).toArray(function(err, items) {
                if (err) { throw err; }

                assert.strictEqual(items.length, 1);
                delete items[0]._id; // delete object id
                assert.deepEqual(items, [{
                  n: { h: { id: collectionName3 + '\x01foo', v: 'Bbbb', pa: ['Aaaa'], pe: pe }, b: { test: true } },
                  l: null,
                  lcas: [],
                  pe: pe,
                  c: null,
                  err: 'no item in collection expected'
                }]);
                done();
              });
            });
          });
        });
      });
    },
    echoOut: false,
    testStdout: function(stdout) {
      assert(/TCP server bound 127.0.0.1:1234/.test(stdout));
      assert(/client connected 127.0.0.1-/.test(stdout));
    }
  };

  spawn([__dirname + '/../../../bin/pdb', __dirname + '/test3_pdb_source_mongo.hjson'], opts);
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
      db.close(function(err) {
        if (err) { throw err; }
        // remove any db
        rimraf(dbroot, function(err) {
          if (err) { console.error(err); }
        });
      });
    });
  });
});
