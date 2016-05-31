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
var fs = require('fs');
var net = require('net');

var async = require('async');
var bson = require('bson');
var LDJSONStream = require('ld-jsonstream');
var MongoClient = require('mongodb').MongoClient;
var rimraf = require('rimraf');

var spawn = require('../../lib/spawn');
var logger = require('../../../lib/logger');

var BSON = new bson.BSONPure.BSON();

// must match with the hjson config files
var collectionName1 = 'test1';
var collectionName2 = 'test2';
var collectionName3 = 'test3';

var tasks = [];
var tasks2 = [];

var db, coll1, coll2, coll3;
var cons, silence;
var chroot = '/var/persdb';
var dbPath1 = '/test1_persdb_mongo_root'; // match with config files
var dbPath2 = '/test2_persdb_mongo_root'; // match with config files
var dbPath3 = '/test3_persdb_mongo_root'; // match with config files

// open loggers and setup db connection
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
        rimraf(chroot + dbPath1, function(err) {
          if (err) { throw err; }
          rimraf(chroot + dbPath2, function(err) {
            if (err) { throw err; }
            rimraf(chroot + dbPath3, function(err) {
              if (err) { throw err; }

              MongoClient.connect('mongodb://127.0.0.1:27019/pdb', function(err, dbc) {
                if (err) { throw err; }

                db = dbc;

                coll1 = db.collection('test1');
                coll2 = db.collection('test2');
                coll3 = db.collection('test3');

                // ensure empty collections
                coll1.remove(function(err) {
                  if (err) { throw err; }
                  coll2.remove(function(err) {
                    if (err) { throw err; }
                    coll3.remove(function(err) {
                      if (err) { throw err; }
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
          db: 'someDb'
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

            // do some inserts and an update in the mongo collection
            coll1.insert({ _id: 'foo' });
            coll1.insert({ _id: 'bar' });
            process.nextTick(function() {
              coll1.update({ _id: 'foo' }, { $set: { test: true } });
            });

            // expect data to echo back
            var i = 0;
            var vs = [];
            client.on('readable', function() {
              var item = client.read();
              if (!item) { return; }

              item = BSON.deserialize(item);
              vs.push(item.h.v);
              if (i === 0) {
                assert.deepEqual(item, { h: { id: collectionName1 + '\x01foo', v: vs[i], pa: [] }, b: { _id: 'foo' } });
              }
              if (i === 1) {
                assert.deepEqual(item, { h: { id: collectionName1 + '\x01bar', v: vs[i], pa: [] }, b: { _id: 'bar' } });
              }
              if (i > 1) {
                assert.deepEqual(item, { h: { id: collectionName1 + '\x01foo', v: vs[i], pa: [vs[i - 2]] }, b: { _id: 'foo', test: true } });
                client.end();
              }
              i++;
            });

            client.on('close', function(err) {
              assert(!err);
              assert.strictEqual(i, 3);
              child.kill();
            });
          });
        });
      }, 1000);
    },
    onExit: done,
    echoOut: false,
    testStdout: function(stdout) {
      assert(/TCP server bound 127.0.0.1:1234/.test(stdout));
      assert(/client connected 127.0.0.1-/.test(stdout));
    }
  };
  spawn([__dirname + '/../../../bin/persdb', __dirname + '/test1_persdb_source_mongo.hjson'], opts);
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
          db: 'someDb'
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
                assert.deepEqual(item, { h: { id: collectionName2 + '\x01bar', v: item.h.v, pa: [] }, b: { _id: 'bar' } });
              }
              if (i > 0) {
                assert.deepEqual(item, { h: { id: collectionName2 + '\x01foo', v: item.h.v, pa: [] }, b: { _id: 'foo', test: true } });
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
      }, 1000);
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
        spawn([__dirname + '/../../../bin/persdb', __dirname + '/test2_persdb_source_mongo.hjson'], opts);
      });
    });
  });
});

// test with empty collection and empty leveldb. spawn db, then save new objects in level via remote
tasks.push(function(done) {
  var opts = {
    onSpawn: function(child) {
      // give some time to setup
      setTimeout(function() {
        var authReq = {
          username: 'someClient',
          password: 'somepass',
          db: 'someDb'
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
            client.write(BSON.serialize({ h: { id: collectionName3 + '\x01foo', v: 'Aaaa', pa: [] },       b: { _id: 'foo' } }));
            client.write(BSON.serialize({ h: { id: collectionName3 + '\x01bar', v: 'Xxxx', pa: [] },       b: { _id: 'bar' } }));
            client.write(BSON.serialize({ h: { id: collectionName3 + '\x01foo', v: 'Bbbb', pa: ['Aaaa'] }, b: { _id: 'foo', test: true } }));

            // wait some time and inspect database
            setTimeout(function() {
              coll3.find({}, { sort: { _id: 1 } }).toArray(function(err, items) {
                if (err) { throw err; }

                assert.strictEqual(items.length, 2);
                assert.deepEqual(items[0], { _id: 'bar' });
                assert.deepEqual(items[1], { _id: 'foo', test: true });
                child.kill();
              });
            }, 800);
          });
        });
      }, 1000);
    },
    onExit: done,
    echoOut: false,
    testStdout: function(stdout) {
      assert(/TCP server bound 127.0.0.1:1234/.test(stdout));
      assert(/client connected 127.0.0.1-/.test(stdout));
    }
  };

  spawn([__dirname + '/../../../bin/persdb', __dirname + '/test3_persdb_source_mongo.hjson'], opts);
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
        // remove any pre-existing dbPath
        rimraf(chroot + dbPath1, function(err) {
          if (err) { throw err; }
          rimraf(chroot + dbPath2, function(err) {
            if (err) { throw err; }
            rimraf(chroot + dbPath3, function(err) {
              if (err) { console.error(err); }
            });
          });
        });
      });
    });
  });
});
