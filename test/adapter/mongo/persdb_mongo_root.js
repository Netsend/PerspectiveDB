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
var BSONStream = require('bson-stream');
var LDJSONStream = require('ld-jsonstream');
var level = require('level-packager')(require('leveldown'));
var MongoClient = require('mongodb').MongoClient;
var rimraf = require('rimraf');

var spawn = require('../../lib/spawn');
var logger = require('../../../lib/logger');
var MergeTree = require('../../../lib/merge_tree');

var BSON = new bson.BSONPure.BSON();

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
tasks2.push(function(done) {
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
                assert.deepEqual(item, { h: { id: 'foo', v: vs[i], pa: [] }, b: { _id: 'foo' } });
              }
              if (i === 1) {
                assert.deepEqual(item, { h: { id: 'bar', v: vs[i], pa: [] }, b: { _id: 'bar' } });
              }
              if (i > 1) {
                assert.deepEqual(item, { h: { id: 'foo', v: vs[i], pa: [vs[i - 2]] }, b: { _id: 'foo', test: true } });
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
    echoOut: true,
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
                assert.deepEqual(item, { h: { id: 'bar', v: item.h.v, pa: [] }, b: { _id: 'bar' } });
              }
              if (i > 0) {
                assert.deepEqual(item, { h: { id: 'foo', v: item.h.v, pa: [] }, b: { _id: 'foo', test: true } });
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
    echoOut: true,
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

/*
// should try to start a WSS server and if this fails because of missing attributes (key, cert, dhparam) automatically shutdown
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = spawn(__dirname + '/../../../bin/persdb', [__dirname + '/test_persdb_wss_wrong_config.hjson']);

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    //assert.strictEqual(stderr.length, 0);
    assert(/msg.key must be a non-empty string/.test(stderr));
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });
});

// should start a WSS server, test by connecting with wss client
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = spawn(__dirname + '/../../../bin/persdb', [__dirname + '/test_persdb_wss_export.hjson']);

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert(/-127.0.0.1-1235: end \(no binary data received\)/.test(stdout));
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  setTimeout(function() {
    ws.connect('wss://localhost:1235', wsClientOpts, function() {
      child.kill();
    });
  }, 1000);
});

// should start a WSS server, and disconnect because of empty data request
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = spawn(__dirname + '/../../../bin/persdb', [__dirname + '/test_persdb_wss_export.hjson']);

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert(/-127.0.0.1-1234 invalid auth request/.test(stderr));
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  setTimeout(function() {
    var authReq = {};

    var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
      client.on('close', function() {
        child.kill();
      });
      client.sendText(JSON.stringify(authReq) + '\n');
    });
  }, 1000);
});

// should start a WSS server, and disconnect because db name does not exist in config
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = spawn(__dirname + '/../../../bin/persdb', [__dirname + '/test_persdb_wss_export.hjson']);

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert(/_verifyAuthRequest no config found for db: baz invalid credentials/.test(stderr));
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  setTimeout(function() {
    var authReq = {
      db: 'baz',
      username: 'foo',
      password: 'bar'
    };

    var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
      client.on('close', function() {
        child.kill();
      });
      client.sendText(JSON.stringify(authReq) + '\n');
    });
  }, 1000);
});

// should start a WSS server, and disconnect because username does not exist in config
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = spawn(__dirname + '/../../../bin/persdb', [__dirname + '/test_persdb_wss_export.hjson']);

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert(/_verifyAuthRequest no users or import \/ export config found someDb invalid credentials/.test(stderr));
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  setTimeout(function() {
    var authReq = {
      username: 'foo',
      password: 'bar',
      db: 'someDb'
    };

    var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
      client.on('close', function() {
        child.kill();
      });
      client.sendText(JSON.stringify(authReq) + '\n');
    });
  }, 1000);
});

// should start a WSS server, and disconnect because password is incorrect
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = spawn(__dirname + '/../../../bin/persdb', [__dirname + '/test_persdb_wss_export.hjson']);

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert(/_verifyAuthRequest someDb someClient invalid credentials/.test(stderr));
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  setTimeout(function() {
    var authReq = {
      username: 'someClient',
      password: 'bar',
      db: 'someDb'
    };

    var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
      client.on('close', function() {
        child.kill();
      });
      client.sendText(JSON.stringify(authReq) + '\n');
    });
  }, 1000);
});

// should start a WSS server, and send a data request that signals no data is expected (no import key in config)
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = spawn(__dirname + '/../../../bin/persdb', [__dirname + '/test_persdb_wss_export.hjson']);

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert(/successfully authenticated "someClient"/.test(stdout));
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  setTimeout(function() {
    var authReq = {
      username: 'someClient',
      password: 'somepass',
      db: 'someDb'
    };

    var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
      client.sendText(JSON.stringify(authReq) + '\n');
      // expect data request
      client.on('text', function(data) {
        assert.deepEqual(data, JSON.stringify({ start: false }) + '\n');
        client.on('close', function(code, reason) {
          assert(code, 9823);
          assert(reason, 'test');
          child.kill();
        });
        client.close(9823, 'test');
      });
    });
  }, 1000);
});

// should start a WSS server, and send a data request that signals data is expected (import true)
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = spawn(__dirname + '/../../../bin/persdb', [__dirname + '/test_persdb_wss_import.hjson']);

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert(/successfully authenticated "someClient"/.test(stdout));
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  setTimeout(function() {
    var authReq = {
      username: 'someClient',
      password: 'somepass',
      db: 'someDb'
    };

    var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
      client.sendText(JSON.stringify(authReq) + '\n');
      // expect data request
      client.on('text', function(data) {
        assert.deepEqual(data, JSON.stringify({ start: true }) + '\n');
        client.on('close', function(code, reason) {
          assert(code, 9823);
          assert(reason, 'test');
          child.kill();
        });
        client.close(9823, 'test');
      });
    });
  }, 1000);
});

// should start a WSS server, and accept a data request + two BSON items
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = spawn(__dirname + '/../../../bin/persdb', [__dirname + '/test_persdb_wss_import.hjson']);

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.on('data', function(data) { stderr += data; });

  var pe = 'someClient';
  var vSize = 3;

  child.on('exit', function(code, sig) {
    // open and search in database if item is written
    level('/var/persdb' + dbPath, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, db) {
      if (err) { throw err; }

      var mt = new MergeTree(db, {
        perspectives: [pe],
        vSize: vSize,
        log: silence
      });
      var rs = mt._pe[pe].createReadStream();
      var i = 0;
      rs.on('data', function(item) {
        i++;
        if (i === 1) {
          assert.deepEqual(item, { h: { id: 'abc', v: 'Aaaa', pe: pe, i: 1, pa: [] }, b: { some: true } });
        }
        if (i === 2) {
          assert.deepEqual(item, { h: { id: 'abc', v: 'Bbbb', pe: pe, i: 2, pa: ['Aaaa'] }, b: { some: 'other' } });
        }
      });

      rs.on('end', function() {
        assert.strictEqual(stderr.length, 0);
        assert.strictEqual(code, 0);
        assert.strictEqual(sig, null);
        assert.strictEqual(i, 2);
        mt.mergeWithLocal(mt._pe[pe], function(err) {
          if (err) { throw err; }
          db.close(done);
        });
      });
    });
  });

  setTimeout(function() {
    var authReq = {
      username: pe,
      password: 'somepass',
      db: 'someDb'
    };
    var dataReq = {
      start: true
    };

    var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
      client.sendText(JSON.stringify(authReq) + '\n');

      // expect data request
      client.on('text', function(data) {
        assert.deepEqual(data, JSON.stringify({ start: true }) + '\n');

        client.sendText(JSON.stringify(dataReq) + '\n');

        // now send a root node and a child
        var obj1 = { h: { id: 'abc', v: 'Aaaa', pa: [] }, b: { some: true } };
        var obj2 = { h: { id: 'abc', v: 'Bbbb', pa: ['Aaaa'] }, b: { some: 'other' } };

        client.sendBinary(BSON.serialize(obj1));
        client.sendBinary(BSON.serialize(obj2));

        client.on('close', function(code, reason) {
          assert(code, 9823);
          assert(reason, 'test');
          child.kill();
        });
        setTimeout(function() {
          client.close(9823, 'test');
        }, 100);
      });
    });
  }, 1000);
});

// should start a WSS server, and accept a data request + one BSON item, and send previously saved items (depends on previous test)
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = spawn(__dirname + '/../../../bin/persdb', [__dirname + '/test_persdb_wss_import_export.hjson']);

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.on('data', function(data) { stderr += data; });

  var pe = 'someClient';
  var vSize = 3;

  child.on('exit', function(code, sig) {
    // open and search in database if item is written
    level('/var/persdb' + dbPath, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, db) {
      if (err) { throw err; }

      var mt = new MergeTree(db, {
        perspectives: [pe],
        vSize: vSize,
        log: silence
      });
      var rs = mt._pe[pe].createReadStream();
      var i = 0;
      rs.on('data', function(item) {
        i++;
        if (i === 1) {
          assert.deepEqual(item, { h: { id: 'abc', v: 'Aaaa', pe: pe, i: 1, pa: [] }, b: { some: true } });
        }
        if (i === 2) {
          assert.deepEqual(item, { h: { id: 'abc', v: 'Bbbb', pe: pe, i: 2, pa: ['Aaaa'] }, b: { some: 'other' } });
        }
        if (i > 2) {
          assert.deepEqual(item, { h: { id: 'abc', v: 'Cccc', pe: pe, i: 3, pa: ['Bbbb'] }, b: { some: 'new' } });
        }
      });

      rs.on('end', function() {
        assert.strictEqual(stderr.length, 0);
        assert.strictEqual(code, 0);
        assert.strictEqual(sig, null);
        assert.strictEqual(i, 3);
        mt.mergeWithLocal(mt._pe[pe], function(err) {
          if (err) { throw err; }
          db.close(done);
        });
      });
    });
  });

  setTimeout(function() {
    var authReq = {
      username: pe,
      password: 'somepass',
      db: 'someDb'
    };
    var dataReq = {
      start: true
    };

    var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
      client.sendText(JSON.stringify(authReq) + '\n');

      // expect data request
      client.on('text', function(data) {
        assert.deepEqual(data, JSON.stringify({ start: 'Bbbb' }) + '\n');

        client.sendText(JSON.stringify(dataReq) + '\n');

        // now send a new child
        var obj = { h: { id: 'abc', v: 'Cccc', pa: ['Bbbb'] }, b: { some: 'new' } };

        client.sendBinary(BSON.serialize(obj));

        var i = 0;
        client.on('binary', function(rs) {
          rs.on('readable', function() {
            i++;
            var item = BSON.deserialize(rs.read());
            if (i === 1) {
              assert.deepEqual(item, { h: { id: 'abc', v: 'Aaaa', pa: [] }, b: { some: true } });
            }
            if (i === 2) {
              assert.deepEqual(item, { h: { id: 'abc', v: 'Bbbb', pa: ['Aaaa'] }, b: { some: 'other' } });
            }
            if (i > 2) {
              assert.deepEqual(item, { h: { id: 'abc', v: 'Cccc', pa: ['Bbbb'] }, b: { some: 'new' } });
              client.close(9823, 'test');
            }
          });
        });

        client.on('close', function(code, reason) {
          assert(code, 9823);
          assert(reason, 'test');
          assert.strictEqual(i, 3);
          child.kill();
        });
      });
    });
  }, 1000);
});
*/

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
