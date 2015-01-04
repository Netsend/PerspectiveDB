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
var net = require('net');
var childProcess = require('child_process');

var async = require('async');
var mongodb = require('mongodb');
var BSONStream = require('bson-stream');

var BSON = mongodb.BSON;
var Timestamp = mongodb.Timestamp;

var tasks = [];
var tasks2 = [];

var db, dbHookTest;
var databaseNames = ['test_versioned_collection_exec_root', 'test_versioned_collection_exec_root_hook'];
var Database = require('../_database');

// open database connection
var database = new Database(databaseNames);
tasks.push(function(done) {
  database.connect(function(err, dbs) {
    db = dbs[0];
    dbHookTest = dbs[1];
    done(err);
  });
});

// should create capped collections
tasks.push(function(done) {
  database.createCappedColl(db, 'm3.test', function(err) {
    if (err) { throw err; }
    database.createCappedColl(dbHookTest, 'm3.test', done);
  });
});

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
    dbName: 'test_versioned_collection_exec_root',
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
    dbName: 'test_versioned_collection_exec_root',
    dbPort: 27019,
    collectionName: 'test',
    chrootUser: 'nobody',
    chrootNewRoot: '/some'
  });
});

// should fail if hooks not found
tasks.push(function(done) {
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  var stderr = '';

  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) {
    stderr += data;
  });

  child.on('exit', function(code, sig) {
    assert(/ENOENT, no such file or directory .*\/foo\//.test(stderr));
    assert.strictEqual(code, 2);
    assert.strictEqual(sig, null);
    done();
  });

  child.send({
    hookPaths: ['foo', 'bar'],
    dbName: 'test_versioned_collection_exec_root',
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

// should not fail with valid configurations
tasks.push(function(done) {
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  var buff = new Buffer(0);

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
    dbName: 'test_versioned_collection_exec_root',
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

// should not fail with valid configurations (include hooks)
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
    hookPaths: ['hooks'],
    dbName: 'test_versioned_collection_exec_root',
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

// should pass through a pull request
tasks.push(function(done) {
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  var stderr = '';

  child.stderr.setEncoding('utf8');
  child.stderr.pipe(process.stderr);
  child.stderr.on('data', function(data) {
    stderr += data;
  });

  var host = '127.0.0.1';
  var port = 1234;

  // start server to check if pull request is sent by vcexec
  var server = net.createServer(function(conn) {
    conn.on('data', function(data) {
      assert.deepEqual(JSON.parse(data), {
        username: 'foo',
        password: 'bar',
        database: 'baz',
        collection: 'qux'
      });
      server.close();
      child.kill();
    });
  });
  server.listen(port, host);

  child.on('exit', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  child.send({
    dbName: 'test_versioned_collection_exec_root',
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
      // send pr
      child.send({
        username: 'foo',
        password: 'bar',
        database: 'baz',
        collection: 'qux',
        host: host,
        port: port
      });
      break;
    }
  });
});

// should save valid incoming BSON data following a pull request
tasks.push(function(done) {
  // then fork a vce
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  // start an echo server that can receive auth requests and sends some BSON data
  var host = '127.0.0.1';
  var port = 1234;

  // start server to check if pull request is sent by vcexec
  var server = net.createServer(function(conn) {
    conn.on('data', function(data) {
      assert.deepEqual(JSON.parse(data), {
        username: 'foo',
        password: 'bar',
        database: 'baz',
        collection: 'qux'
      });

      // now send a root node
      var obj = { _id: { _id: 'abc', _v: 'def', _pa: [] } };
      conn.write(BSON.serialize(obj));

      setTimeout(function() {
        // open and search in database
        db.collection('m3.test').find().toArray(function(err, items) {
          if (err) { throw err; }

          assert.strictEqual(items.length, 2);
          assert.deepEqual(items[0], {
            _id: {
              _id: 'abc',
              _v: 'def',
              _pa: [],
              _pe: 'baz',
              _co: 'test'
            },
            _m3: {
              _ack: false,
              _op: new Timestamp(0, 0)
            }
          });
          assert.deepEqual(items[1], {
            _id: {
              _id: 'abc',
              _v: 'def',
              _pa: [],
              _pe: '_local',
              _co: 'test',
              _i: 1
            },
            _m3: {
              _ack: false,
              _op: new Timestamp(0, 0)
            }
          });

          server.close();
          child.kill();
        });
      }, 200);
    });
  });
  server.listen(port, host);

  var vcCfg = {
    dbName: 'test_versioned_collection_exec_root',
    dbPort: 27019,
    debug: false,
    collectionName: 'test',
    chrootUser: 'nobody',
    chrootNewRoot: '/var/empty',
    autoProcessInterval: 50,
    size: 1,
    remotes: ['baz'], // list PR sources
  };

  // pull request
  var pr = {
    username: 'foo',
    password: 'bar',
    database: 'baz',
    collection: 'qux',
    host: host,
    port: port
  };

  var stderr = '';

  child.stdout.setEncoding('utf8');

  child.stderr.setEncoding('utf8');
  child.stderr.pipe(process.stderr);
  child.stderr.on('data', function(data) {
    stderr += data;
  });

  child.on('exit', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  child.send(vcCfg);

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      break;
    case 'listen':
      child.send(pr);
      break;
    }
  });
});

// should send BSON data following a push request
tasks.push(function(done) {
  // then fork a vce
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  // start an echo server that can receive auth requests and sends some BSON data
  var host = '127.0.0.1';
  var port = 1234;

  // start server to check if pull request is sent by vcexec
  var server = net.createServer(function(conn) {
    var bs = conn.pipe(new BSONStream());
    bs.on('data', function(item) {
      assert.deepEqual(item, {
        _id: {
          _id: 'abc',
          _v: 'def',
          _pa: [],
          _co: 'test'
        },
        _m3: { }
      });

      server.close();
      child.kill();
    });
  });
  server.listen(port, host);

  var vcCfg = {
    dbName: 'test_versioned_collection_exec_root',
    dbPort: 27019,
    debug: false,
    collectionName: 'test',
    chrootUser: 'nobody',
    chrootNewRoot: '/var/empty',
    autoProcessInterval: 50,
    size: 1
  };

  // push request
  var pr = {
  };

  var stderr = '';

  child.stdout.setEncoding('utf8');

  child.stderr.setEncoding('utf8');
  child.stderr.pipe(process.stderr);
  child.stderr.on('data', function(data) {
    stderr += data;
  });

  child.on('exit', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  child.send(vcCfg);

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      break;
    case 'listen':
      //var s = net.createConnection(port, host
      var s = net.createConnection(port, host, function() {
        child.send(pr, s);
      });
      break;
    }
  });
});

// should disconnect if requested hooks can not be loaded
tasks.push(function(done) {
  // then fork a vce
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  // start an echo server that can receive auth requests and sends some BSON data
  var host = '127.0.0.1';
  var port = 1234;

  // start server to check if pull request with unloaded hook is disconnected by vcexec
  var server = net.createServer(function(conn) {
    conn.on('error', done);
    conn.on('close', function() {
      server.close();
      child.kill();
    });
  });
  server.listen(port, host);

  var vcCfg = {
    dbName: 'test_versioned_collection_exec_root',
    dbPort: 27019,
    debug: false,
    collectionName: 'test',
    size: 1
  };

  // push request
  var stderr = '';

  child.stdout.setEncoding('utf8');

  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) {
    stderr += data;
  });

  child.on('exit', function(code, sig) {
    assert(/Error: hook requested that is not loaded/.test(stderr.toString()));
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  child.send(vcCfg);

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      break;
    case 'listen':
      var s = net.createConnection(port, host, function() {
        child.send({ hooks: ['a'] }, s);
      });
      break;
    }
  });
});

// should insert some dummies in the collection to version on the server side
tasks.push(function(done) {
  var coll = dbHookTest.collection('m3.test');
  var item1 = {
    _id: { _co: 'someColl', _id: 'key1', _v: 'A', _pe: '_local', _pa: [], _lo: true },
    _m3: { _op: new Timestamp(1, 2), _ack: true },
    foo: 'bar',
    someKey: 'someVal',
    someOtherKey: 'B'
  };
  var item2 = {
    _id: { _co: 'someColl', _id: 'key2', _v: 'A', _pe: '_local', _pa: [], _lo: true },
    _m3: { _op: new Timestamp(2, 3), _ack: true },
    foo: 'baz',
    someKey: 'someVal'
  };
  var item3 = {
    _id: { _co: 'someColl', _id: 'key3', _v: 'A', _pe: '_local', _pa: [], _lo: true },
    _m3: { _op: new Timestamp(3, 4), _ack: true },
    quz: 'zab',
    someKey: 'someVal'
  };

  var items = [item1, item2, item3];
  coll.insert(items, done);
});

// should run export hooks
tasks.push(function(done) {
  // then fork a vce
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  // start an echo server that can receive auth requests and sends some BSON data
  var host = '127.0.0.1';
  var port = 1234;

  // start server to check if pull request with unloaded hook is disconnected by vcexec
  var expectedItems = [{
    _id: { _co: 'someColl', _id: 'key1', _v: 'A', _pa: [] },
    _m3: {},
    foo: 'bar',
    // someKey: 'someVal', this key should be stripped because of hook
    someOtherKey: 'B'
  }, {
    _id: { _co: 'someColl', _id: 'key2', _v: 'A', _pa: [] },
    _m3: {},
    foo: 'baz',
    someKey: 'someVal'
  }, {
    _id: { _co: 'someColl', _id: 'key3', _v: 'A', _pa: [] },
    _m3: {},
    quz: 'zab',
    someKey: 'someVal'
  }];
  var i = 0;
  var bs = new BSONStream();
  var server = net.createServer(function(conn) {
    conn.pipe(bs).on('data', function(obj) {
      assert.deepEqual(obj, expectedItems[i]);
      i++;
      if (i === 3) {
        conn.end();
      }
    });
    conn.on('error', done);
    conn.on('close', function() {
      server.close();
      child.kill();
    });
  });
  server.listen(port, host);

  var vcCfg = {
    hookPaths: ['hooks'],
    dbName: 'test_versioned_collection_exec_root_hook',
    dbPort: 27019,
    debug: true,
    collectionName: 'test',
    autoProcessInterval: 50,
    size: 1
  };

  // push request
  var stderr = '';

  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stderr.pipe(process.stderr);
  child.stderr.on('data', function(data) {
    stderr += data;
  });

  child.on('exit', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  child.send(vcCfg);

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      break;
    case 'listen':
      var s = net.createConnection(port, host, function() {
        var pr = {
          hooks: ['strip_field_if_holds'],
          hooksOpts: {
            field: 'someKey',
            fieldFilter: { foo: 'bar' }
          }
        };
        child.send(pr, s);
      });
      break;
    }
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
