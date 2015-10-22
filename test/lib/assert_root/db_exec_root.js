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
var fs = require('fs');
var tmpdir = require('os').tmpdir;
var childProcess = require('child_process');

var async = require('async');
var BSONStream = require('bson-stream');
var LDJSONStream = require('ld-jsonstream');
var rimraf = require('rimraf');
var bson = require('bson');
var BSON = new bson.BSONPure.BSON();
var Timestamp = {};

var logger = require('../../../lib/logger');

var tasks = [];
var tasks2 = [];

// print line number
function lnr() {
  return new Error().stack.split('\n')[2].match(/db_exec_root.js:([0-9]+):[0-9]+/)[1];
}

var logger = require('../../../lib/logger');

var db, cons, silence;
var chroot = '/var/persdb';
var user = 'nobody';
var group = 'nobody';
var dbPath = '/test_db_exec_root';

// open database
tasks2.push(function(done) {
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
        rimraf(chroot + dbPath, done);
      });
    });
  });
});

// should require user to be an existing username
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert(/user id does not exist/.test(stderr));
    assert.strictEqual(code, 3);
    assert.strictEqual(sig, null);
    done();
  });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true },
        path: dbPath,
        name: 'test_vce_root',
        user: 'test',
        chroot: chroot
      });
      break;
    default:
      console.error(msg);
      throw new Error('unknown state');
    }
  });
});

// should require chroot to be a valid path
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert(/path creation failed /.test(stderr));
    assert.strictEqual(code, 5);
    assert.strictEqual(sig, null);
    done();
  });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true },
        path: dbPath,
        name: 'test_vce_root',
        user: user,
        chroot: '/some'
      });
      break;
    default:
      console.error(msg);
      throw new Error('unknown state');
    }
  });
});

// should accept writable stream for log files (regression)
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var logFile = fs.createWriteStream(tmpdir() + 'vce-test.log', { flags: 'a' });

  logFile.on('open', function() {
    var child = childProcess.spawn(process.execPath, [__dirname + '/../../../lib/db_exec'], {
      cwd: '/',
      env: {},
      stdio: ['pipe', 'pipe', 'pipe', 'ipc', logFile]
    });

    //child.stdout.pipe(process.stdout);
    //child.stderr.pipe(process.stderr);

    var stderr = '';
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', function(data) { stderr += data; });

    child.on('exit', function(code, sig) {
      assert.strictEqual(stderr.length, 0);
      assert.strictEqual(code, 0);
      assert.strictEqual(sig, null);
      done();
    });

    // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
    child.on('message', function(msg) {
      switch(msg) {
      case 'init':
        child.send({
          log: { console: true },
          path: dbPath,
          name: 'test_vce_root',
        });
        break;
      case 'listen':
        child.kill();
        break;
      default:
        console.error(msg);
        throw new Error('unknown state');
      }
    });
  });
});

// should fail if hooks not found
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert(/error loading hooks: "foo,bar" /.test(stderr));
    assert.strictEqual(code, 2);
    assert.strictEqual(sig, null);
    done();
  });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true },
        path: dbPath,
        hookPaths: ['foo', 'bar'],
        name: 'test_vce_root',
        user: user,
        chroot: chroot
      });
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

// should fail with a valid configuration but non-existing hook paths
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert(/error loading hooks: "\/some"/.test(stderr));
    assert.strictEqual(code, 2);
    assert.strictEqual(sig, null);
    done();
  });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true },
        path: dbPath,
        hookPaths: ['/some'],
        name: 'test_vce_root',
        user: user,
        chroot: chroot
      });
      break;
    case 'listen':
      child.kill();
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

// should not fail with valid configurations (include existing hook paths)
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true },
        path: dbPath,
        hookPaths: ['/var/empty'],
        name: 'test_vce_root',
        user: user,
        chroot: chroot
      });
      break;
    case 'listen':
      child.kill();
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

// should pass through a pull request
/*
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

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
      conn.end();
      server.close(function() {
        child.kill();
      });
    });
  });
  server.listen(port, host);

  child.on('exit', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true },
        path: dbPath,
        name: 'test_vce_root',
        user: user,
        chroot: chroot
      });
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
    default:
      throw new Error('unknown state');
    }
  });
});
*/

// should save valid incoming BSON data following a pull request
/*
tasks.push(function(done) {
  console.log('test #%d', lnr());

  // then fork a vce
  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

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

          conn.end();
          server.close(function() {
            child.kill();
          });
        });
      }, 200);
    });
  });
  server.listen(port, host);

  var vcCfg = {
    log: { console: true },
    path: dbPath,
    name: 'test_vce_root',
    debug: false,
    user: user,
    chroot: chroot,
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

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send(vcCfg);
      break;
    case 'listen':
      // send pr
      child.send(pr);
      break;
    default:
      throw new Error('unknown state');
    }
  });
});
*/

// should send auth response following a stripped auth request
tasks.push(function(done) {
  console.log('test #%d', lnr());

  // then fork a vce
  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  // start an echo server that can receive auth responses and BSON data
  var host = '127.0.0.1';
  var port = 1234;

  var authResponseReceived;

  // start server to check if data is sent by db_exec
  var server = net.createServer(function(conn) {
    // expect first message to be an auth response (line delimited json) 
    conn.pipe(new LDJSONStream({ maxBytes: 64 })).on('data', function(item) {
      assert.deepEqual(item, { start: true });
      authResponseReceived = true;
      conn.end();
      server.close(function() {
        child.kill();
      });
    });
  });
  server.listen(port, host);

  child.on('exit', function(code, sig) {
    assert(authResponseReceived);
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG2 },
        path: dbPath,
        name: 'test_vce_root',
        user: user,
        chroot: chroot,
        perspectives: [{
          name: 'webclient',
          import: true,
          export: true
        }]
      });
      break;
    case 'listen':
      // send stripped auth request
      var s = net.createConnection(port, host, function() {
        child.send({
          username: 'webclient'
        }, s);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

// should send auth response and accept incoming bson data
tasks2.push(function(done) {
  console.log('test #%d', lnr());

  // then fork a vce
  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  // start an echo server that can receive auth responses and BSON data
  var host = '127.0.0.1';
  var port = 1234;

  var bsonSent;

  // start server to check if data is sent by db_exec
  var server = net.createServer(function(conn) {
    // expect first message to be an auth response (line delimited json), then send a valid BSON object
    conn.pipe(new LDJSONStream({ maxBytes: 64 })).on('data', function(item) {
      assert.deepEqual(item, { start: true });

      // expect first message to be an auth response (line delimited json), then send a valid BSON object
      conn.write(BSON.serialize({
        h: { id: 'XI', v: 'Aaaa', pa: [] },
        b: { some: 'attr' }
      }), function(err) {
        if (err) { throw err; }
        bsonSent = true;
        conn.end();
        server.close(function() {
          child.kill();
        });
      });
    });
  });
  server.listen(port, host);

  child.on('exit', function(code, sig) {
    assert(bsonSent);
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG2 },
        path: dbPath,
        name: 'test_vce_root',
        user: user,
        chroot: chroot,
        perspectives: [{
          name: 'webclient',
          import: true,
          export: true
        }],
        mergeTree: {
          vSize: 3
        }
      });
      break;
    case 'listen':
      // send stripped auth request followed by a BSON object
      var s = net.createConnection(port, host, function() {
        child.send({
          username: 'webclient'
        }, s);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

// should send auth response and the BSON data following a stripped auth request
tasks.push(function(done) {
  console.log('test #%d', lnr());

  // then fork a vce
  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

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

      conn.end();
      server.close(function() {
        child.kill();
      });
    });
  });
  server.listen(port, host);

  var vcCfg = {
    log: { console: true },
    path: dbPath,
    name: 'test_vce_root',
    debug: false,
    user: user,
    chroot: chroot,
    autoProcessInterval: 50,
    size: 1
  };

  // push request
  var pr = {
  };

  child.on('exit', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send(vcCfg);
      break;
    case 'listen':
      // send pr
      //var s = net.createConnection(port, host
      var s = net.createConnection(port, host, function() {
        child.send(pr, s);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

// should disconnect if requested hooks in push request can not be loaded
tasks.push(function(done) {
  console.log('test #%d', lnr());

  // then fork a vce
  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  // start an echo server that can receive auth requests and sends some BSON data
  var host = '127.0.0.1';
  var port = 1234;

  // start server to check if pull request with unloaded hook is disconnected by vcexec
  var server = net.createServer(function(conn) {
    conn.on('error', done);
    conn.on('close', function() {
      server.close(function() {
        child.kill();
      });
    });
  });
  server.listen(port, host);

  var vcCfg = {
    log: { console: true },
    path: dbPath,
    name: 'test_vce_root',
    debug: false,
    size: 1
  };

  child.on('exit', function(code, sig) {
    assert(/Error: hook requested that is not loaded/.test(stderr));
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send(vcCfg);
      break;
    case 'listen':
      // send pr
      var s = net.createConnection(port, host, function() {
        child.send({ hooks: ['a'] }, s);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

// should insert some dummies in the collection to version on the server side
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var coll = dbHookPush.collection('m3.test');
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

// should run export hooks of push request
tasks.push(function(done) {
  console.log('test #%d', lnr());

  // then fork a vce
  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

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
      server.close(function() {
        child.kill();
      });
    });
  });
  server.listen(port, host);

  var vcCfg = {
    log: { console: true },
    path: dbPath,
    hookPaths: ['hooks'],
    name: 'test_vce_root_hook_push',
    debug: true,
    autoProcessInterval: 50,
    size: 1
  };

  child.on('exit', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send(vcCfg);
      break;
    case 'listen':
      // send pr
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
    default:
      throw new Error('unknown state');
    }
  });
});

// should run import hooks of pull request
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  var host = '127.0.0.1';
  var port = 1234;

  // start server to check if pull request is sent by vcexec, and to send some data that should be run through the hooks
  var server = net.createServer(function(conn) {
    conn.on('data', function(data) {
      assert.deepEqual(JSON.parse(data), {
        username: 'foo',
        password: 'bar',
        database: 'baz',
        collection: 'qux'
      });

      // send some data
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

      conn.write(BSON.serialize(item1));
      conn.write(BSON.serialize(item2));
      conn.write(BSON.serialize(item3));
      conn.end();
    });
  });
  server.listen(port, host);

  child.stdout.setEncoding('utf8');
  child.stdout.on('data', function(data) {
    // wait till the last item is synced
    if (/_syncLocalHeadsWithCollection synced .*key3.*_local.*_i":3/.test(data)) {
      dbHookPull.collection('m3.test').find().toArray(function(err, items) {
        if (err) { throw err; }

        assert.strictEqual(items.length, 6);
        assert.deepEqual(items[0], {
          _id: { _co: 'test', _id: 'key1', _v: 'A', _pe: 'baz', _pa: [] },
          _m3: { _op: new Timestamp(0, 0), _ack: false }, // oplog reader is not connected so _m3._ack is never updated
          foo: 'bar',
          // someKey: 'someVal', this key should be stripped because of hook
          someOtherKey: 'B'
        });
        assert.deepEqual(items[1], {
          _id: { _co: 'test', _id: 'key2', _v: 'A', _pe: 'baz', _pa: [] },
          _m3: { _op: new Timestamp(0, 0), _ack: false }, // oplog reader is not connected so _m3._ack is never update
          foo: 'baz',
          someKey: 'someVal'
        });
        assert.deepEqual(items[2], {
          _id: { _co: 'test', _id: 'key3', _v: 'A', _pe: 'baz', _pa: [] },
          _m3: { _op: new Timestamp(0, 0), _ack: false }, // oplog reader is not connected so _m3._ack is never update
          quz: 'zab',
          someKey: 'someVal'
        });
        assert.deepEqual(items[3], {
          _id: { _co: 'test', _id: 'key1', _v: 'A', _pe: '_local', _pa: [], _i: 1 },
          _m3: { _op: new Timestamp(0, 0), _ack: false }, // oplog reader is not connected so _m3._ack is never update
          foo: 'bar',
          // someKey: 'someVal', this key should be stripped because of hook
          someOtherKey: 'B'
        });
        assert.deepEqual(items[4], {
          _id: { _co: 'test', _id: 'key2', _v: 'A', _pe: '_local', _pa: [], _i: 2 },
          _m3: { _op: new Timestamp(0, 0), _ack: false }, // oplog reader is not connected so _m3._ack is never update
          foo: 'baz',
          someKey: 'someVal'
        });
        assert.deepEqual(items[5], {
          _id: { _co: 'test', _id: 'key3', _v: 'A', _pe: '_local', _pa: [], _i: 3 },
          _m3: { _op: new Timestamp(0, 0), _ack: false }, // oplog reader is not connected so _m3._ack is never update
          quz: 'zab',
          someKey: 'someVal'
        });
        server.close(function() {
          child.kill();
        });
      });
    }
  });

  child.on('exit', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG },
        path: dbPath,
        hookPaths: ['hooks'],
        name: 'test_vce_root_hook_pull',
        debug: true,
        autoProcessInterval: 50,
        user: user,
        chroot: chroot
      });
      break;
    case 'listen':
      // send pr
      child.send({
        hooks: ['strip_field_if_holds'],
        hooksOpts: {
          field: 'someKey',
          fieldFilter: { foo: 'bar' }
        },
        username: 'foo',
        password: 'bar',
        database: 'baz',
        collection: 'qux',
        host: host,
        port: port
      });
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

tasks2.push(function(done) {
  cons.close(function(err) {
    if (err) { throw err; }
    silence.close(function(err) {
      if (err) { throw err; }
      rimraf(chroot + dbPath, done);
    });
  });
});

async.series(tasks2, function(err) {
  if (err) {
    console.error(err);
  } else {
    console.log('ok');
  }
});
