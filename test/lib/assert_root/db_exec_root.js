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
var level = require('level');
var rimraf = require('rimraf');
var bson = require('bson');

var BSON = new bson.BSONPure.BSON();
var Timestamp = {};

var logger = require('../../../lib/logger');
var MergeTree = require('../../../lib/merge_tree');

var tasks = [];
var tasks2 = [];

// print line number
function lnr() {
  return new Error().stack.split('\n')[2].match(/db_exec_root.js:([0-9]+):[0-9]+/)[1];
}

var logger = require('../../../lib/logger');

var cons, silence;
var chroot = '/var/persdb';
var user = 'nobody';
var group = 'nobody';
var dbPath = '/test_db_exec_root';

// open loggers
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
        chroot: chroot,
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

// should fail if configured hook is not loaded
tasks.push(function(done) {
  console.log('test #%d', lnr());

  // then fork a vce
  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  child.on('exit', function(code, sig) {
    assert(/hook requested that is not loaded/.test(stderr));
    assert.strictEqual(code, 1);
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
      child.send({
        log: { console: true },
        path: dbPath,
        name: 'test_vce_root',
        debug: false,
        size: 1,
        perspectives: [{ name: 'foo', import: { hooks: ['a'] } }]
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

// should require a connection
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert(/connection missing/.test(stderr));
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
      // send stripped auth request
      child.send({
        username: 'foo'
      });
      child.kill();
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

// should require a valid remote identity
tasks.push(function(done) {
  console.log('test #%d', lnr());

  // start an echo server that can receive auth responses
  var host = '127.0.0.1';
  var port = 1234;

  var server = net.createServer(function(conn) {
    conn.on('close', function() {
      server.close(function() {
        child.kill();
      });
    });
  });
  server.listen(port, host);

  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert(/unknown remote/.test(stderr));
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
        group: group,
        chroot: chroot
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

// should respond with an auth response that indicates no data is requested and disconnect
tasks.push(function(done) {
  console.log('test #%d', lnr());

  // start an echo server that can receive auth responses
  var host = '127.0.0.1';
  var port = 1234;

  var server = net.createServer(function(conn) {
    // expect an auth response that indicates no data is requested
    conn.on('data', function(data) {
      assert.deepEqual(JSON.parse(data), {
        start: false
      });

      conn.on('end', function() {
        server.close(function() {
          child.kill();
        });
      });
    });
  });
  server.listen(port, host);

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
        log: { console: true, mask: 7 },
        path: dbPath,
        name: 'test_vce_root',
        user: user,
        group: group,
        chroot: chroot,
        perspectives: [{ name: 'webclient' }]
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

// should accept incoming BSON data if a replication config is given
tasks2.push(function(done) {
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
        start: true
      });

      // now send a root node
      var obj = { h: { id: 'abc', v: 'Aaaa', pa: [] }, b: { some: true } };
      conn.end(BSON.serialize(obj));
      conn.on('close', function() {
        server.close(function() {
          child.kill();
        });
      });
    });
  });
  server.listen(port, host);

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  var pe = 'baz';
  var perspectives = [{ name: pe, import: true }];
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
        assert.deepEqual(item, {
          h: { id: 'abc', v: 'Aaaa', pa: [], pe: 'baz', i: 1 },
          b: { some: true }
        });
      });

      rs.on('end', function() {
        assert.strictEqual(stderr.length, 0);
        assert.strictEqual(code, 0);
        assert.strictEqual(sig, null);
        assert.strictEqual(i, 1);
        mt.mergeWithLocal(mt._pe[pe], function(err) {
          if (err) { throw err; }
          db.close(done);
        });
      });
    });
  });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: 7 },
        path: dbPath,
        name: 'test_vce_root',
        user: user,
        group: group,
        chroot: chroot,
        perspectives: perspectives,
        mergeTree: {
          vSize: vSize
        }
      });
      break;
    case 'listen':
      // send stripped auth request
      var s = net.createConnection(port, host, function() {
        child.send({
          username: pe
        }, s);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

// should send back previously saved item (depends on previous test)
tasks2.push(function(done) {
  console.log('test #%d', lnr());

  // then fork a vce
  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  // start an echo server that can receive auth requests and sends some BSON data
  var host = '127.0.0.1';
  var port = 1234;

  // start server to inspect response
  var server = net.createServer(function(conn) {
    conn.once('data', function(data) {
      assert.deepEqual(JSON.parse(data), {
        start: 'Aaaa'
      });

      conn.on('data', function(data) {
        assert.deepEqual(BSON.deserialize(data), {
          h: { id: 'abc', v: 'Aaaa', pa: [] },
          b: { some: true }
        });

        // now send a new child
        var obj = { h: { id: 'abc', v: 'Bbbb', pa: ['Aaaa'] }, b: { some: 'other' } };
        conn.end(BSON.serialize(obj), function() {
          server.close(function() {
            child.kill();
          });
        });
      });
    });
  });
  server.listen(port, host);

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  var pe = 'baz';
  var perspectives = [{ name: pe, import: true, export: true }];
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
          assert.deepEqual(item, {
            h: { id: 'abc', v: 'Aaaa', pa: [], pe: 'baz', i: 1 },
            b: { some: true }
          });
        }
        if (i === 2) {
          assert.deepEqual(item, {
            h: { id: 'abc', v: 'Bbbb', pa: ['Aaaa'], pe: 'baz', i: 2 },
            b: { some: 'other' }
          });
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

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: 7 },
        path: dbPath,
        name: 'test_vce_root',
        user: user,
        group: group,
        chroot: chroot,
        perspectives: perspectives,
        mergeTree: {
          vSize: vSize
        }
      });
      break;
    case 'listen':
      // send stripped auth request
      var s = net.createConnection(port, host, function() {
        child.send({
          username: pe
        }, s);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

// should run export hooks and send back previously saved items (depends on two previous tests)
tasks2.push(function(done) {
  console.log('test #%d', lnr());

  // then fork a vce
  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  // start an echo server that can receive auth requests and sends some BSON data
  var host = '127.0.0.1';
  var port = 1234;

  // start server to inspect response
  var server = net.createServer(function(conn) {
    conn.once('data', function(data) {
      assert.deepEqual(JSON.parse(data), {
        start: false
      });

      var i = 0;
      conn.pipe(new BSONStream()).on('data', function(item) {
        i++;
        if (i === 1) {
          assert.deepEqual(item, {
            h: { id: 'abc', v: 'Aaaa', pa: [] },
            b: { some: true }
          });
        }
        if (i === 2) {
          assert.deepEqual(item, {
            h: { id: 'abc', v: 'Bbbb', pa: ['Aaaa'] },
            b: { } // should have been stripped by export hook
          });
        }
      });

      conn.on('end', function() {
        assert.strictEqual(i, 2);
        server.close(function() {
          child.kill();
        });
      });
    });
  });
  server.listen(port, host);

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  var pe = 'baz';
  var perspectives = [{ name: pe, export: {
    hooks: ['strip_field_if_holds'],
    hooksOpts: {
      field: 'some',
      fieldFilter: { some: 'other' }
    }
  }}];
  var vSize = 3;

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
        log: { console: true, mask: 7 },
        path: dbPath,
        name: 'test_vce_root',
        hookPaths: [__dirname + '/../../../hooks'],
        user: user,
        group: group,
        chroot: chroot,
        perspectives: perspectives,
        mergeTree: {
          vSize: vSize
        }
      });
      break;
    case 'listen':
      // send stripped auth request
      var s = net.createConnection(port, host, function() {
        child.send({
          username: pe
        }, s);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

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

async.series(tasks2, function(err) {
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
      rimraf(chroot + dbPath, function(err) {
        if (err) { console.error(err); }
      });
    });
  });
});
