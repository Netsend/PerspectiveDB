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
var level = require('level-packager')(require('leveldown'));
var rimraf = require('rimraf');
var bson = require('bson');

var BSON = new bson.BSONPure.BSON();

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

// should show info on SIGUSR2
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert(/{"local":{"heads":{"count":0,"conflict":0,"deleted":0}},"stage":{"heads":{"count":0,"conflict":0,"deleted":0}/.test(stdout));
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
      child.kill('SIGUSR2');
      process.nextTick(function() {
        child.kill();
      });
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
        perspective: 'foo'
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
    assert(/unknown perspective/.test(stderr));
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
          perspective: 'webclient'
        }, s);
      });

      break;
    default:
      throw new Error('unknown state');
    }
  });
});

// should respond with a data request that indicates no data is requested
tasks.push(function(done) {
  console.log('test #%d', lnr());

  // start an echo server that can receive data requests
  var host = '127.0.0.1';
  var port = 1234;

  var server = net.createServer(function(conn) {
    // expect a data request that indicates no data is requested
    conn.on('data', function(data) {
      assert.deepEqual(JSON.parse(data), {
        start: false
      });

      conn.end(function() {
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
          perspective: 'webclient'
        }, s);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

// should accept a data request followed by BSON data if an import config is given
tasks.push(function(done) {
  console.log('test #%d', lnr());

  // then fork a vce
  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  // start an echo server that can receive auth requests and sends some BSON data
  var host = '127.0.0.1';
  var port = 1234;

  // start server to check response from db_exec
  var server = net.createServer(function(conn) {
    conn.on('data', function(data) {
      assert.deepEqual(JSON.parse(data), {
        start: true
      });

      // send a data request signalling no data is requested
      conn.write(JSON.stringify({ start: false }) + '\n');

      // follow up with a root node in BSON
      var obj = { h: { id: 'abd', v: 'Aaaa', pa: [] }, b: { some: true } };
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
          h: { id: 'abd', v: 'Aaaa', pa: [], pe: 'baz', i: 1 },
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
      // forward authorized connection with connection
      var s = net.createConnection(port, host, function() {
        child.send({
          perspective: pe
        }, s);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

// should send back previously saved item (depends on previous test)
tasks.push(function(done) {
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

      // send a data request signalling data is requested from the start
      conn.write(JSON.stringify({ start: true }) + '\n');

      conn.on('data', function(data) {
        assert.deepEqual(BSON.deserialize(data), {
          h: { id: 'abd', v: 'Aaaa', pa: [] },
          b: { some: true }
        });

        // now send a new child
        var obj = { h: { id: 'abd', v: 'Bbbb', pa: ['Aaaa'] }, b: { some: 'other' } };
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
            h: { id: 'abd', v: 'Aaaa', pa: [], pe: 'baz', i: 1 },
            b: { some: true }
          });
        }
        if (i === 2) {
          assert.deepEqual(item, {
            h: { id: 'abd', v: 'Bbbb', pa: ['Aaaa'], pe: 'baz', i: 2 },
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
          perspective: pe
        }, s);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

// should run export hooks and send back previously saved items (depends on two previous tests)
tasks.push(function(done) {
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

      // send a data request signalling data is requested from the start
      conn.write(JSON.stringify({ start: true }) + '\n');

      var i = 0;
      conn.pipe(new BSONStream()).on('data', function(item) {
        i++;
        if (i === 1) {
          assert.deepEqual(item, {
            h: { id: 'abd', v: 'Aaaa', pa: [] },
            b: { some: true }
          });
        }
        if (i > 1) {
          assert.deepEqual(item, {
            h: { id: 'abd', v: 'Bbbb', pa: ['Aaaa'] },
            b: { } // should have been stripped by export hook
          });
          conn.end();
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
          perspective: pe
        }, s);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

// should run import hooks
tasks.push(function(done) {
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
        start: 'Bbbb'
      });

      // send a data request signalling data is requested from the start
      conn.write(JSON.stringify({ start: true }) + '\n');

      conn.end(BSON.serialize({
        h: { id: 'def', v: 'Dddd', pa: [] },
        b: { some: true, other: false }
      }));

      conn.on('end', function() {
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
  var perspectives = [{ name: pe, import: {
    hooks: ['strip_field_if_holds'],
    hooksOpts: {
      field: 'some',
      fieldFilter: { some: true }
    }
  }}];
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
            h: { id: 'abd', v: 'Aaaa', pa: [], pe: 'baz', i: 1 },
            b: { some: true }
          });
        }
        if (i === 2) {
          assert.deepEqual(item, {
            h: { id: 'abd', v: 'Bbbb', pa: ['Aaaa'], pe: 'baz', i: 2 },
            b: { some: 'other' }
          });
        }
        if (i === 3) {
          assert.deepEqual(item, {
            h: { id: 'def', v: 'Dddd', pa: [], pe: 'baz', i: 3 },
            b: { other: false } // should have stripped "some"
          });
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
          perspective: pe
        }, s);
      });
      break;
    default:
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
      if (err) { throw err; }
      rimraf(chroot + dbPath, function(err) {
        if (err) { console.error(err); }
      });
    });
  });
});
