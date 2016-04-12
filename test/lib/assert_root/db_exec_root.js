/**
 * Copyright 2014-2016 Netsend.
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
var net = require('net');
var fs = require('fs');
var tmpdir = require('os').tmpdir;
var childProcess = require('child_process');

var async = require('async');
var BSONStream = require('bson-stream');
var level = require('level-packager')(require('leveldown'));
var rimraf = require('rimraf');
var bson = require('bson');
var xtend = require('xtend');

var BSON = new bson.BSONPure.BSON();

var logger = require('../../../lib/logger');
var MergeTree = require('../../../lib/merge_tree');

var noop = function() {};

var tasks = [];
var tasks2 = [];

function spawn(options, spawnOpts) {
  var opts = xtend({
    configBase: __dirname,
    config: 'test_persdb.hjson',
    echoOut: false,
    echoErr: true,
    onSpawn: noop,
    onMessage: null,                                          // handle ipc messages
    onExit: noop,
    exitCode: 0,                                              // test if exit code is 0
    exitSignal: null,                                         // test if exit signal is empty
    testStdout: null,
    testStderr: function(s) {                               // test if stderr is empty
      if (s.length) { throw new Error(s); }
    }
  }, options);

  var sOpts = xtend({
    args: [__dirname + '/../../../lib/db_exec'],
    stdio: ['pipe', 'pipe', 'pipe', 'ipc']
  }, spawnOpts);

  // print line number
  console.log('test #%d', new Error().stack.split('\n')[2].match(/db_exec_root.js:([0-9]+):[0-9]+/)[1]);

  var extraArgs = [];
  var child = childProcess.spawn(process.execPath, sOpts.args.concat(extraArgs), sOpts);

  if (opts.echoOut) { child.stdout.pipe(process.stdout); }
  if (opts.echoErr) { child.stderr.pipe(process.stderr); }

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    if (opts.testStdout) {
      opts.testStdout(stdout);
    }
    if (opts.testStderr) {
      opts.testStderr(stderr);
    }
    assert.strictEqual(code, opts.exitCode);
    assert.strictEqual(sig, opts.exitSignal);
    opts.onExit(null, code, sig, stdout, stderr);
  });

  if (opts.onMessage) {
    child.on('message', function(msg) {
      opts.onMessage(msg, child);
    });
  }
  opts.onSpawn(child);
}

var logger = require('../../../lib/logger');

var cons, silence;
var chroot = '/var/persdb';
var user = 'pdblevel';
var group = 'pdblevel';
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
  var logFile = fs.createWriteStream(tmpdir() + 'dbe-test.log', { flags: 'a' });

  logFile.on('open', function() {
    function onMessage(msg, child) {
      switch(msg) {
      case 'init':
        child.send({
          log: { console: true },
          path: dbPath,
          name: 'test_dbe_root',
        });
        break;
      case 'listen':
        child.kill();
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
      stdio: ['pipe', 'pipe', 'pipe', 'ipc', logFile]
    };

    spawn(opts, spawnOpts);
  });
});

// should fail if hooks not found
tasks.push(function(done) {
  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true },
        path: dbPath,
        hookPaths: ['foo', 'bar'],
        name: 'test_dbe_root',
        user: user,
        chroot: chroot
      });
      break;
    default:
      throw new Error('unknown state');
    }
  }

  spawn({
    onMessage: onMessage,
    onExit: done,
    echoErr: false,
    exitCode: 2,
    testStderr: function(stderr) {
      assert(/error loading hooks: "foo,bar" /.test(stderr));
    }
  });
});

// should fail with a valid configuration but non-existing hook paths
tasks.push(function(done) {
  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true },
        path: dbPath,
        hookPaths: ['/some'],
        name: 'test_dbe_root',
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
  }

  spawn({
    onMessage: onMessage,
    onExit: done,
    echoErr: false,
    exitCode: 2,
    testStderr: function(stderr) {
      assert(/error loading hooks: "\/some"/.test(stderr));
    }
  });
});

// should not fail with valid configurations (include existing hook paths)
tasks.push(function(done) {
  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true },
        path: dbPath,
        hookPaths: ['/var/empty'],
        name: 'test_dbe_root',
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
  }

  spawn({
    onMessage: onMessage,
    onExit: done
  });
});

// should show info on SIGUSR2
tasks.push(function(done) {
  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true },
        path: dbPath,
        hookPaths: ['/var/empty'],
        name: 'test_dbe_root',
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
  }

  spawn({
    onMessage: onMessage,
    onExit: done,
    testStdout: function(stdout) {
      assert(/{"local":{"heads":{"count":0,"conflict":0,"deleted":0}},"stage":{"heads":{"count":0,"conflict":0,"deleted":0}/.test(stdout));
    }
  });
});

// should fail if configured hook is not loaded
tasks.push(function(done) {
  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true },
        path: dbPath,
        name: 'test_dbe_root',
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
  }

  spawn({
    onMessage: onMessage,
    onExit: done,
    echoErr: false,
    exitCode: 1,
    testStderr: function(stderr) {
      assert(/hook requested that is not loaded/.test(stderr));
    }
  });
});

// should require a connection
tasks.push(function(done) {
  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true },
        path: dbPath,
        name: 'test_dbe_root',
        user: user,
        chroot: chroot
      });
      break;
    case 'listen':
      // send stripped auth request
      child.send({
        type: 'remoteDataChannel',
        perspective: 'foo'
      });
      child.kill();
      break;
    default:
      throw new Error('unknown state');
    }
  }

  spawn({
    onMessage: onMessage,
    onExit: done,
    echoErr: false,
    testStderr: function(stderr) {
      assert(/connection missing/.test(stderr));
    }
  });
});

// should require a valid remote identity
tasks.push(function(done) {
  // start an echo server that can receive auth responses
  var host = '127.0.0.1';
  var port = 1234;

  function onSpawn(child) {
    var server = net.createServer(function(conn) {
      conn.on('close', function() {
        server.close(function() {
          child.kill();
        });
      });
    });
    server.listen(port, host);
  }

  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true },
        path: dbPath,
        name: 'test_dbe_root',
        user: user,
        group: group,
        chroot: chroot
      });
      break;
    case 'listen':
      // send stripped auth request
      var s = net.createConnection(port, host, function() {
        child.send({
          type: 'remoteDataChannel',
          perspective: 'webclient'
        }, s);
      });

      break;
    default:
      throw new Error('unknown state');
    }
  }

  spawn({
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: done,
    echoErr: false,
    testStderr: function(stderr) {
      assert(/unknown perspective/.test(stderr));
    }
  });
});

// should respond with a data request that indicates no data is requested
tasks.push(function(done) {
  // start an echo server that can receive data requests
  var host = '127.0.0.1';
  var port = 1234;

  function onSpawn(child) {
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
  }

  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: 7 },
        path: dbPath,
        name: 'test_dbe_root',
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
          type: 'remoteDataChannel',
          perspective: 'webclient'
        }, s);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  }

  spawn({
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: done
  });
});

// should accept a data request followed by BSON data if an import config is given
tasks.push(function(done) {
  // start an echo server that can receive auth requests and sends some BSON data
  var host = '127.0.0.1';
  var port = 1234;

  // start server to check response from db_exec
  function onSpawn(child) {
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
  }

  var pe = 'baz';
  var perspectives = [{ name: pe, import: true }];
  var vSize = 3;

  function onExit() {
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
        assert.strictEqual(i, 1);
        mt.mergeWithLocal(mt._pe[pe], function(err) {
          if (err) { throw err; }
          db.close(done);
        });
      });
    });
  }

  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: 7 },
        path: dbPath,
        name: 'test_dbe_root',
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
          type: 'remoteDataChannel',
          perspective: pe
        }, s);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  }

  spawn({
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: onExit
  });
});

// should send back previously saved item (depends on previous test)
tasks.push(function(done) {
  // start an echo server that can receive auth requests and sends some BSON data
  var host = '127.0.0.1';
  var port = 1234;

  // start server to inspect response
  function onSpawn(child) {
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
  }

  var pe = 'baz';
  var perspectives = [{ name: pe, import: true, export: true }];
  var vSize = 3;

  function onExit() {
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
        assert.strictEqual(i, 2);
        mt.mergeWithLocal(mt._pe[pe], function(err) {
          if (err) { throw err; }
          db.close(done);
        });
      });
    });
  }

  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: 7 },
        path: dbPath,
        name: 'test_dbe_root',
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
          type: 'remoteDataChannel',
          perspective: pe
        }, s);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  }

  spawn({
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: onExit
  });
});

// should run export hooks and send back previously saved items (depends on two previous tests)
tasks.push(function(done) {
  // start an echo server that can receive auth requests and sends some BSON data
  var host = '127.0.0.1';
  var port = 1234;

  // start server to inspect response
  function onSpawn(child) {
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
  }

  var pe = 'baz';
  var perspectives = [{ name: pe, export: {
    hooks: ['strip_field_if_holds'],
    hooksOpts: {
      field: 'some',
      fieldFilter: { some: 'other' }
    }
  }}];
  var vSize = 3;

  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: 7 },
        path: dbPath,
        name: 'test_dbe_root',
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
          type: 'remoteDataChannel',
          perspective: pe
        }, s);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  }

  spawn({
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: done
  });
});

// should run import hooks
tasks.push(function(done) {
  // start an echo server that can receive auth requests and sends some BSON data
  var host = '127.0.0.1';
  var port = 1234;

  // start server to inspect response
  function onSpawn(child) {
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
  }

  var pe = 'baz';
  var perspectives = [{ name: pe, import: {
    hooks: ['strip_field_if_holds'],
    hooksOpts: {
      field: 'some',
      fieldFilter: { some: true }
    }
  }}];
  var vSize = 3;

  function onExit() {
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
        assert.strictEqual(i, 3);
        mt.mergeWithLocal(mt._pe[pe], function(err) {
          if (err) { throw err; }
          db.close(done);
        });
      });
    });
  }

  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: 7 },
        path: dbPath,
        name: 'test_dbe_root',
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
          type: 'remoteDataChannel',
          perspective: pe
        }, s);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  }

  spawn({
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: onExit
  });
});

// should accept a new local data channel request and write data to the local tree
tasks.push(function(done) {
  // start an echo server that can receive auth requests and sends some BSON data
  var host = '127.0.0.1';
  var port = 1234;

  // start server to check response from db_exec
  function onSpawn(child) {
    var server = net.createServer(function(conn) {
      conn.on('data', function() {
        throw new Error('unexpected merge');
      });

      // send a new node
      var obj = {
        h: { id: 'abd', v: 'Aaaa' },
        b: { some: true }
      };
      conn.write(BSON.serialize(obj));
      conn.on('close', function() {
        server.close(function() {
          child.kill();
        });
      });
      // give some time to start merge handler
      setTimeout(conn.end.bind(conn), 110);
    });
    server.listen(port, host);
  }

  var vSize = 3;
  var localName = 'testWithLocalDataChannel';

  function onExit() {
    // open and search in database if item is written
    level('/var/persdb' + dbPath, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, db) {
      if (err) { throw err; }

      var mt = new MergeTree(db, {
        local: localName,
        vSize: vSize,
        log: silence
      });
      var rs = mt.createReadStream();
      var i = 0;
      rs.on('data', function(item) {
        i++;
        assert.deepEqual(item, {
          h: { id: 'abd', v: 'Aaaa', pa: [] },
          b: { some: true }
        });
      });

      rs.on('end', function() {
        assert.strictEqual(i, 1);
        db.close(done);
      });
    });
  }

  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: 7 },
        path: dbPath,
        name: 'test_dbe_root',
        user: user,
        group: group,
        chroot: chroot,
        mergeTree: {
          local: localName,
          vSize: vSize
        }
      });
      break;
    case 'listen':
      var s = net.createConnection(port, host, function() {
        child.send({
          type: 'localDataChannel'
        }, s);

        // and send a startMerge signal
        child.send({
          type: 'startMerge',
          interval: 100
        });
      });
      break;
    default:
      throw new Error('unknown state');
    }
  }

  spawn({
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: onExit
  });
});

// should write merges with remotes back to the local data channel
tasks.push(function(done) {
  // start an echo server for the local channel and one for the remote channel
  var host = '127.0.0.1';
  var port = 1234;
  var port2 = 1235;

  var i = 0;
  // start server for local data channel
  function onSpawn(child) {
    var server = net.createServer(function(conn) {
      // expect a merge with the item sent by server2
      conn.pipe(new BSONStream()).on('data', function(item) {
        i++;
        assert.deepEqual(item, {
          n: {
            h: { id: 'abd', v: 'Aaaa', pa: [], pe: 'buzz', i: 1 },
            b: { some: true }
          },
          o: null
        });

        conn.end();
      });

      conn.on('close', function() {
        server.close(function() {
          child.kill();
        });
      });
    });
    server.listen(port, host);

    // start server for remote data channel
    var server2 = net.createServer(function(conn) {
      conn.on('data', function(data) {
        assert.deepEqual(JSON.parse(data), {
          start: true
        });

        // send a data request signalling no data is requested
        conn.write(JSON.stringify({ start: false }) + '\n');

        // send a root node in BSON
        conn.write(BSON.serialize({
          h: { id: 'abd', v: 'Aaaa', pa: [] },
          b: { some: true }
        }));

        conn.on('close', function() {
          server2.close();
        });
        conn.end();
      });
    });
    server2.listen(port2, host);
  }

  var pe = 'buzz';
  var perspectives = [{ name: pe, import: true }];
  var localName = 'testWithLocalAndRemoteDataChannel';
  var vSize = 3;

  function onExit() {
    assert.strictEqual(i, 1);
    done();
  }

  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: 7 },
        path: dbPath,
        name: 'test_dbe_root',
        user: user,
        group: group,
        chroot: chroot,
        perspectives: perspectives,
        mergeTree: {
          local: localName,
          vSize: vSize
        }
      });
      break;
    case 'listen':
      // setup local data channel
      var s = net.createConnection(port, host, function() {
        child.send({
          type: 'localDataChannel'
        }, s);

        // and send a startMerge signal
        child.send({
          type: 'startMerge',
          interval: 100
        });
      });

      // setup remote data channel
      var s2 = net.createConnection(port2, host, function() {
        child.send({
          type: 'remoteDataChannel',
          perspective: pe
        }, s2);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  }

  spawn({
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: onExit
  });
});

// should accept and respond to a head lookup request for a non-existing id
tasks.push(function(done) {
  // start an echo server for the head lookup channel
  var host = '127.0.0.1';
  var port = 1234;

  var i = 0;
  // start server for head lookup channel
  function onSpawn(child) {
    var server = net.createServer(function(conn) {
      // request a head lookup for non-existing "abd"
      conn.write(JSON.stringify({ id: 'abd'}) + '\n');

      // expect the requested version
      conn.pipe(new BSONStream()).on('data', function(item) {
        i++;
        assert.deepEqual(item, {});
        conn.end();
      });

      conn.on('close', function() {
        server.close(function() {
          child.kill();
        });
      });
    });
    server.listen(port, host);
  }

  var localName = 'testHeadLookup';
  var vSize = 3;

  function onExit() {
    assert.strictEqual(i, 1);
    done();
  }

  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: 7 },
        path: dbPath,
        name: 'test_dbe_root',
        user: user,
        group: group,
        chroot: chroot,
        mergeTree: {
          local: localName,
          vSize: vSize
        }
      });
      break;
    case 'listen':
      // setup head lookup channel
      var s = net.createConnection(port, host, function() {
        child.send({
          type: 'headLookup'
        }, s);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  }

  spawn({
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: onExit
  });
});

// should accept and respond to a head lookup request (write a new object via a local data channel first)
tasks.push(function(done) {
  // start an echo server that can receive auth requests and sends some BSON data
  var host = '127.0.0.1';
  var port = 1234;
  var port2 = 1235;

  var i = 0;
  // start local data channel to send a new object that can later be looked up
  function onSpawn(child) {
    var localDataServer = net.createServer(function(conn) {
      conn.on('data', function() {
        throw new Error('unexpected merge');
      });

      conn.on('close', function() {
        localDataServer.close();
      });
      // send a new node
      conn.write(BSON.serialize({
        h: { id: 'abd', v: 'Aaaa' },
        b: { some: true }
      }));
      conn.end();

      // give it some time to write to the local tree, then setup connection to head lookup server
      setTimeout(function() {
        var s2 = net.createConnection(port2, host, function() {
          child.send({
            type: 'headLookup'
          }, s2);
        });
      }, 100);
    });
    localDataServer.listen(port, host);

    // start server for head lookup channel
    var headLookupServer = net.createServer(function(conn) {
      // do a head lookup for "abd"
      conn.write(JSON.stringify({ id: 'abd'}) + '\n');

      // expect the requested version
      conn.pipe(new BSONStream()).on('data', function(item) {
        i++;
        assert.deepEqual(item, {
          h: { id: 'abd', v: 'Aaaa', pa: [], i: 1 },
          b: { some: true }
        });
        conn.end();
      });

      conn.on('close', function() {
        headLookupServer.close(function() {
          child.kill();
        });
      });
    });
    headLookupServer.listen(port2, host);
  }

  var vSize = 3;
  var localName = 'testWithLocalDataChannel';

  function onExit() {
    assert.strictEqual(i, 1);
    done();
  }

  function onMessage(msg, child) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: 7 },
        path: dbPath,
        name: 'test_dbe_root',
        user: user,
        group: group,
        chroot: chroot,
        mergeTree: {
          local: localName,
          vSize: vSize
        }
      });
      break;
    case 'listen':
      var s = net.createConnection(port, host, function() {
        child.send({
          type: 'localDataChannel'
        }, s);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  }

  spawn({
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: onExit
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
