/**
 * Copyright 2014, 2015, 2016 Netsend.
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
var net = require('net');
var fs = require('fs');
var tmpdir = require('os').tmpdir;

var async = require('async');
var BSONStream = require('bson-stream');
var level = require('level-packager')(require('leveldown'));
var rimraf = require('rimraf');
var bson = require('bson');

var BSON = new bson.BSONPure.BSON();

var logger = require('../../../lib/logger');
var MergeTree = require('../../../lib/merge_tree');
var spawn = require('../../lib/spawn');

var tasks = [];
var tasks2 = [];

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
        child.send({ type: 'kill' });
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

    spawn([__dirname + '/../../../lib/db_exec'], opts, spawnOpts);
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

  var opts = {
    onMessage: onMessage,
    onExit: done,
    echoErr: false,
    exitCode: 2,
    testStderr: function(stderr) {
      assert(/error loading hooks: "foo,bar" /.test(stderr));
    }
  };
  spawn([__dirname + '/../../../lib/db_exec'], opts);
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
      child.send({ type: 'kill' });
      break;
    default:
      throw new Error('unknown state');
    }
  }

  var opts = {
    onMessage: onMessage,
    onExit: done,
    echoErr: false,
    exitCode: 2,
    testStderr: function(stderr) {
      assert(/error loading hooks: "\/some"/.test(stderr));
    }
  };
  spawn([__dirname + '/../../../lib/db_exec'], opts);
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
      child.send({ type: 'kill' });
      break;
    default:
      throw new Error('unknown state');
    }
  }

  var opts = {
    onMessage: onMessage,
    onExit: done
  };
  spawn([__dirname + '/../../../lib/db_exec'], opts);
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
      setTimeout(function() {
        child.send({ type: 'kill' });
      }, 10);
      break;
    default:
      throw new Error('unknown state');
    }
  }

  var opts = {
    onMessage: onMessage,
    onExit: done,
    testStdout: function(stdout) {
      assert(/{"local":{"heads":{"count":0,"conflict":0,"deleted":0}},"stage":{"heads":{"count":0,"conflict":0,"deleted":0}/.test(stdout));
    }
  };
  spawn([__dirname + '/../../../lib/db_exec'], opts);
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
      child.send({ type: 'kill' });
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
      assert(/hook requested that is not loaded/.test(stderr));
    }
  };
  spawn([__dirname + '/../../../lib/db_exec'], opts);
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
      child.send({ type: 'kill' });
      break;
    default:
      throw new Error('unknown state');
    }
  }

  var opts = {
    onMessage: onMessage,
    onExit: done,
    echoErr: false,
    testStderr: function(stderr) {
      assert(/connection missing/.test(stderr));
    }
  };
  spawn([__dirname + '/../../../lib/db_exec'], opts);
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
          child.send({ type: 'kill' });
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

  var opts = {
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: done,
    echoErr: false,
    testStderr: function(stderr) {
      assert(/unknown perspective/.test(stderr));
    }
  };
  spawn([__dirname + '/../../../lib/db_exec'], opts);
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
            child.send({ type: 'kill' });
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

  var opts = {
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: done
  };
  spawn([__dirname + '/../../../lib/db_exec'], opts);
});

// should accept a data request followed by BSON data if import is true
tasks.push(function(done) {
  // start an echo server that can receive auth requests and sends some BSON data
  var host = '127.0.0.1';
  var port = 1234;
  var port2 = 1235;

  // start servers to check responses from db_exec
  function onSpawn(child) {
    // server that resembles a remote data handler
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
      });
    }).listen(port, host);

    // server that resembles a local data handler
    var server2 = net.createServer(function(conn) {
      conn.on('data', function(data) {
        // expect a merge
        var obj = BSON.deserialize(data);
        assert.deepEqual(obj, {
          n: {
            h: { id: 'abd', v: 'Aaaa', pa: [], pe: 'baz' },
            b: { some: true }
          },
          l: null,
          lcas: [],
          pe: 'baz',
          c: null
        });

        // confirm merge
        conn.end(data, function() {
          server.close(function() {
            server2.close(function() {
              child.send({ type: 'kill' });
            });
          });
        });
      });
    }).listen(port2, host);
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

        // should have updated last by perspective of local tree
        mt._local.lastByPerspective(pe, 'base64', function(err, v) {
          if (err) { throw err; }
          assert.strictEqual(v, 'Aaaa');
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
      // forward authorized remote connection with connection
      var s = net.createConnection(port, host, function() {
        child.send({
          type: 'remoteDataChannel',
          perspective: pe
        }, s);
      });
      // forward authorized local connection with connection
      var s2 = net.createConnection(port2, host, function() {
        child.send({
          type: 'localDataChannel',
        }, s2);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  }

  var opts = {
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: onExit
  };
  spawn([__dirname + '/../../../lib/db_exec'], opts);
});

// should send back previously saved item (depends on previous test)
tasks.push(function(done) {
  // start an echo server that can receive auth requests and sends some BSON data
  var host = '127.0.0.1';
  var port = 1234;
  var port2 = 1235;

  // start server to inspect response
  function onSpawn(child) {
    // server that resembles a remote data handler
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
          conn.end(BSON.serialize(obj));
        });
      });
    }).listen(port, host);

    // server that resembles a local data handler
    var server2 = net.createServer(function(conn) {
      conn.on('data', function(data) {
        // expect a merge
        var obj = BSON.deserialize(data);
        assert.deepEqual(obj, {
          n: {
            h: { id: 'abd', v: 'Bbbb', pa: ['Aaaa'], pe: 'baz' },
            b: { some: 'other' }
          },
          l: {
            h: { id: 'abd', v: 'Aaaa', pa: [], pe: 'baz', i: 1 },
            b: { some: true }
          },
          lcas: ['Aaaa'],
          pe: 'baz',
          c: null
        });


        // confirm merge
        conn.end(data, function() {
          server.close(function() {
            server2.close(function() {
              child.send({ type: 'kill' });
            });
          });
        });
      });
    }).listen(port2, host);
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
        perspectives: perspectives,
        mergeTree: {
          vSize: vSize
        }
      });
      break;
    case 'listen':
      // forward authorized remote connection with connection
      var s = net.createConnection(port, host, function() {
        child.send({
          type: 'remoteDataChannel',
          perspective: pe
        }, s);
      });
      // forward authorized local connection with connection
      var s2 = net.createConnection(port2, host, function() {
        child.send({
          type: 'localDataChannel',
        }, s2);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  }

  var opts = {
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: onExit
  };
  spawn([__dirname + '/../../../lib/db_exec'], opts);
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
            child.send({ type: 'kill' });
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
        hookPaths: [__dirname + '/../../../hooks/core'],
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

  var opts = {
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: done
  };
  spawn([__dirname + '/../../../lib/db_exec'], opts);
});

// should run import hooks (depends on previous 3 tests);
tasks.push(function(done) {
  // start an echo server that can receive auth requests and sends some BSON data
  var host = '127.0.0.1';
  var port = 1234;
  var port2 = 1235;

  // start server to inspect response
  function onSpawn(child) {
    // server that resembles a remote data handler
    var server = net.createServer(function(conn) {
      conn.once('data', function(data) {
        assert.deepEqual(JSON.parse(data), {
          start: 'Bbbb'
        });

        // send a data request signalling data is not requested
        conn.write(JSON.stringify({ start: false }) + '\n');

        conn.end(BSON.serialize({
          h: { id: 'def', v: 'Dddd', pa: [] },
          b: { some: true, other: false }
        }));

      });
    }).listen(port, host);

    // server that resembles a local data handler
    var server2 = net.createServer(function(conn) {
      conn.on('data', function(data) {
        // expect a merge
        var obj = BSON.deserialize(data);
        assert.deepEqual(obj, {
          n: {
            h: { id: 'def', v: 'Dddd', pa: [], pe: 'baz' },
            b: { other: false } // should have stripped "some"
          },
          l: null,
          lcas: [],
          pe: 'baz',
          c: null
        });

        // confirm merge
        conn.end(data, function() {
          server.close(function() {
            server2.close(function() {
              child.send({ type: 'kill' });
            });
          });
        });
      });
    }).listen(port2, host);
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

        // should have updated last by perspective of local tree
        mt._local.lastByPerspective(pe, 'base64', function(err, v) {
          if (err) { throw err; }
          assert.strictEqual(v, 'Dddd');
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
        hookPaths: [__dirname + '/../../../hooks/core'],
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
      // forward authorized remote connection with connection
      var s = net.createConnection(port, host, function() {
        child.send({
          type: 'remoteDataChannel',
          perspective: pe
        }, s);
      });
      // forward authorized local connection with connection
      var s2 = net.createConnection(port2, host, function() {
        child.send({
          type: 'localDataChannel',
        }, s2);
      });
      break;
    default:
      throw new Error('unknown state');
    }
  }

  var opts = {
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: onExit
  };
  spawn([__dirname + '/../../../lib/db_exec'], opts);
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
      conn.end(BSON.serialize({ n: obj }), function() {
        server.close(function() {
          child.send({ type: 'kill' });
        });
      });
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
      });
      break;
    default:
      throw new Error('unknown state');
    }
  }

  var opts = {
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: onExit
  };
  spawn([__dirname + '/../../../lib/db_exec'], opts);
});

// should accept and respond to a head lookup request (depends on successful write in previous test)
tasks.push(function(done) {
  // start an echo server that can receive auth requests and sends some BSON data
  var host = '127.0.0.1';
  var port = 1234;

  var i = 0;
  // lookup the item from the previous test
  function onSpawn(child) {
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
          child.send({ type: 'kill' });
        });
      });
    }).listen(port, host);
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

  var opts = {
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: onExit
  };
  spawn([__dirname + '/../../../lib/db_exec'], opts);
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
          child.send({ type: 'kill' });
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

  var opts = {
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: onExit
  };
  spawn([__dirname + '/../../../lib/db_exec'], opts);
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
