/**
 * Copyright 2015 Netsend.
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

var c = require('constants');
var assert = require('assert');
var fs = require('fs');
var net = require('net');
var cp = require('child_process');

var async = require('async');
var bson = require('bson');
var BSON = new bson.BSONPure.BSON();
var level = require('level-packager')(require('leveldown'));
var rimraf = require('rimraf');
var ws = require('nodejs-websocket');
var xtend = require('xtend');

var logger = require('../../../lib/logger');
var MergeTree = require('../../../lib/merge_tree');

var noop = function() {};

var cert;

cert = __dirname + '/cert.pem';

var wsClientOpts = {
  ca: fs.readFileSync(cert),
  secureProtocol: 'SSLv23_client_method',
  secureOptions: c.SSL_OP_NO_SSLv2|c.SSL_OP_NO_SSLv3|c.SSL_OP_NO_TLSv1|c.SSL_OP_NO_TLSv1_1,
  ciphers: 'ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-CHACHA20-POLY1305:ECDHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES128-GCM-SHA256'
};

var tasks = [];
var tasks2 = [];

var cons, silence;
var chroot = '/var/persdb';
var dbPath = '/test_persdb_root';

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
    args: [__dirname + '/../../../bin/persdb'],
    stdio: ['pipe', 'pipe', 'pipe', 'ipc']
  }, spawnOpts);

  // print line number
  console.log('test #%d', new Error().stack.split('\n')[2].match(/persdb_root.js:([0-9]+):[0-9]+/)[1]);

  var extraArgs = [opts.configBase + '/' + opts.config];
  var child = cp.spawn(process.execPath, sOpts.args.concat(extraArgs), sOpts);

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

// should start a TCP server and listen on port 1234, test by connecting only
tasks.push(function(done) {
  function onSpawn(child) {
    setTimeout(function() {
      net.createConnection(1234, function() {
        child.kill();
      });
    }, 1000);
  }

  spawn({
    onSpawn: onSpawn,
    onExit: done,
    testStdout: function(stdout) {
      assert(/TCP server bound 127.0.0.1:1234/.test(stdout));
      assert(/client connected 127.0.0.1-/.test(stdout));
    }
  });
});

// should try to start a WSS server and if this fails because of missing attributes (key, cert, dhparam) automatically shutdown
tasks.push(function(done) {
  spawn({
    config: 'test_persdb_wss_wrong_config.hjson',
    echoErr: false,
    onExit: done,
    testStderr: function(stderr) {
      assert(/msg.key must be a non-empty string/.test(stderr));
    }
  });
});

// should start a WSS server, test by connecting with wss client
tasks.push(function(done) {
  function onSpawn(child) {
    setTimeout(function() {
      ws.connect('wss://localhost:1235', wsClientOpts, function() {
        child.kill();
      });
    }, 1000);
  }

  spawn({
    config: 'test_persdb_wss_export.hjson',
    onSpawn: onSpawn,
    onExit: done,
    testStdout: function(stdout) {
      assert(/-127.0.0.1-1235: end \(no binary data received\)/.test(stdout));
    }
  });
});

// should start a WSS server, and disconnect because of empty data request
tasks.push(function(done) {
  function onSpawn(child) {
    setTimeout(function() {
      var authReq = {};

      var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
        client.on('close', function() {
          child.kill();
        });
        client.sendText(JSON.stringify(authReq) + '\n');
      });
    }, 1000);
  }

  spawn({
    config: 'test_persdb_wss_export.hjson',
    onSpawn: onSpawn,
    echoErr: false,
    onExit: done,
    testStderr: function(stderr) {
      assert(/-127.0.0.1-1234 invalid auth request/.test(stderr));
    }
  });
});

// should start a WSS server, and disconnect because db name does not exist in config
tasks.push(function(done) {
  function onSpawn(child) {
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
  }

  spawn({
    config: 'test_persdb_wss_export.hjson',
    onSpawn: onSpawn,
    echoErr: false,
    onExit: done,
    testStderr: function(stderr) {
      assert(/_verifyAuthRequest no config found for db: baz invalid credentials/.test(stderr));
    }
  });
});

// should start a WSS server, and disconnect because username does not exist in config
tasks.push(function(done) {
  function onSpawn(child) {
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
  }

  spawn({
    config: 'test_persdb_wss_export.hjson',
    onSpawn: onSpawn,
    echoErr: false,
    onExit: done,
    testStderr: function(stderr) {
      assert(/_verifyAuthRequest no users or import \/ export config found someDb invalid credentials/.test(stderr));
    }
  });
});

// should start a WSS server, and disconnect because password is incorrect
tasks.push(function(done) {
  function onSpawn(child) {
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
  }

  spawn({
    config: 'test_persdb_wss_export.hjson',
    onSpawn: onSpawn,
    echoErr: false,
    onExit: done,
    testStderr: function(stderr) {
      assert(/_verifyAuthRequest someDb someClient invalid credentials/.test(stderr));
    }
  });
});

// should start a WSS server, and send a data request that signals no data is expected (no import key in config)
tasks.push(function(done) {
  function onSpawn(child) {
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
  }

  spawn({
    config: 'test_persdb_wss_export.hjson',
    onSpawn: onSpawn,
    onExit: done,
    testStdout: function(stdout) {
      assert(/successfully authenticated "someClient"/.test(stdout));
    }
  });
});

// should start a WSS server, and send a data request that signals data is expected (import true)
tasks.push(function(done) {
  function onSpawn(child) {
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
  }

  spawn({
    config: 'test_persdb_wss_import.hjson',
    onSpawn: onSpawn,
    onExit: done,
    testStdout: function(stdout) {
      assert(/successfully authenticated "someClient"/.test(stdout));
    }
  });
});

// should start a WSS server, and accept a data request + two BSON items
tasks.push(function(done) {
  var pe = 'someClient';
  var vSize = 3;

  function onSpawn(child) {
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
  }

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
          assert.deepEqual(item, { h: { id: 'abc', v: 'Aaaa', pe: pe, i: 1, pa: [] }, b: { some: true } });
        }
        if (i === 2) {
          assert.deepEqual(item, { h: { id: 'abc', v: 'Bbbb', pe: pe, i: 2, pa: ['Aaaa'] }, b: { some: 'other' } });
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

  spawn({
    config: 'test_persdb_wss_import.hjson',
    onSpawn: onSpawn,
    onExit: onExit,
  });
});

// should start a WSS server, and accept a data request + one BSON item, and send previously saved items (depends on previous test)
tasks.push(function(done) {
  var pe = 'someClient';
  var vSize = 3;

  function onSpawn(child) {
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
  }

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
        assert.strictEqual(i, 3);
        mt.mergeWithLocal(mt._pe[pe], function(err) {
          if (err) { throw err; }
          db.close(done);
        });
      });
    });
  }

  spawn({
    config: 'test_persdb_wss_import_export.hjson',
    onSpawn: onSpawn,
    onExit: onExit,
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
