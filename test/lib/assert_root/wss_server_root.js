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

var assert = require('assert');
var net = require('net');
var childProcess = require('child_process');
var c = require('constants');
var fs = require('fs');

var async = require('async');
var bson = require('bson');
var BSON = new bson.BSONPure.BSON();
var ws = require('nodejs-websocket');
var xtend = require('xtend');

var cert, key, dhparam;

cert = __dirname + '/cert.pem';
key = __dirname + '/key.pem';
dhparam = __dirname + '/dhparam.pem';

var wsClientOpts = {
  ca: fs.readFileSync(cert),
  secureProtocol: 'SSLv23_client_method',
  secureOptions: c.SSL_OP_NO_SSLv2|c.SSL_OP_NO_SSLv3|c.SSL_OP_NO_TLSv1|c.SSL_OP_NO_TLSv1_1,
  ciphers: 'ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-CHACHA20-POLY1305:ECDHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES128-GCM-SHA256'
};

var noop = function() {};
var tasks  = [];
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
    testStderr: function(s) {                                 // test if stderr is empty
      if (s.length) { throw new Error(s); }
    }
  }, options);

  var sOpts = xtend({
    args: [__dirname + '/../../../lib/wss_server'],
    stdio: ['pipe', 'pipe', 'pipe', 'ipc']
  }, spawnOpts);

  // print line number
  console.log('test #%d', new Error().stack.split('\n')[2].match(/wss_server_root.js:([0-9]+):[0-9]+/)[1]);

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

// open loggers
tasks.push(function(done) {
  logger({ console: true, mask: logger.DEBUG2 }, function(err, l) {
    if (err) { throw err; }
    cons = l;
    logger({ silence: true }, function(err, l) {
      if (err) { throw err; }
      silence = l;
      done();
    });
  });
});

// should require msg.log to be an object
tasks.push(function(done) {
  function onMessage(msg, child) {
    switch (msg) {
    case 'init':
      child.send({
        log: 'foo'
      });
      break;
    case 'listen':
      break;
    }
  }

  spawn({
    onMessage: onMessage,
    onExit: done,
    echoErr: false,
    exitCode: 1,
    testStderr: function(stderr) {
      assert(/msg.log must be an object/.test(stderr));
    }
  });
});

// should require msg.key to not be group or world readable or writable
tasks.push(function(done) {
  function onMessage(msg, child) {
    switch (msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG },
        cert: cert,
        key: cert,
        dhparam: 'foo'
      });
      break;
    case 'listen':
      break;
    }
  }

  spawn({
    onMessage: onMessage,
    onExit: done,
    echoErr: false,
    exitCode: 12,
    testStderr: function(stderr) {
      assert(/msg.key should not be group or world readable or writable/.test(stderr));
    }
  });
});

// should require msg.cert, key and dhparam to exist
tasks.push(function(done) {
  function onMessage(msg, child) {
    switch (msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG },
        cert: 'foo',
        key: key,
        dhparam: 'foo'
      });
      break;
    case 'listen':
      break;
    }
  }

  spawn({
    onMessage: onMessage,
    onExit: done,
    echoErr: false,
    exitCode: 1,
    testStderr: function(stderr) {
      assert(/Error: ENOENT: no such file or directory, open 'foo'/.test(stderr));
    }
  });
});

// should chroot and listen
tasks.push(function(done) {
  function onMessage(msg, child) {
    switch (msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG },
        cert: cert,
        key: key,
        dhparam: dhparam
      });
      break;
    case 'listen':
      var client = net.createConnection(3344, function() {
        client.end(function() {
          child.kill();
        });
      });
    }
  }

  spawn({
    onMessage: onMessage,
    onExit: done,
    testStdout: function(stdout) {
      assert(/wss changed root to \/var\/empty and user:group to _pdbnull:_pdbnull/.test(stdout));
    }
  });
});

// should chroot, connect and do websocket handshake and proxy to the proxyPort
tasks.push(function(done) {
  var tcpPort = 1234;
  function onSpawn(child) {
    // start a tcp server that the websocket server should proxy to
    var tcpServer = net.createServer(function(conn) {
      // expect data that is fed to the wss server
      conn.setEncoding('utf8');
      conn.on('data', function(data) {
        assert.strictEqual(data, 'some text text data');
        tcpServer.close();
        child.kill();
      });
    });
    tcpServer.listen(tcpPort);
  }

  function onMessage(msg, child) {
    switch (msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG },
        cert: cert,
        key: key,
        dhparam: dhparam,
        proxyPort: tcpPort
      });
      break;
    case 'listen':
      var client = ws.connect('wss://localhost:3344', wsClientOpts, function() {
        client.sendText('some text text data');
      });
    }
  }

  spawn({
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: done,
    testStdout: function(stdout) {
      assert(/wss changed root to \/var\/empty and user:group to _pdbnull:_pdbnull/.test(stdout));
    }
  });
});

// should proxy data request back to websocket client
tasks.push(function(done) {
  var authReq = {
    username: 'foo',
    password: 'bar',
    db: 'qux'
  };

  var tcpPort = 1234;
  function onSpawn() {
    // start a tcp server that the websocket server should proxy to
    var tcpServer = net.createServer(function(conn) {
      // expect data that is fed to the wss server
      conn.on('data', function(data) {
        assert.strictEqual(data.toString(), JSON.stringify(authReq) + '\n');
        conn.write(JSON.stringify({ start: true }) + '\n');
        tcpServer.close();
      });
    });
    tcpServer.listen(tcpPort);
  }

  function onMessage(msg, child) {
    switch (msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG },
        cert: cert,
        key: key,
        dhparam: dhparam,
        proxyPort: tcpPort
      });
      break;
    case 'listen':
      var client = ws.connect('wss://localhost:3344', wsClientOpts, function() {
        client.sendText(JSON.stringify(authReq) + '\n');
        // expect data request
        client.on('text', function(data) {
          assert.strictEqual(data, JSON.stringify({ start: true }) + '\n');
          client.on('close', function(code, reason) {
            assert.strictEqual(code, 9823);
            assert.strictEqual(reason, 'test');
            child.kill();
          });
          client.close(9823, 'test');
        });
      });
    }
  }

  spawn({
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: done,
    testStdout: function(stdout) {
      assert(/wss changed root to \/var\/empty and user:group to _pdbnull:_pdbnull/.test(stdout));
    }
  });
});

// should proxy data request + BSON response back to websocket client in two separate writes
tasks.push(function(done) {
  var allDone;

  function onExit() {
    assert.strictEqual(allDone, true);
    done();
  }

  var authReq = {
    username: 'foo',
    password: 'bar',
    db: 'qux'
  };

  var dataReq = {
    start: true
  };

  var bsonObj = { h: { id: true } };

  var tcpPort = 1234;
  // start a tcp server that the websocket server should proxy to
  function onSpawn() {
    var tcpServer = net.createServer(function(conn) {
      // expect data that is fed to the wss server
      conn.on('data', function(data) {
        assert.strictEqual(data.toString(), JSON.stringify(authReq) + '\n');
        conn.write(JSON.stringify(dataReq) + '\n');
        conn.write(BSON.serialize(bsonObj));
        tcpServer.close();
      });
    });
    tcpServer.listen(tcpPort);
  }

  function onMessage(msg, child) {
    switch (msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG },
        cert: cert,
        key: key,
        dhparam: dhparam,
        proxyPort: tcpPort
      });
      break;
    case 'listen':
      var client = ws.connect('wss://localhost:3344', wsClientOpts, function() {
        client.sendText(JSON.stringify(authReq) + '\n');
        // expect data request back
        client.on('text', function(data) {
          assert.strictEqual(data, JSON.stringify(dataReq) + '\n');

          // expect bson
          client.on('binary', function(rs) {
            rs.on('readable', function() {
              var data = rs.read();
              assert.strictEqual(data.toString('hex'), BSON.serialize(bsonObj).toString('hex'));
              client.on('close', function(code, reason) {
                assert.strictEqual(code, 9823);
                assert.strictEqual(reason, 'test');
                allDone = true;
                child.kill();
              });
              client.close(9823, 'test');
            });
          });
        });
      });
    }
  }

  spawn({
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: onExit,
    testStdout: function(stdout) {
      assert(/wss changed root to \/var\/empty and user:group to _pdbnull:_pdbnull/.test(stdout));
    }
  });
});

// should proxy data request + BSON response back to websocket client in one write (check pipe(ls) unpipe(ls))
// TODO: fix ld-jsonstream
tasks2.push(function(done) {
  var allDone;

  function onExit() {
    assert.strictEqual(allDone, true);
    done();
  }

  var authReq = {
    username: 'foo',
    password: 'bar',
    db: 'qux'
  };

  var dataReq = {
    start: true
  };

  var bsonObj = { h: { id: true } };

  var tcpPort = 1234;
  // start a tcp server that the websocket server should proxy to
  function onSpawn() {
    var tcpServer = net.createServer(function(conn) {
      // expect data that is fed to the wss server
      conn.on('data', function(data) {
        assert.strictEqual(data.toString(), JSON.stringify(authReq) + '\n');
        conn.write(JSON.stringify(dataReq) + '\n' + BSON.serialize(bsonObj));
        tcpServer.close();
      });
    });
    tcpServer.listen(tcpPort);
  }

  function onMessage(msg, child) {
    switch (msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG },
        cert: cert,
        key: key,
        dhparam: dhparam,
        proxyPort: tcpPort
      });
      break;
    case 'listen':
      var client = ws.connect('wss://localhost:3344', wsClientOpts, function() {
        client.sendText(JSON.stringify(authReq) + '\n');
        // expect data request back
        client.on('text', function(data) {
          assert.strictEqual(data, JSON.stringify(dataReq) + '\n');

          // expect bson
          client.on('binary', function(rs) {
            rs.on('readable', function() {
              var data = rs.read();
              assert.strictEqual(data.toString('hex'), BSON.serialize(bsonObj).toString('hex'));
              client.on('close', function(code, reason) {
                assert.strictEqual(code, 9823);
                assert.strictEqual(reason, 'test');
                allDone = true;
                child.kill();
              });
              client.close(9823, 'test');
            });
          });
        });
      });
    }
  }

  spawn({
    onSpawn: onSpawn,
    onMessage: onMessage,
    onExit: onExit,
    testStdout: function(stdout) {
      assert(/wss changed root to \/var\/empty and user:group to _pdbnull:_pdbnull/.test(stdout));
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
      if (err) { console.error(err); }
    });
  });
});
