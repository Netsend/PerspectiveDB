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
var net = require('net');

var async = require('async');
var LDJSONStream = require('ld-jsonstream');

var spawn = require('../../lib/spawn');

var tasks = [];
var tasks2 = [];

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

  var opts = {
    onMessage: onMessage,
    onExit: done,
    echoErr: false,
    exitCode: 1,
    testStderr: function(stderr) {
      assert(/msg.log must be an object/.test(stderr));
    }
  };
  spawn([__dirname + '/../../../lib/client_exec'], opts);
});

// should require msg.username
tasks.push(function(done) {
  function onMessage(msg, child) {
    switch (msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG }
      });
      break;
    case 'listen':
      break;
    }
  }

  var opts = {
    onMessage: onMessage,
    onExit: done,
    echoErr: false,
    exitCode: 1,
    testStderr: function(stderr) {
      assert(/msg.username must be a string/.test(stderr));
    }
  };
  spawn([__dirname + '/../../../lib/client_exec'], opts);
});

// should require msg.port to be a number
tasks.push(function(done) {
  function onMessage(msg, child) {
    switch (msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG },
        username: 'foo',
        password: 'foo',
        database: 'foo',
        port: 'foo'
      });
      break;
    case 'listen':
      break;
    }
  }

  var opts = {
    onMessage: onMessage,
    onExit: done,
    echoErr: false,
    exitCode: 1,
    testStderr: function(stderr) {
      assert(/msg.port must be a number/.test(stderr));
    }
  };
  spawn([__dirname + '/../../../lib/client_exec'], opts);
});

// should send auth request to server
tasks.push(function(done) {
  var port = 12345;

  var srv = net.createServer(function(conn) {
    conn.pipe(new LDJSONStream()).once('data', function(obj) {
      assert.deepEqual(obj, {
        username: 'foo',
        password: 'bar',
        db: 'qux'
      });
      conn.end('ack');
    });
  }).listen(port, '127.0.0.1');

  function onMessage(msg, child, handle) {
    switch (msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG },
        username: 'foo',
        password: 'bar',
        database: 'qux',
        port: port
      });
      break;
    case 'listen':
      break;
    default:
      assert.strictEqual(msg, 'connection');
      handle.on('data', function(data) {
        assert.strictEqual(data.toString(), 'ack');
        child.send({ type: 'kill' });
        srv.close();
      });
    }
  }

  var opts = {
    onMessage: onMessage,
    onExit: done
  };
  spawn([__dirname + '/../../../lib/client_exec'], opts);
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
