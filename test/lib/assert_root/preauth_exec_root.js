/**
 * Copyright 2014 Netsend.
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
var fs = require('fs');
var c = require('constants');

var async = require('async');
var ws = require('nodejs-websocket');

var tasks = [];
var tasks2 = [];

// print line number
function lnr() {
  return new Error().stack.split('\n')[2].match(/preauth_exec_root.js:([0-9]+):[0-9]+/)[1];
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
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/preauth_exec', { silent: true });

  //child.stderr.pipe(process.stderr);
  //child.stdout.pipe(process.stdout);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });
  child.on('close', function(code, sig) {
    assert(/msg.log must be an object/.test(stderr));
    assert.strictEqual(code, 1);
    assert.strictEqual(sig, null);
    done();
  });

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      child.send({
        log: 'foo'
      });
      break;
    case 'listen':
      break;
    }
  });
});

// should require msg.port to be a number
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/preauth_exec', { silent: true });

  //child.stderr.pipe(process.stderr);
  //child.stdout.pipe(process.stdout);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });
  child.on('close', function(code, sig) {
    assert(/msg.port must be a number/.test(stderr));
    assert.strictEqual(code, 1);
    assert.strictEqual(sig, null);
    done();
  });

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG },
        port: 'foo'
      });
      break;
    case 'listen':
      break;
    }
  });
});

// should disconnect after receiving more than 512 bytes of data without newlines
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/preauth_exec', { silent: true });

  //child.stderr.pipe(process.stderr);
  //child.stdout.pipe(process.stdout);

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.on('data', function(data) { stderr += data; });
  child.on('close', function(code, sig) {
    assert(/Error: more than maxBytes received/.test(stderr));
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG },
        port: 1234
      });
      break;
    case 'listen':
      assert(/preauth changed root to \/var\/empty and user:group to nobody:nobody/.test(stdout));

      var ms = net.createConnection(1234);

      var pattern = 'abcdefgh';
      var i = 0;
      var flood = function() {
        i++;
        setTimeout(function() {
          ms.write(pattern, function(err) {
            if (err) {
              assert(/EPIPE|This socket is closed|This socket has been ended by the other party/.test(err.message));
              assert(i >= 65 && i <= 67);
              child.kill();
            } else {
              flood();
            }
          });
        }, 0);
      };

      // start as valid JSON data
      ms.write('{ "username" : "');
      flood(ms);

      ms.on('error', function(err) {
        assert(/EPIPE|ECONNRESET|This socket is closed|This socket has been ended by the other party/.test(err.message));
      });
      break;
    }
  });
});


// should not parse objects larger than max length
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/preauth_exec', { silent: true });

  //child.stderr.pipe(process.stderr);
  //child.stdout.pipe(process.stdout);

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.on('data', function(data) { stderr += data; });
  child.on('close', function(code, sig) {
    assert(/Error: more than maxBytes received/.test(stderr));
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG },
        port: 1234
      });
      break;
    case 'listen':
      assert(/preauth changed root to \/var\/empty and user:group to nobody:nobody/.test(stdout));

      var ms = net.createConnection(1234);

      var pattern = 'abcdefgh';
      var obj = { username: '' };
      for (var i = 0; i < 130; i++) {
        obj.username += pattern;
      }

      ms.write(JSON.stringify(obj) + '\n');

      ms.on('close', function() {
        child.kill();
      });
      /*
      ms.on('error', function(err) {
        assert(/EPIPE/.test(err.code));
        console.log(err, i);
        child.kill();
      });
      */
      break;
    }
  });
});

// should pass auth request to parent
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var authReq = {
    username: 'foo',
    password: 'bar',
    offset: 'qux'
  };

  var child = childProcess.fork(__dirname + '/../../../lib/preauth_exec', { silent: true });

  child.stderr.pipe(process.stderr);
  //child.stdout.pipe(process.stdout);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });
  child.on('close', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG },
        port: 1234
      });
      break;
    case 'listen':
      // write auth request
      var ms = net.createConnection(1234);
      ms.end(JSON.stringify(authReq) + '\n');
      break;
    default:
      assert.deepEqual(msg, {
        username: 'foo',
        password: 'bar',
        offset: 'qux'
      });
      child.kill();
    }
  });
});

// should start a secure websocket server and pass auth request to parent
tasks.push(function(done) {
  console.log('test #%d', lnr());

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

  var authReq = {
    username: 'foo',
    password: 'bar',
    offset: 'qux'
  };

  var child = childProcess.fork(__dirname + '/../../../lib/preauth_exec', { silent: true });

  child.stderr.pipe(process.stderr);
  //child.stdout.pipe(process.stdout);

  var stderr = '';
  var stdout = '';
  child.stderr.setEncoding('utf8');
  child.stdout.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });
  child.stdout.on('data', function(data) { stdout += data; });
  child.on('close', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG },
        port: 1234,
        wss: true,
        cert: cert,
        key: key,
        dhparam: dhparam
      });
      break;
    case 'listen':
      // write auth request via a wss client
      assert(/wss changed root to \/var\/empty and user:group to nobody:nobody/.test(stdout));

      var client = ws.connect('wss://localhost:3344', wsClientOpts, function() {
        client.sendText(JSON.stringify(authReq) + '\n');
        // expect auth response
        client.on('text', function(data) {
          assert(data, JSON.stringify({ start: true }));
          client.on('close', function(code, reason) {
            assert(code, 9823);
            assert(reason, 'test');
            child.kill();
          });
          client.close(9823, 'test');
        });
      });
      break;
    default:
      assert.deepEqual(msg, {
        username: 'foo',
        password: 'bar',
        offset: 'qux'
      });
      child.kill();
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
