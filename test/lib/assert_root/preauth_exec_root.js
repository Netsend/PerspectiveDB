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
var fs = require('fs');
var net = require('net');
var childProcess = require('child_process');

var async = require('async');

var logger = require('../../../lib/logger');

var tasks = [];
var tasks2 = [];

// should require logCfg.to be an object
tasks.push(function(done) {
  console.log('test l' + new Error().stack.split('\n')[1].match(/preauth_exec_root.js:([0-9]+):[0-9]+/)[1]); // print current line number

  var child = childProcess.fork(__dirname + '/../../../lib/preauth_exec', { silent: true });

  //child.stderr.pipe(process.stderr);
  //child.stdout.pipe(process.stdout);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });
  child.on('close', function(code, sig) {
    assert(/msg.logCfg must be an object/.test(stderr));
    assert.strictEqual(code, 8);
    assert.strictEqual(sig, null);
    done();
  });

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      child.send({
        logCfg: 'foo'
      });
      break;
    case 'listen':
      break;
    }
  });
});

// should require serverConfig.port to be a number
tasks.push(function(done) {
  console.log('test l' + new Error().stack.split('\n')[1].match(/preauth_exec_root.js:([0-9]+):[0-9]+/)[1]); // print current line number

  var child = childProcess.fork(__dirname + '/../../../lib/preauth_exec', { silent: true });

  //child.stderr.pipe(process.stderr);
  //child.stdout.pipe(process.stdout);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });
  child.on('close', function(code, sig) {
    assert(/msg.serverConfig.port must be a number/.test(stderr));
    assert.strictEqual(code, 8);
    assert.strictEqual(sig, null);
    done();
  });

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      child.send({
        logCfg: { console: true, mask: logger.DEBUG },
        serverConfig: {
          port: 'foo'
        }
      });
      break;
    case 'listen':
      break;
    }
  });
});

// should not start if socket path is taken by a regular file
tasks.push(function(done) {
  console.log('test l' + new Error().stack.split('\n')[1].match(/preauth_exec_root.js:([0-9]+):[0-9]+/)[1]); // print current line number

  // ensure normal file
  if (fs.existsSync('/var/run/ms-1234.sock')) {
    fs.unlinkSync('/var/run/ms-1234.sock');
  }
  fs.open('/var/run/ms-1234.sock', 'wx', function(err) {
    if (err) { throw err; }

    var child = childProcess.fork(__dirname + '/../../../lib/preauth_exec', { silent: true });

    //child.stderr.pipe(process.stderr);
    //child.stdout.pipe(process.stdout);

    var stderr = '';
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', function(data) { stderr += data; });
    child.on('close', function(code, sig) {
      assert(/path already exists and is not a socket/.test(stderr));
      assert.strictEqual(code, 8);
      assert.strictEqual(sig, null);
      fs.unlink('/var/run/ms-1234.sock');
      done();
    });

    child.on('message', function(msg) {
      switch (msg) {
      case 'init':
        child.send({
          logCfg: { console: true, mask: logger.DEBUG },
          serverConfig: {
            port: 1234
          }
        });
        break;
      }
    });
  });
});

// should open a socket and chroot
tasks.push(function(done) {
  console.log('test l' + new Error().stack.split('\n')[1].match(/preauth_exec_root.js:([0-9]+):[0-9]+/)[1]); // print current line number

  // remove any previously created socket
  if (fs.existsSync('/var/run/ms-1234.sock')) {
    fs.unlink('/var/run/ms-1234.sock');
  }

  var child = childProcess.fork(__dirname + '/../../../lib/preauth_exec', { silent: true });

  child.stderr.pipe(process.stderr);
  //child.stdout.pipe(process.stdout);

  var stdout = '';
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });

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
        logCfg: { console: true, mask: logger.DEBUG },
        serverConfig: {
          port: 1234
        }
      });
      break;
    case 'listen':
      assert(fs.existsSync('/var/run/ms-1234.sock'));
      assert(/preauth .*: changed root to "\/var\/empty" and user to "nobody"/.test(stdout));
      child.kill();
      break;
    }
  });
});

// should remove any previously created sockets
tasks.push(function(done) {
  console.log('test l' + new Error().stack.split('\n')[1].match(/preauth_exec_root.js:([0-9]+):[0-9]+/)[1]); // print current line number

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
        logCfg: { console: true, mask: logger.DEBUG },
        serverConfig: {
          port: 1234
        }
      });
      break;
    case 'listen':
      child.kill();
      break;
    }
  });
});

// should disconnect after receiving more than 1KB of data without newlines
tasks.push(function(done) {
  console.log('test l' + new Error().stack.split('\n')[1].match(/preauth_exec_root.js:([0-9]+):[0-9]+/)[1]); // print current line number

  // remove any previously created socket
  if (fs.existsSync('/var/run/ms-1234.sock')) {
    fs.unlink('/var/run/ms-1234.sock');
  }

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
    assert(/preauth .*: ldjsonstream error UNIX domain socket Error: more than maxBytes received/.test(stderr));
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      child.send({
        logCfg: { console: true, mask: logger.DEBUG },
        serverConfig: {
          port: 1234
        }
      });
      break;
    case 'listen':
      assert(fs.existsSync('/var/run/ms-1234.sock'));
      assert(/preauth .*: changed root to "\/var\/empty" and user to "nobody"/.test(stdout));

      var ms = net.createConnection('/var/run/ms-1234.sock');

      var pattern = 'abcdefgh';
      var i = 0;
      var flood = function() {
        i++;
        setTimeout(function() {
          ms.write(pattern, function(err) {
            if (err) {
              assert(/EPIPE|This socket is closed|This socket has been ended by the other party/.test(err.message));
              assert(i >= 127 && i <= 132);
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
  console.log('test l' + new Error().stack.split('\n')[1].match(/preauth_exec_root.js:([0-9]+):[0-9]+/)[1]); // print current line number

  // remove any previously created socket
  if (fs.existsSync('/var/run/ms-1234.sock')) {
    fs.unlink('/var/run/ms-1234.sock');
  }

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
    assert(/preauth .*: ldjsonstream error UNIX domain socket Error: more than maxBytes received/.test(stderr));
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      child.send({
        logCfg: { console: true, mask: logger.DEBUG },
        serverConfig: {
          port: 1234
        }
      });
      break;
    case 'listen':
      assert(fs.existsSync('/var/run/ms-1234.sock'));
      assert(/preauth .*: changed root to "\/var\/empty" and user to "nobody"/.test(stdout));

      var ms = net.createConnection('/var/run/ms-1234.sock');

      var pattern = 'abcdefgh';
      var obj = { username: '' };
      for (var i = 0; i < 130; i++) {
        obj.username += pattern;
      }

      ms.write(JSON.stringify(obj) + '\n');

      ms.on('close', function() {
        child.kill();
      });
      ms.on('error', function(err) {
        assert(/EPIPE/.test(err.code));
        console.log(err, i);
        child.kill();
      });
      break;
    }
  });
});

// should make socket world writable
tasks.push(function(done) {
  console.log('test l' + new Error().stack.split('\n')[1].match(/preauth_exec_root.js:([0-9]+):[0-9]+/)[1]); // print current line number

  // remove any previously created socket
  if (fs.existsSync('/var/run/ms-1234.sock')) {
    fs.unlink('/var/run/ms-1234.sock');
  }

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
        logCfg: { console: true, mask: logger.DEBUG },
        serverConfig: {
          port: 1234
        }
      });
      break;
    case 'listen':
      fs.stat('/var/run/ms-1234.sock', function(err, stat) {
        assert(stat.mode & 2);
        child.kill();
      });
      break;
    }
  });
});

// should pass auth request to parent
tasks.push(function(done) {
  console.log('test l' + new Error().stack.split('\n')[1].match(/preauth_exec_root.js:([0-9]+):[0-9]+/)[1]); // print current line number

  var authReq = {
    username: 'foo',
    password: 'bar',
    database: 'qux',
    collection: 'baz'
  };

  // remove any previously created socket
  if (fs.existsSync('/var/run/ms-1234.sock')) {
    fs.unlink('/var/run/ms-1234.sock');
  }

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
        logCfg: { console: true, mask: logger.DEBUG },
        serverConfig: {
          port: 1234
        }
      });
      break;
    case 'listen':
      // write auth request
      var ms = net.createConnection('/var/run/ms-1234.sock');
      ms.end(JSON.stringify(authReq) + '\n');
      break;
    default:
      assert.deepEqual(msg, {
        username: 'foo',
        password: 'bar',
        database: 'qux',
        collection: 'baz'
      });
      child.kill();
    }
  });
});

// should open a port on the local network interface by default
tasks.push(function(done) {
  console.log('test l' + new Error().stack.split('\n')[1].match(/preauth_exec_root.js:([0-9]+):[0-9]+/)[1]); // print current line number

  var authReq = {
    username: 'foo',
    password: 'bar',
    database: 'qux',
    collection: 'baz'
  };

  // remove any previously created socket
  if (fs.existsSync('/var/run/ms-1234.sock')) {
    fs.unlink('/var/run/ms-1234.sock');
  }

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
        logCfg: { console: true, mask: logger.DEBUG },
        serverConfig: {
          port: 1234
        }
      });
      break;
    case 'listen':
      // write auth request
      var ms = net.createConnection(1234, '127.0.0.1');
      ms.end(JSON.stringify(authReq) + '\n');
      break;
    default:
      assert.deepEqual(msg, {
        username: 'foo',
        password: 'bar',
        database: 'qux',
        collection: 'baz'
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
});
