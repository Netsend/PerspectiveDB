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

var tasks = [];
var tasks2 = [];

// should require serverConfig.port to be a number
tasks.push(function(done) {
  var child = childProcess.fork(__dirname + '/../../lib/preauth_exec', { silent: true });

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/msg.serverConfig.port must be a number/.test(buff.toString()));
    assert.strictEqual(code, 8);
    assert.strictEqual(sig, null);
    done();
  });

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      child.send({
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

// should open a socket and chroot
tasks.push(function(done) {
  // remove any previously created socket
  if (fs.existsSync('/var/run/ms-1234.sock')) {
    fs.unlink('/var/run/ms-1234.sock');
  }

  var child = childProcess.fork(__dirname + '/../../lib/preauth_exec', { silent: true });

  var stderr = new Buffer(0);
  var stdout = new Buffer(0);
  child.stdout.on('data', function(data) {
    stdout = Buffer.concat([stdout, data]);
  });
  child.stderr.on('data', function(data) {
    stderr = Buffer.concat([stderr, data]);
  });
  child.on('exit', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 143);
    assert.strictEqual(sig, null);
    done();
  });

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      child.send({
        serverConfig: {
          port: 1234
        }
      });
      break;
    case 'listen':
      assert(fs.existsSync('/var/run/ms-1234.sock'));
      assert(/preauth_exec: changed root to "\/var\/empty" and user to "nobody"/.test(stdout.toString()));
      child.kill();
      break;
    }
  });
});

// should disconnect after receiving more than 1KB of data without newlines
tasks.push(function(done) {
  // remove any previously created socket
  if (fs.existsSync('/var/run/ms-1234.sock')) {
    fs.unlink('/var/run/ms-1234.sock');
  }

  var child = childProcess.fork(__dirname + '/../../lib/preauth_exec', { silent: true });

  var stderr = new Buffer(0);
  var stdout = new Buffer(0);
  child.stdout.on('data', function(data) {
    stdout = Buffer.concat([stdout, data]);
  });
  child.stderr.on('data', function(data) {
    stderr = Buffer.concat([stderr, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/preauth_exec: connection error: "Error: more than maxBytes received"/.test(stderr.toString()));
    assert.strictEqual(code, 143);
    assert.strictEqual(sig, null);
    done();
  });

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      child.send({
        serverConfig: {
          port: 1234
        }
      });
      break;
    case 'listen':
      assert(fs.existsSync('/var/run/ms-1234.sock'));
      assert(/preauth_exec: changed root to "\/var\/empty" and user to "nobody"/.test(stdout.toString()));

      var ms = net.createConnection('/var/run/ms-1234.sock');

      var pattern = 'abcdefgh';
      var i = 0;
      var flood = function() {
        i++;
        setTimeout(function() {
          ms.write(pattern, function(err) {
            if (err) {
              assert(/EPIPE/.test(err.code));
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
        assert(/EPIPE/.test(err.code));
      });
      break;
    }
  });
});


// should not parse objects larger than max length
tasks.push(function(done) {
  // remove any previously created socket
  if (fs.existsSync('/var/run/ms-1234.sock')) {
    fs.unlink('/var/run/ms-1234.sock');
  }

  var child = childProcess.fork(__dirname + '/../../lib/preauth_exec', { silent: true });

  var stderr = new Buffer(0);
  var stdout = new Buffer(0);
  child.stdout.on('data', function(data) {
    stdout = Buffer.concat([stdout, data]);
  });
  child.stderr.on('data', function(data) {
    stderr = Buffer.concat([stderr, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/preauth_exec: connection error: "Error: more than maxBytes received"/.test(stderr.toString()));
    assert.strictEqual(code, 143);
    assert.strictEqual(sig, null);
    done();
  });

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      child.send({
        serverConfig: {
          port: 1234
        }
      });
      break;
    case 'listen':
      assert(fs.existsSync('/var/run/ms-1234.sock'));
      assert(/preauth_exec: changed root to "\/var\/empty" and user to "nobody"/.test(stdout.toString()));

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

async.series(tasks, function(err) {
  if (err) {
    console.error(err);
  } else {
    console.log('ok');
  }
});
