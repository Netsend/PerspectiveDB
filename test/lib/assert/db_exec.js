/**
 * Copyright 2014, 2015 Netsend.
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

var assert = require('assert');
var childProcess = require('child_process');

var async = require('async');

var tasks = [];

// print line number
function lnr() {
  return new Error().stack.split('\n')[2].match(/db_exec.js:([0-9]+):[0-9]+/)[1];
}

// should fail if required directly
tasks.push(function(done) {
  console.log('test #%d', lnr());

  assert.throws(function () {
    require('../../../lib/db_exec');
  }, /this module should be invoked via child_process.fork/);
  done();
});

// should fail if spawned
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.spawn('node', [__dirname + '/../../../lib/db_exec']);

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/this module should be invoked via child_process.fork/.test(buff.toString()));
    assert.strictEqual(code, 1);
    assert.strictEqual(sig, null);
    done();
  });
});

// should not fail if forked, exit because of SIGTERM
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert.strictEqual(buff.length, 0);
    assert.strictEqual(code, null);
    assert.strictEqual(sig, 'SIGTERM');
    done();
  });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.once('message', function(msg) {
    assert.strictEqual(msg, 'init');
    child.kill();
  });
});

// should fail if first message is not an object
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  child.on('exit', function(code, sig) {
    assert(/msg must be an object/.test(buff.toString()));
    assert.strictEqual(code, 1);
    assert.strictEqual(sig, null);
    done();
  });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.once('message', function(msg) {
    assert.strictEqual(msg, 'init');
    child.send(0);
  });
});

// should fail if first message does not contain a log object
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/msg.log must be an object/.test(buff.toString()));
    assert.strictEqual(code, 1);
    assert.strictEqual(sig, null);
    done();
  });

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.once('message', function(msg) {
    assert.strictEqual(msg, 'init');
    child.send({});
  });
});

// should fail path is not a string
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/msg.path must be a string/.test(buff.toString()));
    assert.strictEqual(code, 1);
    assert.strictEqual(sig, null);
    done();
  });

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      child.send({
        log: {},
        path: 1
      });
      break;
    default:
      console.error(msg);
      throw new Error('unknown state');
    }
  });
});

// should fail if user does not exist
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/user id does not exist test/.test(buff.toString()));
    assert.strictEqual(code, 3);
    assert.strictEqual(sig, null);
    done();
  });

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      child.send({
        log: { console: true },
        user: 'test'
      });
      break;
    default:
      console.error(msg);
      throw new Error('unknown state');
    }
  });
});

// should fail if not executed as root
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/db_exec', { silent: true });

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/chroot must be called while running as root/.test(buff.toString()));
    assert.strictEqual(code, 8);
    assert.strictEqual(sig, null);
    done();
  });

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  child.on('message', function(msg) {
    switch (msg) {
    case 'init':
      child.send({
        log: { console: true },
        path: '.',
        user: 'pdblevel',
        chroot: '/var/empty'
      });
      break;
    default:
      console.error(msg);
      throw new Error('unknown state');
    }
  });
});

// SEE assert_root/db_exec_root.js FOR FURTHER TESTING

async.series(tasks, function(err) {
  if (err) {
    console.error(err);
  } else {
    console.log('ok');
  }
});
