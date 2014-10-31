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

var assert = require('assert');
var childProcess = require('child_process');

var async = require('async');

var tasks = [];

// should fail if required directly
tasks.push(function(done) {
  assert.throws(function () {
    require('./../../lib/versioned_collection_exec');
  }, /this module should be invoked via child_process.fork/);
  done();
});

// should fail if spawned
tasks.push(function(done) {
  var child = childProcess.spawn('node', [__dirname + '/../../lib/versioned_collection_exec']);

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/this module should be invoked via child_process.fork/.test(buff.toString()));
    assert.strictEqual(code, 8);
    assert.strictEqual(sig, null);
    done();
  });
});

// should not fail if forked
tasks.push(function(done) {
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

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
  setTimeout(function() {
    child.kill();
  }, 0);
});

// should fail if first message is not an object
tasks.push(function(done) {
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/msg must be an object/.test(buff.toString()));
    assert.strictEqual(code, 8);
    assert.strictEqual(sig, null);
    done();
  });

  child.send(0);
});

// should fail if first message does not contain a db config
tasks.push(function(done) {
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/msg.dbConfig must be an object/.test(buff.toString()));
    assert.strictEqual(code, 8);
    assert.strictEqual(sig, null);
    done();
  });

  child.send({});
});

// should fail if first message does not contain a chroot config
tasks.push(function(done) {
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/msg.chrootConfig must be an object/.test(buff.toString()));
    assert.strictEqual(code, 8);
    assert.strictEqual(sig, null);
    done();
  });

  child.send({
    dbConfig: {}
  });
});

// should fail if first message does not contain a vc config
tasks.push(function(done) {
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/msg.vcConfig must be an object/.test(buff.toString()));
    assert.strictEqual(code, 8);
    assert.strictEqual(sig, null);
    done();
  });

  child.send({
    dbConfig: {},
    chrootConfig: {}
  });
});

// should fail if dbConfig in first message does not contain valid db name
tasks.push(function(done) {
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/config.dbName must be a string/.test(buff.toString()));
    assert.strictEqual(code, 8);
    assert.strictEqual(sig, null);
    done();
  });

  child.send({
    dbConfig: {},
    chrootConfig: {},
    vcConfig: {}
  });
});

// should fail if chrootConfig in first message does not contain valid user name
tasks.push(function(done) {
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/chrootCfg.user must be a string/.test(buff.toString()));
    assert.strictEqual(code, 8);
    assert.strictEqual(sig, null);
    done();
  });

  child.send({
    dbConfig: {
      dbName: 'test',
      dbPort: 27019
    },
    chrootConfig: {},
    vcConfig: {}
  });
});

// should fail if chrootConfig in first message does not contain valid root path
tasks.push(function(done) {
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/chrootCfg.newRoot must be a string/.test(buff.toString()));
    assert.strictEqual(code, 8);
    assert.strictEqual(sig, null);
    done();
  });

  child.send({
    dbConfig: {
      dbName: 'test',
      dbPort: 27019
    },
    chrootConfig: {
      user: 'test'
    },
    vcConfig: {}
  });
});

// should fail if vcCfg in first message does not contain valid collection name
tasks.push(function(done) {
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/vcCfg.collectionName must be a string/.test(buff.toString()));
    assert.strictEqual(code, 8);
    assert.strictEqual(sig, null);
    done();
  });

  child.send({
    dbConfig: {
      dbName: 'test',
      dbPort: 27019
    },
    chrootConfig: {
      user: 'test',
      newRoot: '/var/empty'
    },
    vcConfig: {}
  });
});

// should fail if not executed as root
tasks.push(function(done) {
  var child = childProcess.fork(__dirname + '/../../lib/versioned_collection_exec', { silent: true });

  var buff = new Buffer(0);
  child.stderr.on('data', function(data) {
    buff = Buffer.concat([buff, data]);
  });
  child.on('exit', function(code, sig) {
    assert(/chroot must be called while running as root/.test(buff.toString()));
    assert.strictEqual(code, 1);
    assert.strictEqual(sig, null);
    done();
  });

  child.send({
    dbConfig: {
      dbName: 'test',
      dbPort: 27019
    },
    chrootConfig: {
      user: 'test',
      newRoot: '/var/empty'
    },
    vcConfig: {
      collectionName: 'test'
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
