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
var childProcess = require('child_process');

var async = require('async');
var LDJSONStream = require('ld-jsonstream');

var logger = require('../../../lib/logger');

// make sure a mongo test database is running
var dbPort = 27019;

var tasks = [];
var tasks2 = [];

// print line number
function lnr() {
  return new Error().stack.split('\n')[2].match(/mongo_client_exec_root.js:([0-9]+):[0-9]+/)[1];
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

// should require db string
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.fork(__dirname + '/../../../lib/mongo_client_exec', { silent: true });

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert(/msg.db must be a non-empty string/.test(stderr));
    assert.strictEqual(code, 1);
    assert.strictEqual(sig, null);
    done();
  });

  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true }
      });
      break;
    default:
      throw new Error('unknown state');
    }
  });
});

// should ask for last version over version control channel (fd 7)
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = childProcess.spawn(process.execPath, [__dirname + '/../../../lib/mongo_client_exec'], {
    cwd: '/',
    env: {},
    stdio: ['pipe', 'pipe', 'pipe', 'ipc', null, null, 'pipe', 'pipe']
  });

  var versionControl = child.stdio[7];

  // expect version requests in ld-json format
  var ls = new LDJSONStream();

  versionControl.pipe(ls);

  var validRequest = false;
  ls.on('data', function(data) {
    assert.deepEqual(data, {
      id: null
    });
    validRequest = true;

    child.kill();
  });

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    assert.strictEqual(validRequest, true);
    done();
  });

  // give the child some time to setup it's handlers https://github.com/joyent/node/issues/8667#issuecomment-61566101
  child.on('message', function(msg) {
    switch(msg) {
    case 'init':
      child.send({
        log: { console: true, mask: logger.DEBUG2 },
        db: 'test_mongo_client_exec_root',
        coll: 'test_mongo_client_exec_root',
        url: 'mongodb://127.0.0.1:' + dbPort
      });
      break;
    case 'listen':
      break;
    default:
      console.error(msg);
      throw new Error('unknown state');
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
