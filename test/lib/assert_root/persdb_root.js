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

var c = require('constants');
var assert = require('assert');
var fs = require('fs');
var net = require('net');
var spawn = require('child_process').spawn;

var async = require('async');
var rimraf = require('rimraf');
var ws = require('nodejs-websocket');

var logger = require('../../../lib/logger');

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

var tasks = [];
var tasks2 = [];

// print line number
function lnr() {
  return new Error().stack.split('\n')[2].match(/persdb_root.js:([0-9]+):[0-9]+/)[1];
}

var cons, silence;
var chroot = '/var/persdb/test_persdb_root';
var dbPath = '/data';

// open loggers
tasks2.push(function(done) {
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
  console.log('test #%d', lnr());

  var child = spawn(__dirname + '/../../../bin/persdb.js', [__dirname + '/test_persdb.hjson']);

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert.strictEqual(stderr.length, 0);
    assert(/INET socket bound 127.0.0.1:1234/.test(stdout));
    assert(/client connected 127.0.0.1-/.test(stdout));
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  setTimeout(function() {
    net.createConnection(1234, function() {
      child.kill();
    });
  }, 1000);
});

// should try to start a WSS server and if this fails because of missing attributes (key, cert, dhparam) automatically shutdown
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = spawn(__dirname + '/../../../bin/persdb.js', [__dirname + '/test_persdb_wss_wrong_config.hjson']);

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    //assert.strictEqual(stderr.length, 0);
    assert(/msg.key must be a non-empty string/.test(stderr));
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });
});

// should start a WSS server, test by connecting with wss client
tasks.push(function(done) {
  console.log('test #%d', lnr());

  var child = spawn(__dirname + '/../../../bin/persdb.js', [__dirname + '/test_persdb_wss.hjson']);

  //child.stdout.pipe(process.stdout);
  child.stderr.pipe(process.stderr);

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert(/-127.0.0.1-1235: end \(no binary data received\)/.test(stdout));
    assert.strictEqual(stderr.length, 0);
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  setTimeout(function() {
    ws.connect('wss://localhost:1235', wsClientOpts, function() {
      child.kill();
    });
  }, 1000);
});

// should start a WSS server, and disconnect because of empty auth request
tasks2.push(function(done) {
  console.log('test #%d', lnr());

  var child = spawn(__dirname + '/../../../bin/persdb.js', [__dirname + '/test_persdb_wss.hjson']);

  //child.stdout.pipe(process.stdout);
  //child.stderr.pipe(process.stderr);

  var stdout = '';
  var stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', function(data) { stdout += data; });
  child.stderr.on('data', function(data) { stderr += data; });

  child.on('exit', function(code, sig) {
    assert(/-127.0.0.1-1234 invalid auth request/.test(stderr));
    assert.strictEqual(code, 0);
    assert.strictEqual(sig, null);
    done();
  });

  setTimeout(function() {
    var authReq = {};

    var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
      client.on('close', function() {
        child.kill();
      });
      client.sendText(JSON.stringify(authReq) + '\n');
      // expect auth response
      /*
      client.on('text', function(data) {
        assert(data, JSON.stringify({ start: true }));
        client.on('close', function(code, reason) {
          assert(code, 9823);
          assert(reason, 'test');
          child.kill();
        });
        client.close(9823, 'test');
      });
      */
    });
  }, 1000);
});

async.series(tasks2, function(err) {
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
