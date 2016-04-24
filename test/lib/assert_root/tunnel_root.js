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

var async = require('async');

var spawn = require('../../lib/spawn');
var logger = require('../../../lib/logger');

var tasks = [];
var tasks2 = [];

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

// should connect to remote server
tasks.push(function(done) {
  var opts = {
    onMessage: function(msg, child) {
      switch (msg) {
      case 'init':
        child.send({
          log: { console: true, mask: logger.DEBUG },
          sshUser: 'foo',
          key: __dirname + '/tunnelkey',
          forward: '27017:127.0.0.1:27017',
          host: 'foo.example.net',
          fingerprint: '3Omnu+Vf28fJAwJyP4xt3J/6hmY7Q1m9otK2tLitHds'
        });
        break;
      case 'listen':
        child.kill();
        break;
      }
    },
    onExit: done,
    echoOut: true,
    testStdout: function(stdout) {
      assert(/tunnel connected to foo.example.net/.test(stdout));
      assert(/tunnel ready/.test(stdout));
    }
  };
  spawn([__dirname + '/../../../lib/tunnel'], opts);
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
    });
  });
});
