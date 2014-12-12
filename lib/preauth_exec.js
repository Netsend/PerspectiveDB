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
  console.error('execute as root');
  process.exit(1);
}

var net = require('net');
var fs = require('fs');

var async = require('async');
var chroot = require('chroot');
var LDJSONStream = require('ld-jsonstream');
var BSON = require('mongodb').BSON;

var authRequest = require('./auth_request');

var programName = require('path').basename(__filename, '.js');

/**
 * Instantiate a server that listens to incoming auth requests.
 *
 * This module should be forked. Once initialized an "init" message is sent. After
 * this and exactly one message should be sent with the
 * following structure:
 *
 * {
 *   [serverConfig]: {}
 *   [chrootConfig]: {}
 * }
 *
 * serverConfig:
 * {
 *   [host]:         {String}     // defaults to 127.0.0.1
 *   [port]:         {Number}     // defaults to 2344
 *   [path]:         {String}     // defaults to /var/run/ms-2344.sock
 * }
 *
 * chrootConfig:
 * {
 *   [user]:         {String}     // defaults to "nobody"
 *   [newRoot]:      {String}     // defaults to /var/empty
 * }
 *
 * No other messages should be sent to this process.
 *
 * If the server is listening a "listen" message is sent. After this only new
 * incoming auth requests are sent from this process to it's parent.
 */

function connErrorHandler(conn, e) {
  console.error('%s: connection error: "%s"', programName, e);
  try {
    var error = { error: 'invalid auth request' };
    console.error(JSON.stringify(conn.address()), error, e);
    conn.write(BSON.serialize(error));
    conn.destroy();
  } catch(err) {
    console.error('%s: connection write or disconnect error: "%s"', programName, err);
  }
}

// filter password out request
function debugReq(req) {
  var result = {};
  Object.keys(req).forEach(function(key) {
    if (key === 'password') {
      result.password = '*****';
    } else {
      result[key] = req[key];
    }
  });
  return JSON.stringify(result);
}

var conns = [];
function connHandler(conn) {
  console.log('%s: client connected: %s', programName, JSON.stringify(conn.address()));

  conn.pipe(new LDJSONStream({ maxBytes: 1024 })).once('data', function(req) {
    console.log('%s: req received', programName, debugReq(req));

    if (!authRequest.valid(req)) {
      connErrorHandler(conn, 'invalid auth request');
      return;
    }

    // send auth request to parent for validation, with the connection socket
    process.send(req, conn);
  }).on('error', function(err) {
    connErrorHandler(conn, err);
  });

  conns.push(conn);

  var remote = conn.remoteAddress + ':' + conn.remotePort;
  conn.on('close', function() {
    console.log('%s: connection closed %s', programName, remote);
  });

  conn.on('end', function() {
    console.log('%s: user disconnected', programName);
  });
}

if (typeof process.send !== 'function') {
  throw new Error('this module should be invoked via child_process.fork');
}

process.send('init');

// expect one init request (request that contains server and chroot config)
process.once('message', function(msg) {
  msg = msg || {};
  msg.serverConfig = msg.serverConfig || {};
  msg.chrootConfig = msg.chrootConfig || {};

  if (typeof msg !== 'object') { throw new TypeError('msg must be an object'); }
  if (typeof msg.serverConfig !== 'object') { throw new TypeError('msg.serverConfig must be an object'); }
  if (typeof msg.chrootConfig !== 'object') { throw new TypeError('msg.chrootConfig must be an object'); }

  // server config
  var host = msg.serverConfig.host || '127.0.0.1';
  var port = msg.serverConfig.port || 2344;
  var path = msg.serverConfig.path || '/var/run/ms-' + port + '.sock';

  // chroot config
  var user = msg.chrootConfig.user || 'nobody';
  var newRoot = msg.chrootConfig.newRoot || '/var/empty';

  if (typeof host !== 'string') { throw new TypeError('msg.serverConfig.host must be a string'); }
  if (typeof port !== 'number') { throw new TypeError('msg.serverConfig.port must be a number'); }
  if (typeof path !== 'string') { throw new TypeError('msg.serverConfig.path must be a string'); }

  if (typeof user !== 'string') { throw new TypeError('msg.chrootConfig.user must be a string'); }
  if (typeof newRoot !== 'string') { throw new TypeError('msg.chrootConfig.newRoot must be a string'); }

  // TODO: prevent race conditions and start listening after chroot

  // cleanup any previously created socket
  try {
    var lstat = fs.lstatSync(path);
    if (lstat.isSocket()) {
      console.log('unlinking previously created socket: %s', path);
      fs.unlinkSync(path);
    } else {
      throw new Error('path already exists and is not a socket');
    }
  } catch(err) {
    if (err.code !== 'ENOENT') {
      console.error(err, path);
      process.emit('error', err);
      return;
    }
  }

  if (path) {
    // make socket world writable
    var omask = process.umask('000');

    var serverDomain = net.createServer(connHandler);

    // open UNIX domain socket
    serverDomain.listen(path, function() {
      console.log('%s: domain socket bound %s', programName, path);
    });

    if (omask !== process.umask()) {
      process.umask(omask);
      console.log('%s: restored process umask %s', programName, omask.toString(8));
    } else {
      console.log('%s: warning, running process without clearing write bit for others %s', programName, omask.toString(8));
    }
  }

  // chroot and drop privileges
  try {
    chroot(newRoot, user);
    console.log('%s: changed root to "%s" and user to "%s"', programName, newRoot, user);
  } catch(err) {
    console.error('%s: changing root or user failed', programName, err);
    process.exit(1);
  }

  if (host) {
    var serverInet = net.createServer(connHandler);

    // open INET domain socket
    serverInet.listen(port, host, function(err) {
      if (err) {
        console.error(err, host, port);
        process.emit('error', err);
        return;
      }
      console.log('%s: inet server bound %s', programName, host, port);
      process.send('listen');
    });
  }

  // handle shutdown
  function shutdown() {
    console.log('%s: shutting down', programName);
    serverInet.close(function() {
      console.log('no new connections are accepted');
      serverDomain.close(function() {
        console.log('no new connections are accepted');
        async.each(conns, function(conn, cb) {
          conn.once('close', function(err) {
            if (err) { cb(new Error('socket closed due to a transmission error')); return; }
            cb();
          });
          // give the peer 200ms to end
          conn.setTimeout(200, function() {
            console.log('socket destroyed', conn.address());
            conn.destroy();
            cb();
          });
          conn.end();
          console.log('exiting');
        }, function(err) {
          if (err) { console.error(err); }
        });
      });
    });
  }

  // listen to kill signals
  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);
});
