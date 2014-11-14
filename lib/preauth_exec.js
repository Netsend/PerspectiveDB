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

var chroot = require('chroot');
var LDJSONStream = require('ld-jsonstream');

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
    conn.write('invalid auth request\n');
    conn.destroy();
  } catch(err) {
    console.error('%s: connection write or disconnect error: "%s"', programName, err);
  }
}

function connHandler(conn) {
  console.log('%s: server connected', programName);

  conn.pipe(new LDJSONStream({ maxBytes: 1024 })).once('data', function(req) {
    console.log('%s: req received', programName, req);

    if (!authRequest.valid(req)) {
      connErrorHandler(conn, 'invalid auth request');
      return;
    }

    // send auth request to parent for validation, with the connection socket
    process.send(req, conn);
  }).on('error', function(err) {
    connErrorHandler(conn, err);
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

  var server = net.createServer(connHandler);

  // make sure socket does not exist
  if (fs.existsSync(path)) {
    var error = new Error('socket already exists');
    console.error('ERROR: socket already exists: %s', path);
    process.emit('error', error);
    return;
  }

  // TODO: prevent race conditions and start listening after chroot

  // open UNIX domain socket
  server.listen(path, function() {
    console.log('%s: server bound %s', programName, path);
  });

  // chroot and drop privileges
  try {
    chroot(newRoot, user);
    console.log('%s: changed root to "%s" and user to "%s"', programName, newRoot, user);
  } catch(err) {
    console.error('%s: changing root or user failed', programName, err);
    process.exit(1);
  }

  // open INET domain socket
  server.listen(port, host, function() {
    console.log('%s: server bound %s', programName, host, port);
  });

  process.send('listen');
});
