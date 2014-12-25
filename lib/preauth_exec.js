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
var keyFilter = require('object-key-filter');

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
  return JSON.stringify(keyFilter(req, ['password']));
}

// return a string identifier of the connection if connected over ip
function createConnId(conn) {
  var connId;
  if (conn.remoteAddress) {
    connId = conn.remoteAddress + '-' + conn.remotePort + '-' + conn.localAddress + '-' + conn.localPort;
  } else {
    connId = 'UNIX domain socket';
  }
  return connId;
}

var conns = [];
function registerConnection(conn) {
  var connId = createConnId(conn);

  console.log('%s: conn registering %s', programName, conns.length, connId);

  conns.push(conn);

  conn.on('error', function(err) {
    console.error('%s: conn error: %s %s', programName, JSON.stringify(err), connId);
  });

  conn.once('end', function() {
    console.log('%s: conn end: %s', programName, connId);
  });

  conn.once('close', function() {
    console.log('%s: conn close: %s', programName, connId);
    var i = conns.indexOf(conn);
    if (~i) {
      console.log('%s: deleting conn %s', programName, i);
      conns.splice(i, 1);
    } else {
      console.error('%s: conn not found %s', programName, i);
    }
  });
}

function closeConnections(cb) {
  async.each(conns, function(conn, cb2) {
    var connId = createConnId(conn);

    console.log('%s: closing connection %s...', programName, connId);

    var closed = false;
    conn.once('close', function() {
      console.log('%s: conn closed %s', programName, connId);
      closed = true;
      cb2();
    });

    // give the peer some time to end
    setTimeout(function() {
      if (closed) { return; }

      console.log('%s: conn destroyed %s', programName, connId);
      conn.destroy();
      cb2();
    }, 100);

    conn.end();
  }, cb);
}

var ldjs = [];
function registerLdjs(ls, conn) {
  var connId = createConnId(conn);

  console.log('%s: ldjsonstream registering %s', programName, connId);

  ldjs.push(ls);

  ls.on('error', function(err) {
    console.error('%s: ldjsonstream error: %s %s', programName, JSON.stringify(err), connId);
    connErrorHandler(conn, err);
  });

  ls.once('end', function() {
    console.log('%s: ldjsonstream end: %s', programName, connId);
  });

  ls.once('close', function() {
    console.log('%s: ldjsonstream close: %s', programName, connId);
    var i = ldjs.indexOf(ls);
    if (~i) {
      console.log('deleting ldjsonstream %s', i);
      ldjs.splice(i, 1);
    } else {
      console.error('ldjsonstream not found %s', i);
    }
  });
}

function closeLdjs(cb) {
  async.each(ldjs, function(ls, cb2) {
    console.log('%s: ldjsonstream closing...', programName);

    var closed = false;
    ls.once('close', function() {
      closed = true;
      cb2();
    });

    ls.once('end', function() {
      closed = true;
      cb2();
    });

    // give the peer 200ms to end
    setTimeout(function() {
      if (closed) { return; }

      console.log('%s: ldjsonstream not destroyed', programName);
      //ls.close();
      cb2();
    }, 1500);

    ls.end();
  }, cb);
}

function connHandler(conn) {
  console.log('%s: client connected: %s %s', programName, createConnId(conn), JSON.stringify(conn.address()));

  registerConnection(conn);

  var ls = new LDJSONStream({ maxBytes: 1024 });

  registerLdjs(ls, conn);

  conn.pipe(ls);

  ls.once('data', function(req) {
    console.log('%s: req received', programName, debugReq(req));

    if (!authRequest.valid(req)) {
      connErrorHandler(conn, 'invalid auth request');
      return;
    }

    // send auth request to parent for validation, with the connection socket
    process.send(req, conn);
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
      console.log('%s: unlinking previously created socket: %s', programName, path);
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

  var shutdownTasks = [];

  shutdownTasks.push(function(cb) {
    closeConnections(function(err) {
      if (err) { cb(err); return; }
      console.log('%s: connections closed', programName);
      cb();
    });
  });

  shutdownTasks.push(function(cb) {
    closeLdjs(function(err) {
      if (err) { cb(err); return; }
      console.log('%s: ldjsonstream closed', programName);
      cb();
    });
  });

  if (path) {
    // make socket world writable
    var omask = process.umask('000');

    var serverDomain = net.createServer(connHandler);

    serverDomain.on('close', function() {
      console.log('%s: UNIX domain socket closed %s', programName, path);
    });

    // open UNIX domain socket
    serverDomain.listen(path, function() {
      console.log('%s: UNIX domain socket bound %s', programName, path);
    });

    if (omask !== process.umask()) {
      process.umask(omask);
      console.log('%s: restored process umask %s', programName, omask.toString(8));
    } else {
      console.log('%s: warning, running process without clearing write bit for others %s', programName, omask.toString(8));
    }

    shutdownTasks.push(function(cb) {
      serverDomain.close(cb);
    });
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

    serverInet.on('error', function(err) {
      console.error('%s: INET socket error %s:%s', programName, host, port, err);
    });

    serverInet.on('close', function() {
      console.log('%s: INET socket closed %s:%s', programName, host, port);
    });

    // open INET socket
    serverInet.listen(port, host, function(err) {
      if (err) {
        console.error(err, host, port);
        process.emit('error', err);
        return;
      }
      console.log('%s: INET socket bound %s:%s', programName, host, port);
      process.send('listen');
    });

    shutdownTasks.push(function(cb) {
      console.log('%s: closing INET socket...', programName);
      serverInet.getConnections(function(err, count) {
        console.log('%s: INET socket connections %s', programName, count);
        serverInet.close(cb);
        // TODO: check why serverInet.close does not always emit close event, even when connection count is 0
        process.nextTick(function() {
          if (!count) {
            console.log('%s: force quit', programName);
            process.exit();
          }
          console.log('%s: waiting...', programName);
        });
      });
    });
  }

  // handle shutdown
  function shutdown() {
    console.log('%s: shutting down', programName);
    async.series(shutdownTasks, function(err) {
      if (err) { console.error('%s: shutdown error', programName, err); }
      console.log('%s: shutdown complete', programName);
    });
  }

  // listen to kill signals
  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);
});
