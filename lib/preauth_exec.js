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

var programName = require('path').basename(__filename, '.js');

if (process.getuid() !== 0) {
  console.error('%s: execute as root', programName);
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
var connManager = require('./conn_manager');
var logger = require('./logger');

/**
 * Instantiate a server that listens to incoming auth requests.
 *
 * Full FSM: init --> listen
 *
 * The first message emitted is "init" which signals that this process is ready to
 * receive the configuration data. This is an optional server config, an optional
 * chroot config and the log configuration. File desciptors for the log config
 * should be passed while forking this process and metioned in the log config.
 *
 * {
 *   logCfg:         {Object}     // log configuration
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

var log; // used after receiving the log configuration

function connErrorHandler(conn, e) {
  log.err('connection error: "%s"', e);
  try {
    var error = { error: 'invalid auth request' };
    log.err('%j, %s, %s', conn.address(), error, e);
    conn.write(BSON.serialize(error));
    conn.destroy();
  } catch(err) {
    log.err('connection write or disconnect error: "%s"', err);
  }
}

// filter password out request
function debugReq(req) {
  return keyFilter(req, ['password']);
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

var ldjs = [];

var cm;

function connHandler(conn) {
  log.info('client connected: %s %j', createConnId(conn), conn.address());

  cm.registerIncoming(conn);

  var connId = createConnId(conn);

  // create line delimited json stream
  var ls = new LDJSONStream({ maxBytes: 1024 });

  log.info('ldjsonstream registering %s', connId);

  ldjs.push(ls);

  ls.on('error', function(err) {
    log.err('ldjsonstream error: %s %s', err, connId);
    conn.unpipe(ls);
    conn.end();
    ls.end();
  });

  ls.once('end', function() {
    log.info('ldjsonstream end: %s', connId);

    log.info('ls end unpipe conn--ls');
    conn.unpipe(ls);
    conn.end();

    var i = ldjs.indexOf(ls);
    if (~i) {
      log.info('deleting ldjsonstream %s', i);
      ldjs.splice(i, 1);
    } else {
      log.err('ldjsonstream not found %s', i);
    }
  });

  ls.once('data', function(req) {
    log.info('req received %j', debugReq(req));

    if (!authRequest.valid(req)) {
      log.err('invalid auth request %j', debugReq(req));
      connErrorHandler(conn, 'invalid auth request');
      return;
    }

    // send auth request to parent for validation, with the connection socket
    process.send(req, conn);
  });

  conn.pipe(ls);

  conn.once('error', function(err) {
    log.info('conn error unpipe conn--ls %s', err);
    conn.unpipe(ls);
    ls.end();
  });

  conn.once('end', function() {
    log.info('conn end');
    // end events will be forwarded down the pipe
  });
}

if (typeof process.send !== 'function') {
  throw new Error('this module should be invoked via child_process.fork');
}

process.send('init');

// expect one init request (request that contains server and chroot config)
process.once('message', function(msg) {
  if (typeof msg !== 'object') { throw new TypeError('msg must be an object'); }
  if (typeof msg.logCfg !== 'object') { throw new TypeError('msg.logCfg must be an object'); }

  msg.serverConfig = msg.serverConfig || {};
  msg.chrootConfig = msg.chrootConfig || {};

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

  programName = 'preauth ' + port;

  process.title = 'ms/' + programName;

  msg.logCfg.ident = programName;

  // open log
  logger(msg.logCfg, function(err, l) {
    if (err) { l.err(err); throw err; }

    log = l;
    msg.log = l; // use this logger in the vc's as well

    cm = connManager.create({ log: log });

    var shutdownTasks = [];

    shutdownTasks.push(function(cb) {
      cm.close(function(err) {
        if (err) { cb(err); return; }
        log.info('connections closed');
        cb();
      });
    });

    shutdownTasks.push(function(cb) {
      async.each(ldjs, function(ls, cb2) {
        log.info('ldjsonstream closing...');
        ls.once('error', cb2);
        ls.once('end', cb2);
        ls.end();
      }, function(err) {
        if (err) { cb(err); return; }
        log.info('ldjsonstream closed');
        cb();
      });
    });

    // TODO: prevent race conditions and start listening after chroot

    // cleanup any previously created socket
    try {
      var lstat = fs.lstatSync(path);
      if (lstat.isSocket()) {
        log.info('unlinking previously created socket: %s', path);
        fs.unlinkSync(path);
      } else {
        throw new Error('path already exists and is not a socket');
      }
    } catch(err) {
      if (err.code !== 'ENOENT') {
        log.err(err, path);
        process.emit('error', err);
        return;
      }
    }

    if (path) {
      // make socket world writable
      var omask = process.umask('000');

      var serverDomain = net.createServer(connHandler);

      serverDomain.on('close', function() {
        log.notice('UNIX domain socket closed %s', path);
      });

      // open UNIX domain socket
      serverDomain.listen(path, function() {
        log.notice('UNIX domain socket bound %s', path);
      });

      if (omask !== process.umask()) {
        process.umask(omask);
        log.info('restored process umask %s', omask.toString(8));
      } else {
        log.info('warning, running process without clearing write bit for others %s', omask.toString(8));
      }

      shutdownTasks.push(function(cb) {
        serverDomain.close(cb);
      });
    }

    // chroot and drop privileges
    try {
      chroot(newRoot, user);
      log.notice('changed root to "%s" and user to "%s"', newRoot, user);
    } catch(err) {
      log.err('changing root or user failed', err);
      process.exit(1);
    }

    if (host) {
      var serverInet = net.createServer(connHandler);

      serverInet.on('error', function(err) {
        log.err('INET socket error %s:%s', host, port, err);
        process.emit('error', err);
      });

      serverInet.on('close', function() {
        log.notice('INET socket closed %s:%s', host, port);
      });

      // open INET socket
      serverInet.listen(port, host, function(err) {
        if (err) {
          log.err(err, host, port);
          process.emit('error', err);
          return;
        }
        log.notice('INET socket bound %s:%s', host, port);
        process.send('listen');
      });

      shutdownTasks.push(function(cb) {
        log.info('closing INET socket...');
        serverInet.getConnections(function(err, count) {
          log.info('INET socket connections %s', count);
          var serverInetClosed = false;
          serverInet.close(function() {
            serverInetClosed = true;
            cb();
          });
          // TODO: check why serverInet.close does not always emit close event, even when connection count is 0
          // https://groups.google.com/forum/#!msg/nodejs/DzxUYzbzkns/9XYC74b0NVUJ might be related
          setTimeout(function() {
            if (serverInetClosed) {
              log.info('server closed');
              process.exit(); // not sure why the process does not exit yet 
              return;
            }
            if (!count) {
              log.info('server not closed, force quit');
              process.exit();
            }
          }, 100);
          log.info('waiting...');
        });
      });
    }

    // handle shutdown
    function shutdown() {
      log.notice('shutting down');
      async.series(shutdownTasks, function(err) {
        if (err) { log.err('shutdown error', err); }
        log.info('shutdown complete');
      });
    }

    // listen to kill signals
    process.once('SIGINT', shutdown);
    process.once('SIGTERM', shutdown);
  });
});
