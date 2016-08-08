/**
 * Copyright 2014, 2015 Netsend.
 *
 * This file is part of PerspectiveDB.
 *
 * PerspectiveDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PerspectiveDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PerspectiveDB. If not, see <https://www.gnu.org/licenses/>.
 */

/* jshint -W116 */

'use strict';

var programName = require('path').basename(__filename, '.js');

if (process.getuid() !== 0) {
  console.error('%s: execute as root', programName);
  process.exit(1);
}

if (typeof process.send !== 'function') {
  console.error('%s: use child_process.spawn to invoke this module', programName);
  process.exit(2);
}

var net = require('net');
var spawn = require('child_process').spawn;

var async = require('async');
var chroot = require('chroot');
var LDJSONStream = require('ld-jsonstream');
var keyFilter = require('object-key-filter');
var posix = require('posix');

var authRequest = require('./auth_request');
var getConnectionId = require('./get_connection_id');
var logger = require('./logger');
var noop = require('./noop');

/**
 * Instantiate a server that listens to incoming auth requests.
 *
 * Full FSM: init --> listen
 *
 * The first message emitted is "init" which signals that this process is ready to
 * receive the configuration data. File desciptors for the log config should be
 * sent in subsequent messages.
 *
 * {
 *   log:            {Object}      // log configuration
 *   [port]:         {Number}      // tcp port, defaults to 2344
 *   [wss]:          {Boolean}     // whether or not to enable secure WebSocket
 *                                 // defaults to false
 *   [wssCert]:      {String}      // X.509 certificate for the TLS server
 *   [wssKey]:       {String}      // RSA private key for the certificate
 *   [wssDhparam]:   {String}      // file that contains a Diffie-Hellman group
 *   [wssHost]:      {String}      // websocket host, defaults to 127.0.0.1
 *                                 // only if "wss" is true
 *   [wssPort]:      {Number}      // websocket port, defaults to 3344
 *                                 // only if "wss" is true
 *   [chroot]:       {String}      // defaults to /var/empty
 *   [user]:         {String}      // defaults to "_pdbnull"
 *   [group]:        {String}      // defaults to "_pdbnull"
 * }
 *
 * An unencrypted tcp server is started at the given port and always bound to
 * 127.0.0.1. This can not be changed. Connect either locally, via an ssh tunnel,
 * or start a secure WebSocket server by passing wss = true and connect with a
 * browser.
 *
 * Only a kill message can be sent to signal the end of the process.
 *
 * As soon as the server is listening, a "listen" message is emitted. After this
 * only new incoming auth requests are sent from this process to it's parent.
 */

var log; // used after receiving the log configuration

function connErrorHandler(conn, connId, e) {
  log.err('connection error %s %s', connId, e);
  try {
    var error = { error: 'invalid auth request' };
    conn.end(JSON.stringify(error));
  } catch(err) {
    log.err('connection write or disconnect error %s', err);
  }
}

// filter password out request
function debugReq(req) {
  return keyFilter(req, ['password']);
}

var connections = {};

function connHandler(conn) {
  log.info('client connected %s', getConnectionId(conn));

  var connId = getConnectionId(conn);
  if (connections[connId]) {
    connErrorHandler(conn, connId, new Error('connection already exists'));
    return;
  }

  connections[connId] = conn;

  // create line delimited json stream
  var ls = new LDJSONStream({ flush: false, maxDocs: 1, maxBytes: 1024 });

  ls.on('error', function(err) {
    connErrorHandler(conn, connId, err);
  });

  conn.pipe(ls).once('readable', function() {
    var req = ls.read();
    if (req == null) { return; }
    log.info('req received %j', debugReq(req));

    conn.unpipe(ls);
    // push back any data in ls
    if (ls.buffer.length) {
      log.info('push back %d bytes', ls.buffer.length);
      conn.unshift(ls.buffer);
    }

    if (!authRequest.valid(req)) {
      log.err('invalid auth request %j', debugReq(req));
      connErrorHandler(conn, connId, 'invalid auth request');
      return;
    }

    // send auth request to parent for validation, with the connection socket
    process.send(req, conn);
  });

  conn.on('error', function(err) {
    log.err('%s: %s', connId, err);
  });
  conn.on('close', function() {
    log.info('%s: close', connId);
    delete connections[connId];
  });
}

/**
 * Expect one init request (request that contains listen config)
 * {
 *   log:            {Object}      // log configuration
 *   [port]:         {Number}      // tcp port, defaults to 2344 (host is 127.0.0.1)
 *   [wss]:          {Boolean}     // whether or not to enable secure WebSocket
 *                                 // defaults to false
 *   [wssCert]:      {String}      // X.509 certificate for the TLS server
 *   [wssKey]:       {String}      // RSA private key for the certificate
 *   [wssDhparam]:   {String}      // file that contains a Diffie-Hellman group
 *   [wssHost]:      {String}      // websocket host, defaults to 127.0.0.1
 *                                 // only if "wss" is true
 *   [wssPort]:      {Number}      // websocket port, defaults to 3344
 *                                 // only if "wss" is true
 *   [chroot]:       {String}      // defaults to /var/empty
 *   [user]:         {String}      // defaults to "_pdbnull"
 *   [group]:        {String}      // defaults to "_pdbnull"
 * }
 */
process.once('message', function(msg) {
  if (typeof msg !== 'object') { throw new TypeError('msg must be an object'); }
  if (typeof msg.log !== 'object') { throw new TypeError('msg.log must be an object'); }

  if (msg.port != null && typeof msg.port !== 'number') { throw new TypeError('msg.port must be a number'); }
  if (msg.wss != null && typeof msg.wss !== 'boolean') { throw new TypeError('msg.wss must be a boolean'); }
  if (msg.wssCert != null && typeof msg.wssCert !== 'string') { throw new TypeError('msg.wssCert must be a non-empty string'); }
  if (msg.wssKey != null && typeof msg.wssKey !== 'string') { throw new TypeError('msg.wssKey must be a non-empty string'); }
  if (msg.wssDhparam != null && typeof msg.wssDhparam !== 'string') { throw new TypeError('msg.wssDhparam must be a non-empty string'); }
  if (msg.wssHost != null && typeof msg.wssHost !== 'string') { throw new TypeError('msg.wssHost must be a string'); }
  if (msg.wssPort != null && typeof msg.wssPort !== 'number') { throw new TypeError('msg.wssPort must be a number'); }
  if (msg.chroot != null && typeof msg.chroot !== 'string') { throw new TypeError('msg.chroot must be a string'); }
  if (msg.user != null && typeof msg.user !== 'string') { throw new TypeError('msg.user must be a string'); }
  if (msg.group != null && typeof msg.group !== 'string') { throw new TypeError('msg.group must be a string'); }

  var host = '127.0.0.1';
  var port = msg.port || 2344;

  programName = 'preauth ' + port;

  process.title = 'pdb/' + programName;

  var ws, wssHost, wssPort;
  if (msg.wss) {
    // only require websocket code if needed
    ws = require('nodejs-websocket');
    wssHost = msg.wssHost || '127.0.0.1';
    wssPort = msg.wssPort || 3344;
  }

  var user = msg.user || '_pdbnull';
  var group = msg.group || '_pdbnull';

  var newRoot = msg.chroot || '/var/empty';

  msg.log.ident = programName;

  // open log
  logger(msg.log, function(err, l) {
    if (err) { l.err(err); throw err; }

    log = l; // use this logger in the mt's as well

    var uid, gid;
    try {
      uid = posix.getpwnam(user).uid;
      gid = posix.getgrnam(group).gid;
    } catch(err) {
      log.err('preauth %s %s:%s', err, user, group);
      process.exit(3);
    }

    // handle shutdown, must be reachable from ws child handler
    var shuttingDown = false;
    function shutdown(tasks) {
      if (shuttingDown) {
        log.info('shutdown already in progress');
        return;
      }
      shuttingDown = true;
      log.info('shutting down...');

      async.series(tasks, function(err) {
        if (err) { log.err('shutdown error', err); }
        // disconnect from parent
        process.disconnect();
      });
    }

    // spawn WebSocket server before chrooting
    var wsCfg, wsChild, childPhase;
    if (ws) {
      var opts = {
        cwd: process.cwd(),
        env: {},
        stdio: ['ignore', 'inherit', 'inherit', 'ipc']
      };

      wsCfg = {
        log:            log.getOpts(),  // log configuration
        cert:           msg.wssCert,    // X.509 certificate for the TLS server
        key:            msg.wssKey,     // RSA private key for the certificate
        dhparam:        msg.wssDhparam, // file that contains a Diffie-Hellman group
        host:           wssHost,        // websocket host, defaults to 127.0.0.1
        port:           wssPort,        // websocket port, defaults to 3344
        proxyPort:      msg.port,       // tcp port of preauth, defaults to 2344
        chroot:         msg.chroot,     // defaults to /var/empty
        user:           msg.user,       // defaults to "_pdbnull"
        group:          msg.group       // defaults to "_pdbnull"
      };

      wsCfg.log.file = log.getFileStream();
      wsCfg.log.error = log.getErrorStream();

      // use fd 4 if a file stream is opened
      if (wsCfg.log.file) {
        opts.stdio[4] = wsCfg.log.file.fd;
        wsCfg.log.file = 4;
      }

      // use fd 5 if an error stream is opened
      if (wsCfg.log.error) {
        opts.stdio[5] = wsCfg.log.error.fd;
        wsCfg.log.error = 5;
      }

      // spawn child that runs the WebSocket server
      wsChild = spawn(process.execPath, ['--abort-on-uncaught-exception', __dirname + '/wss_server.js'], opts);

      log.info('ws child spawned');

      wsChild.on('exit', function(code, signal) {
        log.info('ws child exit', code, signal);
        // close on premature exit
        if (childPhase !== 'listen') {
          log.info('ws child pre-mature exit');
          shutdown();
        }
        childPhase = 'close';
      });
      wsChild.on('error', function(err) {
        log.err('ws child error', err);
      });
    }

    // chroot or exit
    try {
      chroot(newRoot, uid, gid);
      log.debug2('preauth changed root to %s and user:group to %s:%s', newRoot, user, group);
    } catch(err) {
      log.err('preauth changing root or user failed %j %s', msg, err);
      process.exit(8);
    }

    // set core limit to maximum allowed size
    posix.setrlimit('core', { soft: posix.getrlimit('core').hard });

    // open servers or exit
    function openServerAndProceed() {
      var startupTasks = [];
      var shutdownTasks = [];

      // ensure at least one async function
      startupTasks.push(function(cb) { process.nextTick(cb); });

      var tcpServer = net.createServer(connHandler);

      tcpServer.on('error', function(err) {
        log.err('TCP server error %s:%s', host, port, err);
      });

      tcpServer.on('close', function() {
        log.notice('TCP server closed %s:%s', host, port);
      });

      // open TCP server
      startupTasks.push(function(cb) {
        tcpServer.listen(port, host, function() {
          log.notice('TCP server bound %s:%s', host, port);
          cb();
        });
      });

      if (wsChild) {
        startupTasks.push(function(cb) {
          // expect "init" -> "listen" messages, send config after init
          wsChild.once('message', function(cmsg) {
            if (cmsg !== 'init') { cb(new Error('expected "init" message from wsChild')); return; }

            childPhase = 'init';

            wsChild.once('message', function(cmsg) {
              if (cmsg !== 'listen') { cb(new Error('expected "listen" message from wsChild')); return; }

              childPhase = 'listen';

              cb();
            });

            // send config
            wsChild.send(wsCfg);
          });
        });

        shutdownTasks.push(function(cb) {
          log.info('closing WebSocket server...');
          if (childPhase !== 'close') {
            wsChild.on('close', function(code, signal) {
              if (code) { log.err('ws child exit code: %d, signal: %s', code, signal); }
              cb();
            });
            wsChild.send({ type: 'kill' });
          } else {
            cb();
          }
        });
      }

      shutdownTasks.push(function(cb) {
        log.info('closing TCP server...');
        tcpServer.close(cb);
        Object.keys(connections).forEach(function(connId) {
          log.info('closing %s', connId);
          connections[connId].end();
        });
      });

      async.series(startupTasks, function(err) {
        if (err) {
          log.crit('not all startup tasks are completed, exiting');
          process.exit(6);
        }

        process.send('listen');
      });

      // ignore kill signals
      process.once('SIGTERM', noop);
      process.once('SIGINT', noop);

      process.once('message', function() {
        shutdown(shutdownTasks);
      }); // expect msg.type == kill
    }

    openServerAndProceed();
  });
});

process.send('init');
