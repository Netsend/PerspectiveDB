/**
 * Copyright 2015 Netsend.
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

/* jshint -W116 */

'use strict';

var programName = require('path').basename(__filename, '.js');

if (process.getuid() !== 0) {
  console.error('%s: execute as root', programName);
  process.exit(1);
}

var spawn = require('child_process').spawn;

var chroot = require('chroot');
var posix = require('posix');

var logger = require('./logger');

/**
 * Spawn a WebSocket server child and handle connections.
 *
 * Full FSM: init --> listen
 *
 * The first message emitted is "init" which signals that this process is ready to
 * receive the configuration data. File desciptors for the log config should be
 * sent in subsequent messages.
 *
 * {
 *   log:            {Object}      // log configuration
 *   [maxconn]:      {Number}      // maximum number of connections, default 100
 *   [chroot]:       {String}      // defaults to /var/empty
 *   [user]:         {String}      // defaults to "nobody"
 *   [group]:        {String}      // defaults to "nobody"
 *
 *   all wss_server options:
 *
 *   cert:           {Object}      // X.509 certificate for the TLS server
 *   key:            {Object}      // RSA private key for the certificate
 *   dhparam:        {Object}      // file that contains a Diffie-Hellman group
 *   [host]:         {String}      // websocket host, defaults to 127.0.0.1
 *   [port]:         {Number}      // websocket port, defaults to 3344
 * }
 *
 * No other messages should be sent to this process.
 *
 * As soon as the server is listening, a "listen" message is emitted. After this
 * only new incoming auth requests are sent from this process to it's parent.
 */

var log; // used after receiving the log configuration

/**
 * Manage open file descriptors used for IPC.
 *
 * @param {Array} stdio  array of stream objects
 * @param {Number} [offset]  optional offset for stdio
 * @return { open(), close(i) } an open and close function
 */
function fdsHandler(stdio, offset) {
  if (!Array.isArray(stdio)) { throw new TypeError('stdio must be an array'); }
  if (offset != null && typeof offset !== 'number') { throw new TypeError('offset must be a number'); }
  offset = offset || 0;
  if (offset >= stdio.length) { throw new Error('offset >= stdio'); } 

  // init fds
  var fds = {};
  for (var i = offset; i < stdio.length; i++) {
    fds[i] = null;
  }

  var openFds = 0;

  /**
   * @return {Boolean|Number} open fd, or false if all are in use
   */
  function open() {
    if (openFds === stdio.length) {
      log.notice('all fds are in use %d', openFds);
      return false;
    }
    var j = offset;
    while (j < stdio.length && fds[j])
      j++;
    if (j < stdio.length) {
      fds[j] = true;
      openFds++;
      return j;
    }

    log.err('no free fd found %d', j);
    throw new Error('no free fd found');
  }

  // close fd if open
  function close(j) {
    if (fds[j]) {
      fds[j] = false;
      openFds--;
    }
  }

  return {
    open: open,
    close: close
  };
}

if (typeof process.send !== 'function') {
  throw new Error('this module should be invoked via child_process.fork');
}

process.send('init');

/**
 * Expect one init request (request that contains listen config)
 * {
 *   log:            {Object}      // log configuration
 *   [maxconn]:      {Number}      // maximum number of connections, default 100
 *   [chroot]:       {String}      // defaults to /var/empty
 *   [user]:         {String}      // defaults to "nobody"
 *   [group]:        {String}      // defaults to "nobody"
 *
 *   all wss_server options:
 *
 *   cert:           {Object}      // X.509 certificate for the TLS server
 *   key:            {Object}      // RSA private key for the certificate
 *   dhparam:        {Object}      // file that contains a Diffie-Hellman group
 *   [host]:         {String}      // websocket host, defaults to 127.0.0.1
 *   [port]:         {Number}      // websocket port, defaults to 3344
 * }
 */
process.once('message', function(msg) {
  if (typeof msg !== 'object') { throw new TypeError('msg must be an object'); }

  if (typeof msg.log !== 'object') { throw new TypeError('msg.log must be an object'); }

  if (msg.maxconn != null && typeof msg.maxconn !== 'number') { throw new TypeError('msg.maxconn must be a number'); }
  if (msg.chroot != null && typeof msg.chroot !== 'string') { throw new TypeError('msg.chroot must be a string'); }
  if (msg.user != null && typeof msg.user !== 'string') { throw new TypeError('msg.user must be a string'); }
  if (msg.group != null && typeof msg.group !== 'string') { throw new TypeError('msg.group must be a string'); }

  var maxconn = msg.maxconn || 100;

  programName = 'wss master';

  process.title = 'pdb/' + programName;

  var user = msg.user || 'nobody';
  var group = msg.group || 'nobody';

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
      log.err('%s %s:%s', err, user, group);
      process.exit(3);
    }

    // spawn child and chroot or exit
    function openServerAndProceed() {
      // create file descriptors for ipc
      var offset = 32;

      // ignore stdin and fd 4-offset, pipe fd offset-maxconn
      var stdio = ['ignore', 'pipe', 'pipe', 'ipc'];
      var i = stdio.length;
      for (; i < offset; i++) {
        stdio.push('ignore');
      }
      for (; i < maxconn; i++) {
        stdio.push('pipe');
      }

      var fds = fdsHandler(stdio, offset);

      var opts = {
        cwd: __dirname,
        env: {},
        stdio: stdio
      };

      // spawn child that runs the WebSocket server
      var child = spawn(process.execPath, [__dirname + '/wss_server.js'], opts);

      log.info('child spawned');

      // chroot or exit
      try {
        chroot(newRoot, user, group);
        log.info('changed root to %s and user:group to %s:%s', newRoot, user, group);
      } catch(err) {
        log.err('changing root or user failed %j %s', msg, err);
        process.exit(8);
      }

      child.on('exit', function(code, signal) {
        log.debug('child exit', code, signal);
      });
      child.on('close', function(code, signal) {
        log.debug('child close', code, signal);
      });
      child.on('error', function(err) {
        log.err('child error', err);
      });

      child.stdout.setEncoding('utf8');
      child.stderr.setEncoding('utf8');

      child.stdout.on('data', function(data) {
        log.debug('child stdout:', data);
      });
      child.stderr.on('data', function(data) {
        log.err('child stderr:', data);
      });

      function handleData(d) {
        log.info('data', d);
      }

      function mainChildHandler(msg, cfd) {
        switch (msg) {
        case 'open':
          if (cfd != null) {
            log.notice('child sent fd with open message');
          }

          var fd = fds.open();
          if (fd) {
            log.info('open fd %d', fd);
            child.send('open', child.stdio[fd]);
            fd.on('data', handleData);
          } else {
            log.debug('could not open fd');
            child.send('close');
          }
          break;
        case 'close':
          if (typeof cfd === 'number') {
            if (cfd < offset || cfd >= stdio.length) {
              log.err('child sent illegal fd: %d, offset: %d, fds: %d', cfd, offset, stdio.length);
            } else {
              cfd.removeListener('data', handleData);
              fds.close(cfd);
            }
          } else {
            log.notice('child sent close without numeric fd: %s', typeof cfd);
          }
          break;
        default:
          log.notice('child sent unknown message "%s"', msg);
        }
      }

      // expect "init" -> "listen" -> "open" || "close" messages
      child.once('message', function(msg) {
        if (msg !== 'init') {
          log.err('expected init message from child: %s', msg);
          process.exit(9);
        }
        child.once('message', function(msg) {
          if (msg !== 'listen') {
            log.err('expected listen message from child: %s', msg);
            process.exit(10);
          }
          child.on('message', mainChildHandler);
          process.send('listen');
        });
      });

      // listen to kill signals
      //process.once('SIGINT', shutdown);
      //process.once('SIGTERM', shutdown);
    }

    openServerAndProceed();
  });
});
