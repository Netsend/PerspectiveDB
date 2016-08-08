/**
 * Copyright 2016 Netsend.
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

var chroot = require('chroot');
var posix = require('posix');

var connManager = require('./conn_manager');
var logger = require('./logger');
var noop = require('./noop');

var log; // used after receiving the log configuration

/**
 * Instantiate a connection to another PerspectiveDB instance.
 *
 * Full FSM: init --> listen
 *
 * The first message emitted is "init" which signals that this process is ready to
 * receive the configuration data. File desciptors for the log config should be
 * sent in subsequent messages.
 *
 * Only a kill message can be sent to signal the end of the process.
 *
 * As soon as the client connected to the remote, a "listen" message is emitted.
 *
 * Expect one init request (request that contains client config)
 * {
 *   log:            {Object}      // log configuration
 *   username:       {String}      // username to login with
 *   password:       {String}      // password to authenticate
 *   database:       {String}      // database to authenticate with
 *   [port]:         {Number}      // port, defaults to 2344 (might be a tunnel)
 *   [name]:         {String}      // custom name for this client
 *   [chroot]:       {String}      // defaults to /var/empty
 *   [user]:         {String}      // defaults to "_pdbnull"
 *   [group]:        {String}      // defaults to "_pdbnull"
 * }
 */
process.once('message', function(msg) {
  if (typeof msg !== 'object') { throw new TypeError('msg must be an object'); }

  if (typeof msg.log !== 'object') { throw new TypeError('msg.log must be an object'); }
  if (!msg.username || typeof msg.username !== 'string') { throw new TypeError('msg.username must be a string'); }
  if (!msg.password || typeof msg.password !== 'string') { throw new TypeError('msg.password must be a string'); }
  if (!msg.database || typeof msg.database !== 'string') { throw new TypeError('msg.database must be a string'); }

  if (msg.port != null && typeof msg.port !== 'number') { throw new TypeError('msg.port must be a number'); }
  if (msg.name != null && typeof msg.name !== 'string') { throw new TypeError('msg.name must be a string'); }
  if (msg.chroot != null && typeof msg.chroot !== 'string') { throw new TypeError('msg.chroot must be a string'); }
  if (msg.user != null && typeof msg.user !== 'string') { throw new TypeError('msg.user must be a string'); }
  if (msg.group != null && typeof msg.group !== 'string') { throw new TypeError('msg.group must be a string'); }

  var port = msg.port || 2344;

  if (msg.name) {
    programName = 'client ' + msg.name;
  } else {
    programName = 'client ' + port;
  }

  process.title = 'pdb/' + programName;

  var user = msg.user || '_pdbnull';
  var group = msg.group || '_pdbnull';

  var newRoot = msg.chroot || '/var/empty';

  msg.log.ident = programName;

  // open log
  logger(msg.log, function(err, l) {
    if (err) { l.err(err); throw err; }

    log = l;

    var uid, gid;
    try {
      uid = posix.getpwnam(user).uid;
      gid = posix.getgrnam(group).gid;
    } catch(err) {
      log.err('client %s %s:%s', err, user, group);
      process.exit(3);
    }

    // chroot or exit
    try {
      chroot(newRoot, uid, gid);
      log.debug2('changed root to %s and user:group to %s:%s', newRoot, user, group);
    } catch(err) {
      log.err('changing root or user failed %j %s', msg, err);
      process.exit(8);
    }

    // set core limit to maximum allowed size
    posix.setrlimit('core', { soft: posix.getrlimit('core').hard });
    log.debug2('core limit: %j, fsize limit: %j', posix.getrlimit('core'), posix.getrlimit('fsize'));

    process.send('listen');

    // create the connection or exit
    var cm = connManager.create({ log: log });

    // give daemon some time to setup
    setTimeout(function() {
      log.notice('connecting to %s:%d', '127.0.0.1', port);
      cm.open('127.0.0.1', port, { reconnectOnError: true }, function(err, conn) {
        if (err) { log.err(err); return; }

        // send auth request
        var authReq = {
          username: msg.username,
          password: msg.password,
          db: msg.database
        };

        // send auth request
        log.notice('sending auth request %s', msg.username);
        conn.write(JSON.stringify(authReq) + '\n');

        process.send('connection', conn);
      });
    }, 1000);

    // handle shutdown
    var shuttingDown = false;
    function shutdown() {
      if (shuttingDown) {
        log.info('shutdown already in progress');
        return;
      }
      shuttingDown = true;
      log.info('shutting down...');
      cm.close(function(err) {
        if (err) {
          log.err(err);
        } else {
          log.info('connection closed');
        }
        // disconnect from parent
        process.disconnect();
      });
    }

    // ignore kill signals
    process.once('SIGTERM', noop);
    process.once('SIGINT', noop);

    process.once('message', shutdown); // expect msg.type == kill
  });
});

process.send('init');
