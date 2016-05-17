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

var programName = require('path').basename(__filename, '.js');

if (process.getuid() !== 0) {
  console.error('%s: execute as root', programName);
  process.exit(1);
}

if (typeof process.send !== 'function') {
  console.error('%s: use child_process.spawn to invoke this module', programName);
  process.exit(2);
}

var dns = require('dns');
var fs = require('fs');
var net = require('net');

var chroot = require('chroot');
var posix = require('posix');
var ssh = require('ssh2');

var logger = require('./logger');
var noop = require('./noop');

var log; // used after receiving the log configuration

/**
 * Instantiate an ssh tunnel.
 *
 * Full FSM: init --> listen
 *
 * The first message emitted is "init" which signals that this process is ready to
 * receive the configuration data. File desciptors for the log config should be
 * sent in subsequent messages.
 *
 * Only a kill message can be sent to signal the end of the process.
 *
 * As soon as the server is listening, a "listen" message is emitted.
 *
 * Expect one init request (request that contains tunnel config)
 * {
 *   log:            {Object}      // log configuration
 *   sshUser:        {String}      // username on the ssh server
 *   key:            {Object}      // path to private key to authenticate
 *   forward:        {String}      // ssh -L format [bind_address:]port:host:hostport
 *   host:           {String}      // ssh server to connect to
 *   fingerprint:    {String}      // sha256 hash of the host key in base64 or hex
 *   [port]:         {Number}      // ssh port, defaults to 22
 *   [name]:         {String}      // custom name for this tunnel
 *   [chroot]:       {String}      // defaults to /var/empty
 *   [user]:         {String}      // defaults to "_pdbnull"
 *   [group]:        {String}      // defaults to "_pdbnull"
 * }
 */
process.once('message', function(msg) {
  if (typeof msg !== 'object') { throw new TypeError('msg must be an object'); }

  if (typeof msg.log !== 'object') { throw new TypeError('msg.log must be an object'); }
  if (!msg.sshUser || typeof msg.sshUser !== 'string') { throw new TypeError('msg.sshUser must be a string'); }
  if (!msg.key || typeof msg.key !== 'string') { throw new TypeError('msg.key must be a string'); }
  if (!msg.forward || typeof msg.forward !== 'string') { throw new TypeError('msg.forward must be a string'); }
  if (!msg.host || typeof msg.host !== 'string') { throw new TypeError('msg.host must be a string'); }
  if (!msg.fingerprint || typeof msg.fingerprint !== 'string') { throw new TypeError('msg.fingerprint must be a string'); }

  if (msg.port != null && typeof msg.port !== 'number') { throw new TypeError('msg.port must be a number'); }
  if (msg.name != null && typeof msg.name !== 'string') { throw new TypeError('msg.name must be a string'); }
  if (msg.chroot != null && typeof msg.chroot !== 'string') { throw new TypeError('msg.chroot must be a string'); }
  if (msg.user != null && typeof msg.user !== 'string') { throw new TypeError('msg.user must be a string'); }
  if (msg.group != null && typeof msg.group !== 'string') { throw new TypeError('msg.group must be a string'); }

  if (msg.fingerprint.length !== 43 && msg.fingerprint.length !== 64) { throw new Error('msg.fingerprint must be a hex or base64 encoded sha256 hash'); }

  programName = 'tunnel ' + msg.forward;
  if (msg.name) {
    programName = 'tunnel ' + msg.name;
  }

  process.title = 'pdb/' + programName;

  var user = msg.user || '_pdbnull';
  var group = msg.group || '_pdbnull';

  var newRoot = msg.chroot || '/var/empty';

  msg.log.ident = programName;

  var forward = msg.forward.split(':'); 
  if (forward.length < 3 || forward.length > 4) {
    throw new Error('msg.forward format must follow [bind_address:]port:host:hostport');
  }

  // open log
  logger(msg.log, function(err, l) {
    if (err) { l.err(err); throw err; }

    log = l;

    // make sure the key is not world readable/writable
    var stats = fs.statSync(msg.key);
    if ((stats.mode & 0o066) !== 0) {
      log.err('msg.key should not be group or world readable or writable');
      process.exit(12);
    }

    var key = fs.readFileSync(msg.key);

    var uid, gid;
    try {
      uid = posix.getpwnam(user).uid;
      gid = posix.getgrnam(group).gid;
    } catch(err) {
      log.err('tunnel %s %s:%s', err, user, group);
      process.exit(3);
    }

    // create the tunnel or exit
    var tunnel = new ssh.Client();

    // start dns lookup of hostname before chrooting
    dns.lookup(msg.host, function(err, hostIp) {
      if (err) {
        log.err('tunnel could not lookup %s %s', msg.host, err);
        process.exit(4);
      }
      log.notice('resolved %s to %s', msg.host, hostIp);

      // ensure hex and base64 fingerprint
      var expectedFingerprintHex, expectedFingerprintBase64;
      if (msg.fingerprint.length === 64) {
        expectedFingerprintHex = msg.fingerprint;
        expectedFingerprintBase64 = new Buffer(msg.fingerprint, 'hex').toString('base64');
      } else {
        expectedFingerprintBase64 = msg.fingerprint;
        expectedFingerprintHex = new Buffer(msg.fingerprint, 'base64').toString('hex');
      }

      tunnel.connect({
        host: hostIp,
        port: msg.port || 22,
        username: msg.sshUser,
        privateKey: key,
        hostHash: 'sha256',
        debug: log.debug2,
        hostVerifier: function(hostFingerprintHex) {
          var hostFingerprintBase64 = new Buffer(hostFingerprintHex, 'hex').toString('base64');
          if (hostFingerprintHex === expectedFingerprintHex) {
            log.info('host key verified: %s', expectedFingerprintBase64);
            return true;
          } else {
            log.err('host key mismatch! expected %s got %s', expectedFingerprintBase64, hostFingerprintBase64);
            return false;
          }
        }
      });
    });

    // chroot or exit
    try {
      chroot(newRoot, uid, gid);
      log.info('changed root to %s and user:group to %s:%s', newRoot, user, group);
    } catch(err) {
      log.err('changing root or user failed %j %s', msg, err);
      process.exit(8);
    }

    // set core limit to maximum allowed size
    posix.setrlimit('core', { soft: posix.getrlimit('core').hard });
    log.info('core limit: %j, fsize limit: %j', posix.getrlimit('core'), posix.getrlimit('fsize'));

    var srcHost = '127.0.0.1';
    if (forward.length === 4) {
      srcHost = forward.shift();
    }

    var dstHost = forward[1];

    var srcPort = parseInt(forward[0]);
    var dstPort = parseInt(forward[2]);
    if (srcPort < 1024) {
      log.err('only source ports >1024 are supported');
      process.exit(5);
    }

    var srv;
    tunnel.on('ready', function() {
      log.notice('tunnel ready %s', msg.host);
      srv = net.createServer(function(sock) {
        tunnel.forwardOut(sock.remoteAddress, sock.remotePort, dstHost, dstPort, function(err, channel) {
          if (err) {
            log.err(err);
            sock.end();
          }
          sock.pipe(channel).pipe(sock);
          log.notice('tunnel ready %s:%d:%s:%d', sock.remoteAddress, sock.remotePort, dstHost, dstPort);
        });
      });
      srv.listen(srcPort, srcHost, function() {
        log.notice('listening on %s:%d', srcHost, srcPort);
        process.send('listen');
      });
    });

    tunnel.on('error', function(err) {
      log.err(err);
    });

    tunnel.on('close', function(hadError) {
      log.notice('tunnel closed');
      if (hadError) {
        log.err('closed with error');
      }
      if (srv) {
        srv.close(function(hadError) {
          log.notice('closed listening socket %s:%d', srcHost, srcPort);
          if (hadError) {
            log.err('closed with error');
          }
        });
      }
    });

    // handle shutdown
    var shuttingDown = false;
    function shutdown() {
      if (shuttingDown) {
        log.info('shutdown already in progress');
        return;
      }
      shuttingDown = true;
      log.info('shutting down...');
      // disconnect from parent
      process.disconnect();
      tunnel.end();
    }

    // ignore kill signals
    process.once('SIGTERM', noop);
    process.once('SIGINT', noop);

    process.once('message', shutdown); // expect msg.type == kill
  });
});

process.send('init');
