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

if (typeof process.send !== 'function') {
  console.error('%s: use child_process.spawn to invoke this module', programName);
  process.exit(2);
}

var net = require('net');
var tls = require('tls');
var fs = require('fs');

var chroot = require('chroot');
var LDJSONStream = require('ld-jsonstream');
var posix = require('posix');
var ws = require('nodejs-websocket');
var c = require('constants');

var logger = require('./logger');
var noop = require('./noop');

/**
 * Instantiate a WebSocket server that listens to incoming auth requests.
 *
 * Full FSM: init --> listen
 *
 * The first message emitted is "init" which signals that this process is ready to
 * receive the configuration data. File desciptors for the log config should be
 * sent in subsequent messages.
 *
 * {
 *   log:            {Object}      // log configuration
 *   cert:           {Object}      // X.509 certificate for the TLS server
 *   key:            {Object}      // RSA private key for the certificate
 *   dhparam:        {Object}      // file that contains a Diffie-Hellman group
 *   [host]:         {String}      // websocket host, defaults to 127.0.0.1
 *   [port]:         {Number}      // websocket port, defaults to 3344
 *   [proxyPort]:    {Number}      // tcp port of preauth, defaults to 2344
 *   [chroot]:       {String}      // defaults to /var/empty
 *   [user]:         {String}      // defaults to "_pdbnull"
 *   [group]:        {String}      // defaults to "_pdbnull"
 * }
 *
 * Only a kill message can be sent to signal the end of the process.
 *
 * As soon as the server is listening, a "listen" message is emitted. After this
 * only new incoming auth requests are sent from this process to it's parent.
 */

var log; // used after receiving the log configuration
var proxyPort; // used after config is recieved

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

function connHandler(conn) {
  var connId = createConnId(conn.socket);
  log.info('%s: connected %j', connId, conn.socket.address());

  var connected, received = [];
  conn.once('text', function(authReq) {
    log.info('%s: received %d bytes', connId, authReq.length);
    received.push(authReq);
    if (connected) {
      log.info('%s: proxy auth request to server', connId);
      preauthSock.write(received.shift());
    }
    conn.once('text', function(dataReq) {
      log.info('%s: received %d bytes "%s"', connId, dataReq.length, dataReq);
      received.push(dataReq);
      if (connected) {
        log.info('%s: proxy auth/data request(s) to server', connId);
        while (received.length) {
          preauthSock.write(received.shift());
        }
      }
      conn.on('text', function() {
        log.err('%s: %s', connId, 'more than two text events emitted');
        preauthSock.end();
        conn.close();
      });
    });
  });

  var connClose, preauthClose;
  var preauthSock = net.createConnection({ port: proxyPort, host: '127.0.0.1' }, function() {
    log.info('%s: connected to proxy', connId);
    connected = true;
    if (received.length) {
      log.info('%s: proxy auth/data requests to server', connId);
      while (received.length) {
        preauthSock.write(received.shift());
      }
    }
    var binRecMode;
    conn.on('binary', function(rs) {
      log.debug('%s: setup binary reader', connId);
      rs.on('readable', function() {
        var chunk;
        while (null !== (chunk = rs.read())) {
          log.debug('%s: write chunk from client', connId);
          preauthSock.write(chunk);
        }
      });
      rs.on('end', function() {
        log.debug('%s: end binary reader', connId);
      });
      binRecMode = true;
    });
    conn.on('error', function(err) {
      log.err('%s: %s', connId, err);
      preauthSock.end();
    });
    conn.on('close', function() {
      log.info('%s: close', connId);
      connClose = true;
      // only end if not in binRecMode yet.
      if (!binRecMode && !preauthClose) {
        log.debug('%s: end (no binary data received)', connId);
        preauthSock.end();
        preauthClose = true;
      }
    });

    // proxy data from preauth back to connection
    var ls = new LDJSONStream({ flush: false, maxDocs: 1, maxBytes: 512, debug: false }); // accept one data req
    preauthSock.pipe(ls).once('readable', function() {
      var data = ls.read();
      if (data == null) { return; }

      var dataReq = JSON.stringify(data);
      log.info('%s: proxy data request to client "%s"', connId, dataReq);
      conn.sendText(dataReq + '\n');

      // setup a direct pipe for binary data (only BSON is expected after data request)
      preauthSock.unpipe(ls);
      // push back any data in ls
      if (ls.buffer.length) {
        log.info('push back %d bytes', ls.buffer.length);
        preauthSock.unshift(ls.buffer);
      }

      preauthSock.on('readable', function() {
        var chunk;
        while (null !== (chunk = preauthSock.read())) {
          conn.sendBinary(chunk);
        }
      });
    });
    preauthSock.on('end', function() {
      log.info('%s: preauth end', connId);
      preauthClose = true;
      if (!connClose) {
        conn.close();
        connClose = true;
      }
    });
  });
}

/**
 * Expect one init request (request that contains listen config)
 * {
 *   log:            {Object}      // log configuration
 *   cert:           {String}      // X.509 certificate for the TLS server
 *   key:            {String}      // RSA private key for the certificate
 *   dhparam:        {String}      // file that contains a Diffie-Hellman group
 *   [host]:         {String}      // websocket host, defaults to 127.0.0.1
 *   [port]:         {Number}      // websocket port, defaults to 3344
 *   [proxyPort]:    {Number}      // tcp port of preauth, defaults to 2344
 *   [chroot]:       {String}      // defaults to /var/empty
 *   [user]:         {String}      // defaults to "_pdbnull"
 *   [group]:        {String}      // defaults to "_pdbnull"
 * }
 */
process.once('message', function(msg) {
  if (typeof msg !== 'object') { throw new TypeError('msg must be an object'); }

  if (typeof msg.log !== 'object') { throw new TypeError('msg.log must be an object'); }
  if (!msg.cert || typeof msg.cert !== 'string') { throw new TypeError('msg.cert must be a non-empty string'); }
  if (!msg.key || typeof msg.key !== 'string') { throw new TypeError('msg.key must be a non-empty string'); }
  if (!msg.dhparam || typeof msg.dhparam !== 'string') { throw new TypeError('msg.dhparam must be a non-empty string'); }

  if (msg.host != null && typeof msg.host !== 'string') { throw new TypeError('msg.host must be a string'); }
  if (msg.port != null && typeof msg.port !== 'number') { throw new TypeError('msg.port must be a number'); }
  if (msg.proxyPort != null && typeof msg.proxyPort !== 'number') { throw new TypeError('msg.proxyPort must be a number'); }
  if (msg.chroot != null && typeof msg.chroot !== 'string') { throw new TypeError('msg.chroot must be a string'); }
  if (msg.user != null && typeof msg.user !== 'string') { throw new TypeError('msg.user must be a string'); }
  if (msg.group != null && typeof msg.group !== 'string') { throw new TypeError('msg.group must be a string'); }

  var host = msg.host || '127.0.0.1';
  var port = msg.port || 3344;
  proxyPort = msg.proxyPort || 2344;

  programName = 'wss ' + host + ':' + port;

  process.title = 'pdb/' + programName;

  var user = msg.user || '_pdbnull';
  var group = msg.group || '_pdbnull';

  var newRoot = msg.chroot || '/var/empty';

  msg.log.ident = programName;

  // open log
  logger(msg.log, function(err, l) {
    if (err) { l.err(err); throw err; }

    log = l; // use this logger in the mt's as well

    // make sure the key is not world readable/writable
    var stats = fs.statSync(msg.key);
    if ((stats.mode & 0o066) !== 0) {
      log.err('msg.key should not be group or world readable or writable');
      process.exit(12);
    }

    var cert = fs.readFileSync(msg.cert);
    var key = fs.readFileSync(msg.key);
    var dhparam = fs.readFileSync(msg.dhparam);

    // determine ciphers
    var ciphers = [];
    if (~tls.getCiphers().indexOf('ecdhe-rsa-chacha20-poly1305')) {
      ciphers.push('ECDHE-RSA-CHACHA20-POLY1305');
    }
    if (~tls.getCiphers().indexOf('dhe-rsa-chacha20-poly1305')) {
      ciphers.push('DHE-RSA-CHACHA20-POLY1305');
    }
    if (~tls.getCiphers().indexOf('ecdhe-rsa-aes128-gcm-sha256')) {
      ciphers.push('ECDHE-RSA-AES128-GCM-SHA256');
    }
    if (~tls.getCiphers().indexOf('dhe-rsa-aes128-gcm-sha256')) {
      ciphers.push('DHE-RSA-AES128-GCM-SHA256');
    }

    if (!ciphers.length) {
      log.err('no supported ciphers found');
      process.exit(11);
    }

    var uid, gid;
    try {
      uid = posix.getpwnam(user).uid;
      gid = posix.getgrnam(group).gid;
    } catch(err) {
      log.err('wss %s %s:%s', err, user, group);
      process.exit(3);
    }

    // chroot or exit
    try {
      chroot(newRoot, uid, gid);
      log.info('wss changed root to %s and user:group to %s:%s', newRoot, user, group);
    } catch(err) {
      log.err('wss changing root or user failed %j %s', msg, err);
      process.exit(8);
    }

    // set core limit to maximum allowed size
    posix.setrlimit('core', { soft: posix.getrlimit('core').hard });
    log.info('core limit: %j, fsize limit: %j', posix.getrlimit('core'), posix.getrlimit('fsize'));

    // open server or exit
    function openServerAndProceed() {
      // create start and stop tasks, ensure at least one async function

      var opts = {
        secure: true,
        cert: cert,
        key: key,
        dhparam: dhparam,
        secureProtocol: 'SSLv23_server_method',
        secureOptions: c.SSL_OP_NO_SSLv2|c.SSL_OP_NO_SSLv3|c.SSL_OP_NO_TLSv1|c.SSL_OP_NO_TLSv1_1,
        ciphers: ciphers.join(':'),
        handshakeTimeout: 20
      };

      var wsServer = ws.createServer(opts, connHandler);

      wsServer.on('error', function(err) {
        log.err(err);
      });

      wsServer.on('close', function() {
        log.notice('closed');
      });

      // start WebSocket server
      wsServer.listen(port, host, function() {
        log.notice('WebSocket bound %s:%s', host, port);
        process.send('listen');
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

        wsServer.socket.close();
        wsServer.connections.forEach(function(conn) {
          conn.close();
        });
      }

      // ignore kill signals
      process.once('SIGTERM', noop);
      process.once('SIGINT', noop);

      process.once('message', shutdown); // expect msg.type == kill
    }

    openServerAndProceed();
  });
});

process.send('init');
