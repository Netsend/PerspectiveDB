/**
 * Copyright 2015 Netsend.
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

var programName = 'cm';

var net = require('net');

var async = require('async');
var crc32 = require('crc-32');

// return a string identifier of the source and destination of the socket
function destination(socket) {
  if (typeof socket !== 'object') { throw new TypeError('socket must be an object'); }

  var dest;
  if (socket.remoteAddress) {
    dest = socket.remoteAddress + '-' + socket.remotePort + '-' + socket.localAddress + '-' + socket.localPort;
  } else if (socket.path) {
    dest = socket.path;
  }
  return dest;
}

/**
 * A connection manager that can handle the addition and removal of multiple
 * connections.
 *
 * @param {Object} [opts]  additional options
 * @return {Object} open, registerIncoming and close methods
 *
 * Options
 *   debug {Boolean, default: false}  whether to do extra console logging or not
 *   hide {Boolean, default: false}  whether to suppress errors or not (used in
 *        tests)
 */
function connManager(opts) {
  opts = opts || {};
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }

  if (typeof opts.debug !== 'undefined' && typeof opts.debug !== 'boolean') { throw new TypeError('opts.debug must be a boolean'); }
  if (typeof opts.hide !== 'undefined' && typeof opts.hide !== 'boolean') { throw new TypeError('opts.hide must be a boolean'); }

  var conns = {};

  /**
   * Open a new connection and callback with the new connection after it is connected.
   * Retry if reconnectOnError is true and callback with any new succesfully connected
   * sockets.
   *
   * @param {String} address  path to unix domain socket or ip address
   * @param {Number} [port]  port in case an ip address is passed
   * @param {Object} [opts2]  additional options
   * @param {Function} [cb]  first parameter will be an error or null, second
   *                         parameter will be the created connection.
   *
   * Options
   *   reconnectOnError {Boolean, default false}  whether to try to reconnect if the
   *                    connection is closed because of an error.
   *   maxRetries {Number, default Infinity}  maximum number of reconnection attempts
   *   maxInterval {Number, default 7200}  maximum number of seconds to wait for a
   *               new connection attempt. A binary exponential backoff algorithm is
   *               used.
   */
  function open(address, port, opts2, cb) {
    if (typeof port === 'object') {
      cb = opts2;
      opts2 = port;
      port = null;
    }
    if (typeof opts2 === 'function') {
      cb = opts2;
      opts2 = {};
    }
    if (typeof address !== 'string') { throw new TypeError('address must be a string'); }

    port = port || 0;
    if (typeof port !== 'number') { throw new TypeError('port must be an number'); }

    opts2 = opts2 || {};
    if (typeof opts2 !== 'object') { throw new TypeError('opts must be an object'); }

    cb = cb || function() {};
    if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

    if (typeof opts2.reconnectOnError === 'undefined') { opts2.reconnectOnError = false; }
    if (typeof opts2.maxRetries === 'undefined') { opts2.maxRetries = Infinity; }
    if (typeof opts2.maxInterval === 'undefined') { opts2.maxInterval = 7200; }

    if (typeof opts2.reconnectOnError !== 'boolean') { throw new TypeError('opts.reconnectOnError must be a boolean'); }
    if (typeof opts2.maxRetries !== 'number') { throw new TypeError('opts.maxRetries must be a number'); }
    if (typeof opts2.maxInterval !== 'number') { throw new TypeError('opts.maxInterval must be a number'); }

    var debugAddress = address;
    if (port) {
      debugAddress += ':' + port;
    }

    // create a unique connection id for this connection request
    var connId = crc32.str('' + Math.random());
    connId = connId < 0 ? 4294967296 + connId : connId;
    connId = connId.toString(36);

    conns[connId] = {
      id: connId, // cross reference id
      address: address,
      port: port,
      opts: opts2
    };

    var conn = conns[connId];
    var nextAttempt = 0;

    function connectHandler() {
      if (!opts.hide) { console.error('%s: %s open: connected to %s in %s attempt(s)', programName, connId, debugAddress, nextAttempt); }

      nextAttempt = 0;

      // callback with the new connection
      setTimeout(function() {
        cb(null, conn.s);
      }, 1);
    }

    // schedule a connection attempt, on error, retry up to maxRetries
    function scheduleConnAttempt(sec) {
      nextAttempt++;

      conn.pendingTimeout = setTimeout(function() {
        if (opts.debug) { console.log('%s: %s open: connecting to %s attempt', programName, connId, debugAddress, nextAttempt); }
        if (!port) {
          conn.s = net.createConnection(address, connectHandler);
          // see https://github.com/joyent/node/issues/9120
          conn.s.path = address;
          conn.path = address;
        } else {
          conn.s = net.createConnection(port, address, connectHandler);
        }

        var lastErr = null;
        conn.s.on('error', function(err) {
          lastErr = err;
          if (!opts.hide) { console.error('%s: %s open: conn error: %s', programName, connId, err); }
        });

        conn.s.once('end', function() {
          if (opts.debug) { console.log('%s: %s open: conn end', programName, connId); }
        });

        conn.s.once('close', function(errOccurred) {
          if (opts.debug) { console.log('%s: %s open: conn close', programName, connId); }

          // check if we should reconnect
          if (!opts2.reconnectOnError) {
            cb(lastErr, null);
            return;
          }

          if (errOccurred) {
            if (!opts.hide) { console.error('%s: %s open: conn error: %s', programName, connId, debugAddress); }

            if (address) {
              if (nextAttempt <= opts2.maxRetries) {
                var wait = Math.pow(2, nextAttempt);
                wait = Math.min(wait, opts2.maxInterval);

                if (!opts.hide) { console.error('%s: %s open: reconnecting to %s in %s seconds. max retries %s', programName, connId, debugAddress, wait, opts2.maxRetries); }
                scheduleConnAttempt(wait);
              } else {
                if (!opts.hide) { console.error('%s: %s open: maximum number of attempts reached %s, no more attemps', programName, connId, nextAttempt); }
                cb(new Error('maxRetries reached'));
                return;
              }
            } else {
              if (!opts.hide) { console.error('%s: %s open: can not reconnect, no path or remote address', programName, connId); }
              cb(new Error('no address to reconnect'));
              return;
            }
          }
        });
      }, sec * 1000);
    }

    scheduleConnAttempt();
  }

  /**
   * @param {net.Socket} socket  connection to register
   */
  function registerIncoming(socket) {
    if (typeof socket !== 'object') { throw new TypeError('socket must be an object'); }

    // create a unique connection id for this connection
    var connId = crc32.str('' + Math.random());
    connId = connId < 0 ? 4294967296 + connId : connId;
    connId = connId.toString(36);

    conns[connId] = {
      id: connId, // cross reference id
      address: socket.remoteAddress,
      port: socket.remotePort,
      s: socket
    };

    if (opts.debug) { console.log('%s: %s registerIncoming: connection %s registered', programName, connId, Object.keys(conns).length); }

    socket.once('close', function() {
      if (opts.debug) { console.log('%s: %s registerIncoming: conn %s closed', programName, connId, Object.keys(conns).length); }
      _remove(connId);
    });
  }

  function close(cb) {
    if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

    async.each(Object.keys(conns), function(connId, cb2) {
      var conn = conns[connId];

      if (opts.debug) { console.log('%s: %s close: closing connection', programName, connId); }

      clearTimeout(conn.pendingTimeout);

      if (!conn.s) {
        // not yet initialized
        _remove(connId);
        process.nextTick(cb2);
        return;
      }

      var closed = false;
      conn.s.once('close', function() {
        if (opts.debug) { console.log('%s: %s close: conn closed', programName, connId); }
        closed = true;
        cb2();
      });

      // give the peer some time to end
      setTimeout(function() {
        if (closed) { return; }

        if (!opts.hide) { console.log('%s: %s close: conn destroyed', programName, connId); }
        conn.s.destroy();
        _remove(connId);
        cb2();
      }, 100);

      conn.s.end();
    }, cb);
  }

  /**
   * Remove given connection.
   *
   * @param {String} connId  connection id to remove
   * @return {Boolean} true if removed, false if not found
   */
  function _remove(connId) {
    if (typeof connId !== 'string') { throw new TypeError('connId must be a string'); }

    if (!conns[connId]) {
      if (!opts.hide) { console.error('%s: %s _remove: not found', programName, connId); }
      return false;
    }

    delete conns[connId];
    if (opts.debug) { console.log('%s: %s _remove: removed', programName, connId); }
    return true;
  }

  return {
    open: open,
    registerIncoming: registerIncoming,
    close: close,
    _conns: conns,
    _remove: _remove
  };
}

module.exports.create = connManager;
module.exports.destination = destination;
