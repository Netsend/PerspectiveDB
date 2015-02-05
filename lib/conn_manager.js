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
function destination(conn) {
  if (typeof conn !== 'object') { throw new TypeError('conn must be an object'); }

  var dest;
  if (conn.remoteAddress) {
    dest = conn.remoteAddress + '-' + conn.remotePort + '-' + conn.localAddress + '-' + conn.localPort;
  } else if (conn.path) {
    dest = conn.path;
  }
  return dest;
}

/**
 * A connection manager that can handle the addition and removal of multiple
 * connections.
 *
 * @param {Object} [opts]  additional options
 * @return {Object} open, register and close methods
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

  var conns = [];

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

    var conn;
    var nextAttempt = 0;

    function connectHandler() {
      if (!opts.hide) { console.error('%s: %s open: connected to %s in %s attempt(s) reset attempts', programName, conn.id, address, nextAttempt); }

      nextAttempt = 0;

      // callback with the new connection
      setTimeout(function() {
        cb(null, conn);
      }, 1);
    }

    // schedule a connection attempt, on error, retry up to maxRetries
    function scheduleConnAttempt(sec) {
      nextAttempt++;

      setTimeout(function() {
        var connId = crc32.str('' + Math.random());
        connId = connId < 0 ? 4294967296 + connId : connId;
        connId = connId.toString(36);

        if (!port) {
          if (opts.debug) { console.log('%s: %s open: connecting to %s attempt', programName, connId, address, nextAttempt); }
          conn = net.createConnection(address, connectHandler);
          // see https://github.com/joyent/node/issues/9120
          conn.path = address;
        } else {
          if (opts.debug) { console.log('%s: %s open: connecting to %s:%s attempt', programName, connId, address, port, nextAttempt); }
          conn = net.createConnection(port, address, connectHandler);
        }

        if (typeof conn.id !== 'undefined') {
          if (!opts.hide) { console.error('%s: open: conn.id is already defined: %s', programName, connId, conn.id); }
          throw new Error('conn.id is already defined');
        }

        conn.id = connId;

        conn.on('error', function(err) {
          if (!opts.hide) { console.error('%s: %s open: conn error: %s', programName, connId, err); }
        });

        conn.once('end', function() {
          if (opts.debug) { console.log('%s: %s open: conn end', programName, connId); }
        });

        conn.once('close', function(errOccurred) {
          if (opts.debug) { console.log('%s: %s open: conn close', programName, connId); }

          if (errOccurred) {
            if (!opts.hide) { console.error('%s: %s open: conn error: %s', programName, connId, address); }

            if (opts2.reconnectOnError) {
              if (address) {
                if (nextAttempt <= opts2.maxRetries) {
                  var wait = Math.pow(2, nextAttempt);
                  wait = Math.min(wait, opts2.maxInterval);

                  if (!opts.hide) { console.error('%s: %s open: reconnecting to %s in %s seconds. max retries %s', programName, connId, address, wait, opts2.maxRetries); }
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
            } else {
              cb(null, null);
            }
          }
        });

        register(conn);
      }, sec * 1000);
    }

    scheduleConnAttempt();
  }

  /**
   * @param {net.Socket} conn  connection to register
   */
  function register(conn) {
    if (typeof conn !== 'object') { throw new TypeError('conn must be an object'); }

    conns.push(conn);
    if (opts.debug) { console.log('%s: %s register: connection registered at %s', programName, conn.id, conns.length - 1); }

    conn.once('close', function() {
      if (opts.debug) { console.log('%s: %s register: conn closed', programName, conn.id); }
      _remove(conn);
    });
  }

  function close(cb) {
    if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

    async.each(conns, function(conn, cb2) {
      var dest = destination(conn);

      if (opts.debug) { console.log('%s: %s close: closing connection %s', programName, conn.id, dest); }

      var closed = false;
      conn.once('close', function() {
        if (opts.debug) { console.log('%s: %s close: conn closed', programName, conn.id); }
        closed = true;
        cb2();
      });

      // give the peer some time to end
      setTimeout(function() {
        if (closed) { return; }

        if (!opts.hide) { console.log('%s: %s close: conn destroyed', programName, conn.id); }
        conn.destroy();
        _remove(conn);
        cb2();
      }, 100);

      conn.end();
    }, cb);
  }

  /**
   * Remove given collection.
   *
   * @param {Object} conn  connection to remove
   * @return {Boolean} true if removed, false if not found
   */
  function _remove(conn) {
    if (typeof conn !== 'object') { throw new TypeError('conn must be an object'); }

    var i = conns.indexOf(conn);
    if (~i) {
      conns.splice(i, 1);
      if (opts.debug) { console.log('%s: %s _remove: removed from %s', programName, conn.id, i); }
      return true;
    }

    if (!opts.hide) { console.error('%s: %s _remove: not at %s', programName, conn.id, i); }
    return false;
  }

  return {
    open: open,
    register: register,
    close: close,
    _conns: conns,
    _remove: _remove
  };
}

module.exports.create = connManager;
module.exports.destination = destination;
