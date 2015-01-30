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

// return a string identifier of the connection if connected over ip
function createId(conn) {
  if (typeof conn !== 'object') { throw new TypeError('conn must be an object'); }

  var connId;
  if (conn.remoteAddress) {
    connId = conn.remoteAddress + '-' + conn.remotePort + '-' + conn.localAddress + '-' + conn.localPort;
  } else {
    connId = 'UNIX domain socket';
  }
  return connId;
}

// return a connection manager with the methods register and close.
/**
 * A connection manager that can handle the addition and removal of multiple
 * connections.
 *
 * @param {Object} [opts]  additional options
 * @return {Object} register and close methods
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
   * @param {Object} [opts]  additional options
   * @return {Object} register and close methods
   *
   * Options
   *   reconnectOnError {Booelan, default false}  whether to try to reconnect if the
   *                    connection is closed because of an error.
   *   maxRetries {Number, default Infinity}  maximum number of reconnection attempts
   *   maxInterval {Number, default 7200}  maximum number of seconds to wait for a
   *               new connection attempt. A binary exponential backoff algorithm is
   *               used.
   */
  function register(conn, opts2) {
    if (typeof conn !== 'object') { throw new TypeError('conn must be an object'); }

    opts2 = opts2 || {};
    if (typeof opts2 !== 'object') { throw new TypeError('opts must be an object'); }

    if (typeof opts2.reconnectOnError === 'undefined') { opts2.reconnectOnError = false; }
    if (typeof opts2.maxRetries === 'undefined') { opts2.maxRetries = Infinity; }
    if (typeof opts2.maxInterval === 'undefined') { opts2.maxInterval = 7200; }

    if (typeof opts2.reconnectOnError !== 'boolean') { throw new TypeError('opts.reconnectOnError must be a boolean'); }
    if (typeof opts2.maxRetries !== 'number') { throw new TypeError('opts.maxRetries must be a number'); }
    if (typeof opts2.maxInterval !== 'number') { throw new TypeError('opts.maxInterval must be a number'); }

    var connId = createId(conn);

    var remoteAddr, remotePort;

    if (opts.debug) { console.log('%s: register: conn registering %s', programName, conns.length, connId); }

    conns.push(conn);

    // update connection id after connected
    conn.once('connect', function() {
      var oConnId = connId;
      connId = createId(conn);
      if (opts.debug) { console.log('%s: register: updated connId from %s to %s', programName, oConnId, connId); }

      remoteAddr = conn.remoteAddress;
      remotePort = conn.remotePort;
    });

    conn.on('error', function(err) {
      if (!opts.hide) { console.error('%s: register: conn error: %s %s', programName, err, connId); }
    });

    conn.once('end', function() {
      if (opts.debug) { console.log('%s: register: conn end: %s', programName, connId); }
    });

    conn.once('close', function(errOccurred) {
      if (opts.debug) { console.log('%s: register: conn close: %s', programName, connId); }
      _remove(conn);

      var nextAttempt = 1;
      // schedule a connection attempt, on error, retry up to maxRetries
      function scheduleConnAttempt(sec) {
        nextAttempt++;

        setTimeout(function() {
          function errHandler(err) {
            if (!opts.hide) { console.error('%s: register: reconnect %s:%s attempt %s error %s', programName, remoteAddr, remotePort, nextAttempt - 1, err); }

            if (nextAttempt <= opts2.maxRetries) {
              var wait = Math.pow(2, nextAttempt);
              wait = Math.min(wait, opts2.maxInterval);

              if (!opts.hide) { console.error('%s: register: retry in %s seconds', programName, wait); }
              scheduleConnAttempt(wait);
            } else {
              if (!opts.hide) { console.error('%s: register: maximum number of attempts reached %s, no more attemps', programName, nextAttempt - 1); }
            }
          }

          var newConn = net.createConnection(remotePort, remoteAddr, function() {
            // connected, remove error handler and register connection
            newConn.removeListener('error', errHandler);
            if (!opts.hide) { console.error('%s: register: reconnected to %s:%s in %s attempt(s)', programName, remoteAddr, remotePort, nextAttempt - 1); }
            register(newConn);
          });

          newConn.once('error', errHandler);
        }, sec * 1000);
      }

      if (errOccurred && opts2.reconnectOnError) {
        if (remoteAddr) {
          if (!opts.hide) { console.log('%s: register: reconnecting to %s:%s in %s seconds', programName, remoteAddr, remotePort, Math.pow(2, nextAttempt)); }
          scheduleConnAttempt(Math.pow(2, nextAttempt));
        } else {
          if (!opts.hide) { console.error('%s: register: can not reconnect, no remote address: %s', programName, connId); }
          // see https://github.com/joyent/node/issues/9120
        }
      }
    });
  }

  function close(cb) {
    if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

    async.each(conns, function(conn, cb2) {
      var connId = createId(conn);

      if (opts.debug) { console.log('%s: close: closing connection %s...', programName, connId); }

      var closed = false;
      conn.once('close', function() {
        if (opts.debug) { console.log('%s: close: conn closed %s', programName, connId); }
        closed = true;
        cb2();
      });

      // give the peer some time to end
      setTimeout(function() {
        if (closed) { return; }

        if (!opts.hide) { console.log('%s: close: conn destroyed %s', programName, connId); }
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

    var connId = createId(conn);

    if (opts.debug) { console.log('%s: _remove: conn close: %s', programName, connId); }
    var i = conns.indexOf(conn);
    if (~i) {
      if (opts.debug) { console.log('%s: _remove: removing conn %s', programName, i); }
      conns.splice(i, 1);
      return true;
    }

    if (!opts.hide) { console.error('%s: _remove: conn not found %s', programName); }
    return false;
  }

  return {
    register: register,
    close: close,
    _conns: conns,
    _remove: _remove
  };
}

module.exports.create = connManager;
module.exports.createId = createId;
