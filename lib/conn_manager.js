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
function connManager(opts) {
  opts = opts || {};
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }

  if (typeof opts.debug !== 'undefined' && typeof opts.debug !== 'boolean') { throw new TypeError('opts.debug must be a boolean'); }
  if (typeof opts.hide !== 'undefined' && typeof opts.hide !== 'boolean') { throw new TypeError('opts.hide must be a boolean'); }

  var conns = [];

  function register(conn) {
    if (typeof conn !== 'object') { throw new TypeError('conn must be an object'); }

    var connId = createId(conn);

    if (opts.debug) { console.log('%s: conn registering %s', programName, conns.length, connId); }

    conns.push(conn);

    // update connection id after connected
    conn.once('connect', function() {
      var oConnId = connId;
      connId = createId(conn);
      if (opts.debug) { console.log('%s: updated connId from %s to %s', programName, oConnId, connId); }
    });

    conn.on('error', function(err) {
      if (!opts.hide) { console.error('%s: conn error: %s %s', programName, err, connId); }
    });

    conn.once('end', function() {
      if (opts.debug) { console.log('%s: conn end: %s', programName, connId); }
    });

    conn.once('close', function() {
      _remove(conn);
    });
  }

  function close(cb) {
    if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

    async.each(conns, function(conn, cb2) {
      var connId = createId(conn);

      if (opts.debug) { console.log('%s: closing connection %s...', programName, connId); }

      var closed = false;
      conn.once('close', function() {
        if (opts.debug) { console.log('%s: conn closed %s', programName, connId); }
        closed = true;
        cb2();
      });

      // give the peer some time to end
      setTimeout(function() {
        if (closed) { return; }

        if (!opts.hide) { console.log('%s: conn destroyed %s', programName, connId); }
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

    if (opts.debug) { console.log('%s: conn close: %s', programName, connId); }
    var i = conns.indexOf(conn);
    if (~i) {
      if (opts.debug) { console.log('%s: removing conn %s', programName, i); }
      conns.splice(i, 1);
      return true;
    }

    if (!opts.hide) { console.error('%s: conn not found %s', programName); }
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
