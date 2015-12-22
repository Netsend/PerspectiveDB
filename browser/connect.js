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

var errors = [];
if (!window.hasOwnProperty('WebSocket')) {
  errors.push('missing WebSocket API');
}

if (errors.length) {
  console.error(errors);
  throw new Error(errors.join(', '));
}

// connect to given url and start syncing
function connect(url, authReq, ready, close) {
  var ws = new WebSocket(url, 1); // support protocol 1 only

  // determine different phases
  var LOGIN = 1;
  var DATA  = 2;

  // LOGIN is the start state
  var state = LOGIN;

  ws.onmessage = function (event) {
    switch (state) {
    case LOGIN:
      // expect a data request
      var dataReq = JSON.parse(event.data);
      console.log('message login data req', dataReq);
      // determine offset and send back a data request
      var req = { start: true };
      ws.send(JSON.stringify(req) + '\n');
      state = DATA;
      break;
    case DATA:
      console.log('message data req', event.data);
      break;
    }
  };

  ws.onerror = function(event) {
    console.error('error', event);
  };

  ws.onclose = close;

  ws.onopen = function(event) {
    setTimeout(function() {
      ready();
    }, 0);
    // send an auth request
    ws.send(JSON.stringify(authReq) + '\n');
  };
}

module.exports = connect;
