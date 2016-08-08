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

// return a string identifier of the connection if connected over ip
function getConnectionId(conn) {
  var connId;
  if (conn.remoteAddress) {
    connId = conn.remoteAddress + '-' + conn.remotePort + '-' + conn.localAddress + '-' + conn.localPort;
  } else if (conn._handle && conn._handle.fd) {
    connId = conn._handle.fd;
  } else {
    connId = 'unknown socket';
  }
  return connId;
}

module.exports = getConnectionId;
