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

var t = require('./core-util-is-fork');

/**
 * Check if id is valid. It should either be a buffer, a String or any other
 * object that implements the "toString" method.
 *
 * @param {Buffer|String|implement "toString"} id  id to test
 * @return {String} empty string if ok otherwise a problem description
 */
function invalidId(id) {
  if (t.isBuffer(id)) {
    if (id.length > 254) {
      return 'id must not exceed 254 bytes';
    }
    return '';
  }

  if (t.isString(id)) {
    if (Buffer.byteLength(id) > 254) {
      return 'id must not exceed 254 bytes';
    }
    return '';
  }

  if (t.isNullOrUndefined(id)) {
    return 'id must be a buffer, a string or implement "toString"';
  }

  try {
    var str = id.toString();
    if (Buffer.byteLength(str) > 254) {
      return 'id must not exceed 254 bytes';
    }
    return '';
  } catch(err) {
    return 'id must be a buffer, a string or implement "toString"';
  }

  return '';
}

module.exports = invalidId;
