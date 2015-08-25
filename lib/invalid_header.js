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
var invalidId = require('./invalid_id');

/**
 * Check if h is a valid remote header.
 *
 * Valid structure:
 *   h: {Object}  header containing the following values:
 *     id:   {mixed}  id of this item
 *     v:    {String}  version
 *     pa:   {Array}  parent versions
 *     [d]:  {Boolean}  true if this id is deleted
 *     [c]:  {Boolean}  true if this item could not be merged with another item
 *
 * @param {Object} h  header to check
 * @return {String} empty string if nothing is wrong or a problem description
 */
function invalidHeader(h) {
  if (!t.isObject(h)) {
    return 'h must be an object';
  }

  var error = invalidId(h.id);
  if (error) {
    return 'h.' + error;
  }

  if (!t.isString(h.v)) {
    return 'h.v must be a string';
  }

  if (!t.isArray(h.pa)) {
    return 'h.pa must be an array';
  }

  var expectedKeys = 3;

  if (!t.isNullOrUndefined(h.d)) {
    if (!t.isBoolean(h.d)) {
      return 'h.d must be a boolean';
    }
    expectedKeys++;
  }

  if (!t.isNullOrUndefined(h.c)) {
    if (!t.isBoolean(h.c)) {
      return 'h.c must be a boolean';
    }
    expectedKeys++;
  }

  if (Object.keys(h).length !== expectedKeys) {
    return 'h should only optionally contain "d" and "c" keys';
  }

  return '';
}

module.exports = invalidHeader;
