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

/* jshint -W116 */

'use strict';

var invalidId = require('./invalid_id');

/**
 * Check if h is a valid header.
 *
 * Valid structure:
 *   h: {Object}  header containing the following values:
 *     id:   {mixed}  id of this item
 *     v:    {String}  version
 *     pa:   {Array}  parent versions
 *     [pe]: {String}  optional perspective
 *     [i]:  {Number}  local monotonically increasing increment
 *     [d]:  {Boolean}  true if this id is deleted
 *     [c]:  {Boolean}  true if this item could not be merged with another item
 *
 * @param {Object} h  header to check
 * @return {String} empty string if nothing is wrong or a problem description
 */
function invalidHeader(h) {
  if (typeof h !== 'object') {
    return 'h must be an object';
  }

  var error = invalidId(h.id);
  if (error) {
    return 'h.' + error;
  }

  if (typeof h.v !== 'string') {
    return 'h.v must be a string';
  }

  if (!Array.isArray(h.pa)) {
    return 'h.pa must be an array';
  }

  var expectedKeys = 3;

  if (h.pe != null) {
    if (typeof h.pe !== 'string') {
      return 'h.pe must be a string';
    }
    expectedKeys++;
  }

  if (h.i != null) {
    if (typeof h.i !== 'number') {
      return 'h.i must be a number';
    }
    expectedKeys++;
  }

  if (h.d != null) {
    if (typeof h.d !== 'boolean') {
      return 'h.d must be a boolean';
    }
    expectedKeys++;
  }

  if (h.c != null) {
    if (typeof h.c !== 'boolean') {
      return 'h.c must be a boolean';
    }
    expectedKeys++;
  }

  if (Object.keys(h).length !== expectedKeys) {
    return 'h should only optionally contain "pe", "i", "d" and "c" keys';
  }

  return '';
}

module.exports = invalidHeader;
