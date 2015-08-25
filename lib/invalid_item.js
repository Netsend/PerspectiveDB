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
var invalidHeader = require('./invalid_header');

/**
 * Check if item contains a valid "h", and optionally a "b" and "m" property.
 *
 * Item should have the following structure:
 * {
 *   h: {Object}  header containing the following values:
 *     id:  {mixed}  id of this h
 *     pe:  {String}  perspective
 *     v:   {base64 String}  version
 *     pa:  {Array}  parent versions
 *     [d]: {Boolean}  true if this id is deleted
 *   [b]: {mixed}  body, the document
 *   [m]: {mixed}  meta info to store with this document
 * }
 *
 * @param {Object} item  item to check
 * @return {String} empty string if nothing is wrong or a problem description
 */
function invalidItem(item) {
  if (!t.isObject(item)) {
    return 'item must be an object';
  }

  var error = invalidHeader(item.h);
  if (error) {
    return 'item.' + error;
  }

  var expectedKeys = 1;

  if (!t.isNullOrUndefined(item.b)) {
    expectedKeys++;
  }

  if (!t.isNullOrUndefined(item.m)) {
    expectedKeys++;
  }

  if (Object.keys(item).length !== expectedKeys) {
    return 'item should only contain "h" and optionally "b" and "m" keys';
  }

  return '';
}

module.exports = invalidItem;
