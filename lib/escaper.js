/**
 * Copyright 2014 Netsend.
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

var keyEsc = require('mongo-key-escape');
var transform = require('./object_transform');

/**
 * Escape any key in the given object that has a $ or . in it.
 *
 * @param {Object} obj  object to transform
 * @param {Boolean, default: false} recurse  whether or not to recurse
 * @return {undefined}  replaces keys in place
 */
function escape(obj, recurse) {
  transform(obj, keyEsc.escape, recurse);
  return obj;
}

/**
 * Unescape any key in the given object that has a $ or . in it.
 *
 * @param {Object} obj  object to transform
 * @param {Boolean, default: false} recurse  whether or not to recurse
 * @return {undefined}  replaces keys in place
 */
function unescape(obj, recurse) {
  transform(obj, keyEsc.unescape, recurse);
  return obj;
}

module.exports.escape = escape;
module.exports.unescape = unescape;
