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

/**
 * Escape any key in the given object that has a $ or . in it.
 *
 * @param {Object} obj  object to transform
 * @param {Function} transform  function that get's a key and should return one
 * @param {Boolean, default: false} recurse  whether or not to recurse
 * @return {undefined}  replaces keys in place
 */
function _transform(obj, transform, recurse) {
  if (typeof obj !== 'object') { throw new TypeError('obj must be an object'); }
  if (typeof transform !== 'function') { throw new TypeError('transform must be a function'); }

  recurse = recurse || false;
  if (typeof recurse !== 'boolean') { throw new TypeError('recurse must be a boolean'); }

  Object.keys(obj).forEach(function(key) {
    // see if we have to recurse
    if (recurse && typeof obj[key] === 'object' && Object.keys(obj[key]).length) {
      _transform(obj[key], transform, recurse);
    }

    var transformed = transform(key);
    if (transformed !== key) {
      obj[transformed] = obj[key];
      delete obj[key];
    }
  });
}

function escape(obj, recurse) {
  _transform(obj, keyEsc.escape, recurse);
  return obj;
}

function unescape(obj, recurse) {
  _transform(obj, keyEsc.unescape, recurse);
  return obj;
}

module.exports._transform = _transform;
module.exports.escape = escape;
module.exports.unescape = unescape;
