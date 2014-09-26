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

/**
 * Iterate over all object keys (and optionally recurse) and run a transformation
 * on each key. Modify the object in-place.
 *
 * @param {Object} obj  object to transform
 * @param {Function} transformer  first parameter will be the key to transform,
 *                                second parameter is the value of that key
 *                                (though this is informational only). This
 *                                function should return the new key to be used.
 * @param {Boolean, default: false} recurse  whether or not to recurse
 * @return {undefined}  replaces keys in-place
 */
function transform(obj, transformer, recurse) {
  if (typeof obj !== 'object') { throw new TypeError('obj must be an object'); }
  if (typeof transformer !== 'function') { throw new TypeError('transformer must be a function'); }

  recurse = recurse || false;
  if (typeof recurse !== 'boolean') { throw new TypeError('recurse must be a boolean'); }

  Object.keys(obj).forEach(function(key) {
    // see if we have to recurse
    if (recurse && typeof obj[key] === 'object' && Object.keys(obj[key]).length) {
      transform(obj[key], transformer, recurse);
    }

    var transformed = transformer(key, obj[key]);
    if (transformed !== key) {
      obj[transformed] = obj[key];
      delete obj[key];
    }
  });
}

module.exports = transform;
