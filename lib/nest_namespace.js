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
 * Create a nested structure based on key names of an object.
 *
 * @param {Object} obj  object with namespace as key and objects
 * @param {String, default: "."} chr  character to split keynames on
 */
function nestNamespace(obj, chr) {
  chr = chr || '.';

  var result = {};
  Object.keys(obj).forEach(function(key) {
    var parts = key.split(chr);
    var firstNs = parts.shift();

    result[firstNs] = result[firstNs] || {};
    var firstObj = result[firstNs];
    var lastObj = firstObj;
    var prevObj = firstObj;

    var lastPart;
    parts.forEach(function(part) {
      lastObj[part] = lastObj[part] || {};
      prevObj = lastObj;
      lastObj = lastObj[part];
      lastPart = part;
    });
    if (parts.length) {
      prevObj[lastPart] = obj[key];
      result[firstNs] = firstObj;
    } else {
      result[firstNs] = obj[key];
    }
  });
  return result;
}

module.exports = nestNamespace;
