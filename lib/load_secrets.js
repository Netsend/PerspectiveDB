/**
 * Copyright 2016 Netsend.
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

'use strict';

var fs = require('fs');

var hjson = require('hjson');

/**
 * Load a hjson file from disk, optionally only take the given keys. Checks
 * permissions as well.
 *
 * @param {String} path  path to the hjson file
 * @param {Array} [keys]  optional set of keys to return if not the whole object is
 *                        needed
 */
function loadSecrets(file, keys) {
  if (!file || typeof file !== 'string') { throw new TypeError('file must be a non-empty string'); }

  if (typeof keys === 'string') { keys = [keys]; }
  if (!keys) { keys = []; }
  if (!Array.isArray(keys)) { throw new TypeError('keys must be a string or an array'); }

  var stats = fs.statSync(file);
  if ((stats.mode & 6) !== 0) { throw new Error('file must not be world readable or writable'); }

  var obj = hjson.parse(fs.readFileSync(file, { encoding: 'utf8' }));
  var result = {};
  if (keys.length) {
    keys.forEach(function(key) {
      result[key] = obj[key];
    });
  } else {
    result = obj;
  }
  return result;
}

module.exports = loadSecrets;
