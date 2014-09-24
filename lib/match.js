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
 * Check if all given criteria hold on the given object.
 *
 * Treats $in special.
 *
 * @param {Object} criteria  search conditions
 * @param {Object} obj  object to compare with
 * @return {Boolean} true if all criteria hold, false otherise
 */
function match(criteria, obj) {
  if (typeof criteria !== 'object') { throw new TypeError('criteria must be an object'); }

  return Object.keys(criteria).every(function(key) {
    // treat $in key special
    if (key === '$in') { return true; }
    if (criteria[key].hasOwnProperty('$in')) {
      if (!Array.isArray(criteria[key].$in)) { throw new TypeError('$in keys should point to an array'); }
      return criteria[key].$in.some(function(el) {
        return el === obj[key];
      });
    }

    // else compare or recurse
    if (~['string', 'number', 'boolean'].indexOf(typeof criteria[key])) {
      return criteria[key] === obj[key];
    } else if (typeof criteria[key] === 'object' && Object.keys(criteria[key]).length) {
      // recurse if obj[key] also has any properties
      if (typeof obj[key] !== 'object' || !Object.keys(obj[key]).length) {
        return false;
      }
      return match(criteria[key], obj[key]);
    } else {
      return JSON.stringify(criteria[key]) === JSON.stringify(obj[key]);
    }
  });
}

module.exports = match;
