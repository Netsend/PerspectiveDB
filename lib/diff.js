/**
 * Copyright 2016 Netsend.
 *
 * This file is part of PerspectiveDB.
 *
 * PerspectiveDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PerspectiveDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PerspectiveDB. If not, see <https://www.gnu.org/licenses/>.
 */

'use strict';

/**
 * Show differences in item compared with base.
 *   + = created
 *   ~ = changed
 *   - = removed
 *
 * @param {Object} item  item to compare with base
 * @param {Object} base  base item
 * @return {Object} object containing all differences
 */
function diff(item, base) {
  var d = {};
  var checked = {};

  // check for added and changed keys
  Object.keys(item).forEach(function(key) {
    if (base.hasOwnProperty(key)) {
      if (JSON.stringify(item[key]) !== JSON.stringify(base[key])) {
        d[key] = '~';
      }
    } else {
      d[key] = '+';
    }

    // speedup check for deleted keys
    checked[key] = true;
  });

  // check for deleted keys
  Object.keys(base).forEach(function(key) {
    if (checked[key]) { return; }
    d[key] = '-';
  });

  return d;
}

module.exports = diff;
