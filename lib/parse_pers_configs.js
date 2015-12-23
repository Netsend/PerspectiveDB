/**
 * Copyright 2014, 2015 Netsend.
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

/* jshint -W116 */

'use strict';

var url = require('url');

/**
 * Quickly parse array of perspective configs. Determine which perspectives should
 * be connected to and parse connection url. Create Index of perspectives by name.
 *
 * Return:
 *   connect: [pers1, pers2, ...]  // perspectives this db should init
 *   pers: {
 *     name:
 *       import:
 *         filter
 *         hooks
 *         hooksOpts
 *       export:
 *         filter
 *         hooks
 *         hooksOpts
 *   }
 */
function parsePersConfigs(arr) {
  var result = {
    connect: [],
    pers: {}
  };

  arr.forEach(function(pcfg) {
    if (pcfg.connect) {
      result.connect.push(pcfg.name);
      pcfg.connect = url.parse(pcfg.connect);
    }
    result.pers[pcfg.name] = pcfg;
  });

  return result;
}

module.exports = parsePersConfigs;
