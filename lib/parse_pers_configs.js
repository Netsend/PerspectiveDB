/**
 * Copyright 2014, 2015 Netsend.
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

/* jshint -W116 */

'use strict';

var url = require('url');

/**
 * Quickly parse array of perspective configs. Determine which perspectives should
 * be connected to and parse connection url. Create Index of perspectives by name.
 *
 * Return:
 *   connect: [ // perspectives this db should init
 *     {
 *       username    {String}
 *       password    {String}
 *       connect {
 *         pathname  {String}
 *         protocol  {String}
 *         hostname  {String}
 *         port      {String}
 *       }
 *     },
 *     ...
 *   ]
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
 *
 * Objects in connect look like:
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
