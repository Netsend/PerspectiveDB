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
 * Check if req is a valid pull request.
 *
 * @param {mixed} req  the request to inspect
 * @return {Boolean} true if the req is a valid pull request, false otherwise
 *
 * {
 *   username:     {String}
 *   password:     {String}
 *   [path]:       {String}
 *   [host]:       {String}
 *   [port]:       {Number}
 *   [database]:   {String}
 *   [collection]: {String}
 * }
 * only path or host and port should be set, not both
 */
function valid(req) {
  /* jshint maxcomplexity: 27 */ /* lot's simple if statements */

  if (typeof req !== 'object' || req === null) { return false; }

  if (typeof req.username   !== 'string') { return false; }
  if (typeof req.password   !== 'string') { return false; }

  var inspectedKeys = 2;

  // either path or host and port should be set, not both
  if (typeof req.path !== 'undefined' && (typeof req.host !== 'undefined' || typeof req.port !== 'undefined')) { return false; }

  // rest is optional
  if (req.hasOwnProperty('path')) {
    if (typeof req.path !== 'undefined' && typeof req.path !== 'string') { return false; }
    inspectedKeys++;
  }

  if (req.hasOwnProperty('host')) {
    if (typeof req.host !== 'undefined' && typeof req.host !== 'string') { return false; }
    inspectedKeys++;
  }

  if (req.hasOwnProperty('port')) {
    if (typeof req.port !== 'undefined' && typeof req.port !== 'number') { return false; }
    inspectedKeys++;
  }

  if (req.hasOwnProperty('database')) {
    if (typeof req.database !== 'undefined' && typeof req.database !== 'string') { return false; }
    inspectedKeys++;
  }

  if (req.hasOwnProperty('collection')) {
    if (typeof req.collection !== 'undefined' && typeof req.collection !== 'string') { return false; }
    inspectedKeys++;
  }

  // check for additional keys
  if (Object.keys(req).length > inspectedKeys) { return false; }

  return true;
}

module.exports.valid = valid;
