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
 * Check if req is a valid push request.
 *
 * @param {Object} [req]  the request to inspect
 * @return {Boolean} true if the req is a valid push request, false otherwise
 *
 * Valid request properties (all optional):
 *   filter {Object}
 *   hooks {Array}
 *   hooksOpts {Object}
 *   offset {String}
 */
function valid(req) {
  if (typeof req === 'undefined' || req === null) { return true; }

  if (typeof req !== 'object') { return false; }

  var expectedNumberOfKeys = 0;

  if (req.hasOwnProperty('filter')) {
    if (typeof req.filter !== 'undefined' && typeof req.filter !== 'object') { return false; }
    expectedNumberOfKeys++;
  }

  if (req.hasOwnProperty('hooks')) {
    if (!Array.isArray(req.hooks)) { return false; }
    expectedNumberOfKeys++;
  }

  if (req.hasOwnProperty('hooksOpts')) {
    if (typeof req.hooksOpts !== 'undefined' && typeof req.hooksOpts !== 'object') { return false; }
    expectedNumberOfKeys++;
  }

  if (req.hasOwnProperty('offset')) {
    if (typeof req.offset !== 'undefined' && typeof req.offset !== 'string') { return false; }
    expectedNumberOfKeys++;
  }

  // check for additional keys
  if (Object.keys(req).length > expectedNumberOfKeys) { return false; }

  // check max string lengths
  if (req.offset && req.offset.length > 128) { return false; }

  return true;
}

module.exports.valid = valid;
