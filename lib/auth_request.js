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
 * Check if req is a valid auth request.
 *
 * @param {mixed} req  the request to inspect
 * @return {Boolean} true if the req is a valid auth request, false otherwise
 */
function valid(req) {
  /* jshint maxcomplexity: 27 */ /* lot's simple if statements */

  if (typeof req !== 'object' || req === null) { return false; }

  if (typeof req.username   !== 'string') { return false; }
  if (typeof req.password   !== 'string') { return false; }
  if (typeof req.database   !== 'string') { return false; }
  if (typeof req.collection !== 'string') { return false; }

  var inspectedKeys = 4;

  // offset is optional
  if (typeof req.offset !== 'undefined') {
    if (typeof req.offset !== 'number') { return false; }
    inspectedKeys = 5;
  }

  // check for additional keys
  if (Object.keys(req).length > inspectedKeys) { return false; }

  // check min string lengths
  if (req.username.length    < 1) { return false; }
  if (req.password.length    < 1) { return false; }
  if (req.database.length    < 1) { return false; }
  if (req.collection.length  < 1) { return false; }

  // check max string lengths
  if (req.username.length   > 128) { return false; }
  if (req.password.length   > 256) { return false; }
  if (req.database.length   > 128) { return false; }
  if (req.collection.length > 128) { return false; }

  return true;
}

/**
 * Map auth fields of the given request.
 *
 * @param {Object} req  the request to copy
 * @return {Object} containing the keys username, password, database and
 *                  collection, and offset only if in req.
 */
function map(req) {
  if (typeof req !== 'object' || req === null) { throw new TypeError('req must be an object'); }

  var result = {
    username: req.username,
    password: req.password,
    database: req.database,
    collection: req.collection,
  };

  if (typeof req.offset !== 'undefined') {
    result.offset = req.offset;
  }

  return result;
}

module.exports.valid = valid;
module.exports.map = map;
