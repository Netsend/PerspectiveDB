/**
 * Copyright 2015 Netsend.
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

/**
 * Check if req is a valid data request.
 *
 * @param {mixed} req  the request to inspect
 * @return {Boolean} true if the req is a valid data request, false otherwise
 */
function valid(req) {
  if (typeof req !== 'object' || req === null) { return false; }

  if (typeof req.start !== 'string') { return false; }

  // check for additional keys
  if (Object.keys(req).length > 1) { return false; }

  // check min string lengths
  if (req.start.length < 1) { return false; }

  // check max string lengths
  if (req.start.length > 256) { return false; }

  return true;
}

module.exports.valid = valid;
