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
 * Creates a capped collection.
 *
 * @param {mongodb.Db] db  database connection
 * @param {String} oplogCollection  name of the collection to create
 * @param {Function} cb  first parameter error or null
 */
function createCappedColl(db, oplogCollection, cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  db.dropCollection(oplogCollection, function(err) {
    if (err) {
      if (err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) {
        console.error('ERROR createCappedColl drop', err);
        return cb(err);
      }
    }
    db.createCollection(oplogCollection, {
      autoIndexId: true,
      capped: true,
      size: 1000,
      strict: true,
      w: 1
    }, function(err) {
      if (err) {
        console.error('ERROR createCappedColl create', err);
        return cb(err);
      }
      cb();
    });
  });
}

module.exports = createCappedColl;
