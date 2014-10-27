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

var async = require('async');

/**
 * Fetch alle items in the provided versioned collections. Returns an object
 * containing the db and collection name as key. The value is an object with 
 * an items and m3items array.
 *
 * @params {VersionedCollection} vc  one or more versioned collections
 * @return {Object} contents of both collections and versioned collections
 */
function fetchItems() {
  var args = Array.prototype.slice.call(arguments);

  var cb = args.pop();

  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var result = {};
  async.eachSeries(args, function(vc, cb2) {
    var key = vc.databaseName + '.' + vc.collectionName;
    vc._collection.find({}, { sort: { $natural: 1 } }).toArray(function(err, items) {
      if (err) { return cb2(err); }
      result[key] = { items: items };
      vc._snapshotCollection.find({}, { sort: { $natural: 1 } }).toArray(function(err, items) {
        if (err) { return cb2(err); }
        result[key].m3items = items;
        cb2();
      });
    });
  }, function(err) {
    cb(err, result);
  });
}

module.exports = fetchItems;
