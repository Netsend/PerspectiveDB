/**
 * Copyright 2015 Netsend.
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

function runHooks(hooks, db, item, opts, cb) {
  async.eachSeries(hooks, function(hook, callback) {
    hook(db, item, opts, function(err, afterItem) {
      if (err) { return callback(err); }
      if (!afterItem) { return callback(new Error('item filtered')); }

      item = afterItem;
      callback(err);
    });
  }, function(err) {
    if (err && err.message === 'item filtered') {
      return cb(null, null);
    }
    cb(err, item);
  });
}

module.exports = runHooks;
