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

var mongodb = require('mongodb');
var async = require('async');

var VersionedCollection = require('./../lib/versioned_collection');

/**
 * Transform, run hooks on given items.
 *
 * @param {MongoDB.Db} db  database connection
 * @param {Array} hooks  array of hook file names
 *
 * @class represents a Transform on an item
 */
function Transform(db, hooks) {
  if (!(db instanceof mongodb.Db)) { throw new TypeError('db must be an instance of mongodb.Db'); }
  if (!Array.isArray(hooks)) { throw new TypeError('hooks must be an array'); }

  hooks = hooks.map(function(path) {
    // if relative, prepend current working dir
    if (path[0] !== '/') {
      path = process.cwd() + '/' + path;
    }

    return require(path);
  });

  this._queue = async.queue(function(item, cb) {
    VersionedCollection.runHooks(hooks, db, {}, item, cb);
  }, 1);
}

module.exports = Transform;

Transform.prototype.run = function run(item, cb) {
  this._queue.push(item, cb);
};
