/**
 * Copyright 2014, 2015 Netsend.
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

var ConcatMongoCursor = require('./concat_mongo_cursor');

/**
 * ConcatMongoCollection
 *
 * A cursor that can be opened as stream or to array.
 *
 * @param {Array} colls  array of mongodb collections
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 */
function ConcatMongoCollection(colls, opts) {
  if (!Array.isArray(colls)) { throw new TypeError('colls must be an array'); }
  if (colls.length < 1) { throw new TypeError('colls must contain at least one element'); }

  opts = opts || {};
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (typeof opts.log !== 'undefined' && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }

  this._log = opts.log || {
    emerg:   console.error,
    alert:   console.error,
    crit:    console.error,
    err:     console.error,
    warning: console.log,
    notice:  console.log,
    info:    console.log,
    debug:   console.log,
    debug2:  console.log
  };

  this._opts = opts;

  var that = this;

  if (!colls.every(function(item, i) {
    if (typeof item === 'object') {
      return true;
    }
    that._log.err('cmcoll item %d is not an object: "%s"', i, item);
    return false;
  })) {
    throw new TypeError('colls must only contain objects');
  }

  this._colls = colls;
}

module.exports = ConcatMongoCollection;

/**
 * Find a single item.
 *
 * @param {Object} selector  mongo selector
 * @param {Object} opts  mongo find options
 * @param {Function} cb  first parameter will be an error or null, second parameter
 *                       will be an item or null.
 */
ConcatMongoCollection.prototype.findOne = function findOne() {
  var cursor = new ConcatMongoCursor(this._colls, this._opts);
  cursor.findOne.apply(cursor, arguments);
};

/**
 * Find multiple objects, return a new cursor.
 *
 * @param {Object} selector  mongo selector
 * @param {Object} opts  mongo find options
 */
ConcatMongoCollection.prototype.find = function find() {
  var cursor = new ConcatMongoCursor(this._colls, this._opts);
  return cursor.find.apply(cursor, arguments);
};
