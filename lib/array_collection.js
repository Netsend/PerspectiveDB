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

var ArrayCursor = require('./array_cursor');

/**
 * ArrayCollection
 *
 * Represents a streaming interface for ArrayCursor..
 *
 * @param {Array} items  the items to append or prepend
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 */
function ArrayCollection(items, opts) {
  if (!Array.isArray(items)) { throw new TypeError('items must be an array'); }

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
  this._items = items;
}

module.exports = ArrayCollection;

/**
 * Find a single item.
 *
 * @param {Object} selector  mongo selector
 * @param {Object} opts  mongo find options
 * @param {Function} cb  first parameter will be an error or null, second parameter
 *                       will be an item or null.
 */
ArrayCollection.prototype.findOne = function findOne() {
  this._log.debug2('acoll findOne');

  var cursor = new ArrayCursor(this._items, this._opts);
  cursor.findOne.apply(cursor, arguments);
};

/**
 * Find multiple objects, return a new cursor.
 *
 * @param {Object} selector  mongo selector
 * @param {Object} opts  mongo find options
 */
ArrayCollection.prototype.find = function find() {
  this._log.debug2('acoll find');

  var cursor = new ArrayCursor(this._items, this._opts);
  return cursor.find.apply(cursor, arguments);
};

// stubs to bind
ArrayCollection.prototype.insert = function insert() {
  throw new Error('not implemented');
};

ArrayCollection.prototype.update = function update() {
  throw new Error('not implemented');
};
