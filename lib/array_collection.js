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
 * @param {Object} [options]  object containing configurable parameters
 *
 * options:
 *   debug {Boolean, default: false}  whether to do extra console logging or not
 */
function ArrayCollection(items, options) {
  if (!Array.isArray(items)) { throw new TypeError('items must be an array'); }

  options = options || {};
  if (typeof options !== 'object') { throw new TypeError('options must be an object'); }

  this._debug = options.debug || false;

  this._items = items;
  this._options = options;
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
  if (this._debug) { console.log('ArrayCollection.findOne'); }

  var cursor = new ArrayCursor(this._items, this._options);
  cursor.findOne.apply(cursor, arguments);
};

/**
 * Find multiple objects, return a new cursor.
 *
 * @param {Object} selector  mongo selector
 * @param {Object} opts  mongo find options
 */
ArrayCollection.prototype.find = function find() {
  if (this._debug) { console.log('ArrayCollection.find'); }

  var cursor = new ArrayCursor(this._items, this._options);
  return cursor.find.apply(cursor, arguments);
};
