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

var VirtualCursor = require('./virtual_cursor');

/**
 * VirtualCollection
 *
 * Represents a streaming interface for collections.
 *
 * Features virtual items that can be appended or prepended to the database
 * collection.
 *
 * @param {mongodb.Collection} collection  handle to the collection
 * @param {Array} virtualItems  the items to append or prepend
 * @param {Object} [options]  object containing configurable parameters
 *
 * options:
 *   prepend {Boolean, default: false}  whether to prepend or append the virtual
 *                                      items to the collection
 *   debug {Boolean, default: false}  whether to do extra console logging or not
 */
function VirtualCollection(collection, virtualItems, options) {
  if (!(collection instanceof mongodb.Collection)) { throw new TypeError('collection must be an instance of mongodb.Collection'); }
  if (!Array.isArray(virtualItems)) { throw new TypeError('virtualItems must be an array'); }

  options = options || {};

  if (typeof options !== 'object') { throw new TypeError('options must be an object'); }

  this.debug = options.debug || false;

  this._collection = collection;
  this._virtualItems = virtualItems;
  this._options = options;
}

module.exports = VirtualCollection;

/**
 * Find a single item.
 *
 * @param {Object} selector  mongo selector
 * @param {Object} opts  mongo find options
 * @param {Function} cb  first parameter will be an error or null, second parameter
 *                       will be an item or null.
 */
VirtualCollection.prototype.findOne = function findOne() {
  var cursor = new VirtualCursor(this._collection, this._virtualItems, this._options);
  cursor.findOne.apply(cursor, arguments);
};

/**
 * Find multiple objects, return a new cursor.
 *
 * @param {Object} selector  mongo selector
 * @param {Object} opts  mongo find options
 */
VirtualCollection.prototype.find = function find() {
  var cursor = new VirtualCursor(this._collection, this._virtualItems, this._options);
  return cursor.find.apply(cursor, arguments);
};
