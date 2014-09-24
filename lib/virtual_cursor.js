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
var VirtualStream = require('./virtual_stream');

/**
 * Cursor
 *
 * A cursor that can be opened as stream or to array.
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
function VirtualCursor(collection, virtualItems, options) {
  if (!(collection instanceof mongodb.Collection)) { throw new TypeError('collection must be an instance of mongodb.Collection'); }
  if (!Array.isArray(virtualItems)) { throw new TypeError('virtualItems must be an array'); }

  options = options || {};
  if (typeof options !== 'object') { throw new TypeError('options must be an object'); }

  this._options = {
    prepend: options.prepend,
    debug: options.debug
  };

  this.debug = options.debug || false;

  this._collection = collection;
  this._virtualItems = virtualItems;
}

module.exports = VirtualCursor;

/**
 * Find one item.
 */
VirtualCursor.prototype.findOne = function findOne(selector, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = {};
  }

  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var s = new VirtualStream(this._collection, this._virtualItems, this._options, arguments);

  var found = null;
  s.on('data', function(item) {
    found = item;
    s.destroy();
  });

  s.on('close', function() {
    cb(null, found);
  });

  s.on('error', cb);

  s.stream();
};

/**
 * Save arguments and return this to support stream and toArray methods.
 *
 * @param {Function} cb  First parameter will be an Error object or null, second
 *                       parameter will be an array of items found.
 */
VirtualCursor.prototype.find = function find() {
  this._findArgs = arguments;
  return this;
};

/**
 * Return all items in an array.
 *
 * @param {Function} cb  First parameter will be an Error object or null, second
 *                       parameter will be an array of items found.
 */
VirtualCursor.prototype.toArray = function toArray(cb) {
  var s = new VirtualStream(this._collection, this._virtualItems, this._options, this._findArgs);
  var items = [];
  s.on('data', function(item) {
    items.push(item);
  });
  s.on('close', function() {
    cb(null, items);
  });
  s.on('error', function(err) {
    cb(err);
  });

  s.stream();
};

/**
 * Start or resume streaming.
 */
VirtualCursor.prototype.stream = function stream() {
  var s = new VirtualStream(this._collection, this._virtualItems, this._options, this._findArgs);
  return s.stream.apply(s, arguments);
};
