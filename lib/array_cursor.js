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

var ArrayStream = require('./array_stream');

/**
 * Cursor
 *
 * A cursor that can be opened as stream or toArray.
 *
 * @param {Array} items  the items to append or prepend
 * @param {Object} [options]  object containing configurable parameters
 *
 * options:
 *   debug {Boolean, default: false}  whether to do extra console logging or not
 */
function ArrayCursor(items, options) {
  if (!Array.isArray(items)) { throw new TypeError('items must be an array'); }

  options = options || {};
  if (typeof options !== 'object') { throw new TypeError('options must be an object'); }

  this._options = options;

  this._debug = options.debug || false;

  this._items = items;
}

module.exports = ArrayCursor;

/**
 * Find one item.
 */
ArrayCursor.prototype.findOne = function findOne(selector, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = {};
  }

  opts = opts || {};

  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var s = this._createStream(selector, opts);

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
ArrayCursor.prototype.find = function find(selector, opts) {
  this._findSelector = selector;
  this._findOpts = opts;
  return this;
};

/**
 * Return all items in an array.
 *
 * @param {Function} cb  First parameter will be an Error object or null, second
 *                       parameter will be an array of items found.
 */
ArrayCursor.prototype.toArray = function toArray(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var s = this._createStream(this._findSelector, this._findOpts);
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
ArrayCursor.prototype.stream = function stream() {
  if (arguments.length > 0) { throw new TypeError('arguments are not supported'); }

  var s = this._createStream(this._findSelector, this._findOpts);
  return s.stream();
};

/* use the order of the first sort key, if any */
ArrayCursor._sortDesc = function _sortDesc(opts) {
  var sortDesc = false;
  if (opts) {
    // treat first key as the sort key
    var keys = Object.keys(opts);
    if (typeof keys[0] !== 'undefined') {
      if (opts[keys[0]] === -1) {
        sortDesc = true;
      }
    }
  }
  return sortDesc;
};

/* create stream given the options */
ArrayCursor.prototype._createStream = function _createStream(selector, opts) {
  // sort descending if sort is -1 and support selector
  var items = this._items;
  var sortDesc = ArrayCursor._sortDesc(opts && opts.sort);
  if (sortDesc) {
    if (this._debug) { console.log('ArrayCursor._createStream sort desc'); }
    items = [];
    for (var i = this._items.length - 1; i >= 0; --i) {
      items.push(this._items[i]);
    }
  }
  this._options.filter = selector;

  if (this._debug) { console.log('ArrayCursor._createStream', items.length, JSON.stringify(this._options)); }
  return new ArrayStream(items, this._options);
};
