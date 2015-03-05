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

var ConcatMongoStream = require('./concat_mongo_stream');

/**
 * ConcatMongoCursor
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
function ConcatMongoCursor(colls, opts) {
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
    that._log.err('cmcurs item %d is not an object: "%s"', i, item);
    return false;
  })) {
    throw new TypeError('colls must only contain objects');
  }

  this._colls = colls;
}

module.exports = ConcatMongoCursor;

/**
 * Find one item.
 */
ConcatMongoCursor.prototype.findOne = function findOne(selector, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = {};
  }

  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var s = new ConcatMongoStream(this._colls, this._opts, arguments);

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
ConcatMongoCursor.prototype.find = function find() {
  this._findArgs = arguments;
  return this;
};

/**
 * Return all items in an array.
 *
 * @param {Function} cb  First parameter will be an Error object or null, second
 *                       parameter will be an array of items found.
 */
ConcatMongoCursor.prototype.toArray = function toArray(cb) {
  var s = new ConcatMongoStream(this._colls, this._opts, this._findArgs);
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
ConcatMongoCursor.prototype.stream = function stream() {
  var s = new ConcatMongoStream(this._colls, this._opts, this._findArgs);
  return s.stream.apply(s, arguments);
};
