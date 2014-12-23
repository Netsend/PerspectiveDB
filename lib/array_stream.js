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

var EE = require('events').EventEmitter;
var util = require('util');

var async = require('async');
var nestNamespace = require('nest-namespace');

var match = require('match-object');

/**
 * Stream
 *
 * A cursor that can be opened as stream or to array.
 *
 * @param {Array} items  the items to stream
 * @param {Object} [options]  object containing configurable parameters
 *
 * options:
 *   debug {Boolean, default: false}  whether to do extra console logging or not
 *   filter {Object}  filter to match to each item
 *
 */
function ArrayStream(items, options) {
  EE.call(this);

  if (!Array.isArray(items)) { throw new TypeError('items must be an array'); }

  this._options = options || {};
  if (typeof this._options !== 'object') { throw new TypeError('options must be an object'); }

  this._debug = this._options.debug || false;

  this._items = items;

  this._filter = nestNamespace(this._options.filter || {});
}

util.inherits(ArrayStream, EE);

module.exports = ArrayStream;

/**
 * Pause streaming.
 */
ArrayStream.prototype.pause = function pause() {
  if (this._debug) { console.log('ArrayStream.pause'); }

  this._paused = true;
};

/**
 * Resume streaming.
 */
ArrayStream.prototype.resume = function resume() {
  if (this._debug) { console.log('ArrayStream.resume'); }

  if (!this._paused) { return; }

  this._paused = false;

  if (this._resumer) {
    this._resumer();
    delete this._resumer;
  }
};

/**
 * Destroy stream.
 */
ArrayStream.prototype.destroy = function destroy() {
  if (this._debug) { console.log('ArrayStream.destroy'); }

  if (this._destroyed) { return; }

  this._destroyed = true;
  var that = this;
  process.nextTick(function() {
    that.emit('close');
  });
};

/**
 * Start streaming items.
 */
ArrayStream.prototype.stream = function stream() {
  if (this._debug) { console.log('ArrayStream.stream'); }

  var that = this;
  function handleItem(item, cb) {
    if (that._destroyed) {
      if (that._debug) { console.log('ArrayStream.stream destroyed'); }
      cb(false);
      return;
    }

    if (that._paused) {
      if (that._debug) { console.log('ArrayStream.stream paused'); }
      that._resumer = function() { handleItem(item, cb); };
      return;
    }

    try {
      if (!match(that._filter, item)) {
        if (that._debug) { console.log('ArrayStream.stream no match', JSON.stringify(item)); }
        cb(true);
        return;
      }
    } catch(err) {
      console.error('ArrayStream.stream', err, JSON.stringify(that._filter), JSON.stringify(item), JSON.stringify(that._items));
      that.emit('error', err);
      return;
    }

    if (that._debug) { console.log('ArrayStream.stream match', JSON.stringify(item)); }
    that.emit('data', item);
    cb(true);
  }

  async.every(this._items, function(item, cb) {
    process.nextTick(function() {
      handleItem(item, cb);
    });
  }, function(all) {
    if (all) { that.destroy(); }
  });

  return this;
};
