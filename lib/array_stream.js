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
var crc32 = require('crc-32');

/**
 * Stream
 *
 * A cursor that can be opened as stream or to array.
 *
 * @param {Array} items  the items to stream
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   filter {Object}  filter to match to each item
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 */
function ArrayStream(items, opts) {
  EE.call(this);

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

  this._filter = nestNamespace(this._opts.filter || {});

  var sum = crc32.str('' + Math.random());
  sum = sum < 0 ? 4294967296 + sum : sum;
  this._id = sum.toString(36);
}

util.inherits(ArrayStream, EE);

module.exports = ArrayStream;

/**
 * Pause streaming.
 */
ArrayStream.prototype.pause = function pause() {
  this._log.debug2('as %s pause', this._id);

  this._paused = true;
};

/**
 * Resume streaming.
 */
ArrayStream.prototype.resume = function resume() {
  this._log.debug2('as %s resume', this._id);

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
  this._log.debug2('as %s destroy', this._id);

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
  this._log.debug2('as %s stream', this._id);

  var that = this;
  function handleItem(item, cb) {
    if (that._destroyed) {
      that._log.info('as %s stream destroyed', that._id);
      cb(new Error('stream destroyed'));
      return;
    }

    if (that._paused) {
      that._log.info('as %s stream paused', that._id);
      that._resumer = function() { handleItem(item, cb); };
      return;
    }

    try {
      if (!match(that._filter, item)) {
        that._log.info('as %s stream no match %j', that._id, item);
        cb();
        return;
      }
    } catch(err) {
      console.error('ArrayStream %s stream match error %s %j %j %j', that._id, err, that._filter, item, that._items);
      cb(err);
      return;
    }

    that._log.info('as %s stream match %j', that._id, item);
    that.emit('data', item);
    cb();
  }

  async.eachSeries(this._items, function(item, cb) {
    process.nextTick(function() {
      handleItem(item, cb);
    });
  }, function(err) {
    if (err && err.message !== 'stream destroyed') {
      that.emit('error', err);
    }

    if (that._paused) {
      that._resumer = function() { that.destroy(); };
    } else {
      that.destroy();
    }
  });

  return this;
};
