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

var crc32 = require('crc-32');
var mongodb = require('mongodb');

var nestNamespace = require('nest-namespace');
var match = require('match-object');

/**
 * Stream
 *
 * A cursor that can be opened as stream or to array.
 *
 * @param {mongodb.Collection} collection  handle to the collection
 * @param {Array} virtualItems  the items to append or prepend
 * @param {Object} [options]  object containing configurable parameters
 * @param {Array} [args]  object containing arguments for mongodb.Cursor
 *
 * options:
 *   prepend {Boolean, default: false}  whether to prepend or append the virtual
 *                                      items to the collection
 *   debug {Boolean, default: false}  whether to do extra console logging or not
 *
 * args:
 *   all mongodb.Collection.find arguments
 *   args[1].sortIndex {column name}  whether to sort on a specific column for the
 *                                    collection part (to support using an index).
 */
function VirtualStream(collection, virtualItems, options, args) {
  EE.call(this);

  if (!(collection instanceof mongodb.Collection)) { throw new TypeError('collection must be an instance of mongodb.Collection'); }
  if (!Array.isArray(virtualItems)) { throw new TypeError('virtualItems must be an array'); }

  this._options = options || {};
  if (typeof this._options !== 'object') { throw new TypeError('options must be an object'); }

  this.debug = this._options.debug || false;

  this._collection = collection;
  this._virtualItems = virtualItems;

  this._args = args || [];

  this._filter = nestNamespace(this._args[0] || {});

  // remove last parameter from arguments if it is a callback
  if (typeof this._args[this._args.length - 1] === 'function') {
    this._args = Array.prototype.slice.call(this._args, 0, this._args.length - 1);
  }

  // try to get a sort from options.args
  this._sortDsc = VirtualStream._sortDesc(this._args[1] && this._args[1].sort);

  this._sort = 1;
  if (this._sortDsc) {
    this._virtualItemIndex = this._virtualItems.length - 1;
    this._sort = -1;
  } else {
    this._virtualItemIndex = 0;
  }

  if (!this._virtualItems.length) {
    this._virtualItemIndex = -1;
  }

  var sum = crc32.str('' + Math.random());
  sum = sum < 0 ? 4294967296 + sum : sum;
  this._id = sum.toString(36);

  if (this.debug) {
    console.log(this._id, 'new VirtualStream args', JSON.stringify(this._args), '_sortDsc', this._sortDsc, '_sort', this._sort, '_virtualItemIndex', this._virtualItemIndex);
  }
}

util.inherits(VirtualStream, EE);

module.exports = VirtualStream;

/**
 * Pause streaming.
 */
VirtualStream.prototype.pause = function pause() {
  if (this.debug) { console.log(this._id, 'pause'); }

  this._paused = true;
  if (this._collectionStream) { this._collectionStream.pause(); }
};

/**
 * Resume streaming.
 */
VirtualStream.prototype.resume = function resume() {
  if (this.debug) { console.log(this._id, 'resume'); }

  if (!this._paused) { return; }

  this._paused = false;

  this.stream();
};

/**
 * Destroy stream.
 */
VirtualStream.prototype.destroy = function destroy() {
  if (this.debug) { console.log(this._id, 'destroy'); }

  if (this._destroyed) { return; }
  this._destroyed = true;
  if (this._collectionStream && this._collectionStream.readable) {
    this._collectionStream.destroy();
  }
  if (this.debug) { console.log(this._id, 'destroyed, emit close'); }
  this.emit('close');
};

/**
 * Start or resume streaming.
 */
VirtualStream.prototype.stream = function stream() {
  if (this.debug) { console.log(this._id, 'VirtualStream stream arguments', JSON.stringify(arguments)); }

  if (this._destroyed) {
    if (this.debug) { console.log(this._id, 'VirtualStream stream return', 'destroyed'); }
    return false;
  }
  if (this._paused) {
    if (this.debug) { console.log(this._id, 'VirtualStream stream return', 'paused'); }
    return;
  }

  var that = this;

  function handleVirtualItems(cb) {
    if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

    if (that._destroyed) {
      if (that.debug) { console.log(that._id, 'VirtualStream stream handleVirtualItems return', 'destroyed'); }
      cb();
      return;
    }
    if (that._paused) {
      if (that.debug) { console.log(that._id, 'VirtualStream stream handleVirtualItems return', 'paused'); }
      return;
    }

    if (that.debug) { console.log(that._id, 'VirtualStream stream handleVirtualItems'); }

    that._streamVirtualItems(function(err, item) {
      if (err) {
        that.emit('error', err);
        return;
      }

      if (item) {
        if (that.debug) { console.log(that._id, 'VirtualStream data virtual'); }
        that.emit('data', item);
        handleVirtualItems(cb);
        return;
      }

      // close
      cb();
    });
  }

  function handleDatabase(cb) {
    /* jshint maxcomplexity: 15 */ /* might need some refactoring */
    if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

    if (that._destroyed) {
      if (that.debug) { console.log(that._id, 'VirtualStream stream handleDatabase return', 'destroyed'); }
      cb();
      return;
    }
    if (that._paused) {
      if (that.debug) { console.log(that._id, 'VirtualStream stream handleDatabase return', 'paused'); }
      return;
    }

    // resume if opened
    if (that._collectionStream && that._collectionStream.readable) {
      if (that.debug) { console.log(that._id, 'VirtualStream stream handleDatabase', 'resuming'); }
      that._collectionStream.resume();
    } else if (that._collectionStream && !that._collectionStream.readable) {
      // proceed if not readable anymore
      if (that.debug) { console.log(that._id, 'VirtualStream stream handleDatabase', 'already done, proceed'); }
      cb();
    } else {
      // otherwise open a stream, callback when done/closing
      if (that.debug) { console.log(that._id, 'VirtualStream stream handleDatabase', 'open new'); }

      if (that._args[1]) {
        // set a comment if none is given
        if (!that._args[1].comment) {
          that._args[1].comment = 'VirtualStream.stream';
        }

        // determine which sort to use (if any), natural or other..
        if (that._args[1].sortIndex) {
          that._args[1].sort = {};
          that._args[1].sort[that._args[1].sortIndex] = that._sort;
        }
      }

      if (that.debug) { console.log(that._id, 'VirtualStream stream handleDatabase args', JSON.stringify(that._args)); }
      var c = that._collection.find.apply(that._collection, that._args);
      that._collectionStream = c.stream.apply(c, arguments);

      that._collectionStream.on('data', function(item) {
        if (that.debug) { console.log(that._id, 'VirtualStream data collection'); }
        that.emit('data', item);
      });

      that._collectionStream.on('error', function(err) {
        that.emit('error', err);
      });

      that._collectionStream.on('close', cb);
    }
  }

  var first, second;
  if (this._sortDsc && !this._options.prepend || !this._sortDsc && this._options.prepend) {
    if (this.debug) { console.log(this._id, 'VirtualStream', 'stream first virtual than collection items'); }
    // start emitting the virtual items first
    first = handleVirtualItems;
    second = handleDatabase;
  } else {
    if (this.debug) { console.log(this._id, 'VirtualStream', 'stream first collection than virtual items'); }
    // start emitting the collection first
    first = handleDatabase;
    second = handleVirtualItems;
  }

  first(function() {
    second(function() {
      that.destroy();
    });
  });

  return this;
};

/**
 * Find all virtual items one by one.
 *
 * @param {Function} cb  First parameter is an error or null. Second an item or
 *                       null if the end is reached.
 */
VirtualStream.prototype._streamVirtualItems = function _streamVirtualItems(cb) {
  var that = this;

  process.nextTick(function() {
    if (!~that._virtualItemIndex) {
      cb(null, null);
      return;
    }

    var item;
    if (that._sortDsc) {
      item = that._virtualItems[that._virtualItemIndex];
      that._virtualItemIndex--;
    } else {
      item = that._virtualItems[that._virtualItemIndex];
      that._virtualItemIndex++;

      // check for finish
      if (that._virtualItemIndex === that._virtualItems.length) {
        that._virtualItemIndex = -1;
      }
    }

    if (!item) {
      var error = new Error('item expected');
      console.error('VirtualStream _streamVirtualItems', error, 'next vidx', that._virtualItemIndex, JSON.stringify(that._virtualItems));
      return cb(error);
    }

    try {
      if (!match(that._filter, item)) {
        if (that.debug) { console.log(that._id, 'VirtualStream _streamVirtualItems skip', JSON.stringify(item._id), 'next vidx', that._virtualItemIndex); }
        that._streamVirtualItems(cb);
        return;
      }
    } catch(err) {
      console.error('VirtualStream _streamVirtualItems', err, JSON.stringify(that._filter), that._virtualItemIndex, JSON.stringify(item), JSON.stringify(that._virtualItems));
      return cb(err);
    }

    if (that.debug) { console.log(that._id, 'VirtualStream _streamVirtualItems cb', JSON.stringify(item._id), 'next vidx', that._virtualItemIndex); }
    cb(null, item);
  });
};

/* use the order of the first sort key, if any */
VirtualStream._sortDesc = function _sortDesc(opts) {
  var sortDesc = false;
  if (opts) {
    // get the first sort key
    var keys = Object.keys(opts);
    if (typeof keys[0] !== 'undefined') {
      if (opts[keys[0]] === -1) {
        sortDesc = true;
      }
    }
  }
  return sortDesc;
};
