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

var EE = require('events').EventEmitter;
var util = require('util');

var crc32 = require('crc-32');

/**
 * ConcatMongoStream
 *
 * Concatenate two mongodb collection streams.
 *
 * @param {Array} colls  array of mongodb collections
 * @param {Object} [opts]  object containing configurable parameters
 * @param {Array} [args]  object containing arguments for mongodb.Cursor
 *
 * opts:
 *   debug {Boolean, default: false}  whether to do extra console logging or not
 *   hide {Boolean, default: false}  whether to hide errors from STDERR or not
 *
 * args:
 *   all mongodb.Collection.find arguments
 *   args[1].sortIndex {column name}  whether to sort on a specific column for the
 *                                    collection part (to support using an index).
 */
function ConcatMongoStream(colls, opts, args) {
  if (!Array.isArray(colls)) { throw new TypeError('colls must be an array'); }
  if (colls.length < 1) { throw new TypeError('colls must contain at least one element'); }

  this._opts = opts || {};
  if (typeof this._opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (typeof this._opts.debug !== 'undefined' && typeof this._opts.debug !== 'boolean') { throw new TypeError('opts.debug must be a boolean'); }
  if (typeof this._opts.hide !== 'undefined' && typeof this._opts.hide !== 'boolean') { throw new TypeError('opts.hide must be a boolean'); }

  var that = this;

  if (!colls.every(function(item, i) {
    if (typeof item === 'object') {
      return true;
    }
    if (!that._opts.hide) { console.error('item %s is not an object: "%s"', i, item); }
    return false;
  })) {
    throw new TypeError('colls must only contain objects');
  }

  EE.call(this);

  if (this._debug) { console.log(this._id, 'ConcatMongoStream constructor opts: %s, args: %s', JSON.stringify(opts), JSON.stringify(args)); }

  this._colls = colls;
  this._args = args || [];

  this._debug = this._opts.debug || false;
  this._hide = this._opts._hide || false;

  // remove last parameter from arguments if it is a callback
  if (typeof this._args[this._args.length - 1] === 'function') {
    this._args = Array.prototype.slice.call(this._args, 0, this._args.length - 1);
  }

  // try to get a sort from opts.args
  this._sortDsc = ConcatMongoStream._sortDesc(this._args[1] && this._args[1].sort);

  // init current coll
  this._currentColl = 0;

  this._sort = 1;
  if (this._sortDsc) {
    this._sort = -1;
  }

  var sum = crc32.str('' + Math.random());
  sum = sum < 0 ? 4294967296 + sum : sum;
  this._id = sum.toString(36);
}

util.inherits(ConcatMongoStream, EE);

module.exports = ConcatMongoStream;

/**
 * Proxy pause.
 */
ConcatMongoStream.prototype.pause = function pause() {
  if (this._debug) { console.log(this._id, 'ConcatMongoStream pause'); }

  if (this._stream) {
    this._stream.pause();
  }
};

/**
 * Proxy resume.
 */
ConcatMongoStream.prototype.resume = function resume() {
  if (this._debug) { console.log(this._id, 'ConcatMongoStream resume'); }

  if (this._stream) {
    this._stream.resume();
  }
};

/**
 * Proxy destroy.
 */
ConcatMongoStream.prototype.destroy = function destroy() {
  if (this._debug) { console.log(this._id, 'ConcatMongoStream destroy'); }

  if (this._destroyed) { return; }

  this._destroyed = true;
  if (this._stream && this._stream.readable) {
    this._stream.destroy();
  }

  if (this._debug) { console.log(this._id, 'ConcatMongoStream destroyed, emit close'); }
  this.emit('close');
};

/**
 * Start streaming.
 */
ConcatMongoStream.prototype.stream = function stream() {
  if (this._debug) { console.log(this._id, 'ConcatMongoStream stream arguments', JSON.stringify(arguments)); }

  if (this._streaming) { return false; }
  this._streaming = true;

  var that = this;

  if (this._sortDsc) {
    if (this._debug) { console.log(this._id, 'ConcatMongoStream stream reverse'); }
    // precompute reverse order
    this._reverseColls = [];
    this._colls.forEach(function(coll) {
      that._reverseColls.unshift(coll);
    });
  }

  if (this._args[1]) {
    // set a comment if none is given
    if (!this._args[1].comment) {
      this._args[1].comment = 'ConcatMongoStream.stream';
    }

    // determine which sort to use (if any), natural or other..
    if (this._args[1].sortIndex) {
      this._args[1].sort = {};
      this._args[1].sort[this._args[1].sortIndex] = this._sort;
    }
  }

  if (this._debug) { console.log(this._id, 'ConcatMongoStream stream args', JSON.stringify(this._args)); }

  process.nextTick(function() {
    that._runner(arguments, function() {});
  });

  return this;
};

/**
 * Stream collections.
 *
 * @param {Function} cb  called when closed (either last collection or destroyed).
 */
ConcatMongoStream.prototype._runner = function _runner(args, cb) {
  if (this._debug) { console.log(this._id, 'ConcatMongoStream _runner'); }

  var that = this;

  var coll = this._sortDsc ? this._reverseColls[this._currentColl] : this._colls[this._currentColl];
  var c = coll.find.apply(coll, this._args);
  this._stream = c.stream.apply(c, args);

  // proxy
  this._stream.on('data', function(item) {
    if (that._debug) { console.log(that._id, 'ConcatMongoStream data', JSON.stringify(item)); }
    that.emit('data', item);
  });

  // proxy
  this._stream.on('error', function(err) {
    that.emit('error', err);
  });

  // either proceed to next collection or destroy
  this._stream.on('close', function() {
    if (that._destroyed) { cb(); return; }

    if (that._currentColl < that._colls.length - 1) {
      that._currentColl++;
      that._runner(args, cb);
    } else {
      that.destroy();
      cb();
    }
  });
};

/* use the order of the first sort key, if any */
ConcatMongoStream._sortDesc = function _sortDesc(opts) {
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
