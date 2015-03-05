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
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 *
 * args:
 *   all mongodb.Collection.find arguments
 *   args[1].sortIndex {column name}  whether to sort on a specific column for the
 *                                    collection part (to support using an index).
 */
function ConcatMongoStream(colls, opts, args) {
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
    that._log.err('cms item %d is not an object: "%s"', i, item);
    return false;
  })) {
    throw new TypeError('colls must only contain objects');
  }

  EE.call(this);

  this._colls = colls;
  this._args = args || [];

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

  this._log.info('cms %s constructor opts: %j, args: %j', this._id, opts, args);
}

util.inherits(ConcatMongoStream, EE);

module.exports = ConcatMongoStream;

/**
 * Proxy pause.
 */
ConcatMongoStream.prototype.pause = function pause() {
  this._log.debug2('cms %s pause', this._id);

  if (this._stream) {
    this._stream.pause();
  }
};

/**
 * Proxy resume.
 */
ConcatMongoStream.prototype.resume = function resume() {
  this._log.debug2('cms %s resume', this._id);

  if (this._stream) {
    this._stream.resume();
  }
};

/**
 * Proxy destroy.
 */
ConcatMongoStream.prototype.destroy = function destroy() {
  this._log.debug2('cms %s destroy', this._id);

  if (this._destroyed) { return; }

  this._destroyed = true;
  if (this._stream && this._stream.readable) {
    this._stream.destroy();
  }

  this._log.info('cms %s destroyed, emit close', this._id);
  this.emit('close');
};

/**
 * Start streaming.
 */
ConcatMongoStream.prototype.stream = function stream() {
  this._log.debug2('cms %s stream arguments %j', this._id, arguments);

  if (this._streaming) { return false; }
  this._streaming = true;

  var that = this;

  if (this._sortDsc) {
    this._log.info('cms %s stream reverse', this._id);
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

  this._log.info('cms %s stream args %j', this._id, this._args);

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
  this._log.debug2('cms %s _runner', this._id);

  var that = this;

  var coll = this._sortDsc ? this._reverseColls[this._currentColl] : this._colls[this._currentColl];
  var c = coll.find.apply(coll, this._args);
  this._stream = c.stream.apply(c, args);

  // proxy
  this._stream.on('data', function(item) {
    that._log.info('cms %s data %j', that._id, item);
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
