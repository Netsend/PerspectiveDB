/**
 * Copyright 2015 Netsend.
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

/* jshint -W116 */

'use strict';

var util = require('util');
var Readable = require('stream').Readable;

/**
 * Create a serial read stream.
 *
 * @param {Array} streams  array of readable streams
 * @param {Object} [opts]  stream.Readable options for this stream
 */
function ConcatReadStream(streams, opts) {
  if (!Array.isArray(streams)) { throw new TypeError('streams must be an array'); }
  if (!streams.length) { throw new Error('provide at least one stream'); }

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }

  this._currStream = streams[0];
  opts.objectMode = opts.objectMode || this._currStream._readableState.objectMode;

  var that = this;

  // ensure all streams are paused and register resume handlers when the previous stream ends
  streams.forEach(function(stream, i) {
    stream.pause();
    stream.on('data', function(data) {
      if (!that.push(data)) {
        stream.pause();
      }
    });
    if (i + 1 < streams.length) {
      stream.on('end', function() {
        that._currStream = streams[i + 1];
        that._currStream.resume();
      });
    } else {
      // this is the last stream
      stream.on('end', function() {
        that.push(null);
      });
    }
  });

  Readable.call(this, opts);
}

util.inherits(ConcatReadStream, Readable);

ConcatReadStream.prototype._read = function() {
  this._currStream.resume();
};

module.exports = ConcatReadStream;
