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
var PassThrough = require('stream').PassThrough;

/**
 * Create a serial read stream.
 *
 * @param {Array} streams  array of readable streams
 * @param {Object} [opts]  stream.PassThrough options for this stream
 */
function ConcatReadStream(streams, opts) {
  if (!Array.isArray(streams)) { throw new TypeError('streams must be an array'); }
  if (!streams.length) { throw new Error('provide at least one stream'); }

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }

  opts.objectMode = opts.objectMode || streams[0]._readableState.objectMode;
  PassThrough.call(this, opts);

  this._streams = streams;
  this._opts = opts;

  var that = this;

  // register a new stream if the current one ends
  var i = 0;
  function setupNext() {
    if (that._currStream) {
      that._currStream.unpipe(that);
    }
    if (i < streams.length) {
      that._currStream = streams[i];
      that._currStream.on('end', setupNext);
      that._currStream.pipe(that, { end: false });
    } else {
      // this was the last stream
      that.push(null);
    }
    i++;
  }
  setupNext();
}

util.inherits(ConcatReadStream, PassThrough);

// cascade reopen method on each stream
ConcatReadStream.prototype.reopen = function() {
  var streams = [];
  this._streams.forEach(function(stream) {
    streams.push(stream.reopen());
  });
  return new ConcatReadStream(streams, this._opts);
};

module.exports = ConcatReadStream;
