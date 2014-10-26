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

var Readable = require('stream').Readable;
var util = require('util');

var mongodb = require('mongodb');

/**
 * OplogReader
 *
 * Read oplog, scoped to a certain namespace.
 *
 * @param {mongodb.Collection} oplogColl  oplog collection
 * @param {String} ns  namespace to read
 * @param {Object} [opts]  object containing optional parameters
 *
 * opts:
 *   filter {Object}  extra filter to apply apart from namespace
 *   offset {mongodb.Timestamp}  timestamp to start at
 *   tailable {Boolean, default false}  whether or not to keep the cursor open and follow the oplog
 *   tailableRetryInterval {Number}  overrule tailableRetryInterval
 *   debug {Boolean, default false}  whether to do extra console logging or not
 *   hide {Boolean, default false}  whether to suppress errors or not (used in tests)
 *
 * @class represents an OplogReader for a certain namespace
 */
function OplogReader(oplogColl, ns, opts) {
  /* jshint maxcomplexity: 21 */ /* lot's of variable checking */

  if (!(oplogColl instanceof mongodb.Collection)) { throw new TypeError('oplogColl must be an instance of mongodb.Collection'); }
  if (typeof ns !== 'string') { throw new TypeError('ns must be a string'); }

  var nsParts = ns.split('.');
  if (nsParts.length < 2) { throw new TypeError('ns must contain at least two parts'); }
  if (!nsParts[0].length) { throw new TypeError('ns must contain a database name'); }
  if (!nsParts[1].length) { throw new TypeError('ns must contain a collection name'); }

  if (typeof opts !== 'undefined' && typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  opts = opts || {};

  if (typeof opts.filter !== 'undefined' && typeof opts.filter !== 'object') { throw new TypeError('opts.filter must be an object'); }
  if (typeof opts.offset !== 'undefined' && !(opts.offset instanceof mongodb.Timestamp)) { throw new TypeError('opts.offset must be an instance of mongodb.Timestamp'); }
  if (typeof opts.tailable !== 'undefined' && typeof opts.tailable !== 'boolean') { throw new TypeError('opts.tailable must be a boolean'); }
  if (typeof opts.tailableRetryInterval !== 'undefined' && typeof opts.tailableRetryInterval !== 'number') { throw new TypeError('opts.tailableRetryInterval must be a number'); }
  if (typeof opts.debug !== 'undefined' && typeof opts.debug !== 'boolean') { throw new TypeError('opts.debug must be a boolean'); }
  if (typeof opts.hide !== 'undefined' && typeof opts.hide !== 'boolean') { throw new TypeError('opts.hide must be a boolean'); }

  Readable.call(this, opts);

  this._debug = opts.debug || false;
  this._hide = !!opts.hide;

  this._databaseName = nsParts.shift();
  this._collectionName = nsParts.join('.');

  // setup CursorStream
  if (this._debug) { console.log('or _read offset', opts.offset); }

  var selector = { ns: ns };
  if (opts.offset) {
    selector.ts = { $gte: opts.offset };
  }
  if (opts.filter) {
    selector = { $and: [selector, opts.filter] };
  }

  var mongoOpts = {
    raw: true,
    sort: { '$natural': 1 },
    comment: 'oplog_reader'
  };
  if (opts.tailable) { mongoOpts.tailable = true; }
  if (opts.tailableRetryInterval) { mongoOpts.tailableRetryInterval = opts.tailableRetryInterval; }

  if (this._debug) { console.log('or _read stream options', JSON.stringify(mongoOpts)); }

  this._source = oplogColl.find(selector, mongoOpts).stream();

  var that = this;

  // proxy errors
  this._source.on('error', function(err) {
    if (!that._hide) { console.error('or _read cursor stream error', err); }
    that.emit('error', err);
  });

  this._source.on('data', function(chunk) {
    if (that._debug) { console.log('or _read cursor stream data'); }
    // if push() returns false, then we need to stop reading from source
    if (!that.push(chunk)) {
      that._source.pause();
    }
  });

  this._source.on('close', function() {
    if (that._debug) { console.log('or _read cursor stream closed'); }
    that.push(null);
  });
}
util.inherits(OplogReader, Readable);

module.exports = OplogReader;

OplogReader.prototype._read = function _read() {
  this._source.resume();
};
