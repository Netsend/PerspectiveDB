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
 *   includeOffset {Boolean, default false}  whether to include or exclude offset
 *   tailable {Boolean, default false}  whether or not to keep the cursor open and follow the oplog
 *   tailableRetryInterval {Number, default 1000}  set tailableRetryInterval
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 *
 * @class represents an OplogReader for a certain namespace
 */
function OplogReader(oplogColl, ns, opts) {
  /* jshint maxcomplexity: 23 */ /* lots of parameter type checking */

  if (!(oplogColl instanceof mongodb.Collection)) { throw new TypeError('oplogColl must be an instance of mongodb.Collection'); }
  if (typeof ns !== 'string') { throw new TypeError('ns must be a string'); }

  var nsParts = ns.split('.');
  if (nsParts.length < 2) { throw new TypeError('ns must contain at least two parts'); }
  if (!nsParts[0].length) { throw new TypeError('ns must contain a database name'); }
  if (!nsParts[1].length) { throw new TypeError('ns must contain a collection name'); }

  opts = opts || {};
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (typeof opts.filter !== 'undefined' && typeof opts.filter !== 'object') { throw new TypeError('opts.filter must be an object'); }
  if (typeof opts.offset !== 'undefined' && !(opts.offset instanceof mongodb.Timestamp)) { throw new TypeError('opts.offset must be an instance of mongodb.Timestamp'); }
  if (typeof opts.includeOffset !== 'undefined' && typeof opts.includeOffset !== 'boolean') { throw new TypeError('opts.includeOffset must be a boolean'); }
  if (typeof opts.tailable !== 'undefined' && typeof opts.tailable !== 'boolean') { throw new TypeError('opts.tailable must be a boolean'); }
  if (typeof opts.tailableRetryInterval !== 'undefined' && typeof opts.tailableRetryInterval !== 'number') { throw new TypeError('opts.tailableRetryInterval must be a number'); }
  if (typeof opts.log !== 'undefined' && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }

  Readable.call(this, opts);

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

  this._databaseName = nsParts.shift();
  this._collectionName = nsParts.join('.');

  // setup CursorStream
  var selector = { ns: ns };
  if (opts.offset) {
    if (opts.includeOffset) {
      selector.ts = { $gte: opts.offset };
    } else {
      selector.ts = { $gt: opts.offset };
    }
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
  mongoOpts.tailableRetryInterval = opts.tailableRetryInterval || 1000;

  this._log.notice('or offset: %s %s, selector: %j, opts: %j', opts.offset, opts.includeOffset ? 'include' : 'exclude', selector, mongoOpts);

  this._source = oplogColl.find(selector, mongoOpts).stream();

  var that = this;

  // proxy errors
  this._source.on('error', function(err) {
    that._log.crit('or cursor stream error %s', err);
    that.emit('error', err);
  });

  this._source.on('data', function(chunk) {
    that._log.debug('or cursor stream data');
    // if push() returns false, then we need to stop reading from source
    if (!that.push(chunk)) {
      that._log.info('or cursor stream data pause');
      that._source.pause();
    }
  });

  this._source.on('close', function() {
    that._log.info('or cursor stream close');
    that.push(null);
  });
}
util.inherits(OplogReader, Readable);

module.exports = OplogReader;

OplogReader.prototype._read = function _read() {
  this._log.debug2('or _read resume cursor stream');
  this._source.resume();
};

/**
 * Stop the oplog reader. An "end" event will be emitted.
 */
OplogReader.prototype.close = function close() {
  this._log.debug2('or close destroy cursor stream');
  this._source.destroy();
};
