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

var async = require('async');
var mongodb = require('mongodb');
var BSON = mongodb.BSON;
var match = require('match-object');

var walkBranch = require('./walk_branch');

/**
 * VersionedCollectionReader
 *
 * Read the versioned collection reader, optionally starting at a certain offset.
 *
 * @param {mongodb.Db} db  database to use
 * @param {String} collectionName  name of collection to process
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   localPerspective {String, default _local}  name of the local perspective, remotes can't be named like this.
 *   debug {Boolean, default false}  whether to do extra console logging or not
 *   hide {Boolean, default false}  whether to suppress errors or not (used in tests)
 *   raw  {Boolean, default false}  whether to emit JavaScript objects or raw Buffers
 *   filter {Object}  conditions a document should hold
 *   offset {String}  version to start tailing after
 *   follow {Boolean, default: true}  whether to keep the tail open or not
 *   tailableRetryInterval {Number, default 2000}  set tailableRetryInterval
 *   hooks {Array}  array of asynchronous functions to execute, each hook has the following signature: db, object, options,
 *                  callback and should callback with an error object, the new item and possibly extra data.
 *   hooksOpts {Object}  options to pass to a hook
 *
 * @class represents a VersionedCollectionReader for a certain database.collection
 */
function VersionedCollectionReader(db, collectionName, opts) {
  /* jshint maxcomplexity: 24 */ /* lot's of parameter checking */

  if (!(db instanceof mongodb.Db)) { throw new TypeError('db must be an instance of mongodb.Db'); }
  if (typeof collectionName !== 'string') { throw new TypeError('collectionName must be a string'); }

  if (typeof opts !== 'undefined' && typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  opts = opts || {};

  if (typeof opts.localPerspective !== 'undefined' && typeof opts.localPerspective !== 'string') { throw new TypeError('opts.localPerspective must be a string'); }
  if (typeof opts.debug !== 'undefined' && typeof opts.debug !== 'boolean') { throw new TypeError('opts.debug must be a boolean'); }
  if (typeof opts.hide !== 'undefined' && typeof opts.hide !== 'boolean') { throw new TypeError('opts.hide must be a boolean'); }
  if (typeof opts.raw !== 'undefined' && typeof opts.raw !== 'boolean') { throw new TypeError('opts.raw must be a boolean'); }
  if (typeof opts.filter !== 'undefined' && typeof opts.filter !== 'object') { throw new TypeError('opts.filter must be an object'); }
  if (typeof opts.offset !== 'undefined' && typeof opts.offset !== 'string') { throw new TypeError('opts.offset must be a string'); }
  if (typeof opts.follow !== 'undefined' && typeof opts.follow !== 'boolean') { throw new TypeError('opts.follow must be a boolean'); }
  if (typeof opts.tailableRetryInterval !== 'undefined' && typeof opts.tailableRetryInterval !== 'number') { throw new TypeError('opts.tailableRetryInterval must be a number'); }
  if (typeof opts.hooks !== 'undefined' && !(opts.hooks instanceof Array)) { throw new TypeError('opts.hooks must be an array'); }
  if (typeof opts.hooksOpts !== 'undefined' && typeof opts.hooksOpts !== 'object') { throw new TypeError('opts.hooksOpts must be an object'); }

  this._db = db;

  this._databaseName = this._db.databaseName;
  this._collectionName = collectionName;
  this._snapshotCollectionName = 'm3.' + this._collectionName;
  this._collection = this._db.collection(this._collectionName);
  this._snapshotCollection = this._db.collection(this._snapshotCollectionName);

  this._localPerspective = opts.localPerspective || '_local';
  this._debug = opts.debug || false;
  this._hide = opts.hide || false;
  this._raw = opts.raw || false;
  this._filter = opts.filter || {};
  this._offset = opts.offset || '';

  if (typeof opts.follow === 'boolean') {
    this._follow = opts.follow;
  } else {
    this._follow = true;
  }
  this._hooks = opts.hooks || [];
  this._hooksOpts = opts.hooksOpts || {};

  this._closed = false;

  Readable.call(this, { objectMode: !this._raw });

  var selector = { '_id._pe': this._localPerspective };
  var mongoOpts = { sort: { '$natural': 1 }, tailable: this._follow, comment: 'tail' };
  mongoOpts.tailableRetryInterval = opts.tailableRetryInterval || 2000;

  if (this._debug) { console.log('vcr %s %s selector %s filter %s opts %s', this._databaseName, this._collectionName, JSON.stringify(selector), JSON.stringify(this._filter), JSON.stringify(mongoOpts)); }

  this._source = this._snapshotCollection.find(selector, mongoOpts).stream();

  var that = this;

  // pause _source to determine maxTries
  this._source.pause();

  // determine the maximum number of versions to examine before the offset must have been encountered
  this._maxTries = 0;
  this._snapshotCollection.count(function(err, count) {
    if (err) { throw err; }
    that._maxTries = count;
    that._source.resume();
  });

  // emit a connected graph by making sure every parent of any filtered item is filtered
  var heads = {};
  var offsetReached = false;
  if (!this._offset) {
    offsetReached = true;
  }
  var i = 0;

  function handleData(item) {
    if (that._debug && offsetReached) { console.log('vcr %s %s _read %s', that._databaseName, that._collectionName, JSON.stringify(item._id)); }

    // only start emitting after the offset is encountered
    if (!offsetReached) {
      if (item._id._v === that._offset) {
        if (that._debug) { console.log('vcr %s %s _read offset reached %s', that._databaseName, that._collectionName, JSON.stringify(item._id)); }
        offsetReached = true;
      } else {
        // the offset should be encountered within maxTries
        i++;
        if (i >= that._maxTries) {
          if (!that._hide) { console.error('vcr %s %s _read offset not found %s %s %s', that._databaseName, that._collectionName, that._offset, i, that._maxTries); }
          if (that._source.destroy()) { throw new Error('offset not found'); }
        }
      }
    }

    heads[item._id._v] = [];

    // move previously emitted parents along with this new branch head
    that._source.pause();
    async.eachSeries(item._id._pa, function(p, callback) {
      if (heads[p]) {
        // ff and takeover parent references from old head
        Array.prototype.push.apply(heads[item._id._v], heads[p]);
        delete heads[p];
        process.nextTick(callback);
      } else {
        // branched off, find the lowest filtered ancestor of this item and save it as parent reference
        var lastEmitted = {};
        lastEmitted['_id._id'] = item._id._id;
        lastEmitted['_id._pe'] = that._localPerspective;

        walkBranch(lastEmitted, p, that._localPerspective, that._snapshotCollection, function(err, anItem, s) {
          if (err) {
            if (!that._hide) { console.error('vcr %s %s could not determine last emitted version before %s %s %s %s', p, lastEmitted, err, JSON.stringify(anItem)); }
            return callback(err);
          }
          // when done
          if (!anItem) {
            return callback();
          }

          // skip if not all criteria hold on this item
          if (!match(that._filter, anItem)) {
            return;
          }

          // make sure the ancestor is not already in the array, this can happen on merge items.
          if (!heads[item._id._v].some(function(pp) { return pp === anItem._id._v; })) {
            heads[item._id._v].push(anItem._id._v);
          }
          if (s) { s.destroy(); }
        });
      }
    }, function(err) {
      if (err) {
        that._source.destroy();
        that.emit('error', err);
        return;
      }

      // don't emit if not all criteria hold on this item
      if (!match(that._filter, item)) {
        that._source.resume();
        return;
      }

      // load all hooks on this item, then, if offset is reached, callback
      VersionedCollectionReader.runHooks(that._hooks, that._db, item, that._hooksOpts, function(err, afterItem) {
        if (err) { return that.emit('error', err); }

        // if hooks filter out the item, do not callback since that would signal the end
        if (!afterItem) {
          if (that._debug && offsetReached) { console.log('vcr %s %s vcr hook filtered %s', that._databaseName, that._collectionName, JSON.stringify(item._id)); }
          that._source.resume();
          return;
        }

        // set parents to last returned version of this branch
        item._id._pa = heads[item._id._v];

        // then update branch with _id of this item
        heads[item._id._v] = [item._id._v];

        // all criteria hold, so return this item if offset is reached
        if (offsetReached) {
          item = afterItem;

          // remove perspective and local state and run any transformation
          delete item._id._pe;
          delete item._id._lo;
          delete item._id._i;
          delete item._m3._op;
          delete item._m3._ack;

          // push the raw or parsed item out to the reader, and resume if not flooded
          if (that._debug) { console.log('vcr %s %s push %s', that._databaseName, that._collectionName, JSON.stringify(item._id)); }
          if (that.push(that._raw ? BSON.serialize(item) : item)) { that._source.resume(); }
        } else {
          that._source.resume();
        }
      });
    });
  }

  this._source.on('data', handleData);

  // proxy error
  this._source.on('error', function(err) {
    if (!that._hide) { console.error('vcr %s %s cursor stream error %s', that._databaseName, that._collectionName, err); }
    that.emit('error', err);
  });

  this._source.on('close', function() {
    if (that._debug) { console.log('vcr %s %s cursor stream closed', that._databaseName, that._collectionName); }
    that._closed = true;
    that.push(null);
  });
}
util.inherits(VersionedCollectionReader, Readable);

module.exports = VersionedCollectionReader;

/**
 * Stop the stream reader. An "end" event will be emitted.
 */
VersionedCollectionReader.prototype.close = function close() {
  if (this._debug) { console.log('vcr %s %s closing cursor', this._databaseName, this._collectionName); }
  this._source.destroy();
};

VersionedCollectionReader.runHooks = function runHooks(hooks, db, item, opts, cb) {
  async.eachSeries(hooks, function(hook, callback) {
    hook(db, item, opts, function(err, afterItem) {
      if (err) { return callback(err); }
      if (!afterItem) { return callback(new Error('item filtered')); }

      item = afterItem;
      callback(err);
    });
  }, function(err) {
    if (err && err.message === 'item filtered') {
      return cb(null, null);
    }
    cb(err, item);
  });
};



/////////////////////
//// PRIVATE API ////
/////////////////////

/**
 * Implementation of _read method of Readable stream. This method is not called
 * directly. In this implementation size of read buffer is ignored
 */
VersionedCollectionReader.prototype._read = function() {
  this._source.resume();
};
