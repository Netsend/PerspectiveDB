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
 *   hooks {Array}  array of asynchronous functions to execute, each hook has the following signature: db, object, options,
 *                  callback and should callback with an error object, the new item and possibly extra data.
 *   hookOpts {Object}  options to pass to a hook
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
  if (typeof opts.hooks !== 'undefined' && !(opts.hooks instanceof Array)) { throw new TypeError('opts.hooks must be an array'); }
  if (typeof opts.hookOpts !== 'undefined' && typeof opts.hookOpts !== 'object') { throw new TypeError('opts.hookOpts must be an object'); }

  this._db = db;

  this.databaseName = this._db.databaseName;
  this.collectionName = collectionName;
  this.snapshotCollectionName = 'm3.' + this.collectionName;

  this._collection = this._db.collection(this.collectionName);
  this._snapshotCollection = this._db.collection(this.snapshotCollectionName);

  this.localPerspective = opts.localPerspective || '_local';
  this._debug = opts.debug || false;
  this._hide = opts.hide || false;
  this._raw = opts.raw || false;
  this.filter = opts.filter || {};
  this.offset = opts.offset || '';
  this.follow = opts.follow !== false;
  this.hooks = opts.hooks || [];
  this.hookOpts = opts.hookOpts || {};

  Readable.call(this, { objectMode: !this._raw });

  var mongoOpts = { sort: { '$natural': 1 }, tailable: opts.follow, tailableRetryInterval: 5000, comment: 'tail' };
  var selector = { '_id._pe': this.localPerspective };
  this._source = this._snapshotCollection.find(selector, mongoOpts).stream();

  var that = this;

  // pause _source to determine maxTries
  this._source.pause();

  // determine the maximum number of versions to examine before the offset must have been encountered
  this.maxTries = 0;
  this._snapshotCollection.count(function(err, count) {
    if (err) { throw err; }
    that.maxTries = count;
    that._source.resume();
  });

  if (this._debug) { console.log(this.databaseName, this.collectionName, 'vcr selector', selector, 'filter', this.filter); }

  // emit a connected graph by making sure every parent of any filtered item is filtered
  var heads = {};
  var offsetReached = false;
  if (!this.offset) {
    offsetReached = true;
  }
  var i = 0;

  function handleData(item) {
    if (that._debug && offsetReached) { console.log(that.databaseName, that.collectionName, '_read', JSON.stringify(item._id)); }

    // only start emitting after the offset is encountered
    if (!offsetReached) {
      if (item._id._v === that.offset) {
        if (that._debug) { console.log(that.databaseName, that.collectionName, '_read offset reached', JSON.stringify(item._id)); }
        offsetReached = true;
      } else {
        // the offset should be encountered within maxTries
        i++;
        if (i >= that.maxTries) {
          if (!that._hide) { console.error(that.databaseName, that.collectionName, '_read', 'offset not found', that.offset, i, that.maxTries); }
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
        lastEmitted['_id._pe'] = that.localPerspective;

        that._walkBranch(lastEmitted, p, function(err, anItem, s) {
          if (err) {
            if (!that._hide) { console.error(that.databaseName, that.collectionName, 'could not determine last emitted version before ' + p, lastEmitted, err, anItem); }
            return callback(err);
          }
          // when done
          if (!anItem) {
            return callback();
          }

          // skip if not all criteria hold on this item
          if (!match(that.filter, anItem)) {
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
      if (!match(that.filter, item)) {
        that._source.resume();
        return;
      }

      // load all hooks on this item, then, if offset is reached, callback
      VersionedCollectionReader.runHooks(that.hooks, that._db, item, that.hookOpts, function(err, afterItem) {
        if (err) { return that._source.emit('error', err); }

        // if hooks filter out the item, do not callback since that would signal the end
        if (!afterItem) {
          if (that._debug && offsetReached) { console.log(that.databaseName, that.collectionName, 'vcr hook filtered', JSON.stringify(item._id)); }
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
          delete item._m3._ack;

          // push the raw or parsed item out to the reader, and resume if not flooded
          if (that._debug) { console.log(that.databaseName, that.collectionName, 'vcr push', JSON.stringify(item._id)); }
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
    if (!that._hide) { console.error('vcr cursor stream error', err); }
    that.emit('error', err);
  });

  this._source.on('close', function() {
    if (that._debug) { console.log('vcr cursor stream closed'); }
    that.push(null);
  });
}
util.inherits(VersionedCollectionReader, Readable);

module.exports = VersionedCollectionReader;

/**
 * Stop the stream reader. An "end" event will be emitted.
 */
VersionedCollectionReader.prototype.close = function close() {
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

/**
 * Travel branches towards the root, based on criteria given.
 *
 * Note: when we reach the end, the callback is called with null for all three
 * parameters.
 *
 * @param {Object} selector  requirements like _id._id and _id._pe
 * @param {String} head  certain version to start tracking from
 * @param {Function} cb  first parameter will be an Error or null. second parameter
 *                       will be the item found or null in case the stream is
 *                       closed. Third parameter will be the stream (so it can be
 *                       closed before we reach the end of the branch.
 */
VersionedCollectionReader.prototype._walkBranch = function _walkBranch(selector, head, cb) {
  // follow parents, take advantage of the fact that the DAG is topologically sorted
  if (!selector) {
    process.nextTick(function() {
      cb(new Error('provide selector'), null);
    });
    return;
  }

  if (!selector['_id._id']) {
    process.nextTick(function() {
      cb(new TypeError('missing selector._id._id'), null);
    });
    return;
  }

  var stream;
  if (selector['_id._pe'] === this.localPerspective) {
    stream = this._snapshotCollection.find(selector, { sort: { '_id._i': -1 }, comment: '_walkBranch' }).stream();
  } else {
    stream = this._snapshotCollection.find(selector, { sort: { $natural: -1 }, comment: '_walkBranch' }).stream();
  }

  stream.on('error', cb);

  var nextParents = {};
  nextParents[head] = true;

  stream.on('data', function(item) {
    // if the current item is in nextParents, replace it by it's parents
    if (nextParents[item._id._v]) {
      delete nextParents[item._id._v];
      item._id._pa.forEach(function(p) {
        nextParents[p] = true;
      });

      cb(null, item, stream);
    }
  });

  stream.on('close', function() {
    cb(null, null, null);
  });
};
