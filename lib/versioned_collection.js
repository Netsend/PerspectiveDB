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

var Writable = require('stream').Writable;
var util = require('util');

var async = require('async');
var mongodb = require('mongodb');
var crypto = require('crypto');

var VirtualCollection = require('./virtual_collection');
var match = require('match-object');

var Timestamp = mongodb.Timestamp;

/**
 * VersionedCollection
 *
 * Version a collection. Use saveRemoteItem and saveOplogItem to add items. Then
 * run processQueue manually or use options.autoProcessInterval to process and
 * sync. Use stopAutoProcessing to cancel.
 *
 * @param {MongoDB.Db} db  database connection
 * @param {String} collectionName  name of the collection
 * @param {Object} [options]  object containing configurable parameters
 *
 * options:
 *   queueLimitRetryTimeout {Number, default 10000}  milliseconds between retrying
 *                                                   a full remote or oplog queue.
 *   proceedOnError {Boolean, default false}  whether to halt if an error occurs
 *   haltOnMergeConflict {Boolean, default true}  whether to halt if merge
 *                                                conflicts arise.
 *   localPerspective {String, default _local}  name of the local perspective,
 *     remotes can't be named like this.
 *   versionKey {String, default: _v}  the name of the version key to use in each
 *     document.
 *   debug {Boolean, default: false}  whether to do extra console logging or not
 *   hide {Boolean, default: false}  whether to suppress errors or not (used in
 *                                   tests)
 *   remotes {Array}  list of remote names to track
 *   autoProcessInterval {Number}  milliseconds between auto processing items.
 *                                 disabled by default.
 *
 * @class represents a VersionedCollection of a database and collection
 */
function VersionedCollection(db, collectionName, options) {
  /* jshint maxcomplexity: 16 */ /* lot's of parameter checking */

  if (!(db instanceof mongodb.Db)) { throw new TypeError('db must be an instance of mongodb.Db'); }
  if (typeof collectionName !== 'string') { throw new TypeError('collectionName must be a string'); }

  options = options || {};

  if (typeof options !== 'object') { throw new TypeError('options must be an object'); }

  this._proceedOnError = false;
  if (typeof options.proceedOnError === 'boolean') {
    this._proceedOnError = options.proceedOnError;
  }
  this._haltOnMergeConflict = true;
  if (typeof options.haltOnMergeConflict === 'boolean') {
    this._haltOnMergeConflict = options.haltOnMergeConflict;
  }
  this.localPerspective = options.localPerspective || '_local';
  this.versionKey = options.versionKey || '_v';
  this._remotes = options.remotes || [];
  this.debug = options.debug || false;
  this._hide = !!options.hide;

  options.objectMode = true;

  Writable.call(this, options);

  if (typeof this.localPerspective !== 'string') { throw new TypeError('options.localPerspective must be a string'); }
  if (typeof this.versionKey !== 'string') { throw new TypeError('options.versionKey must be a string'); }
  if (!Array.isArray(this._remotes)) { throw new TypeError('options.remotes must be an array'); }

  this._db = db;

  this.databaseName = this._db.databaseName;
  this.collectionName = collectionName;
  this.ns = this.databaseName + '.' + this.collectionName;
  this.snapshotCollectionName = 'm3.' + this.collectionName;
  this.tmpCollectionName = 'm3._m3tmp';

  this._collection = this._db.collection(this.collectionName);
  this._snapshotCollection = this._db.collection(this.snapshotCollectionName);
  this._tmpCollection = this._db.collection(this.tmpCollectionName);

  // Process the oplog queue by inserting the first item into the database. Maintain
  // insertion order by only running one instance at a time.
  this._oplogBuffer = [];

  this.setRemotes(this._remotes);

  this._queueLimit = 5000;
  this._queueLimitRetryTimeout = options.queueLimitRetryTimeout || 15000; // timeout if queue is full in ms

  if (options.autoProcessInterval) {
    this.startAutoProcessing(options.autoProcessInterval);
  }

  // init _lastReturnedInc
  this._lastReturnedInc = 0;
}

util.inherits(VersionedCollection, Writable);
module.exports = VersionedCollection;

/**
 * Start or restart auto processing.
 *
 * @param {Number, default: 2000} interval  milliseconds between auto processing
 *                                          queue items.
 */
VersionedCollection.prototype.startAutoProcessing = function startAutoProcessing(interval) {
  interval = interval || 2000;
  if (typeof interval !== 'number') { throw new TypeError('interval must be a number'); }

  if (this.debug) { console.log(this.databaseName, this.collectionName, 'startAutoProcessing', interval); }

  var that = this;
  if (this._autoProcessor) {
    this.stopAutoProcessing();
  }
  this._autoProcessor = setInterval(function() {
    if (!that._locked) {
      that.processQueues(function(err) {
        if (err) {
          if (!that._hide) { console.error(that.databaseName, that.collectionName, 'startAutoProcessing processor', err); }
          console.trace();
          throw err;
        }
      });
    } else {
      if (that.debug) { console.log(that.databaseName, that.collectionName, 'startAutoProcessing locked skip iteration'); }
    }
  }, interval);
};

/**
 * Stop auto processing.
 *
 * Do one extra round of queue processing to make sure everything that was queued
 * at call time is flushed.
 *
 * If currently processing the queues, retry every 100ms until it's unlocked.
 *
 * @param {Function} [cb]  optional callback, called when stopped.
 */
VersionedCollection.prototype.stopAutoProcessing = function stopAutoProcessing(cb) {
  if (this.debug) { console.log(this.databaseName, this.collectionName, 'stopAutoProcessing'); }

  cb = cb || function() {};
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (this._autoProcessor) {
    clearInterval(this._autoProcessor);
    delete this._autoProcessor;
  }

  var that = this;

  if (this._locked) {
    setTimeout(function() {
      that.stopAutoProcessing(cb);
    }, 100);
    return;
  }

  this.processQueues(cb);
};

/**
 * Set remotes.
 *
 * @param {Array} remotes  array containing strings of incoming remote names
 */
VersionedCollection.prototype.setRemotes = function setRemotes(remotes) {
  if (!Array.isArray(remotes)) { throw new TypeError('remotes must be an array'); }

  this._remotes = remotes;
  var that = this;
  this._remoteQueues = {};
  this._remotes.forEach(function(remote) {
    that._remoteQueues[remote] = [];
  });
};

/**
 * Add remote.
 *
 * @param {String} remote  name of new incoming remote
 */
VersionedCollection.prototype.addRemote = function addRemote(remote) {
  if (typeof remote !== 'string') { throw new TypeError('remote must be a string'); }

  this._remotes.push(remote);
  this._remoteQueues[remote] = [];
};

/**
 * Remove remote.
 *
 * @param {String} remote  name of remote to remove
 */
VersionedCollection.prototype.removeRemote = function removeRemote(remote) {
  if (typeof remote !== 'string') { throw new TypeError('remote must be a string'); }

  var that = this;
  this._remotes.forEach(function(name, i) {
    if (remote === name) {
      that._remotes.splice(i, 1);
    }
  });
  delete this._remoteQueues[remote];
};

/**
 * Add collection item as a new version to the DAG.
 *
 * @param {Object} item  collection item
 * @param {Array, default []} pa  array of strings with parent versions
 * @param {Object} [oplogItem]  oplogItem responsible for this item if any
 * @param {Function} cb  First parameter is either an Error object or null. Second
 *                       parameter is the newly created snapshot item.
 */
VersionedCollection.prototype.saveCollectionItem = function saveCollectionItem(item, pa, oplogItem, cb) {
  if (typeof item !== 'object') { throw new TypeError('item must be an object'); }
  if (typeof oplogItem === 'function') {
    cb = oplogItem;
    oplogItem = undefined;
  }
  if (oplogItem !== undefined && typeof oplogItem !== 'object') { throw new TypeError('oplogItem must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  pa = pa || [];
  if (!Array.isArray(pa)) { throw new TypeError('pa must be an Array'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  // version the collection item and add to the DAG
  var newObj = this.versionDoc(item, item[this.versionKey]);
  newObj._id._pa = pa;
  var ts = new Timestamp(0, 0);
  if (oplogItem && oplogItem.ts) { ts = oplogItem.ts; }
  newObj._m3 = { _ack: false, _op: ts };

  // and create a merge if needed
  this._addAllToDAG([{ item: newObj }], function(err) {
    cb(err, newObj);
    return;
  });
};

/**
 * Find all id's that have multiple heads.
 *
 * @param {Function} iterator  called once for each id. first parameter is the id,
 *                             second parameter are all heads
 * @param {Function} cb  First parameter is an Error object or null.
 */
VersionedCollection.prototype.allHeads = function allHeads(iterator, cb) {
  if (typeof iterator !== 'function') { throw new TypeError('iterator must be a function'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  // first get a list of all id's
  this._snapshotCollection.distinct('_id._id', function(err, ids) {
    if (err) {
      if (!that._hide) { console.error(that.databaseName, that.collectionName, 'allHeads', err); }
      cb(err);
      return;
    }

    if (that.debug) { console.log(that.databaseName, that.collectionName, 'allHeads ids to inspect', ids.length); }

    // foreach id, find all open heads by following the complete DAG.
    var count = 0;
    async.eachSeries(ids, function(id, cb2) {
      count++;
      if (that.debug2) { console.log(count, id); }

      var stream = that._snapshotCollection.find({ '_id._id': id, '_id._pe': that.localPerspective }, { sort: { '_id._i': 1 } }).stream();

      var parents = {};

      stream.on('data', function(item) {
        // update ancestors
        item._id._pa.forEach(function(pa) {
          if (parents[pa]) {
            delete parents[pa];
          }
        });

        // add current version
        parents[item._id._v] = true;
      });

      stream.on('error', cb2);
      stream.on('close', function() {
        var heads = Object.keys(parents);
        if (heads.length < 1) {
          cb2(new Error('no heads found'));
          return;
        }
        iterator(id, heads);
        cb2();
      });
    }, cb);
  });
};

/**
 * Acknowledge any versions that have ackd children.
 *
 * NOTE: this function is not branch-safe, make sure each _id has a single head.
 *
 * Iterate over local nackd items, and check if there is an ackd child. If so
 * ack all ancestors. Assume there are always less nackd than ackd items.
 *
 * @param {Function} cb  First parameter is an Error object or null.
 */
VersionedCollection.prototype.ackAncestorsAckd = function ackAncestorsAckd(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  var error = null;

  var stream = this._snapshotCollection.find({ '_id._pe': that.localPerspective, '_m3._ack': { $ne: true } }).stream();

  stream.on('data', function(item) {
    stream.pause();

    if (that.debug) { console.log(that.databaseName, that.collectionName, 'ackAncestorsAckd', JSON.stringify(item)); }

    // find last ackd item of this id
    that._snapshotCollection.findOne({ '_id._id': item._id._id, '_id._pe': that.localPerspective, '_m3._ack': true }, { sort: { '_id._i': -1 } }, function(err, ackdItem) {
      if (err) {
        error = err;
        if (!that._hide) { console.error(that.databaseName, that.collectionName, 'ackAncestorsAckd', error); }
        stream.destroy();
        return;
      }

      if (!ackdItem) {
        stream.resume();
        return;
      }

      // set all ancestors ackd
      that._snapshotCollection.update({ '_id._id': item._id._id, '_id._pe': that.localPerspective, '_id._i': { $lt: ackdItem._id._i }, '_m3._ack': { $ne: true } }, { $set: { '_m3._ack': true } }, { multi: true }, function(err, updated) {
        if (err) {
          error = err;
          if (!that._hide) { console.error(that.databaseName, that.collectionName, 'ackAncestorsAckd update ackd', error); }
          stream.destroy();
          return;
        }

        if (that.debug) { console.log(that.databaseName, that.collectionName, 'ackAncestorsAckd ackd', updated); }

        stream.resume();
      });
    });
  });

  stream.on('error', cb);
  stream.on('close', function() {
    cb(error);
  });
};

/**
 * Copy all collection items that differ from the snapshot, to the snapshot as the
 * latest version.
 *
 * NOTE: this function is not branch-safe, make sure each _id has a single head.
 *
 * @param {Function} cb  First parameter is an Error object or null.
 */
VersionedCollection.prototype.copyCollectionOverSnapshot = function copyCollectionOverSnapshot(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  var error = null;

  var stream = this._collection.find().stream();

  stream.on('data', function(item) {
    stream.pause();

    if (that.debug2) {
      console.log(that.databaseName, that.collectionName, 'copyCollectionOverSnapshot item', JSON.stringify(item));
    } else if (that.debug) {
      process.stdout.write('.');
    }

    that._snapshotCollection.findOne({ '_id._id': item._id, '_id._pe': that.localPerspective }, { sort: { '_id._i': -1 } }, function(err, snapshotItem) {
      if (err) {
        error = err;
        if (!that._hide) { console.error(that.databaseName, that.collectionName, 'copyCollectionOverSnapshot', error); }
        stream.destroy();
        return;
      }

      if (that.debug2) { console.log(that.databaseName, that.collectionName, 'copyCollectionOverSnapshot snapshotItem', JSON.stringify(snapshotItem)); }

      if (!snapshotItem) {
        that.saveCollectionItem(item, [], function(err, newObj) {
          if (err) { error = err; stream.destroy(); return; }
          if (that.debug) { console.log('\n', that.databaseName, that.collectionName, 'copyCollectionOverSnapshot item added as new root', JSON.stringify(newObj._id)); }

          // set ackd
          that._setAckd(newObj._id._id, newObj._id._v, newObj._id._pe, new Timestamp(0, 0), function(err) {
            if (err) { error = err; stream.destroy(); return; }
            if (that.debug) { console.log('\n', that.databaseName, that.collectionName, 'copyCollectionOverSnapshot and ackd', JSON.stringify(item)); }
            stream.resume();
          });
        });
        return;
      }

      if (!that.compareDAGItemWithCollectionItem(snapshotItem, item)) {
        delete item[that.versionKey];
        that.saveCollectionItem(item, [snapshotItem._id._v], function(err, newObj) {
          if (err) { error = err; stream.destroy(); return; }
          if (that.debug) { console.log('\n', that.databaseName, that.collectionName, 'copyCollectionOverSnapshot mismatch, item added as new version', JSON.stringify(newObj._id)); }
          // set ackd
          that._setAckd(newObj._id._id, newObj._id._v, newObj._id._pe, new Timestamp(0, 0), function(err) {
            if (err) { error = err; stream.destroy(); return; }
            if (that.debug) { console.log('\n', that.databaseName, that.collectionName, 'copyCollectionOverSnapshot and ackd', JSON.stringify(item)); }
            stream.resume();
          });
        });
        return;
      }

      if (that.debug2) { console.log(that.databaseName, that.collectionName, 'copyCollectionOverSnapshot match'); }

      if (!snapshotItem._m3._ack) {
        if (that.debug) { console.log('\n', that.databaseName, that.collectionName, 'copyCollectionOverSnapshot not ackd', JSON.stringify(snapshotItem)); }
        that._setAckd(snapshotItem._id._id, snapshotItem._id._v, snapshotItem._id._pe, new Timestamp(0, 0), function(err) {
          if (err) { error = err; stream.destroy(); return; }
          if (that.debug) { console.log('\n', that.databaseName, that.collectionName, 'copyCollectionOverSnapshot set ackd', JSON.stringify(item)); }
          stream.resume();
        });
        return;
      } else {
        stream.resume();
      }
    });
  });

  stream.on('error', cb);
  stream.on('close', function() {
    cb(error);
  });
};

/**
 * Get the last version of a given perspective, optionally ackd.
 *
 * @param {String} perspective  the perspective to lookup
 * @param {Boolean, default: null} [ack]  whether the item has to be ackd
 *                                         or not, or if any state is ok (default).
 * @param {Function} cb  The first parameter will contain either an Error object or
 *                       null. The second parameter the last object from this
 *                       perspective.
 */
VersionedCollection.prototype.lastByPerspective = function lastByPerspective(perspective, ack, cb) {
  if (this.debug) { console.log(this.databaseName, this.collectionName, 'lastByPerspective'); }

  if (typeof ack === 'function') {
    cb = ack;
    ack = null;
  }

  if (typeof perspective !== 'string') { throw new TypeError('perspective must be a string'); }

  var selector = { '_id._pe': perspective };
  if (typeof ack === 'boolean') {
    selector['_m3._ack'] = ack;
  }

  this._snapshotCollection.findOne(selector, { sort: { $natural: -1 }, comment: 'lastByPerspective' }, cb);
};

/**
 * Get the max oplog pointer used in the snapshot collection
 *
 * @param {Function} cb  The first parameter will contain either an Error object or
 *                       null. The second parameter is the max oplog pointer or null
 *                       if no oplog pointer is found or oplog pointer is Timestamp(0, 0)
 */
VersionedCollection.prototype.maxOplogPointer = function maxOplogPointer(cb) {
  if (this.debug) { console.log(this.databaseName, this.collectionName, 'maxOplogPointer'); }

  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var selector = { '_m3._op': { '$exists' : true} };
  this._snapshotCollection.findOne(selector, { sort: { '_m3._op' : -1 }, comment: 'maxOplogPointer' }, function(err, item) {
    if (err) { cb(err); return; }

    if (!item || item._m3._op.isZero()) {
      cb(null, null);
      return;
    }

    cb(null, item._m3._op);
  });
};

/**
 * Queue a version of a document from a non-local perspective.
 *
 * First adds items to a FIFO queue in order to ensure insertion order and calls
 * back. Optionally calls back after the item is saved with the new item as well.
 *
 * Note: call processQueues manually or provide a non-zero value for
 * options.autoProcessInterval to the constructor. If the queue is full, it retries
 * in this._queueLimitRetryTimeout ms.
 *
 * @param {Object} item  item to save
 * @param {Function} [afterCb]  Callback that is called once the item is saved.
 *                              First parameter is an error or null, second
 *                              parameter is the newly created document or null.
 * @param {Function} cb  Callback that is called once the item is added to the
 *                       queue. First parameter will be an error object or null.
 */
VersionedCollection.prototype.saveRemoteItem = function saveRemoteItem(item, afterCb, cb) {
  if (typeof item !== 'object') { throw new TypeError('item must be an object'); }

  if (typeof afterCb === 'function' && !cb) {
    cb = afterCb;
    afterCb = function() {};
  }
  afterCb = afterCb || function() {};
  if (typeof afterCb !== 'function') { throw new TypeError('afterCb must be a function'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  var msg = VersionedCollection.invalidItem(item);
  if (msg) {
    process.nextTick(function() {
      cb(new Error(msg));
    });
    return;
  }

  if (item._id._pe === that.localPerspective) {
    process.nextTick(function() {
      cb(new Error('remote must not equal local perspective'));
    });
    return;
  }

  var queue = that._remoteQueues[item._id._pe];
  var error;

  if (!queue) {
    process.nextTick(function() {
      error = 'item perspective does not equal remote';
      if (!that._hide) { console.error(that.databaseName, that.collectionName, 'saveRemoteItem', error, item._id._pe, that._remotes); }
      cb(new Error(error));
    });
    return;
  }

  if (this._queueLimit <= queue.length) {
    // wait with adding for timeout seconds
    if (!this._hide) { console.log(this.databaseName, this.collectionName, 'saveRemoteItem queue full, wait and retry. queue length: ', queue.length); }
    setTimeout(function() {
      if (that.debug) { console.log(that.databaseName, that.collectionName, 'saveRemoteItem retry...', queue.length); }
      that.saveRemoteItem(item, afterCb, cb);
    }, this._queueLimitRetryTimeout);
    return;
  }

  // clear _id._lo since this item is not created locally
  delete item._id._lo;

  // ensure local collection name
  item._id._co = that.collectionName;

  // set _m3._ack to false
  item._m3 = item._m3 || {};
  item._m3._ack = false;
  // set _m3._op to empty timestamp
  item._m3._op = new Timestamp(0, 0);

  queue.push({ item: item, cb: afterCb });
  cb(null);
};

/**
 * Create a new version of a document by the given oplog item.
 *
 * First adds items to a FIFO queue in order to ensure insertion order and calls
 * back. Optionally calls back after the item is saved with the new item as well.
 *
 * Note: call processQueues manually or provide a non-zero value for
 * options.autoProcessInterval to the constructor. If the queue is full, it retries
 * in this._queueLimitRetryTimeout ms.
 *  
 * @param {Object} item  a mongo oplog item
 * @param {Function} [afterCb]  Callback that is called once the item is saved.
 *                              First parameter is an error or null, second
 *                              parameter is the newly created document or null.
 * @param {Function} cb  Callback that is called once the item is added to the
 *                       queue. First parameter will be an error object or null.
 */
VersionedCollection.prototype.saveOplogItem = function saveOplogItem(item, afterCb, cb) {
  if (typeof item !== 'object') { throw new TypeError('item must be an object'); }

  if (typeof afterCb === 'function' && !cb) {
    cb = afterCb;
    afterCb = function() {};
  }
  afterCb = afterCb || function() {};
  if (typeof afterCb !== 'function') { throw new TypeError('afterCb must be a function'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (VersionedCollection.invalidOplogItem(item)) {
    process.nextTick(function() {
      cb(new Error('invalid oplogItem'), item);
    });
    return;
  }

  var that = this;
  if (this._queueLimit <= this._oplogBuffer.length) {
    // wait with adding for timeout seconds
    if (!this._hide) { console.log(this.databaseName, this.collectionName, 'saveOplogItem queue full, wait and retry. queue length: ', this._oplogBuffer.length); }
    setTimeout(function() {
      if (that.debug) { console.log(that.databaseName, that.collectionName, 'saveOplogItem retry...', that._oplogBuffer.length); }
      that.saveOplogItem(item, afterCb, cb);
    }, this._queueLimitRetryTimeout);
    return;
  }

  this._oplogBuffer.push({ item: item, cb: afterCb });
  cb(null);
};

/**
 * Create a new version of a document by the given oplog item.
 *
 * First adds items to a FIFO queue in order to ensure insertion order and calls
 * back. Optionally calls back after the item is saved with the new item as well.
 *
 * Note: call processQueues manually or provide a non-zero value for
 * options.autoProcessInterval to the constructor. If the queue is full, it retries
 * in this._queueLimitRetryTimeout ms.
 *
 * @param {Object} item  a mongo oplog item
 * @param {Function} cb  Callback that is called once the item is saved.
 *                       First parameter is an error or null, second parameter is
 *                       the newly created document or null.
 */
VersionedCollection.prototype.saveOplogItem2 = function saveOplogItem2(item, cb) {
  if (typeof item !== 'object') { throw new TypeError('item must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (VersionedCollection.invalidOplogItem(item)) {
    process.nextTick(function() {
      cb(new Error('invalid oplogItem'), item);
    });
    return;
  }

  this._oplogBuffer.push({ item: item, cb: cb });
};

/**
 * Start tailing the DAG starting at a given offset. If offset not found within
 * the number of snaphost items, callback with an error.
 *
 * @param {Object} filter  conditions a document should hold
 * @param {String} offset  version to start tailing after
 * @param {Object} options  see options below
 * @param {Function} cb  First parameter will be an error if an error occurred.
 *                       Second parameter will be an item or null on close. Third
 *                       parameter is a callback to get the next item or null..
 * @return {Function} return a close handler. closes the stream when called.
 *
 * options:
 *   {Object} hookOptions  options to pass to a hook
 *   {Function, default: identity} transform  transformation to apply to each doc
 *   {Boolean, default: true} follow  whether to keep the tail open or not
 *   {Array of functions} hooks  a serie of asynchronous functions to execute, each
 *     hook has the following signature: db, object, options, callback and should
 *     callback with an error object, the new item and possibly extra data.
 */
VersionedCollection.prototype.tail = function tail(filter, offset, options, cb) {
  /* jshint maxcomplexity: 16 */ /* lot's of parameter checking */

  if (typeof options === 'function') {
    cb = options;
    options = null;
  }

  options = options || {};

  if (typeof filter !== 'object' || Array.isArray(filter)) {
    process.nextTick(function() {
      cb(new TypeError('filter must be an object'));
    });
    return;
  }

  if (typeof offset !== 'string') {
    process.nextTick(function() {
      cb(new TypeError('offset must be a string'));
    });
    return;
  }

  if (typeof options !== 'object' || Array.isArray(options)) {
    process.nextTick(function() {
      cb(new TypeError('options must be an object'));
    });
    return;
  }

  var transform = options.transform || function(d) { return d; }; // default to identity function
  if (typeof transform !== 'function') {
    process.nextTick(function() {
      cb(new TypeError('options.transform must be a function'));
    });
    return;
  }

  var hooks = options.hooks || [function(db, object, opts, callback) { callback(null, object); }]; // default to identity function
  if (!Array.isArray(hooks)) {
    process.nextTick(function() {
      cb(new TypeError('options.hooks must be an array'));
    });
    return;
  }

  var mongoOpts = { sort: { '$natural': 1 }, tailable: true, tailableRetryInterval: 5000, comment: 'tail' };

  if (options.hasOwnProperty('follow')) {
    if (typeof options.follow === 'boolean') {
      mongoOpts.tailable = options.follow;
    } else {
      process.nextTick(function() {
        cb(new TypeError('options.follow must be a boolean'));
      });
      return;
    }
  }

  var selector = { '_id._pe': this.localPerspective };
  var stream = this._snapshotCollection.find(selector, mongoOpts).stream();
  stream.pause();

  // determine the maximum number of versions to examine before the offset must have been encountered
  var maxTries;
  this._snapshotCollection.count(function(err, count) {
    if (err) { return cb(err); }
    maxTries = count;
    stream.resume();
  });

  if (this.debug) { console.log(this.databaseName, this.collectionName, 'vc tail selector', selector, 'filter', filter); }

  // emit a connected graph by making sure every parent of any filtered item is filtered
  var heads = {};
  var offsetReached = false;
  if (!offset) {
    offsetReached = true;
  }
  var exitError = null;
  var i = 0;
  var that = this;
  function handleData(item) {
    if (that.debug && offsetReached) { console.log(that.databaseName, that.collectionName, 'tail', JSON.stringify(item._id)); }

    // only start emitting after the offset is encountered
    if (!offsetReached) {
      if (item._id._v === offset) {
        if (that.debug && offsetReached) { console.log(that.databaseName, that.collectionName, 'tail offset reached', JSON.stringify(item._id)); }
        offsetReached = true;
      } else {
        // the offset should be encountered within maxTries
        i++;
        if (i >= maxTries) {
          exitError = new Error('offset not found');
          if (!that._hide) { console.error(that.databaseName, that.collectionName, 'tail', exitError, offset, i, maxTries); }
          return stream.destroy();
        }
      }
    }

    heads[item._id._v] = [];

    // move previously emitted parents along with this new branch head
    stream.pause();
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
          if (!match(filter, anItem)) {
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
        exitError = err;
        return stream.destroy();
      }

      // don't emit if not all criteria hold on this item
      if (!match(filter, item)) {
        stream.resume();
        return;
      }

      // load all hooks on this item, then, if offset is reached, transform and callback
      VersionedCollection.runHooks(hooks, that._db, item, options.hookOptions, function(err, afterItem) {
        if (err) { return stream.emit('error', err); }

        // if hooks filter out the item, do not callback since that would signal the end
        if (!afterItem) {
          if (that.debug && offsetReached) { console.log(that.databaseName, that.collectionName, 'tail hook filtered', JSON.stringify(item._id)); }
          stream.resume();
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
          delete item._m3._op;

          cb(null, transform(item), function(err) {
            if (err) { throw err; }
            stream.resume();
          });
        } else {
          stream.resume();
        }
      });
    });
  }

  stream.on('data', handleData);
  stream.on('error', cb);
  stream.on('close', function() {
    if (that.debug) { console.log(that.databaseName, that.collectionName, 'vc tail close'); }
    process.nextTick(function() {
      cb(exitError, null, null);
    });
  });

  return stream;
};

/**
 * Resolve a version to a local increment.
 *
 * @param {String} version  version to resolve
 * @param {Function} cb  First parameter is an error object or null, second parameter a number or null.
 */
VersionedCollection.prototype.resolveVersionToIncrement = function resolveVersionToIncrement(version, cb) {
  if (typeof version !== 'string') { throw new TypeError('version must be a string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  // resolve version to increment
  var that = this;
  var error;

  this._snapshotCollection.findOne({ '_id._v': version, '_id._pe': this.localPerspective }, { '_id._i': true }, function(err, incr) {
    if (err) { cb(err); return; }

    if (!incr || !incr._id._i) {
      error = new Error('version could not be resolved to an increment');
      if (!that._hide) { console.error(that.databaseName, that.collectionName, 'vc resolveVersionToIncrement', error, version, incr); }
      cb(error, null);
      return;
    }

    cb(null, incr._id._i);
  });
};

/**
 * Clear the snapshot collection and restart by versioning every document currently
 * in the collection.
 *
 * Warning: No updates should occur on the collection, this is not enforced by this
 * function.
 *
 * @param {Number} [size]  the number of bytes to reserve
 * @param {Function} cb  first parameter is an error object or null
 */
VersionedCollection.prototype.rebuild = function rebuild(size, cb) {
  if (this.debug) { console.log(this.databaseName, this.collectionName, 'vc rebuild'); }

  if (typeof size === 'function') {
    cb = size;
    size = null;
  }

  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  // drop and recreate the snapshot collection
  this._clearSnapshot(size, function(err) {
    if (err) {
      if (!that._hide) { console.error(that.databaseName, that.collectionName, 'vc rebuild', err); }
      return cb(err);
    }

    // find all docs in the collection
    var cursor = that._collection.find();
    cursor.count(function(err, count) {
      if (err) {
        if (!that._hide) { console.error(that.databaseName, that.collectionName, 'vc rebuild', err); }
        return cb(err);
      }

      if (that.debug) { console.log(that.databaseName, that.collectionName, 'vc rebuild count', count); }

      var error, lastObj;
      var stream = cursor.stream();

      stream.on('data', function(doc) {
        stream.pause();

        lastObj = doc;

        var newObj;
        if (doc[that.versionKey]) {
          newObj = that.versionDoc(doc, true);
        } else {
          newObj = that.versionDoc(doc);
        }

        newObj._m3 = { _ack: false, _op: new Timestamp(0, 0) };

        // save document
        that._getNextIncrement(function(err, i) {
          if (err) {
            if (!that._hide) { console.error(that.databaseName, that.collectionName, 'vc rebuild seq', err, i, newObj); }
            error = err;
            return stream.destroy();
          }

          newObj._id._i = i;

          that._save(newObj, function(err) {
            if (err) {
              if (!that._hide) { console.error(that.databaseName, that.collectionName, 'vc rebuild error', err); }
              error = err;
              return stream.destroy();
            }

            // copy to collection (also to ensure oplog has the right items)
            that._syncDAGItemWithCollection(newObj, function(err) {
              if (err) {
                if (!that._hide) { console.error(that.databaseName, that.collectionName, 'vc rebuild copy error', err); }
                error = err;
                return stream.destroy();
              }
              if (that.debug) { console.log(that.databaseName, that.collectionName, 'vc rebuild', JSON.stringify(newObj._id)); }
              stream.resume();
            });
          });
        });
      });

      stream.on('error', function(err) {
        if (!that._hide) { console.error(that.databaseName, that.collectionName, 'vc rebuild', err); }
        cb(err);
      });

      stream.on('close', function() {
        cb(error);
      });
    });
  });
};

/**
 * Check if the item contains a valid _id._id, _id._v, _id._pe and _id._pa
 *
 * @params {Object} item  item to check  
 * @return {String} empty string if nothing is wrong or a problem description
 */
VersionedCollection.invalidItem = function invalidItem(item) {
  if (typeof item !== 'object' || Array.isArray(item)) {
    return ('item must be an object');
  }

  if (typeof item._id !== 'object' || Array.isArray(item._id)) {
    return ('item._id must be an object');
  }

  if (!item._id._id) {
    return ('missing item._id._id');
  }

  if (typeof item._id._v !== 'string') {
    return ('item._id._v must be a string');
  }

  if (typeof item._id._pe !== 'string') {
    return ('item._id._pe must be a string');
  }

  if (!Array.isArray(item._id._pa)) {
    return ('item._id._pa must be an array');
  }

  return '';
};

/**
 * Process oplog and remote queues, one at each time.
 *
 * @param {Function} [cb]  First parameter is an Error object on error or null
 */
VersionedCollection.prototype.processQueues = function processQueues(cb) {
  if (this.debug2) { console.log(this.databaseName, this.collectionName, 'processQueues'); }

  cb = cb || function() {};

  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  var error;

  if (this._locked) {
    error = new Error('already processing queues');
    if (!that._hide) { console.error(that.databaseName, that.collectionName, 'processQueues', error); }
    process.nextTick(function() {
      cb(error);
    });
    return;
  }

  this._locked = true;

  // check if any remote queue has any items
  var newRemoteItems = Object.keys(this._remoteQueues).some(function(remote) {
    return that._remoteQueues[remote].length;
  });

  if (!newRemoteItems && !this._oplogBuffer.length) {
    process.nextTick(function() {
      that._locked = false;
      cb(null);
    });
    return;
  }

  if (this.debug) { console.log(this.databaseName, this.collectionName, 'processQueues start'); }

  if (this.debug) { console.log(this.databaseName, this.collectionName, 'processQueues oplog'); }
  that._processOplogQueue(function(err) {
    if (that.debug) { console.log(that.databaseName, that.collectionName, 'processQueues oplog done'); }
    if (err) {
      that._locked = false;
      return cb(err);
    }

    if (that.debug) { console.log(that.databaseName, that.collectionName, 'processQueues remote'); }
    that._processRemoteQueues(function(err) {
      if (that.debug) { console.log(that.databaseName, that.collectionName, 'processQueues remote done'); }
      that._locked = false;
      return cb(err);
    });
  });
};

/**
 * Clone a snapshot item so that it can be modified..
 *
 * @param {Object} dagItem  item from the DAG
 * @return {Object} new shallow clone of the DAG item
 */
VersionedCollection.prototype.cloneDAGItem = function cloneDAGItem(dagItem) {
  if (typeof dagItem !== 'object') { throw new TypeError('dagItem1 must be an object'); }

  var newItem = {};
  Object.keys(dagItem).forEach(function(key) {
    if (key === '_id') {
      newItem._id = {};
      Object.keys(dagItem._id).forEach(function(idKey) {
        newItem._id[idKey] = dagItem._id[idKey];
      });
      return;
    }
    if (key === '_m3') {
      newItem._m3 = {};
      Object.keys(dagItem._m3).forEach(function(m3Key) {
        newItem._m3[m3Key] = dagItem._m3[m3Key];
      });
      return;
    }
    newItem[key] = dagItem[key];
  });
  return newItem;
};

/**
 * Compare two items from the DAG on equality.
 *
 * @param {Object} dagItem1  first item from the DAG
 * @param {Object} dagItem2  second item from the DAG
 * @return {Boolean} true if all equal, false if inequal
 */
VersionedCollection.prototype.compareDAGItems = function compareDAGItems(dagItem1, dagItem2) {
  if (typeof dagItem1 !== 'object') { throw new TypeError('dagItem1 must be an object'); }
  if (typeof dagItem2 !== 'object') { throw new TypeError('dagItem2 must be an object'); }

  var keys1 = Object.keys(dagItem1);
  var keys2 = Object.keys(dagItem2);

  var idKeys1 = [] || dagItem1._id && Object.keys(dagItem1._id);
  var idKeys2 = [] || dagItem2._id && Object.keys(dagItem2._id);

  var m3Keys1 = [] || dagItem1._m3 && Object.keys(dagItem1._m3);
  var m3Keys2 = [] || dagItem2._m3 && Object.keys(dagItem2._m3);

  delete keys1._id;
  delete keys2._id;

  delete keys1._m3;
  delete keys2._m3;

  function areEqual(keys1, keys2, item1, item2) {
    function comparator(key) {
      delete keys2[key];
      // only use strict comparison on certain types
      if (~['string', 'number', 'boolean'].indexOf(typeof item1[key])) {
        return item1[key] === item2[key];
      }
      // use JSON.stringify comparison in all other cases
      return JSON.stringify(item1[key]) === JSON.stringify(item2[key]);
    }

    return keys1.every(comparator) && keys2.every(comparator);
  }

  return areEqual(idKeys1, idKeys2, dagItem1._id, dagItem2._id) &&
         areEqual(m3Keys1, m3Keys2, dagItem1._m3, dagItem2._m3) &&
         areEqual(keys1, keys2, dagItem1, dagItem2);
};

/**
 * Compare two values. Supports strings and anything that is JSON.stringifyable.
 *
 * @param {mixed} val1  first value
 * @param {mised} val2  second value
 * @return {Boolean} true if equal, false if inequal
 */
VersionedCollection.equalValues = function equalValues(val1, val2) {
  if (typeof val1 === 'string') {
    return val1 === val2;
  }

  return JSON.stringify(val1) === JSON.stringify(val2);
};

/**
 * Compare an item from the DAG with an item from the collection on equality.
 *
 * @param {Object} dagItem  item from the DAG
 * @param {Object} collectionItem  item from the collection to compare with
 * @return {Boolean} true if equal, false if inequal
 */
VersionedCollection.prototype.compareDAGItemWithCollectionItem = function compareDAGItemWithCollectionItem(dagItem, collectionItem) {
  var that = this;
  var comparator = function(key) {
    delete keys2[key];
    if (key === '_id') {
      return VersionedCollection.equalValues(dagItem._id._id, collectionItem._id) && VersionedCollection.equalValues(dagItem._id._v, collectionItem[that.versionKey]);
    } else if (key === '_m3') {
      return true;
    } else if (key === that.versionKey) {
      return true;
    } else {
      var eq = VersionedCollection.equalValues(collectionItem[key], dagItem[key]);
      if (that.debug2) { console.log(key, 'new item equals oplog item', eq); }
      return eq;
    }
  };

  var keys1 = Object.keys(dagItem);
  var keys2 = Object.keys(collectionItem);

  return keys1.every(comparator) && keys2.every(comparator);
};

/**
 * If a collection exists, make sure it is capped, has the right size and index and
 * is not full. If the snapshot collection does not exist yet, create it.
 *
 * Furthermore, make sure all items in the collection are in the versioned
 * collection by rebuilding empty versioned collections.
 *
 * @param {Number} size  the size the snapshotCollection should have
 * @param {Number, default 5} [free]  the percentage of free space that should be
 *                                    available
 * @param {Function} cb  The first parameter will contain either an Error object or
 *                       null.
 */
VersionedCollection.prototype.ensureSnapshotCollection = function ensureSnapshotCollection(size, free, cb) {
  if (this.debug) { console.log(this.databaseName, this.collectionName, 'ensureSnapshotCollection'); }

  if (typeof free === 'function') {
    cb = free;
    free = 5;
  }

  free = free || 5;

  if (typeof size !== 'number') { throw new TypeError('size must be a number'); }
  if (typeof free !== 'number') { throw new TypeError('free must be a number'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var error;
  var that = this;
  that._snapshotCollection.stats(function(err, stats) {
    if (err) {
      if (err.message === 'ns not found' || /^Collection .* not found/.test(err.message)) {
        // create by rebuilding
        that.rebuild(size, cb);
        return;
      }

      if (!that._hide) { console.error(that.databaseName, that.collectionName, 'ensureSnapshotCollection', err); }
      return cb(err);
    }

    if (stats.capped !== true) {
      // rebuild if empty
      if (!stats.count) {
        that.rebuild(size, cb);
        return;
      }

      error = new Error('snapshot collection not capped');
      if (!that._hide) { console.error(that.databaseName, that.collectionName, 'ensureSnapshotCollection', error, JSON.stringify(stats)); }
      return cb(error);
    }

    if (stats.storageSize < size) {
      // rebuild if empty
      if (stats.count === 0) {
        that.rebuild(size, cb);
        return;
      }

      error = new Error('snapshot collection too small');
      if (!that._hide) { console.error(that.databaseName, that.collectionName, 'ensureSnapshotCollection', error, JSON.stringify(stats), size); }
      return cb(error);
    }

    // check free space
    if (100 - (100 / stats.storageSize) * stats.size < free) {
      error = new Error('not enough free space');
      if (!that._hide) { console.error(that.databaseName, that.collectionName, 'ensureSnapshotCollection', error, JSON.stringify(stats), size); }
      return cb(error);
    }

    that._snapshotCollection.indexInformation(function(err, ixNfo) {
      if (err && err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) { return cb(err); }

      // see ensureIndex for the right name and size
      if (!ixNfo || !ixNfo['_id_pe_i'] || ixNfo['_id_pe_i'].length !== 3) {
        // rebuild if empty
        if (stats.count === 0) {
          that.rebuild(size, cb);
          return;
        }

        error = new Error('snapshot collection index not ok');
        if (!that._hide) { console.error(that.databaseName, that.collectionName, 'ensureSnapshotCollection', error, JSON.stringify(ixNfo)); }
        return cb(error);
      }

      cb();
    });
  });
};

/**
 * Create a new versioned document, version the _id property. If the doc does not 
 * already have a version, create a random one.
 *
 * @param {Object} doc  document to save, it should have an _id property
 * @param {Boolean, default false} [keepVersion]  whether to generate a new version
 *                                                or not
 * @return {Object} a new versioned document
 */
VersionedCollection.prototype.versionDoc = function versionDoc(doc, keepVersion) {
  // shallow clone to prevent having side-effects
  var vDoc = {};
  Object.keys(doc).forEach(function(prop) {
    vDoc[prop] = doc[prop];
  });

  vDoc._id = {
    _co: this.collectionName,
    _id: doc._id,
    _v: keepVersion ? doc[this.versionKey] : VersionedCollection._generateRandomVersion(),
    _pe: this.localPerspective,
    _pa: [],
    _lo: true
  };
  if (vDoc[this.versionKey]) {
    delete vDoc[this.versionKey];
  }
  return vDoc;
};

/**
 * Show differences in item compared with base.
 *   + = created
 *   ~ = changed
 *   - = removed
 *
 * @param {Object} item  item to compare with base
 * @param {Object} base  base item
 * @return {Object} object containing all differences
 */
VersionedCollection.diff = function diff(item, base) {
  var d = {};
  var checked = {};

  // check for added and changed keys
  Object.keys(item).forEach(function(key) {
    if (base.hasOwnProperty(key)) {
      if (JSON.stringify(item[key]) !== JSON.stringify(base[key])) {
        d[key] = '~';
      }
    } else {
      d[key] = '+';
    }

    // speedup check for deleted keys
    checked[key] = true;
  });

  // check for deleted keys
  Object.keys(base).forEach(function(key) {
    if (checked[key]) { return; }
    d[key] = '-';
  });

  return d;
};

/**
 * If the first character of the first key of the object equals "$" then this item
 * contains one or more modifiers.
 *
 * @param {Object} oplogItem  the oplog item.
 * @return {Boolean} true if the object contains any modifiers, false otherwise.
 */
VersionedCollection.oplogUpdateContainsModifier = function oplogUpdateContainsModifier(oplogItem) {
  if (!oplogItem) { return false; }
  if (!oplogItem.o) { return false; }
  if (typeof oplogItem.o !== 'object' || Array.isArray(oplogItem)) { return false; }

  var keys = Object.keys(oplogItem.o);
  if (keys[0] && keys[0][0] === '$') {
    return true;
  }

  return false;
};

/**
 * Create a new version by applying an update document in a temporary collection.
 *
 * @param {Object} dagItem  item from the snapshot
 * @param {Object} oplogItem  the update item from the oplog.
 * @param {Function} cb  On error the first parameter will be the Error object and
 *                       the second parameter will be the original document. On 
 *                       success the first parameter will be null and the second
 *                       parameter will be the new version of the document.
 */
VersionedCollection.prototype.createNewVersionByUpdateDoc = function createNewVersionByUpdateDoc(dagItem, oplogItem, cb) {
  if (typeof dagItem !== 'object') { throw new TypeError('dagItem must be an object'); }
  if (typeof oplogItem !== 'object') { throw new TypeError('oplogItem must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  try {
    if (oplogItem.op !== 'u') { throw new Error('oplogItem op must be "u"'); }
    if (!oplogItem.o2._id) { throw new Error('missing oplogItem.o2._id'); }
    if (oplogItem.o._id) { throw new Error('oplogItem contains o._id'); }
  } catch(err) {
    process.nextTick(function() {
      cb(err, oplogItem);
    });
    return;
  }

  var that = this;

  // create a new _id with a new version and set parent to be the dagItem
  // update the dagItem with this new _id and insert it into a temp collection
  // apply the update modifiers to get the new version of the doc
  var vDoc = that.versionDoc({ _id: oplogItem.o2._id });
  // the new doc should have a parent pointer to the ancestor and a perspective
  vDoc._id._pa = [dagItem._id._v];
  dagItem._id = vDoc._id;
  dagItem._m3._ack = false;
  dagItem._m3._op = oplogItem.ts;

  that._tmpCollection.insert(dagItem, {w: 1, comment: 'createNewVersionByUpdateDoc'}, function(err) {
    if (err) { return cb(err, oplogItem); }

    // update the just created copy
    var selector = { '_id._id': dagItem._id._id, '_id._v': dagItem._id._v, '_id._pe': dagItem._id._pe };
    if (that.debug) { console.log(that.databaseName, that.collectionName, 'createNewVersionByUpdateDoc selector', JSON.stringify(selector)); }

    that._tmpCollection.findAndModify(selector, [], oplogItem.o, {w: 0, new: true}, function(err, newObj) {
      if (err) {
        if (!that._hide) { console.error(that.databaseName, that.collectionName, 'createNewVersionByUpdateDoc', err); }
        return cb(err, oplogItem);
      }
      if (!newObj) {
        if (that.debug) { console.log(that.databaseName, that.collectionName, 'createNewVersionByUpdateDoc new doc not created', JSON.stringify(dagItem)); }
        return cb(new Error('new doc not created'), oplogItem);
      }

      that._tmpCollection.drop(function(err) {
        if (err) {
          if (!that._hide) { console.error(that.databaseName, that.collectionName, 'createNewVersionByUpdateDoc dropping _tmpCollection', err); }
          return cb(err, oplogItem);
        }

        cb(null, newObj);
      });
    });
  });
};

/**
 * Merge all given objects sequentially.
 *
 * @param {Array} dagItems  items to merge, should all have the same perspective
 *                          and merges should not exist in the DAG yet.
 * @param {Function} cb  First parameter will be the Error object or null.
 */
VersionedCollection.prototype.mergeAndSave = function mergeAndSave(dagItems, cb) {
  if (!Array.isArray(dagItems)) { throw new TypeError('dagItems must be an array'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  var first = dagItems.shift();
  try {
    var id = first._id._id;
    var pe = first._id._pe;
    dagItems.forEach(function(dagItem) {
      try {
        if (dagItem._id._id !== id) { throw new Error('ids must be equal'); }
        if (dagItem._id._pe !== pe) { throw new Error('perspectives must be equal'); }
      } catch(err) {
        if (!that._hide) { console.error(that.databaseName, that.collectionName, 'mergeAndSave', err, JSON.stringify(first._id), JSON.stringify(dagItem._id)); }
        throw err;
      }
    });
  } catch(err) {
    process.nextTick(function() {
      cb(err);
    });
    return;
  }

  async.reduce(dagItems, first, function(memo, item, cb2) {
    that._merge(memo, item, function(err, merged) {
      if (err) {
        if (!that._hide) { console.error(that.databaseName, that.collectionName, 'mergeAndSave', err); }
        cb2(err);
        return;
      }

      var newObj = merged[0];

      if (newObj._id._v && newObj._m3) {
        // skip created version
        if (that.debug) { console.log(that.databaseName, that.collectionName, 'mergeAndSave', 'skip saved item', JSON.stringify(newObj._id)); }
        cb2(null);
        return;
      }

      if (!newObj._id._v) {
        // generate new version
        if (that.debug) { console.log(that.databaseName, that.collectionName, 'mergeAndSave', 'new version created'); }
        newObj._id._v = VersionedCollection._generateRandomVersion();
      }

      if (!newObj._m3 || !newObj._m3.hasOwnProperty('_ack')) {
        // set _m3
        if (that.debug) { console.log(that.databaseName, that.collectionName, 'mergeAndSave', 'new merge created'); }
        newObj._m3 = { _ack: false, _op: new Timestamp(0, 0) };
      }

      // save document
      that._getNextIncrement(function(err, i) {
        if (err) {
          if (!that._hide) { console.error(that.databaseName, that.collectionName, 'mergeAndSave', err, i, newObj); }
          cb2(err);
          return;
        }

        newObj._id._i = i;

        that._save(newObj, function(err) {
          if (err) {
            if (!that._hide) { console.error(that.databaseName, that.collectionName, 'mergeAndSave error', err); }
            cb2(err);
            return;
          }

          // copy to collection (also to ensure oplog has the right items)
          that._syncDAGItemWithCollection(newObj, function(err) {
            if (err) {
              if (!that._hide) { console.error(that.databaseName, that.collectionName, 'mergeAndSave copy error', err); }
              cb2(err);
              return;
            }

            if (that.debug) { console.log(that.databaseName, that.collectionName, 'mergeAndSave', JSON.stringify(newObj)); }

            cb2(null, newObj);
          });
        });
      });
    });
  }, cb);
};

// run a set of export hooks on an item
VersionedCollection.runHooks = function runHooks(hooks, db, item, opts, cb) {
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
 * Implementation of _write method of Writable stream. This method is not called
 * directly.
 */
VersionedCollection.prototype._write = function(item, encoding, cb) {
  this.saveRemoteItem(item, cb);
};

/**
 * Set a snapshot item ackd by _id, _pe and _v
 *
 * @param {mixed} id  id of the item
 * @param {String} v  version
 * @param {String} pe  perspective
 * @param {mongodb.Timestamp} op  oplog pointer
 * @param {Function} cb  The first parameter will contain either an Error object or
 *                       null.
 */
VersionedCollection.prototype._setAckd = function _setAckd(id, v, pe, op, cb) {
  if (typeof id === 'undefined') { throw new TypeError('id must be defined'); }
  if (typeof v !== 'string') { throw new TypeError('v must be a string'); }
  if (typeof pe !== 'string') { throw new TypeError('pe must be a string'); }
  if (typeof op !== 'object') { throw new TypeError('op must be a object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  that._snapshotCollection.update({ '_id._id': id, '_id._v': v, '_id._pe': pe }, { $set: { '_m3._ack': true, '_m3._op': op }}, {w: 1, comment: '_setAckd'}, function(err, updated) {
    if (err) {
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_setAckd', err); }
      cb(err);
      return;
    }

    if (updated !== 1) {
      var error = new Error('could not set ackd');
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_setAckd', error, updated, id, v, pe); }
      cb(error);
      return;
    }

    cb(err);
  });
};

/**
 * Get the next increment number.
 *
 * @param {Function} cb  The first parameter will contain either an Error object or
 *                       null, second parameter will be a Number.
 */
VersionedCollection.prototype._getNextIncrement = function _getNextIncrement(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  // first check if the last increment is known.
  if (this._lastReturnedInc) {
    process.nextTick(function() {
      cb(null, ++that._lastReturnedInc);
    });
    return;
  }

  this._snapshotCollection.findOne({}, { sort: { '_id._i': -1 }, comment: '_getNextIncrement' }, function(err, item) {
    if (err) {
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_getNextIncrement', err); }
      return cb(err);
    }

    var i = 0;
    try {
      if (typeof item._id._i === 'number') { i = item._id._i; }
    } catch(e) {
    }

    that._lastReturnedInc = i;
    cb(null, ++that._lastReturnedInc);
  });
};

/**
 * Process remote queues one by one, cleaning up memory. Return with any errors.
 *
 * @param {Function} cb  if an error occurred the first parameter will be an error
 *                       object containing the number of errors. The second
 *                       parameter will be an object of remote names and the error
 *                       object.
 */
VersionedCollection.prototype._processRemoteQueues = function _processRemoteQueues(cb) {
  if (this.debug) { console.log(this.databaseName, this.collectionName, '_processRemoteQueues'); }

  var that = this;
  var lastRemote;
  async.eachSeries(this._remotes, function(remote, cb2) {
    lastRemote = remote;

    if (that.debug) { console.log(that.databaseName, that.collectionName, '_processRemoteQueues remote', remote, that._remoteQueues[remote].length); }

    // process 500 items at most each time
    var items = that._remoteQueues[remote].splice(0, 500);

    that._addAllToDAG(items, function(err) {
      // trigger callback on each item
      items.forEach(function(item) {
        item.cb(err);
      });
      cb2(err);
    });
  }, function(err) {
    if (err) {
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_processRemoteQueues error remove remote', lastRemote, err); }
      that.removeRemote(lastRemote);
      return cb(err);
    }
    cb();
  });
};

/**
 * Process oplog queue, cleaning up memory. Return with any errors.
 *
 * @param {Function} cb  if an error occurred the first parameter will be an error
 *                       object containing the number of errors.
 */
VersionedCollection.prototype._processOplogQueue = function _processOplogQueue(cb) {
  var that = this;
  async.eachSeries(this._oplogBuffer, function(item, callback) {
    if (item.done) { return process.nextTick(callback); }
    that._applyOplogItem(item.item, function(err, newItem) {
      if (err) {
        if (!that._hide) { console.error(that.databaseName, that.collectionName, '_processOplogQueue', err, JSON.stringify(item)); }
      } else {
        item.done = true;
      }
      if (item.cb) { item.cb(err, newItem); }
      callback(err);
    });
  }, function(err) {
    if (!err) { that._oplogBuffer = []; }
    cb(err);
  });
};

/**
 * Clear the snapshot collection by recreating it. Base sizes on current snapshot
 * collection allocation size or three times the size of the collection.
 *
 * @param {Nuber} [size]  optional size in bytes, default to the current size or
 *                        three times the collection size.
 * @param {Function} cb  First parameter will be an error object or null.
 * @return {null}
 */
VersionedCollection.prototype._clearSnapshot = function _clearSnapshot(size, cb) {
  if (typeof size === 'function') {
    cb = size;
    size = null;
  }
  var that = this;
  this._snapshotCollection.stats(function(err, stats) {
    if (err) {
      if (err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) {
        if (!that._hide) { console.error(that.databaseName, that.collectionName, '_clearSnapshot snapshot collection stats error', err); }
        return cb(err);
      }
      stats = {};
    }
    if (that.debug) { console.log(that.databaseName, that.collectionName, '_clearSnapshot snapshot collection stats', JSON.stringify(stats)); }

    size = size || stats.storageSize;

    that._collection.stats(function(err, stats) {
      if (err) {
        if (err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) {
          if (!that._hide) { console.error(that.databaseName, that.collectionName, '_clearSnapshot collection stats error', err); }
          return cb(err);
        }
        stats = {};
      }

      if (that.debug) { console.log(that.databaseName, that.collectionName, '_clearSnapshot collection stats', JSON.stringify(stats)); }

      // allocate 3 times the current collection size if no size determined yet
      size = size || (stats.size * 3);

      if (!size) { return cb(new Error('could not determine size')); }

      that._db.dropCollection(that.snapshotCollectionName, function(err, result) {
        if (err && err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) {
          if (!that._hide) { console.error(that.databaseName, that.collectionName, '_clearSnapshot drop snapshot collection error', err); }
          return cb(err);
        }
        if (that.debug) { console.log(that.databaseName, that.collectionName, '_clearSnapshot drop snapshot collection', result, that.snapshotCollectionName); }

        that._createSnapshotCollection(size, cb);
      });
    });
  });
};

/**
 * Create a snapshot collection which is capped and has a certain index.
 *
 * @param {Nuber} size  size in bytes
 * @param {Function} cb  First parameter will be an error object or null.
 */
VersionedCollection.prototype._createSnapshotCollection = function _createSnapshotCollection(size, cb) {
  if (typeof size !== 'number') { throw new TypeError('size must be a number'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var opts = {
    strict: true, // throw an error if it already exists
    autoIndexId: true,
    capped: true,
    size: size,
    w: 1,
  };

  if (this.debug) { console.log(this.databaseName, this.collectionName, '_createSnapshotCollection collection opts', JSON.stringify(opts)); }

  var that = this;
  this._db.createCollection(this.snapshotCollectionName, opts, function(err, snapshotCollection) {
    if (err) {
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_createSnapshotCollection error', err); }
      return cb(err);
    }

    that._snapshotCollection = snapshotCollection;

    // support _applyOplogInsertItem, _findLastAckdOrLocallyCreated and  addAllToDAG
    that._ensureIndex(function(err) {
      if (err) {
        if (!that._hide) { console.error(that.databaseName, that.collectionName, '_createSnapshotCollection index error', err); }
        return cb(err);
      }

      cb(null);
    });
  });
};

/**
 * Make sure the right index exists. This index is used in _applyOplogInsertItem,
 * _findLastAckdOrLocallyCreated, addAllToDAG and others.
 *
 * @param {Function} cb  First parameter will be an error object or null.
 */
VersionedCollection.prototype._ensureIndex = function _ensureIndex(cb) {
  this._db.ensureIndex(this.snapshotCollectionName, { '_id._id': 1, '_id._pe': 1, '_id._i': -1 }, { name: '_id_pe_i' }, cb);
};

/**
 * Save a version of a document to the snapshot collection. The document should
 * have a _id._v property.
 *
 * @param {Object} doc  document to save
 * @param {Function} cb  callback will have an Error object or null as the first
 *                       argument. Second parameter will be the document.
 */
VersionedCollection.prototype._save = function _save(doc, cb) {
  if (typeof cb !== 'function') { throw new Error('cb must be a function'); }

  try {
    if (!doc._id._id) { throw new Error('missing doc._id._id'); }
    if (!doc._id._v) { throw new Error('missing doc._id._v'); }
    if (!doc._id._pe) { throw new Error('missing doc._id._pe'); }
  } catch(err) {
    process.nextTick(function() {
      cb(err, doc);
    });
    return;
  }

  // create and insert doc
  this._snapshotCollection.insert(doc, {w: 1, comment: '_save'}, function(err) {
    cb(err, doc);
  });
};

/**
 * Sync a dagItem to the collection.
 * - if it's a new version, than sync this version to the collection
 * - if it's a deleted item, than remove this id from the collection
 *
 * Note: this should correspond with the right skip logic in
 * _applyOplogUpdateFullDoc.
 *
 * @param {Object} item  the new document
 * @param {Function} cb  first parameter will be an error object or null.
 */
VersionedCollection.prototype._syncDAGItemWithCollection = function _syncDAGItemWithCollection(item, cb) {
  if (typeof cb !== 'function') { throw new Error('cb must be a function'); }

  try {
    if (!item._id._id) { throw new Error('missing item._id._id'); }
    if (!item._id._v) { throw new Error('missing item._id._v'); }
  } catch(err) {
    process.nextTick(function() {
      cb(err);
    });
    return;
  }

  if (item._id._d) {
    if (this.debug) { console.log(this.databaseName, this.collectionName, '_syncDAGItemWithCollection delete from collection', JSON.stringify(item._id)); }
    this._collection.remove({ _id: item._id._id }, {w: 1, comment: '_syncDAGItemWithCollection'}, cb);
  } else {
    if (this.debug) { console.log(this.databaseName, this.collectionName, '_syncDAGItemWithCollection add to collection', JSON.stringify(item._id)); }
    // prevent side effects
    var newItem = {};
    Object.keys(item).forEach(function(key) {
      newItem[key] = item[key];
    });

    newItem[this.versionKey] = newItem._id._v;
    newItem._id = newItem._id._id;
    delete newItem._m3;
    this._collection.update({ _id: newItem._id }, newItem, {w: 1, upsert: true, comment: '_syncDAGItemWithCollection'}, cb);
  }
};

/**
 * Ensure ._id._v, generates a 48 bit base64 string if _id._v does not exist.
 *
 * @param {Object} item  item to version, it should have an _id object
 * @return {String|null} the new version if one is generated, otherwise null
 */
VersionedCollection.prototype._ensureVersion = function _ensureVersion(item) {
  if (typeof item._id !== 'object') { throw new TypeError('item._id must be an object'); }

  var v = null;
  if (!item._id._v) {
    v = VersionedCollection._generateRandomVersion();
    item._id._v = v;
  }
  return v;
};

/**
 * Do a three-way-merge.
 *
 * @param {Object} itemA  version a
 * @param {Object} itemB  version b
 * @param {Object} lca  lowest common ancestor of itemA and itemB
 * @param {Object} [lcaB]  lowest common ancestor of itemB if perspectives differ
 *                         lca and itemA will always be leading in this case.
 * @return {Object|Array} merged item or an array with conflicting key names
 */
VersionedCollection._threeWayMerge = function _threeWayMerge(itemA, itemB, lca, lcaB) {
/*
* w = lowest common ancestor

ouput each attribute that’s either

    common to all three sequences, or
    present in x but absent in y and w, or
    present in y but absent in x and w,

while we delete the attributes that are either

    present in y and w but absent in x, or
    present in x and w but absent in y.

auto merge when

    present in x and y but absent in w, or
    present in w but absent in x and y.

mark in conflict when

    present in x and same attibute present in y but with different values
*/
  lcaB = lcaB || lca;

  var keysLcaA = Object.keys(lca);
  var keysLcaB = Object.keys(lcaB);
  var keysItemA = Object.keys(itemA);
  var keysItemB = Object.keys(itemB);
  var mergedItem = {};
  var conflicts = [];


  var diffA = {}, diffB = {};

  // calculate diff of itemA
  // + = create
  // ~ = changed
  // - = removed
  // check for added and changed keys
  keysItemA.forEach(function(keyA) {
    // copy itemA while we're on it
    mergedItem[keyA] = itemA[keyA];

    // check if only one version is different from lca
    if (lca.hasOwnProperty(keyA)) {
      if (JSON.stringify(itemA[keyA]) !== JSON.stringify(lca[keyA])) {
        diffA[keyA] = '~';
      }
    } else {
      diffA[keyA] = '+';
    }
  });
  // check for deleted keys
  keysLcaA.forEach(function(keyA) {
    if (!itemA.hasOwnProperty(keyA)) {
      diffA[keyA] = '-';
    }
  });

  // calculate diff of itemB
  // check for added and changed keys
  keysItemB.forEach(function(keyB) {
    // check if only one version is different from lca
    if (lcaB.hasOwnProperty(keyB)) {
      if (JSON.stringify(itemB[keyB]) !== JSON.stringify(lcaB[keyB])) {
        diffB[keyB] = '~';
      }
    } else {
      diffB[keyB] = '+';
    }
  });
  // check for deleted keys, and keys created in diffA that were already in lcaB
  keysLcaB.forEach(function(keyB) {
    if (!itemB.hasOwnProperty(keyB)) {
      diffB[keyB] = '-';
    }
    if (diffA.hasOwnProperty(keyB) && diffA[keyB] === '+') {
      conflicts.push(keyB);
    }
  });

  // detect any conflicts
  Object.keys(diffB).forEach(function(delta) {
    if (diffA.hasOwnProperty(delta) && JSON.stringify(diffA[delta]) !== JSON.stringify(diffB[delta])) {
      conflicts.push(delta);
    } else {
      // handle other scenarios
      // either apply delta, or add conflict
      if (diffB[delta] === '-') {
        delete mergedItem[delta];
      } else if (diffA[delta] === '~' && JSON.stringify(itemA[delta]) !== JSON.stringify(itemB[delta])) {
        // both updated to different values
        conflicts.push(delta);
      } else if (diffA[delta] === '+' && JSON.stringify(itemA[delta]) !== JSON.stringify(itemB[delta])) {
        // both created with different values
        conflicts.push(delta);
      } else if (diffB[delta] === '+' && itemA.hasOwnProperty(delta) && JSON.stringify(itemA[delta]) !== JSON.stringify(itemB[delta])) {
        // created at B but already existed in A
        conflicts.push(delta);
      } else {
        mergedItem[delta] = itemB[delta];
      }
    }
  });

  if (conflicts.length) { return conflicts; }

  return mergedItem;
};

/**
 * Find lowest common ancestor(s) of x and y. DAGs are only topologically sorted
 * per perspective.
 *
 * @param {Object} itemX  item x
 * @param {Object} itemY  item y
 * @param {Function} cb  First parameter will be an Error or null. Second parameter
 *                       will be an array with all lowest common ancestor versions.
 */
VersionedCollection.prototype._findLCAs = function _findLCAs(itemX, itemY, cb) {
  /* jshint maxcomplexity: 23 */ /* might need some refactoring */

  if (this.debug) { console.log(this.databaseName, this.collectionName, '_findLCAs', JSON.stringify(itemX._id), JSON.stringify(itemY._id)); }

  var that = this;
  if (!itemX) {
    process.nextTick(function() {
      var err = new Error('provide itemX');
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_findLCAs error', err); }
      cb(err, itemX);
    });
    return;
  }

  if (!itemY) {
    process.nextTick(function() {
      var err = new Error('provide itemY');
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_findLCAs error', err); }
      cb(err, itemY);
    });
    return;
  }

  if (!itemX._id) {
    process.nextTick(function() {
      var err = new TypeError('missing itemX._id');
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_findLCAs error', err); }
      cb(err, itemY);
    });
    return;
  }

  if (!itemY._id) {
    process.nextTick(function() {
      var err = new TypeError('missing itemY._id');
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_findLCAs error', err); }
      cb(err, itemY);
    });
    return;
  }

  if (typeof itemX._id !== 'object') {
    process.nextTick(function() {
      var err = new TypeError('itemX._id must be an object');
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_findLCAs error', err); }
      cb(err, itemY);
    });
    return;
  }

  if (typeof itemY._id !== 'object') {
    process.nextTick(function() {
      var err = new TypeError('itemY._id must be an object');
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_findLCAs error', err); }
      cb(err, itemY);
    });
    return;
  }

  // check if ids are equal
  // only use strict comparison on certain types
  if (~['string', 'number', 'boolean'].indexOf(typeof itemX._id._id)) {
    if (itemX._id._id !== itemY._id._id) {
      process.nextTick(function() {
        var err = new TypeError('itemX._id._id must equal itemY._id._id');
        if (!that._hide) { console.error(that.databaseName, that.collectionName, '_findLCAs error', err, itemX._id._id, itemY._id._id); }
        cb(err, itemY);
      });
      return;
    }
  } else {
    // use JSON.stringify comparison in all other cases
    if (JSON.stringify(itemX._id._id) !== JSON.stringify(itemY._id._id)) {
      process.nextTick(function() {
        var err = new TypeError('itemX._id._id must equal itemY._id._id');
        if (!that._hide) { console.error(that.databaseName, that.collectionName, '_findLCAs error', err, itemX._id._id, itemY._id._id); }
        cb(err, itemY);
      });
      return;
    }
  }

  if (!itemX._id._pe) {
    process.nextTick(function() {
      var err = new TypeError('missing itemX._id._pe');
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_findLCAs error', err); }
      cb(err, itemY);
    });
    return;
  }

  if (!itemY._id._pe) {
    process.nextTick(function() {
      var err = new TypeError('missing itemY._id._pe');
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_findLCAs error', err); }
      cb(err, itemY);
    });
    return;
  }

  var perspectiveX = itemX._id._pe;
  var perspectiveY = itemY._id._pe;

  var perspectives = [perspectiveX];
  if (perspectiveX !== perspectiveY) {
    perspectives.push(perspectiveY);
  }

  var cas = {};
  var lcas = []; // list of lowest common ancestors

  // init
  var headsX = {};
  var headsY = {};

  // if this is a virtual merge (an item without _id._v), use it's parents
  if (itemX._id._v) {
    headsX[itemX._id._v] = perspectiveX;
  } else {
    itemX._id._pa.forEach(function(p) {
      headsX[p] = perspectiveX;
    });
  }

  if (itemY._id._v) {
    headsY[itemY._id._v] = perspectiveY;
  } else {
    itemY._id._pa.forEach(function(p) {
      headsY[p] = perspectiveY;
    });
  }

  // shortcut case where one item is the parent of the other and both are from the same perspective
  // only do it with exact one parent and not on virtual merges (items without a version)
  // FIXME: check if this really has to be perspective bound
  if (perspectives.length === 1) {
    if (itemX._id._pa.length === 1 && itemY._id._v && itemX._id._pa[0] === itemY._id._v) { lcas.push(itemY._id._v); }
    if (itemY._id._pa.length === 1 && itemX._id._v && itemY._id._pa[0] === itemX._id._v) { lcas.push(itemX._id._v); }
    if (lcas.length) {
      if (that.debug) { console.log(that.databaseName, that.collectionName, '_findLCAs shortcut', lcas); }
      process.nextTick(function() {
        cb(null, lcas);
      });
      return;
    }
  }

  var ancestorsX = [];
  var ancestorsY = [];

  // determin selector and sort
  var selectorPerspectives = { $in: [perspectiveX, perspectiveY] };
  var sort = { '$natural': -1 };
  if (perspectiveX === perspectiveY) {
    selectorPerspectives = perspectiveX;

    if (perspectiveX === this.localPerspective) {
      sort = { '_id._i': -1 };
    }
  }

  // go through the DAG from heads to root
  var selector = { '_id._id': itemX._id._id, '_id._pe': selectorPerspectives };
  var stream = this._snapshotCollection.find(selector, { sort: sort, comment: '_findLCAs' }).stream();

  stream.on('data', function(item) {
    var version = item._id._v;
    var perspective = item._id._pe;
    var parents = item._id._pa || [];

    if (that.debug2) {
      console.log('\nversion:', version, perspective);
      console.log('START HEADSX', headsX);
      console.log('START HEADSY', headsY);
      console.log('START ANCESTORSX', ancestorsX);
      console.log('START ANCESTORSY', ancestorsY);
      console.log('START LCAS', lcas);
      console.log('START CAS', cas);
    }

    // track branches of X and Y by updating heads by perspective on match and keep track of ancestors
    if (headsX[version] === perspective) {
      delete headsX[version];
      parents.forEach(function(p) {
        headsX[p] = perspectiveX;
      });
      ancestorsX.unshift(version);

      // now check if current item is in the ancestors of the other DAG, if so, we have a ca
      if (~ancestorsY.indexOf(version)) {
        if (!cas[version]) {
          lcas.push(version);
        }
        // make sure any of it's ancestors won't count as a ca (which makes this an lca)
        parents.forEach(function(p) {
          cas[p] = true;
        });
      }
    }

    // same with the heads of y
    if (headsY[version] === perspective) {
      delete headsY[version];
      parents.forEach(function(p) {
        headsY[p] = perspectiveY;
      });
      ancestorsY.unshift(version);

      if (~ancestorsX.indexOf(version)) {
        if (!cas[version]) {
          lcas.push(version);
        }
        parents.forEach(function(p) {
          cas[p] = true;
        });
      }
    }

    if (that.debug2) {
      console.log('\nEND HEADSX', headsX);
      console.log('END HEADSY', headsY);
      console.log('END ANCESTORSX', ancestorsX);
      console.log('END ANCESTORSY', ancestorsY);
      console.log('END LCAS', lcas);
      console.log('END CAS', cas);
    }

    // as soon as both sets of open heads are equal, we have seen all lca(s)
    VersionedCollection._intersect(Object.keys(headsX), Object.keys(headsY), function(err, intersection, subset) {
      if (subset === 0) {
        // finish up
        stream.pause();
        // add any of the open heads that are not a common ancestor and are in the database
        async.eachSeries(Object.keys(headsX), function(head, callback) {
          if (cas[head]) { return process.nextTick(callback); }

          selector = { '_id._id': itemX._id._id, '_id._v': head, '_id._pe': selectorPerspectives};
          that._snapshotCollection.find(selector, { comment: '_findLCAs2' }).toArray(function(err, items) {
            if (err) { return callback(err); }
            if (items.length !== perspectives.length) {
              var msg = new Error('missing at least one perspective when fetching lca ' + head + '. perspectives: ' + perspectives.join(', '));
              return callback(msg);
            }

            lcas.push(head);
            callback();
          });
        }, function(err) {
          if (err) { return cb(err); }

          stream.destroy();
          return;
        });
      }
    });
  });

  stream.on('close', function() {
    if (that.debug) { console.log(that.databaseName, that.collectionName, '_findLCAs found', lcas); }
    cb(null, lcas);
  });

  stream.on('error', function(err) {
    if (!that._hide) { console.error(that.databaseName, that.collectionName, '_findLCAs error', err); }
    cb(err);
  });
};

/**
 * Get the elements that are in both given arrays.
 *
 * O(m+n) where m and n are the number of elements in arr1 and arr2 respectively.
 *
 * @param {Array} arr1  first array of elements
 * @param {Array} arr2  second array of elements
 * @param {Function} cb  First parameter will be an Error if any. Second parameter
 *                       is an array with items in arr1 that are also in arr2,
 *                       while maintaining order of arr1. Third parameter will be
 *                       false if both objects are not subsets of each other. -1 if
 *                       arr1 is a subset of arr2, or 1 if arr2 is a subset of arr1
 *                       and 0 if both sets are equal.
 */
VersionedCollection._intersect = function _intersect(arr1, arr2, cb) {
  if (!arr1) {
    process.nextTick(function() {
      cb(new Error('provide arr1'), arr1);
    });
    return;
  }

  if (!arr2) {
    process.nextTick(function() {
      cb(new Error('provide arr2'), arr2);
    });
    return;
  }

  if (!Array.isArray(arr1)) {
    process.nextTick(function() {
      cb(new TypeError('arr1 must be an array'), arr1);
    });
    return;
  }

  if (!Array.isArray(arr2)) {
    process.nextTick(function() {
      cb(new TypeError('arr2 must be an array'), arr2);
    });
    return;
  }

  var intersection = [];
  var all1in2 = true;
  var all2in1 = true;

  // create object for constant lookup times
  var obj2 = {};
  arr2.forEach(function(el) {
    obj2[el] = true;
  });

  arr1.forEach(function(el) {
    if (obj2.hasOwnProperty(el)) {
      intersection.push(el);
      delete obj2[el];
    } else {
      all1in2 = false;
    }
  });

  if (Object.keys(obj2).length) {
    all2in1 = false;
  }

  var count = 0;
  if (all1in2) { count = -1; }
  if (all2in1) { count += 1; }

  var subset = false;
  if (all1in2 || all2in1) { subset = count; }

  process.nextTick(function() {
    cb(null, intersection, subset);
  });
  return;
};

/**
 * Get all heads of the provided DAG, which is all leaves (that are not deleted).
 *
 * @param {Array} items  DAG, sorted list of items with parents, first item will be
 *                       the root node.
 * @param {Boolean} [includeDeleted]  whether or not to include deleted children
 * @return {Array} an array of head items, last version of a branch
 */
VersionedCollection._branchHeads = function _branchHeads(items, includeDeleted) {
  var heads = {};
  var deleted = {};

  if (!items.length) { return []; }

  var id = items[0]._id._id;

  // bootstrap with all items as a possible head
  items.forEach(function(item) {
    if (!VersionedCollection.equalValues(id, item._id._id)) {
      throw new Error('id mismatch');
    }
    heads[item._id._v] = item;
  });

  // go from root to leaves, eliminating all parents
  items.forEach(function(item) {
    // keep track of deleted items, but not as a head
    if (item._id._d) {
      deleted[item._id._id] = item;
      delete heads[item._id._v];
    }
    if (!item._id._pa.length) {
      // a new root, don't keep track of any previously deleted item
      delete deleted[item._id._id];
    } else {
      // remove parents
      item._id._pa.forEach(function(p) {
        delete heads[p];
        if (deleted[item._id._id] && deleted[item._id._id]._id._v === p) {
          delete deleted[item._id._id];
        }
      });
    }
  });

  var key, values = [];
  for (key in heads) {
    if (heads.hasOwnProperty(key)) { values.push(heads[key]); }
  }
  if (includeDeleted) {
    for (key in deleted) {
      if (deleted.hasOwnProperty(key)) { values.push(deleted[key]); }
    }
  }
  return values;
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
VersionedCollection.prototype._walkBranch = function _walkBranch(selector, head, cb) {
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

/**
 * Find out if version is an ancestor of a given item. Follows branches.
 *
 * @param {Object} version  version that might be an ancestor of item
 * @param {Object} item  item from the versioned collection
 * @param {Function} cb  first parameter will be an Error or null. second parameter
 *                       will be a boolean whether ancestor is really an ancestor
 *                       or null on error.
 */
VersionedCollection.prototype._isAncestorOf = function _isAncestorOf(version, item, cb) {
  var found = false;

  // shortcut if version is equal or direct parent
  if (version === item._id._v || ~item._id._pa.indexOf(version)) {
    if (this.debug) { console.log(this.databaseName, this.collectionName, '_isAncestorOf shortcut equal version', JSON.stringify(item)); }
    process.nextTick(function() {
      cb(null, true);
    });
    return;
  }
  this._walkBranch({ '_id._id': item._id._id, '_id._pe': item._id._pe }, item._id._v, function(err, anItem, stream) {
    if (err) { return cb(err, null); }
    if (!anItem) { return cb(null, found); }
    if (anItem._id._v === version) {
      found = true;
      stream.destroy();
    }
  });
};

/**
 * Add a set of items from a remote or local queue to the DAG iff it can be merged
 * without conflict.
 * Create local perspectives for each item if necessary.
 *
 * 1. check if perspectives of all items are equal, init _m3._ack on false
 * 2. make sure the new DAGs only have one head
 * 3. make sure every parent of every new item exists in the virtual DAG.
 * 4. make sure every new version by perspective does not exist in the persisted collection yet
 * 5. ensure a local perspective of each version
 * 6. merge new heads with latest local head from persistent storage
 * 7. insert the new local and possibly remote items into the DAG
 * 8. sync the new heads with the collection
 *
 * @param {Array} newItems  one or more DAGs by the same perspective
 * @param {Function} cb  First parameter will an Error object or null.
 */
VersionedCollection.prototype._addAllToDAG = function _addAllToDAG(newItems, cb) {
  var that = this;

  if (!newItems.length) {
    process.nextTick(function() {
      cb(null);
    });
    return;
  }

  var error = null;

  var perspective = newItems[0].item._id._pe;

  var allItems = [];
  var localItems = [];
  var remoteItems = [];
  var newRoots = {};

  // 1. check if perspectives of all items are equal, and ensure an _m3 object with a _ack attribute
  var DAGs = {};
  try {
    // get items only and check perspective
    newItems.forEach(function(itemNcb) {
      var item = itemNcb.item;
      if (perspective !== item._id._pe) {
        if (!that._hide) { console.error(that.databaseName, that.collectionName, '_addAllToDAG 1 perspective mismatch', perspective, item._id._pe); }
        throw new TypeError('perspective mismatch');
      }

      // ensure _m3 object
      item._m3 = item._m3 || {};
      if (!item._m3.hasOwnProperty('_ack')) { item._m3._ack = false; }
      if (!item._m3.hasOwnProperty('_op')) { item._m3._op = new Timestamp(0, 0); }

      DAGs[item._id._id] = DAGs[item._id._id] || [];

      // record new roots
      if (!item._id._pa.length) {
        newRoots[item._id._id] = true;

        // TODO: tmp connect new roots if previous item is a deletion
        var length = DAGs[item._id._id].length;
        // error if there is a previous item
        if (length && DAGs[item._id._id][length - 1]._id._d) {
          // link this new root to previous (deleted) item
          item._id._pa[0] = DAGs[item._id._id][length - 1]._id._v;
          if (that.debug) { console.log(that.databaseName, that.collectionName, '_addAllToDAG 1 connected new root to previous (deleted) item', JSON.stringify(item)); }
        } else if (length) {
          error = new Error('root preceded');
          if (!that._hide) { console.error(that.databaseName, that.collectionName, '_addAllToDAG 1', error, JSON.stringify(item)); }
          throw error;
        }
      }
      DAGs[item._id._id].push(item);

      allItems.push(item);
    });
  } catch(err) {
    process.nextTick(function() {
      cb(err);
    });
    return;
  }

  var localOnly = false;
  if (perspective === this.localPerspective) {
    localOnly = true;
    localItems = allItems;
  } else {
    remoteItems = allItems;
  }

  if (that.debug) {
    console.log(that.databaseName, that.collectionName, '_addAllToDAG 1 remote items', JSON.stringify(remoteItems));
    console.log(that.databaseName, that.collectionName, '_addAllToDAG 1 local items', JSON.stringify(localItems));
  }

  async.eachSeries(Object.keys(DAGs), function(id, callback) {
    var items = DAGs[id];

    // 2. make sure the new DAGs only have one head
    if (that.debug) { console.log(that.databaseName, that.collectionName, '_addAllToDAG 2 items', id, items.length); }

    var nackdHeads = VersionedCollection._branchHeads(items, true);

    if (nackdHeads.length !== 1) {
      error = new Error('not exactly one head');
      if (!that._hide) {
        console.error(that.databaseName, that.collectionName, '_addAllToDAG 2 not exactly one head', error, nackdHeads.length, JSON.stringify(items));
      }
      process.nextTick(function() {
        callback(error);
      });
      return;
    }

    that._virtualCollection = new VirtualCollection(that._snapshotCollection, items);

    // create a new context with _snapshotCollection set to _virtualCollection
    var newThis = {
      debug: that.debug,
      _hide: that._hide,
      databaseName: that.databaseName,
      localPerspective: that.localPerspective,
      versionKey: that.versionKey,
      collectionName: that.collectionName,
      _snapshotCollection: that._virtualCollection,
      _findLCAs: that._findLCAs,
      _merge: that._merge
    };

    // 3. make sure every parent of every new item exists in the virtual DAG.
    async.eachSeries(items, function(item, cb2) {
      if (that.debug) { console.log(that.databaseName, that.collectionName, '_addAllToDAG 3', JSON.stringify(item)); }

      async.each(item._id._pa, function(p, cb3) {
        var selector = { '_id._id': item._id._id, '_id._v': p, '_id._pe': item._id._pe };
        that._virtualCollection.findOne(selector, { sort: { '_id._i': -1 }, comment: '_addAllToDAG3' }, function(err, theParent) {
          if (err) {
            if (!that._hide) { console.error(that.databaseName, that.collectionName, '_addAllToDAG 3', err, JSON.stringify(item)); }
            return cb3(err);
          }
          if (!theParent) {
            error = new Error('parent not found');
            if (!that._hide) { console.error(that.databaseName, that.collectionName, '_addAllToDAG 3', error, p, JSON.stringify(item), JSON.stringify(selector)); }
            return cb3(error);
          }
          cb3();
        });
      }, function(err) {
        // 4. make sure every new version by perspective does not exist in the persisted collection yet
        if (that.debug) { console.log(that.databaseName, that.collectionName, '_addAllToDAG 4'); }

        if (err) { return cb2(err, item); }

        var selector = { '_id._id': item._id._id, '_id._v': item._id._v, '_id._pe': item._id._pe };
        that._snapshotCollection.findOne(selector, { comment: '_addAllToDAG4' }, function(err, exists) {
          if (err) { return cb2(err); }
          if (exists) {
            error = new Error('version already exists');
            if (!that._hide) { console.error(that.databaseName, that.collectionName, '_addAllToDAG 4', error, JSON.stringify(exists)); }
            return cb2(error);
          }
          cb2();
        });
      });
    }, function(err) {
      // 5. ensure a local perspective of each version
      if (err) { return callback(err); }

      that._ensureLocalPerspective(items, function(err, newLocalItems) {
        if (err) { return callback(err); }

        if (that.debug) { console.log(that.databaseName, that.collectionName, '_addAllToDAG 5 new local items', JSON.stringify(newLocalItems)); }

        var toProcess = items;
        if (newLocalItems.length) {
          if (localOnly) {
            error = new Error('local duplicates created');
            if (!that._hide) { console.error(that.databaseName, that.collectionName, '_addAllToDAG 5', error); }
            return callback(error);
          }

          // make sure items that are used in virtual collection are updated
          Array.prototype.push.apply(items, newLocalItems);

          // make sure items that are used at the end on insertion are updated
          Array.prototype.push.apply(localItems, newLocalItems);

          toProcess = newLocalItems;
        }

        var newHeads = VersionedCollection._branchHeads(toProcess, true);

        if (that.debug) { console.log(that.databaseName, that.collectionName, '_addAllToDAG 5 new heads', JSON.stringify(newHeads)); }

        if (newHeads.length !== 1) {
          error = new Error('not exactly one head');
          if (!that._hide) {
            console.error(that.databaseName, that.collectionName, '_addAllToDAG 5 not exactly one head', error, newHeads.length, JSON.stringify(items));
          }
          process.nextTick(function() {
            callback(error);
          });
          return;
        }

        // 6. merge new heads with latest local head from persistent storage
        async.eachSeries(newHeads, function(head, cb4) {
          if (that.debug) { console.log(that.databaseName, that.collectionName, '_addAllToDAG 6', JSON.stringify(head._id)); }

          // selector and sort combination should use index created by _createSnapshotCollection
          var selector = { '_id._id': head._id._id, '_id._pe': that.localPerspective };
          that._snapshotCollection.findOne(selector, { sort: { '_id._i': -1 }, comment: '_addAllToDAG6' }, function(err, localHead) {
            if (err) { return cb4(err); }

            if (newRoots[head._id._id] && !head._id._pa.length && localHead && localHead._id._d) {
              head._id._pa[0] = localHead._id._v;
              if (that.debug) { console.log(that.databaseName, that.collectionName, '_addAllToDAG 6 connected new root to previous (deleted) item', JSON.stringify(head)); }
            }

            // check if new root and not linked to deleted item and if the version does not exist yet for this _id and local head 
            if (newRoots[head._id._id] && !head._id._pa.length && localHead && !localHead._id._pa.length && localHead._id._v !== head._id._v) {
              error = new Error('different root already in snapshot');
              if (!that._hide) { console.error(that.databaseName, that.collectionName, '_addAllToDAG 6', head._id._id, error, JSON.stringify(head), JSON.stringify(localHead)); }
              if (that._proceedOnError) {
                return cb4();
              }
              cb4(error);
              return;
            }

            if (!localHead) {
              return cb4();
            } else if (localHead._id._v === head._id._v) {
              // both heads have the same version
              return cb4();
            } else if (localHead._id._d) {
              // last item is deleted
              return cb4();
            }

            var oldSync = head._m3._ack;
            var oldOp = head._m3._op;
            newThis._merge(localHead, head, function(err, merged) {
              // restore ackd
              head._m3._ack = oldSync;
              head._m3._op = oldOp;

              if (err) {
                if (that.debug) {
                  console.log(that.databaseName, that.collectionName, '_addAllToDAG 6 merge error', head._id._id, err, JSON.stringify(localHead), JSON.stringify(head));
                }
                // skip merge conflicts
                if (err.message === 'merge conflict' && (that._proceedOnError || !that._haltOnMergeConflict)) {
                  return cb4();
                }
                return cb4(err);
              }

              // the branch can be fast-forwarded to without merge
              if (merged[0]._id._v) {
                // it is a fast forward by merge, the local perspective should have been created previously
                if (that.debug) { console.log(that.databaseName, that.collectionName, '_addAllToDAG 6 ff without merge'); }
                return cb4();
              }

              if (that.debug) { console.log(that.databaseName, that.collectionName, '_addAllToDAG 6 merge created', JSON.stringify(merged[0]._id)); }

              // this is a genuine merge, version and add this new item
              merged[0]._m3 = {
                _ack: false,
                _op: new Timestamp(0, 0)
              };

              that._ensureVersion(merged[0]);

              localItems.push(merged[0]);

              cb4();
            });
          });
        }, callback);
      });
    });
  }, function(err) {
    // 7. insert the new local and possibly remote items into the DAG
    if (err) { return cb(err); }

    var allNew = [];
    if (!localOnly) {
      Array.prototype.push.apply(allNew, remoteItems);
    }
    Array.prototype.push.apply(allNew, localItems);

    if (that.debug) {
      console.log(that.databaseName, that.collectionName, '_addAllToDAG 7 remote items', JSON.stringify(remoteItems));
      console.log(that.databaseName, that.collectionName, '_addAllToDAG 7 local items', JSON.stringify(localItems));
    }

    var localItemsPerDAG = {};
    localItems.forEach(function(item) {
      localItemsPerDAG[item._id._id] = localItemsPerDAG[item._id._id] || [];
      localItemsPerDAG[item._id._id].push(item);
    });

    var newLocalHeads = [];
    Object.keys(localItemsPerDAG).forEach(function(key) {
      Array.prototype.push.apply(newLocalHeads, VersionedCollection._branchHeads(localItemsPerDAG[key], true));
    });

    if (that.debug) {
      console.log(that.databaseName, that.collectionName, '_addAllToDAG 7 new local heads', JSON.stringify(newLocalHeads));

      console.log(that.databaseName, that.collectionName, '_addAllToDAG 7 remote items length', remoteItems.length);
      console.log(that.databaseName, that.collectionName, '_addAllToDAG 7 local items length', localItems.length);
      console.log(that.databaseName, that.collectionName, '_addAllToDAG 7 new local heads length', newLocalHeads.length);
    }

    // (re-)set sequence number on local items
    async.eachSeries(localItems, function(item, cb6) {
      that._getNextIncrement(function(err, i) {
        if (err) {
          if (!that._hide) { console.error(that.databaseName, that.collectionName, '_addAllToDAG 7 add seq', err, i, item); }
          return cb6(err);
        }

        item._id._i = i;
        cb6();
      });
    }, function(err) {
      if (err) { return cb(err); }

      that._snapshotCollection.insert(allNew, {w: 1, comment: '_addAllToDAG7' }, function(err) {
        // 8. sync the new local heads with the collection
        if (err) {
          if (!that._hide) { console.error(that.databaseName, that.collectionName, '_addAllToDAG 8 CRITICAL', err); }
          return cb(err);
        }

        async.eachSeries(newLocalHeads, function(head, cb5) {
          that._syncDAGItemWithCollection(head, function(err) {
            if (err) {
              if (!that._hide) { console.error(that.databaseName, that.collectionName, '_addAllToDAG 8 CRITICAL', err); }
              return cb5(err);
            }

            if (that.debug) { console.log(that.databaseName, that.collectionName, '_addAllToDAG 8 synced', JSON.stringify(head)); }

            cb5();
          });
        }, cb);
      });
    });
  });
};

/**
 * Ensure a local perspective exists of the given items by creating any missing
 * ones. It adds newly created items to the original input and as a separate
 * array in the callback.
 *
 * @param {Array} items  new items to create local perspective for.
 * @param {Function} cb  First parameter will be an Error object or null, second
 *                       parameter an array with newly created local versions of
 *                       non-local input.
 */
VersionedCollection.prototype._ensureLocalPerspective = function _ensureLocalPerspective(items, cb) {
  var that = this;

  var newLocalItems = [];

  if (!items.length) {
    process.nextTick(function() {
      cb(null, newLocalItems);
    });
    return;
  }

  var error = null;

  var perspective;
  try {
    perspective = items[0]._id._pe;
  } catch(err) {
    process.nextTick(function() {
      cb(new Error('could not determine perspective'));
    });
    return;
  }

  try {
    // check perspective
    items.forEach(function(item) {
      if (perspective !== item._id._pe) {
        if (!that._hide) { console.error(that.databaseName, that.collectionName, '_ensureLocalPerspective perspective mismatch', perspective, item._id._pe); }
        throw new TypeError('perspective mismatch');
      }
    });
  } catch(err) {
    process.nextTick(function() {
      cb(err);
    });
    return;
  }

  if (perspective === this.localPerspective) {
    process.nextTick(function() {
      cb(null, newLocalItems);
    });
    return;
  }

  // create a copy to iterate over, so the newly created local items can be appended to the "items" array.
  var newItems = items.slice(0);
  that._virtualCollection = new VirtualCollection(that._snapshotCollection, newItems, { debug: that.debug });

  // create a new context with _snapshotCollection set to _virtualCollection
  var newThis = {
    debug: that.debug,
    _hide: that._hide,
    databaseName: that.databaseName,
    localPerspective: that.localPerspective,
    versionKey: that.versionKey,
    collectionName: that.collectionName,
    _snapshotCollection: that._virtualCollection,
    _isAncestorOf: that._isAncestorOf,
    _walkBranch: that._walkBranch,
    _findLCAs: that._findLCAs,
    _merge: that._merge
  };

  async.eachSeries(items, function(item, cb2) {
    if (that.debug) { console.log(that.databaseName, that.collectionName, '_ensureLocalPerspective 1 new item', JSON.stringify(item._id)); }

    // 1. ensure a local perspective of each version
    // create one by merging this item with the lca of the last version of the local perspective
    var selector = { '_id._id': item._id._id, '_id._pe': that.localPerspective };

    var opts = { sort: { $natural: -1 }, sortIndex: '_id._i', comment: '_ensureLocalPerspective1' };
    that._virtualCollection.findOne(selector, opts, function(err, lastLocalItem) {
      if (err) { return cb2(err); }

      if (that.debug) { console.log(that.databaseName, that.collectionName, '_ensureLocalPerspective 1 last local item', JSON.stringify(lastLocalItem)); }

      if (!lastLocalItem) {
        // it's the first version, simply clone the current item and set perspective to local
        var localVersion = {};
        Object.keys(item).forEach(function(key) { localVersion[key] = item[key]; });
        // create a shallow clone from the _id
        localVersion._id = {};
        Object.keys(item._id).forEach(function(key) { localVersion._id[key] = item._id[key]; });
        localVersion._id._pe = that.localPerspective;
        // set new _m3
        localVersion._m3 = {
          _ack: false,
          _op: new Timestamp(0, 0)
        };
        // do not set _id._lo since this version is not locally created
        if (that.debug) { console.log(that.databaseName, that.collectionName, '_ensureLocalPerspective 1 new root', JSON.stringify(localVersion)); }
        newLocalItems.push(localVersion);
        newItems.push(localVersion);
        return cb2();
      }

      // check if the new item is an ancestor of the local item
      newThis._isAncestorOf(item._id._v, lastLocalItem, function(err, isAncestor) {
        if (err) { return cb2(err); }

        if (isAncestor) {
          // we already have a local perspective of this version, do not create one
          if (that.debug) { console.log(that.databaseName, that.collectionName, '_ensureLocalPerspective 2 remote version is ancestor of last local item', item._id._v); }
          return cb2();
        }

        // now find the lca of the two
        newThis._findLCAs(item, lastLocalItem, function(err, lcas) {
          if (err) { return cb2(err); }

          var description = item._id._id +' '+ item._id._v + ' pe '+ item._id._pe +' and '+ lastLocalItem._id._pe;
          if (lcas.length < 1) {
            if (lastLocalItem._id._d && item._id._pa.length === 0) {
              // it's the first new version, simply clone the current item and set perspective to local
              var localVersion = {};
              Object.keys(item).forEach(function(key) { localVersion[key] = item[key]; });
              // create a shallow clone from the _id
              localVersion._id = {};
              Object.keys(item._id).forEach(function(key) { localVersion._id[key] = item._id[key]; });
              localVersion._id._pe = that.localPerspective;
              // set new _m3
              localVersion._m3 = {
                _ack: false,
                _op: new Timestamp(0, 0)
              };
              // do not set _id._lo since this version is not locally created
              newLocalItems.push(localVersion);
              newItems.push(localVersion);
              return cb2();
            }

            error = new Error('no lca found');
            if (!that._hide) { console.error(that.databaseName, that.collectionName, item._id._id, '_ensureLocalPerspective 2', error, description); }
            // skip if ignoring merge conflicts
            if (that._proceedOnError) {
              return cb2();
            }
            return cb2(error);
          }
          if (lcas.length > 1) {
            error = new Error('more than one lca found');
            if (!that._hide) { console.error(that.databaseName, that.collectionName, '_ensureLocalPerspective 2', error, description); }
            return cb2(error);
          }

          if (that.debug) { console.log(that.databaseName, that.collectionName, '_ensureLocalPerspective 2 lca', JSON.stringify(lcas[0])); }

          // fetch the lca with local perspective
          selector = { '_id._id': item._id._id, '_id._v': lcas[0], '_id._pe': that.localPerspective };
          that._virtualCollection.findOne(selector, { sort: { '_id._i': -1 }, comment: '_ensureLocalPerspective3' }, function(err, lca) {
            if (err) { return cb2(err); }
            if (!lca) {
              error = new Error('lca with local perspective not found');
              if (!that._hide) { console.error(that.databaseName, that.collectionName, '_ensureLocalPerspective 3', error + description); }
              return cb2(error);
            }

            if (that.debug) { console.log(that.databaseName, that.collectionName, '_ensureLocalPerspective 3 fetched lca', JSON.stringify(lca._id)); }

            // create a perspective bound item by merged fast-forward
            newThis._merge(item, lca, function(err, merged) {
              if (err) {
                if (!that._hide) { console.error(that.databaseName, that.collectionName, '_ensureLocalPerspective 4 merge', err); }
                return cb2(err);
              }

              if (!merged[1]._id._v) {
                error = new Error('new version created while expecting fast-forward by merge');
                if (!that._hide) { console.error(that.databaseName, that.collectionName, '_ensureLocalPerspective 4', error, JSON.stringify(merged)); }
                return cb2(error);
              }

              if (merged[1]._m3) {
                if (that.debug) {
                  console.log(that.databaseName, that.collectionName, '_ensureLocalPerspective 4 local perspective already exists', JSON.stringify(merged[1]));
                }
                cb2();
                return;
              }

              // restore _m3
              item._m3._ack = false;
              item._m3._op = new Timestamp(0, 0);

              merged[1]._m3 = {
                _ack: false,
                _op: new Timestamp(0, 0)
              };

              if (that.debug) {
                console.log(that.databaseName, that.collectionName, '_ensureLocalPerspective 4 merge created', JSON.stringify(merged[1]));
              }

              // queue for insertion
              newLocalItems.push(merged[1]);
              newItems.push(merged[1]);
              cb2();
            });
          });
        });
      });
    });
  }, function(err) {
    if (err) {
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_ensureLocalPerspective', err); }
      return cb(err);
    }

    cb(null, newLocalItems);
  });
};

/**
 * Merge two versions using a recursive three-way merge strategy.
 * 1. find the lowest common ancestor(s) (by perspective)
 *    if there are two lca's, recurse
 * 2. do a three-way-merge of the two versions with the lca
 *
 * Note: if perspectives of the given items are different, two merged items will
 * be created where the first merge matches the perspective of objX and the 
 * second merge matches the perspective of objY.
 * If a merged item has a ._m3 and ._id._v property, it's a clean fast-forward
 * to an item that is already saved. If it has a ._id._v but no ._m3 it's a
 * replayed fast-forward that is not saved yet. And if it misses both it's a
 * genuine merge that's not saved either.
 *
 * TODO: use memoization for recursively generated virtual merges
 *
 * @param {Object} objX  item version x
 * @param {Object} objY  item version y
 * @param {Function} cb  first parameter will be an Error object or null, second
 *                       parameter will be an array with merged items on success
 *                       or a debug object on error. Third parameter will be an 
 *                       Array of perspectives.
 */
VersionedCollection.prototype._merge = function _merge(objX, objY, cb) {
  if (this.debug) { console.log(this.databaseName, this.collectionName, '_merge objX', JSON.stringify(objX._id), 'objY', JSON.stringify(objY._id)); }

  try {
    if (!VersionedCollection.equalValues(objX._id._id, objY._id._id)) { throw new Error('merge id mismatch'); }
  } catch(err) {
    if (!this._hide) { console.error(this.databaseName, this.collectionName, '_merge', err, JSON.stringify(objX._id._id), JSON.stringify(objY._id._id)); }
    process.nextTick(function() {
      cb(err, null);
    });
    return;
  }

  // do not alter original objects, create shallow clones of the objects, exclude _m3
  var itemX = {}, itemY = {};
  Object.keys(objX).forEach(function(key) { itemX[key] = objX[key]; });
  Object.keys(objY).forEach(function(key) { itemY[key] = objY[key]; });
  // create a shallow clone of _id
  if (typeof objX._id === 'object') {
    itemX._id = {};
    Object.keys(objX._id).forEach(function(key) { itemX._id[key] = objX._id[key]; });
  }
  if (typeof objY._id === 'object') {
    itemY._id = {};
    Object.keys(objY._id).forEach(function(key) { itemY._id[key] = objY._id[key]; });
  }

  // ignore any _m3
  delete itemX._m3;
  delete itemY._m3;

  var debugObj = { objX: objX, objY: objY };

  var that = this;

  /**
   * Merge two items:
   * 1. if more than one lca per perspective is found, recurse
   * 2. given one lca per perspective is found:
   *   - case of one perspective:
   *    * if both versions are equal, fast-forward with one item  
   *    * if the lca version equals the version of one of the items, fast-forward to the other item
   *    * in all other cases create one merged item
   *   - case of two perspectives:
   *    * if both versions are equal, fast-forward with both items
   *    * if the lca version equals the version of one of the items, fast-forward to the other item
   *      with one fast-forwarded item per perspective (by recreating one fast-forward)
   *    * in all other cases create two merged items (one per perspective)
   */
  function done(err, nlcas, perspectives) {
    /* jshint maxcomplexity: 35 */ /* might need some refactoring */
    if (err) {
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_merge err', err, nlcas, perspectives, debugObj); }
      cb(err);
      return;
    }
    if (!nlcas.length) {
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_merge no lca found', debugObj); }
      cb(new Error('no lca found'));
      return;
    }
    if (!perspectives.length) {
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_merge no perspectives', debugObj); }
      cb(new Error('no perspectives'));
      return;
    }

    // prevent side-effects, shallow clone lca and _id, rm _m3
    var lcas = [];
    nlcas.forEach(function(lca) {
      var obj = {};
      Object.keys(lca).forEach(function(key) {
        obj[key] = lca[key];
      });
      obj._id = {};
      Object.keys(lca._id).forEach(function(key) {
        obj._id[key] = lca._id[key];
      });
      delete obj._m3;
      lcas.push(obj);
    });

    var effectiveNumberOfLCAs = lcas.length / perspectives.length;

    function createFunc(j) {
      return function(merged, pe, callback) {
        // on first iteration async calls with callback as the only (and first) parameter
        // in subsequent calls the callback parameter is preceded by the parameters from the
        // callback call at the end of _merge(), which are "err", "merged" and "perspectives".
        // err is handled differently by async and not passed to the waterfall functions.

        // maintain original perspective order
        var first = lcas[j * perspectives.length];
        var second = lcas[j * perspectives.length + perspectives.length];

        // if we have a virtual merge, use it as an lca for merge with the next lca
        if (callback) {
          first = merged[0];
          if (itemX._id._pe !== first._id._pe) {
            first = merged[1];
          }
        } else {
          // first call
          callback = merged;

          if (itemX._id._pe !== first._id._pe) {
            first = lcas[j * perspectives.length + 1];
          }
        }

        if (itemY._id._pe !== second._id._pe) {
          second = lcas[j * perspectives.length + perspectives.length + 1];
        }
        that._merge(first, second, callback);
      };
    }

    // if there is one effective lca, fast-forward or merge
    // otherwise recurse with both lca versions
    if (effectiveNumberOfLCAs === 1) {
      // first reset _id to prevent incorrect conflicts on any future merge
      // save a reference of the id's in case a fast-forward by merge is done.
      var lcaIds = [];
      lcas.forEach(function(lca, i) {
        lcaIds[i] = lca._id;
        lca._id = lca._id._id;
        // make sure _m3 is never set
        delete lca._m3;
      });

      var itemXid = itemX._id;
      itemX._id = itemX._id._id;
      // make sure _m3 is never set
      delete itemX._m3;

      var itemYid = itemY._id;
      itemY._id = itemY._id._id;
      // make sure _m3 is never set
      delete itemY._m3;

      var merge;

      // case of one perspective
      if (perspectives.length === 1) {
        // if versions are equal, fast-forward with original item
        if (itemXid._v && itemXid._v === itemYid._v) { cb(null, [objX], perspectives); return; }

        // if lca equals one item, fast-forward to the other item
        if (lcaIds[0]._v && lcaIds[0]._v === itemXid._v) { cb(null, [objY], perspectives); return; } // ff to original objY
        if (lcaIds[0]._v && lcaIds[0]._v === itemYid._v) { cb(null, [objX], perspectives); return; } // ff to original objX

        // merge
        merge = VersionedCollection._threeWayMerge(itemX, itemY, lcas[0]);

        if (Array.isArray(merge)) {
          if (!that._hide) { console.error(that.databaseName, that.collectionName, '_merge error', merge, 'itemX', JSON.stringify(itemXid), 'itemY', JSON.stringify(itemYid)); }
          cb(new Error('merge conflict'), merge, perspectives);
          return;
        }

        // create new _id without _v
        merge._id = {
          _co: that.collectionName,
          _id: merge._id,
          _v: null,
          _pe: itemXid._pe,
          _pa: [],
          _lo: true
        };

        // only set _id._d if both versions have it
        if (itemXid._d && itemYid._d) { merge._id._d = true; }

        // use item version's as parent
        // If there is no version, this is a virtual merge and we are in the process of recursively creating one
        // virtual lca out of multiple lca's. The current algorithm adds the intermediate virtual merge in itemX and an
        // lca from the database to merge with as itemY, hence we only have to check on itemX if it's virtual or not.
        if (itemXid._v) {
          merge._id._pa.push(itemXid._v);
        } else {
          Array.prototype.push.apply(merge._id._pa, itemXid._pa);
        }
        merge._id._pa.push(itemYid._v);

        cb(null, [merge], perspectives);
        return;
      }

      // case of two perspectives
      if (perspectives.length === 2) {
        var merged;

        // if versions are equal, fast-forward with original items
        if (itemXid._v && itemXid._v === itemYid._v) { cb(null, [objX, objY], perspectives); return; }

        // if lca equals one item, fast-forward to the other item and create a merged fast-forward for the missing perspective
        if (lcaIds[0]._v && lcaIds[0]._v === itemXid._v) {
          // ff to original objY and recreate objY from the other perspective
          if (objY._id._pe === lcaIds[0]._pe) {
            merge = VersionedCollection._threeWayMerge(itemX, itemY, lcas[1], lcas[0]);
          } else {
            merge = VersionedCollection._threeWayMerge(itemX, itemY, lcas[0], lcas[1]);
          }

          merged = [merge, objY];

          if (Array.isArray(merged[0])) {
            if (!that._hide) { console.error(that.databaseName, that.collectionName, '_merge error', merged, 'itemX', JSON.stringify(itemXid), 'itemY', JSON.stringify(itemYid)); }
            cb(new Error('merge conflict'), merged, perspectives);
            return;
          }

          // set existing _id of itemY, with perspective of X
          merge._id = itemYid;
          merge._id._pe = itemXid._pe;

          cb(null, merged, perspectives);
          return;
        }
        if (lcaIds[0]._v && lcaIds[0]._v === itemYid._v) {
          // ff to original objX and recreate objX from the other perspective
          if (objX._id._pe === lcaIds[0]._pe) {
            merge = VersionedCollection._threeWayMerge(itemY, itemX, lcas[1], lcas[0]);
          } else {
            merge = VersionedCollection._threeWayMerge(itemY, itemX, lcas[0], lcas[1]);
          }

          merged = [objX, merge];

          if (Array.isArray(merge)) {
            if (!that._hide) { console.error(that.databaseName, that.collectionName, '_merge error', merged, 'itemX', JSON.stringify(itemXid), 'itemY', JSON.stringify(itemYid)); }
            cb(new Error('merge conflict'), merged, perspectives);
            return;
          }

          // set existing _id of itemX, with perspective of Y
          merge._id = itemXid;
          merge._id._pe = itemYid._pe;

          cb(null, merged, perspectives);
          return;
        }

        // merge from both perspectives
        merged = [];
        if (itemXid._pe === lcaIds[0]._pe) {
          merged.push(VersionedCollection._threeWayMerge(itemX, itemY, lcas[0], lcas[1]));
          merged.push(VersionedCollection._threeWayMerge(itemY, itemX, lcas[1], lcas[0]));
        } else {
          merged.push(VersionedCollection._threeWayMerge(itemX, itemY, lcas[1], lcas[0]));
          merged.push(VersionedCollection._threeWayMerge(itemY, itemX, lcas[0], lcas[1]));
        }

        if (Array.isArray(merged[0]) || Array.isArray(merged[1])) {
          if (!that._hide) { console.error(that.databaseName, that.collectionName, '_merge error', merged, 'itemX', JSON.stringify(itemXid), 'itemY', JSON.stringify(itemYid)); }
          cb(new Error('merge conflict'), merged, perspectives);
          return;
        }

        // create new _id without _v
        merged[0]._id = {
          _co: that.collectionName,
          _id: merged[0]._id,
          _v: null,
          _pe: itemXid._pe,
          _pa: [],
          _lo: true
        };

        merged[1]._id = {
          _co: that.collectionName,
          _id: merged[1]._id,
          _v: null,
          _pe: itemYid._pe,
          _pa: [],
          _lo: true
        };

        // only set _id._d if both versions have it
        if (itemXid._d && itemYid._d) {
          merged[0]._id._d = true;
          merged[1]._id._d = true;
        }

        // use item version's as parent
        // If there is no version, that is a virtual merge and we are in the process of recursively creating one
        // virtual lca out of multiple lca's. The current algorithm adds the intermediate virtual merge in itemX and an
        // lca from the database to merge with as itemY, hence we only have to check on itemX if it's virtual or not.
        // and if so, only add it to merged[0] (corresponding to itemX) of our result.
        if (itemXid._v) {
          merged[0]._id._pa.push(itemXid._v);
          merged[1]._id._pa.push(itemXid._v);
        } else {
          Array.prototype.push.apply(merged[0]._id._pa, itemXid._pa);
        }
        merged[0]._id._pa.push(itemYid._v);
        merged[1]._id._pa.push(itemYid._v);

        cb(null, merged, perspectives);
        return;
      }
    } else {
      // create one virtual lca by combining all lca's into one merge
      // recurse with all lca's, two at a time, adding the next to it till all are done
      // and one big lca is created for use in the next merge step

      // sort lcas by version and perspective
      VersionedCollection._sortByVersionAndPerspective(lcas);

      var tasks = [];
      for (var i = 0; i < lcas.length / perspectives.length -1; i++) {
        tasks.push(createFunc(i));
      }
      async.waterfall(tasks, done);
    }
  }

  var perspectives = [itemX._id._pe];
  if (itemX._id._pe !== itemY._id._pe) {
    perspectives.push(itemY._id._pe);
  }

  // find lca(s) and resolve versions to items
  that._findLCAs(itemX, itemY, function(err, lcas) {
    debugObj.lcas = lcas;
    if (err) {
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_merge error', err, debugObj); }
      cb(err, debugObj);
      return;
    }
    if (lcas.length < 1) {
      if (!that._hide) { console.error(that.databaseName, that.collectionName, '_merge no lca found', JSON.stringify(debugObj), perspectives); }
      cb(new Error('no lca found'), debugObj);
      return;
    }

    // fetch lca's, with bound perspectives
    var selector = { '_id._id': objX._id._id, '_id._v': { $in: lcas }, '_id._pe': { $in: perspectives }};
    that._snapshotCollection.find(selector, { comment: '_merge' }).toArray(function(err, lcaPerspectives) {
      debugObj.lcaPerspectives = lcaPerspectives;
      if (err) { cb(err, debugObj, perspectives); return; }
      if (lcaPerspectives.length !== lcas.length * perspectives.length) {
        if (!that._hide) {
          console.error(that.databaseName, that.collectionName,
            '_merge error when fetching perspective bound lca\'s',
            lcaPerspectives.length, JSON.stringify(lcaPerspectives),
            lcas.length, JSON.stringify(lcas),
            perspectives.length, JSON.stringify(perspectives));
        }
        cb(new Error('error when fetching perspective bound lca\'s'), debugObj, perspectives);
        return;
      }

      return done(null, lcaPerspectives, perspectives);
    });
  });
};

/**
 * Sort an array of items by version and then perspective.
 *
 * @param {Array} items  list of items to sort
 */
VersionedCollection._sortByVersionAndPerspective = function _sortByVersionAndPerspective(items) {
  items.sort(function(a, b) {
    return '' + a._id._v + a._id._pe > ''+ b._id._v + b._id._pe;
  });
};

/**
 * Apply oplog item on the last saved document and save it to the snapshotCollection
 *
 * @param {Object} oplogItem  item from the oplog
 * @param {Function} cb  On error the first parameter will be the Error object and
 *                       the second parameter will be the original document. On 
 *                       success the first parameter will be null and the second
 *                       parameter will be the new version of the document.
 */
VersionedCollection.prototype._applyOplogItem = function _applyOplogItem(oplogItem, cb) {
  if (typeof cb !== 'function') { throw new Error('cb must be a function'); }

  try {
    if (!oplogItem.o) { throw new Error('missing oplogItem.o'); }
  } catch(err) {
    process.nextTick(function() {
      cb(err, oplogItem);
    });
    return;
  }

  // determine the type of operator
  var operator = oplogItem.op;

  // if updating in non-modifier mode (by full document), do an update insert.
  if (operator === 'u' && !VersionedCollection.oplogUpdateContainsModifier(oplogItem)) {
    // ensure _id
    try {
      if (!oplogItem.o2._id) { throw new Error('missing oplogItem.o2._id'); }
    } catch(err) {
      process.nextTick(function() {
        cb(err, oplogItem);
      });
      return;
    }
    oplogItem.o._id = oplogItem.o2._id;
    operator = 'uf';
  }

  switch (operator) {
  case 'i':
    this._applyOplogInsertItem(oplogItem, cb);
    break;
  case 'uf':
    this._applyOplogUpdateFullDoc(oplogItem, cb);
    break;
  case 'u':
    this._applyOplogUpdateModifier(oplogItem, cb);
    break;
  case 'd':
    this._applyOplogDeleteItem(oplogItem, cb);
    break;
  default:
    process.nextTick(function() {
      cb(new Error('unsupported operator: ' + operator), oplogItem);
    });
    return;
  }
};

/**
 * Insert a new root element into the DAG. The root element can be inserted in the
 * collection first (locally created), in which case it might still need a new
 * version, or in the DAG first (from a remote) and then in the collection.
 *
 * @param {Object} oplogItem  the item from the oplog.
 * @param {Function} cb  On error the first parameter will be the Error object and
 *                       the second parameter will be the original document. On 
 *                       success the first parameter will be null and the second
 *                       parameter will be the saved versioned document.
 */
VersionedCollection.prototype._applyOplogInsertItem = function _applyOplogInsertItem(oplogItem, cb) {
  if (this.debug) { console.log(this.databaseName, this.collectionName, '_applyOplogInsertItem', JSON.stringify(oplogItem)); }
  this._applyOplogUpdateFullDoc(oplogItem, cb);
};

/**
 * Update an existing version of a document by applying an oplog update item with
 * full doc. Either insert a new document in the DAG, or set _m3._ack to true, if
 * the oplog item exactly matches a document in the DAG.
 *
 * @param {Object} oplogItem  the update item from the oplog.
 * @param {Function} cb  On error the first parameter will be the Error object and
 *                       the second parameter will be the original document. On 
 *                       success the first parameter will be null and the second
 *                       parameter will be the new version of the document.
 */
VersionedCollection.prototype._applyOplogUpdateFullDoc = function _applyOplogUpdateFullDoc(oplogItem, cb) {
  if (this.debug) { console.log(this.databaseName, this.collectionName, '_applyOplogUpdateFullDoc', JSON.stringify(oplogItem)); }

  if (typeof cb !== 'function') { throw new Error('cb must be a function'); }

  try {
    if (oplogItem.op !== 'u' && oplogItem.op !== 'i') { throw new Error('oplogItem.op must be "u" or "i"'); }
    if (!oplogItem.o._id) { throw new Error('missing oplogItem.o._id'); }
  } catch(err) {
    process.nextTick(function() {
      cb(err, oplogItem);
    });
    return;
  }

  // note: this skip logic should correspond with the way _syncDAGItemWithCollection operates.
  // if matches by full doc including version, set ack true
  // else create a new version

  // find out if this item is already in the DAG or not
  var that = this;
  var error;

  var selector = { '_id._id': oplogItem.o._id, '_id._v': oplogItem.o[that.versionKey], '_id._pe': that.localPerspective };
  if (that.debug) { console.log(that.databaseName, that.collectionName, '_applyOplogUpdateFullDoc selector', JSON.stringify(selector)); }
  this._snapshotCollection.findOne(selector, { comment: '_applyOplogUpdateFullDoc' }, function(err, item) {
    if (err) { return cb(err, oplogItem); }

    if (that.debug) {
      console.log(that.databaseName, that.collectionName, '_applyOplogUpdateFullDoc comparing', JSON.stringify(oplogItem.o), JSON.stringify(item));
    }
    if (item && that.compareDAGItemWithCollectionItem(item, oplogItem.o)) {
      if (that.debug) { console.log(that.databaseName, that.collectionName, '_applyOplogUpdateFullDoc, set ackd', JSON.stringify(item._id)); }

      // it should not be ackd
      if (item._m3._ack) { console.log(that.databaseName, that.collectionName, '_applyOplogUpdateFullDoc item already ackd', JSON.stringify(item)); }

      // set ackd
      that._setAckd(item._id._id, item._id._v, item._id._pe, oplogItem.ts, function(err) {
        if (err) {
          if (!that._hide) { console.error(that.databaseName, that.collectionName, '_applyOplogUpdateFullDoc set ackd err', err); }
          cb(err, oplogItem);
          return;
        }

        item._m3._ack = true;
        item._m3._op = oplogItem.ts;
        cb(null, item);
      });
    } else {
      if (that.debug) { console.log(that.databaseName, that.collectionName, '_applyOplogUpdateFullDoc create new version', JSON.stringify(oplogItem)); }

      // find parent, which is the last ackd or locally created item
      that._findLastAckdOrLocallyCreated(oplogItem.o._id, function(err, item2) {
        if (err) { return cb(err, oplogItem); }

        if (oplogItem.op === 'i' && !item2) {
          if (that.debug) { console.log(that.databaseName, that.collectionName, '_applyOplogUpdateFullDoc create new version', JSON.stringify(oplogItem)); }

          // version the newly inserted document and copy to collection
          that.saveCollectionItem(oplogItem.o, [], oplogItem, function(err, newObj) {
            if (err) { return cb(err, oplogItem); }
            cb(null, newObj);
          });
          return;
        }

        if (oplogItem.op === 'u' && !item2) {
          error = new Error('previous version of item not found');
          if (!that._hide) { console.error(that.databaseName, that.collectionName, '_applyOplogUpdateFullDoc', error); }
          return cb(error, oplogItem);
        }

        // only connect new roots if previous item is a deletion
        if (oplogItem.op === 'i' && item2 && !item2._id._d) {
          error = new Error('previous version of item not a deletion');
          if (!that._hide) { console.error(that.databaseName, that.collectionName, '_applyOplogUpdateFullDoc', error); }
          return cb(error, oplogItem);
        }

        var newObj = that.versionDoc(oplogItem.o);
        // link to the found parent
        newObj._id._pa = [item2._id._v];

        newObj._m3 = { _ack: false, _op: oplogItem.ts };

        // and create a merge if needed
        that._addAllToDAG([{ item: newObj }], function(err) {
          if (err) { return cb(err); }
          cb(null, newObj);
        });
      });
    }
  });
};

/**
 * Update an existing version of a document by applying an oplog update item.  
 *
 * Every mongodb update modifier is supported since the update operation is executed
 * by the database engine in a temporary collection.
 *
 * @param {Object} oplogItem  the update item from the oplog.
 * @param {Function} cb  On error the first parameter will be the Error object and
 *                       the second parameter will be the original document. On 
 *                       success the first parameter will be null and the second
 *                       parameter will be the new version of the document.
 */
VersionedCollection.prototype._applyOplogUpdateModifier = function _applyOplogUpdateModifier(oplogItem, cb) {
  if (this.debug) { console.log(this.databaseName, this.collectionName, '_applyOplogUpdateModifier', JSON.stringify(oplogItem)); }

  if (typeof cb !== 'function') { throw new Error('cb must be a function'); }

  // copy the parent of this item from the DAG to a temporary collection
  // update it there and insert it back into the DAG, we cannot update inplace because the collection is capped.

  // find the last ackd version by id and perspective
  var that = this;
  that._findLastAckdOrLocallyCreated(oplogItem.o2._id, function(err, head) {
    if (err) { return cb(err, oplogItem); }
    if (!head) { return cb(new Error('previous version of doc not found'), oplogItem); }

    that.createNewVersionByUpdateDoc(head, oplogItem, function(err, newObj) {
      if (err) { return cb(err, oplogItem); }

      // and create a merge if needed
      that._addAllToDAG([{ item: newObj }], function(err) {
        if (err) { return cb(err); }
        cb(err, newObj);
      });
    });
  });
};

/**
 * Save a new document with only the _id of the doc, _d: true and a reference to
 * it's parent.
 *
 * @param {Object} oplogItem  the delete item from the oplog.
 * @param {Function} cb  On error the first parameter will be the Error object and
 *                       the second parameter will be the original document. On 
 *                       success the first parameter will be null and the second
 *                       parameter will be the new version of the document.
 */
VersionedCollection.prototype._applyOplogDeleteItem = function _applyOplogDeleteItem(oplogItem, cb) {
  if (this.debug) { console.log(this.databaseName, this.collectionName, '_applyOplogDeleteItem', JSON.stringify(oplogItem)); }

  if (typeof cb !== 'function') { throw new Error('cb must be a function'); }

  try {
    if (oplogItem.op !== 'd') { throw new Error('oplogItem.op must be "d"'); }
    if (!oplogItem.o._id) { throw new Error('missing oplogItem.o._id'); }
  } catch(err) {
    process.nextTick(function() {
      cb(err, oplogItem);
    });
    return;
  }

  // find the parent by id and perspective
  var that = this;
  that._findLastAckdOrLocallyCreated(oplogItem.o._id, function(err, p) {
    if (err) { return cb(err, oplogItem); }
    if (!p) { return cb(new Error('previous version of doc not found'), oplogItem); }

    // set _d attribute for this doc and save, the whole doc is needed for export filters
    var oldId = p._id;
    p._id = oplogItem.o._id;
    var newObj = that.versionDoc(p);
    newObj._id._pa = [oldId._v];
    newObj._id._d = true;
    newObj._m3 = { _ack: true, _op: oplogItem.ts };

    // see if a merge is needed
    // and create a merge if needed
    that._addAllToDAG([{ item: newObj }], function(err) {
      if (err) { return cb(err); }
      cb(err, newObj);
    });
  });
};

/**
 * Check if given oplog item has the following attributes:
 * - has "o", "ts", "ns" and "op" properties. 
 * - "op" is one of "i", "u" or "d".
 *
 * @param {Object} data  object that needs to be tested
 * @return {String} empty string if nothing is wrong or a problem description
 */
VersionedCollection.invalidOplogItem = function invalidOplogItem(item) {
  // check if all fields are present
  if (!item)    { return 'missing item'; }
  if (!item.o)  { return 'missing item.o'; }
  if (!item.ts) { return 'missing item.ts'; }
  if (!item.ns) { return 'missing item.ns'; }
  if (!item.op) { return 'missing item.op'; }

  // ignore if operation is not "i", "u" or "d"
  if (item.op !== 'i' && item.op !== 'u' && item.op !== 'd') { return 'invalid item.op'; }

  return '';
};

/**
 * Find the last ackd or locally created version of a certain _id._id, whichever
 * is newer.
 *
 * Note: this function relies on the property that oplog items and remote items are
 * not inserted simultaneously. Furthermore _m3._ack should only be set true on
 * items that have been confirmed by the oplog to be copied to the collection.
 * Note2: relies for correct lookup on the index set by _createSnapshotCollection
 *
 * @param {Object} oplogItem  the locally created item from the oplog to find the
 *                            parent for
 * @param {Function} cb  The first parameter will contain either an Error object or
 *                       null. The second parameter the found parent or null.
 */
VersionedCollection.prototype._findLastAckdOrLocallyCreated = function _findLastAckdOrLocallyCreated(id, cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var selector = {
    '_id._id': id,
    '_id._pe': this.localPerspective,
    '$or': [
      { '_id._lo': true },
      { '_m3._ack': true }
    ]
  };

  if (this.debug) { console.log(this.databaseName, this.collectionName, '_findLastAckdOrLocallyCreated selector', JSON.stringify(selector)); }

  var that = this;
  // selector and sort combination should use index created by _createSnapshotCollection
  this._snapshotCollection.findOne(selector, { sort: { '_id._i': -1 }, comment: '_findLastAckdOrLocallyCreated' }, function(err, lastHead) {
    if (err) { return cb(err); }
    if (that.debug) { console.log(that.databaseName, that.collectionName, '_findLastAckdOrLocallyCreated lastHead', JSON.stringify(lastHead)); }

    cb(null, lastHead);
  });
};

/**
 * Generate a random byte string.
 *
 * By default generates a 48 bit base64 queue id (string of 8 characters)
 *
 * @param {Number, default: 6} [size]  number of random bytes te generate
 * @return {String} the random bytes encoded in base64
 */
VersionedCollection._generateRandomVersion = function _generateRandomVersion(size) {
  var data = crypto.pseudoRandomBytes(size || 6);
  return data.toString('base64');
};
