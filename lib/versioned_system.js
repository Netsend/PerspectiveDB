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

var async = require('async');
var mongodb = require('mongodb');
var Timestamp = mongodb.Timestamp;
var EE = require('events').EventEmitter;
var util = require('util');

var VersionedCollection = require('./versioned_collection');
var OplogResolver = require('./oplog_resolver');
var Replicator = require('./replicator');

/**
 * VersionedSystem
 *
 * Verification and tracking of the oplog into versioned collections. Ensures
 * every versioned collection has a snapshot collection and is auto merged.
 *
 * @param {mongodb.Db} oplogDb  authorized connection to the database that
 *                              contains the oplog.
 * @param {Object} [options]  additional options
 *
 * Options
 * - **oplogCollectionName** {String, default: "oplog.$main"}, name of the
 *     collection to read from. use "oplog.rs" if running a replica set.
 * - **localDbName** {String, default: "local"}, the database name of the local
 *     database to use for storing the local cache, one per database server.
 * - **databaseNames** {Array}, the names of the databases to track
 * - **debug** {Boolean, default: false}  whether to do extra console logging or not
 * - **hide** {Boolean, default: false}  whether to suppress errors or not (used
 *                                       in tests)
 * - **proceedOnError** {Boolean, default false}  whether to proceed on error
 * - **haltOnMergeConflict** {Boolean, default true}  whether to halt if merge
 *                                                conflicts arise.
 * - **collectionNames** {Array}, the names of the collections to track in each
 *     database.
 * - **localStorageName** {String, default: "m3"}, the collection name in the
 *     local database to use for storing the last read item from the oplog.
 * - **remotes** {Array}  list of remote names to track
 * - **snapshotSize** {Number, default: 10}  default size of snapshot collections
 *                                           in mega bytes
 * - **snapshotSizes** {Object}  size of snapshot collections by database and
 *                               collection name in mega bytes
 * - **replicate** {Object}, replication configuration
 * - **autoProcessInterval** {Number, default 2000}  interval to process vc's
 * - **tailableRetryInterval** {Number, default 1500}  interval to tail the oplog
 */
function VersionedSystem(oplogDb, databaseNames, collectionNames, options) {
  /* jshint maxcomplexity: 18 */ /* lot's of variable setup */

  if (!oplogDb) { throw new Error('provide oplogDb'); }
  if (!(oplogDb instanceof mongodb.Db)) { throw new TypeError('oplogDb must be a mongdb.Db'); }
  this._oplogDb = oplogDb;

  if (!Array.isArray(databaseNames)) { throw new TypeError('databaseNames must be an array'); }
  if (!databaseNames.length) { throw new TypeError('databaseNames must not be empty'); }

  if (!Array.isArray(collectionNames)) { throw new TypeError('collectionNames must be an array'); }
  if (!collectionNames.length) { throw new TypeError('collectionNames must not be empty'); }

  EE.call(this);

  options = options || {};

  this._options = options;
  this._oplogCollectionName = options.oplogCollectionName || 'oplog.$main';
  this._oplogCollection = this._oplogDb.collection(this._oplogCollectionName);
  this._localDbName = options.localDbName || 'local';
  this._localStorageName = options.localStorageName || 'm3';
  this._snapshotSize = options.snapshotSize || 10;
  this._snapshotSizes = options.snapshotSizes || {};
  this._autoProcessInterval = options.autoProcessInterval || 2000;
  this._tailableRetryInterval = options.tailableRetryInterval || 1500;

  this._replicate = options.replicate || {};
  if (typeof this._replicate !== 'object' || Array.isArray(this._replicate)) {
    throw new TypeError('options.replicate must be an object');
  }

  this._localStorageCollection = this._oplogDb.db(this._localDbName).collection(this._localStorageName);

  this.debug = !!this._options.debug;
  this._hide = !!this._options.hide;

  this._databaseNames = databaseNames;
  this._collectionNames = collectionNames;

  this._vcToVcTailStreams = {};

  // open database connections and setup versioned databases
  this._databaseCollections = {};
  var that = this;
  databaseNames.forEach(function(databaseName) {
    collectionNames.forEach(function(collectionName) {
      // create references of db name + collection name by key for easy lookup of oplog items
      var key = databaseName+'.'+collectionName;
      var vc = new VersionedCollection(that._oplogDb.db(databaseName), collectionName, {
        proceedOnError: options.proceedOnError,
        haltOnMergeConflict: options.haltOnMergeConflict,
        remotes: options.remotes,
        debug: that.debug,
        hide: that._hide
      });
      that._databaseCollections[key] = vc;
    });
  });
}

util.inherits(VersionedSystem, EE);
module.exports = VersionedSystem;

/**
 * Return stats of all collections.
 *
 * @param {Boolean} [extended, default false]  whether to add _m3._ack counts
 * @param {Function} cb  The first parameter will contain either an Error object or
 *                       null. The second parameter is an object with collection
 *                       info.
 *
 * extended object:
 *   ack {Number}  the number of documents where _m3._ack = true
 */
VersionedSystem.prototype.info = function info(extended, cb) {
  if (typeof extended === 'function') {
    cb = extended;
    extended = false;
  }

  if (typeof extended !== 'boolean') { throw new TypeError('extended must be a boolean'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  var result = {};
  async.each(Object.keys(this._databaseCollections), function(key, cb2) {
    that._databaseCollections[key]._collection.stats(function(err, resultCollection) {
      if (err) {
        if (err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) {
          cb2(err);
          return;
        }
        resultCollection = {};
      }

      result[key] = { collection: resultCollection };

      that._databaseCollections[key]._snapshotCollection.stats(function(err, resultSnapshotCollection) {
        if (err) {
          if (err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) {
            cb2(err);
            return;
          }
          resultSnapshotCollection = {};
        }


        result[key].snapshotCollection = resultSnapshotCollection;

        if (extended) {
          that._databaseCollections[key]._snapshotCollection.count({ '_m3._ack': true }, function(err, count) {
            if (err) { cb2(err); return; }

            result[key].extended = { ack: count };
            cb2();
          });
        } else {
          cb2();
        }
      });
    });
  }, function(err) {
    cb(err, result);
  });
};

/**
 * Copy versioned collections into another database.
 *
 * @param {Boolean} [db, default "dump"]  which database to copy to
 * @param {Function} cb  The first parameter will contain either an Error object or
 *                       null.
 */
 /*
VersionedSystem.prototype.dump = function dump(db, cb) {
  if (typeof db === 'function') {
    cb = db;
    db = 'dump';
  }

  if (typeof db !== 'string') { throw new TypeError('db must be a string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var dstDb = this._oplogDb.db(db);

  var that = this;
  async.eachSeries(Object.keys(this._databaseCollections), function(key, cb2) {
    var vc = that._databaseCollections[key];
    var coll = vc._snapshotCollection;
    var dstName = vc.databaseName + '.' + coll.collectionName;
    var dst = dstDb.collection(vc.databaseName + '.' + coll.collectionName);

    if (that.debug) { console.log('vs dump', vc.databaseName, coll.collectionName, 'into', db, dstName); }

    var stream = coll.find().stream();

    stream.on('data', function(item) {
      dst.insert(item, { w: 1 }, function(err) {
        if (err) { return cb2(err); }
      });
    });

    stream.on('error', cb2);
    stream.on('close', cb2);
  }, cb);
};
 */

/**
 * Check if the most recently used oplog item (if any) is still in the oplog. If no
 * last used oplog item is found and the oplog is empty, callback with true.
 *
 * @param {Function} cb  The first parameter will contain either an Error object or
 *                       null. The second parameter is a boolean indicating whether
 *                       the last used oplog item is still in the oplog or not.
 */
VersionedSystem.prototype.lastUsedOplogInOplog = function lastUsedOplogInOplog(cb) {
  var that = this;
  var error;

  this._getOldestOplogItem(function(err, oldestOplog) {
    if (err) { return cb(err, null); }

    if (!oldestOplog) {
      error = new Error('first oplog item not found');
      if (!that._hide) { console.error('vs lastUsedOplogInOplog', error); }
      process.nextTick(function() {
        cb(error);
      });
      return;
    }

    that._getLastUsedOplogItem(function(err, lastUsedOplog) {
      if (err) { return cb(err, null); }

      if (that.debug) { console.log('vs lastUsedOplogInOplog first oplog item', oldestOplog && oldestOplog.ts, 'last used', lastUsedOplog && lastUsedOplog.ts); }

      if (!lastUsedOplog) { return cb(null, true); }

      // verify with oplog, if lastUsedOplog.ts >= oldestOplog.ts everything is ok
      if (oldestOplog && lastUsedOplog.ts.greaterThanOrEqual(oldestOplog.ts)) {
        return cb(null, true);
      }

      cb(null, false);
    });
  });
};

/**
 * Determine the oplog item offset and start tailing the oplog.
 *
 * Versioned collections each have their own offset after which they should receive
 * oplog items.
 *
 * @param {Boolean} [follow, default true]  whether or not to follow the oplog
 * @param {Function} cb  This will be called as soon as the oplog tail is closed.
 *                       This happens on error or after calling stopTrack(). The
 *                       first parameter will contain either an Error object or
 *                       null.
 *
 * Events:
 *  trackOplog  emitted on each item that is sent to a versioned collection. first
 *    parameter is the item, second parameter is the versioned collection.
 *  trackOplogStartTail  emitted right after the offsets are determined and before the
 *    oplog tail is started. first parameter is the offset after which it starts.
 */
VersionedSystem.prototype.trackOplog = function trackOplog(follow, cb) {
  if (typeof follow === 'function') {
    cb = follow;
    follow = true;
  }
  if (typeof follow !== 'boolean') { throw new TypeError('follow must be a boolean'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  // determine all offsets
  that._determineOplogOffsets(function(err, offsets) {
    if (err) { return cb(err); }

    // use the last oplog items as default offset on empty versioned collections
    that._getLatestOplogItem(function(err, latestOplogItem) {
      if (err) { return cb(err); }

      // label the oplog item
      latestOplogItem = latestOplogItem || { ts: new Timestamp(0, (new Date()).getTime() / 1000) };

      Object.keys(offsets).forEach(function(key) {
        if (!offsets[key]) {
          if (that.debug) { console.log('vs trackOplog set default', key, latestOplogItem.ts); }
          offsets[key] = latestOplogItem.ts;
        }
      });

      var maxTs = offsets[that._localDbName + '.' + that._oplogCollectionName];

      that._trackOplog(follow, maxTs, offsets, cb);
    });
  });
};

/**
 * Start tailing the oplog after the given offset.
 *
 * Versioned collections each have their own offset after which they should receive
 * oplog items.
 *
 * Tracks last processed oplog item every 2 minutes.
 *
 * @param {Boolean} follow  whether or not to follow the oplog
 * @param {mongodb.Timestamp} oplogOffset  mongodb.Timestamp for the oplog offset
 * @param {Object} offsets  object containing one mongodb.Timestamp per versioned
 *                          colletion
 * @param {Number} [tailableRetryInterval]  overrule this.tailableRetryInterval
 * @param {Function} cb  This will be called as soon as the oplog tail is closed.
 *                       This happens on error or after calling stopTrack(). The
 *                       first parameter will contain either an Error object or
 *                       null.
 *
 * Events:
 *  trackOplog  emitted on each item that is sent to a versioned collection. first
 *    parameter is the item, second parameter is the versioned collection.
 *  trackOplogStartTail  emitted right after the offsets are determined and before the
 *    oplog tail is started. first parameter is the offset after which it starts.
 */
VersionedSystem.prototype._trackOplog = function _trackOplog(follow, oplogOffset, offsets, tailableRetryInterval, cb) {
  if (typeof tailableRetryInterval === 'function') {
    cb = tailableRetryInterval;
    tailableRetryInterval = this._tailableRetryInterval;
  }

  tailableRetryInterval = tailableRetryInterval || this._tailableRetryInterval;

  if (typeof follow !== 'boolean') { throw new TypeError('follow must be a boolean'); }
  if (typeof offsets !== 'object') { throw new TypeError('offsets must be an object'); }
  if (!(oplogOffset instanceof mongodb.Timestamp)) { throw new TypeError('oplogOffset must be a mongodb.Timestamp'); }
  if (typeof tailableRetryInterval !== 'number') { throw new TypeError('tailableRetryInterval must be a number'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  var error;

  if (this._trackOplogRunning) {
    error = new Error('trackOplog already running');
    if (!that._hide) { console.error('vs trackOplog', error); }
    process.nextTick(function() {
      cb(error);
    });
    return;
  }
  this._trackOplogRunning = true;

  var tailClosed = false;
  var running = 0;

  var lastUsedOplogItem, lastUsedCollectionOplogItem;

  var tasks = [];
  tasks.push(function(callback) {
    if (lastUsedOplogItem) {
      that._saveLastUsedOplogItem(lastUsedOplogItem, callback);
    } else {
      if (that.debug) { console.log('vs trackOplog NO lastUsedOplogItem'); }
      process.nextTick(callback);
    }
  });
  tasks.push(function(callback) {
    if (lastUsedCollectionOplogItem) {
      that._saveLastUsedCollectionOplogItem(lastUsedCollectionOplogItem, callback);
    } else {
      if (that.debug) { console.log('vs trackOplog NO lastUsedCollectionOplogItem'); }
      process.nextTick(callback);
    }
  });

  function finalize() {
    if (!tailClosed) { return; }
    if (running) { return; }

    if (follow) {
      if (that.debug) { console.log('vs trackOplog oplog tail closed while following, this should have been caused by stopTrack()'); }
    } else {
      if (that.debug) { console.log('vs trackOplog oplog tail closed'); }
    }
    async.series(tasks, function(err) {
      if (err) { return cb(err); }
      clearInterval(that._saveLastUsedOplogTimeout);
      that._trackOplogRunning = false;
      cb();
    });
    return;
  }

  // start saving the last used oplog item periodically
  // do this to prevent loosing track with the last oplog item (this can happen if non-verisoned collections are updated
  // more frequently)
  that._saveLastUsedOplogTimeout = setInterval(function() {
    if (lastUsedOplogItem) {
      that._saveLastUsedOplogItem(lastUsedOplogItem, function(err) {
        if (err) {
          console.error('error: vs trackOplog saving last used oplog item', err, JSON.stringify(lastUsedOplogItem));
        }
      });
    }
    if (lastUsedCollectionOplogItem) {
      that._saveLastUsedCollectionOplogItem(lastUsedCollectionOplogItem, function(err) {
        if (err) {
          console.error('error: vs trackOplog saving last used collection oplog item', err, JSON.stringify(lastUsedCollectionOplogItem));
        }
      });
    }
  }, 120000);

  // start tailing and distributing the oplog after the given offset
  // make sure the respective versioned collections offset is reached
  that._oplogTail = that._tailOplog(oplogOffset, follow, tailableRetryInterval, function(err, oplogItem, vc, next) {
    if (err) {
      if (!that._hide) { console.error('vs trackOplog', err); }
      return cb(err);
    }

    if (!oplogItem) {
      if (that.debug) { console.log('vs trackOplog no oplog tail, so assume tail closed'); }
      return;
    }

    if (!vc) {
      if (lastUsedOplogItem && lastUsedOplogItem.ts.greaterThan(oplogItem.ts)) {
        if (that.debug) { console.log('vs trackOplog SKIP lastUsedOplogItem', oplogItem.ts, lastUsedOplogItem.ts); }
      } else {
        lastUsedOplogItem = oplogItem;
      }
      return next();
    }

    // make sure the offset of the versioned collection is reached
    var offset = offsets[oplogItem.ns];
    if (offset) {
      if (offset.greaterThanOrEqual(oplogItem.ts)) {
        if (lastUsedOplogItem && lastUsedOplogItem.ts.greaterThan(oplogItem.ts)) {
          if (that.debug) { console.log('vs trackOplog SKIP lastUsedOplogItem', oplogItem.ts, lastUsedOplogItem.ts); }
        } else {
          lastUsedOplogItem = oplogItem;
        }
        return next();
      } else {
        if (that.debug) { console.log('vs trackOplog beyond offset', oplogItem.ns, offset, oplogItem.ts); }
        delete offsets[oplogItem.ns];
      }
    }

    // note: waiting on saveOplogItem before calling next makes it process only one item at a time, which is way too slow
    running++;
    vc.saveOplogItem(oplogItem, function(err, newItem) {
      running--;
      if (err) {
        console.error('vs trackOplog save oplog item error', err);
        return cb(error);
      }
      // save a copy of this item so this can be used as last oplog item
      if (lastUsedOplogItem && lastUsedOplogItem.ts.greaterThan(oplogItem.ts)) {
        if (that.debug) { console.log('vs trackOplog SKIP lastUsedOplogItem', oplogItem.ts, lastUsedOplogItem.ts); }
      } else {
        lastUsedOplogItem = oplogItem;
      }

      // save a copy of this item if it is a user initiated collection modification
      if (!newItem._m3._ack) {
        if (lastUsedCollectionOplogItem && lastUsedCollectionOplogItem.ts.greaterThan(oplogItem.ts)) {
          if (that.debug) { console.log('vs trackOplog SKIP lastUsedCollectionOplogItem', oplogItem.ts, lastUsedCollectionOplogItem.ts); }
        } else {
          lastUsedCollectionOplogItem = oplogItem;
          if (that.debug) { console.log('vs trackOplog lastUsedCollectionOplogItem', JSON.stringify(lastUsedCollectionOplogItem)); }
        }
      }

      if (that.debug) { console.log('vs trackOplog tail oplog saveOplogItem', JSON.stringify(oplogItem), running); }
      finalize();
    }, function(err) {
      if (err) { next(err); return; }
      that.emit('trackOplog', oplogItem, vc);
      next();
    });
  });

  that._oplogTail.on('close', function() {
    if (that.debug) { console.log('vs trackOplog tail oplog closing, finalize'); }
    tailClosed = true;
    that.emit('trackOplogTailClosed');
    finalize();
    return;
  });

  that.emit('trackOplogStartTail', oplogOffset);
};

/**
 * Stop tracking:
 *   - stop tailing the oplog
 *   - stop all vc2vc tails
 *
 * @param {Function} cb  first parameter will be an error object or null.
 */
VersionedSystem.prototype.stopTrack = function stopTrack(cb) {
  cb = cb || function() {};

  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  var tasks = [];

  if (this._oplogTail && this._oplogTail.readable) {
    tasks.push(function(cb2) {
      that._oplogTail.on('close', cb2);
      that._oplogTail.destroy();
    });
  }

  if (Object.keys(this._vcToVcTailStreams).length) {
    tasks.push(function(cb2) {
      // stop _vcToVcTailStreams
      async.each(Object.keys(that._vcToVcTailStreams), function(key, cb3) {
        var stream = that._vcToVcTailStreams[key];
        stream.destroy();
        process.nextTick(cb3);
      }, cb2);
    });
  }

  async.parallel(tasks, cb);
};

/**
 * Rebuild every versioned collection.
 *
 * Clears every versioned collection and restarts by versioning every document
 * currently in the collection.
 *
 * Tracks last processed oplog item.
 *
 * Notes:
 * * this destroys any previous versions and creates new versions of all
 *   versioned collections.
 * * No updates should occur on the collections, this is not enforced by
 *   this function.
 * * auto processing should be disabled
 *
 * @param {Function} cb  called with an Error object or null
 */
VersionedSystem.prototype.rebuild = function rebuild(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (this._autoProcessing) { throw new Error('stop auto processing before rebuilding'); }

  var that = this;
  var error;

  if (this._rebuildRunning) {
    error = new Error('rebuild already running');
    if (!this._hide) { console.error('vs rebuild', error); }
    process.nextTick(function() {
      cb(error);
    });
    return;
  }
  this._rebuildRunning = true;

  function finalize(err) {
    if (err) { cb(err); return; }
    that._stopAutoProcessing(function(err) {
      if (err) { cb(err); return; }
      that._rebuildRunning = false;
      cb(null);
    });
  }

  this._startAutoProcessing(100, function(err) {
    if (err) { finalize(err); return; }
  });

  // use the last oplog items as default offset on empty versioned collections
  that._getLatestOplogItem(function(err, latestOplogItem) {
    if (err) { return finalize(err); }

    latestOplogItem = latestOplogItem || { ts: new Timestamp(0, (new Date()).getTime() / 1000) };

    // set offsets to latest oplog item
    var offsets = {};
    Object.keys(that._databaseCollections).forEach(function(key) {
      offsets[key] = latestOplogItem.ts;
    });

    // clean up _removeLastUsedOplogItem and _removeLastUsedCollectionOplogItem
    that._removeLastUsedOplogItem(function(err) {
      if (err) { return finalize(err); }
      that._removeLastUsedCollectionOplogItem(function(err) {
        if (err) { return finalize(err); }

        // start rebuilding each versioned collection
        async.eachSeries(Object.keys(that._databaseCollections), function(databaseCollectionName, callback) {
          var vc = that._databaseCollections[databaseCollectionName];
          vc.rebuild((that._snapshotSizes[databaseCollectionName] || that._snapshotSize) * 1024 * 1024, callback);
        }, function(err) {
          if (err) { return finalize(err); }

          // tail (don't follow), starting from the oplog item we found before we started, with 100ms as tailableRetryInterval
          that._trackOplog(false, latestOplogItem.ts, offsets, 100, finalize);
        });
      });
    });
  });
};

/**
 * Make sure every versioned collection has a snapshot collection. Sizes are
 * determined by the snapshotSize and snapshotSizes options provided to the
 * constructor.
 *
 * @param {Function} cb  first parameter will be an error object or null.
 */
VersionedSystem.prototype.ensureSnapshotCollections = function ensureSnapshotCollections(cb) {
  var that = this;
  async.eachSeries(Object.keys(this._databaseCollections), function(key, cb2) {
    var vc = that._databaseCollections[key];
    vc.ensureSnapshotCollection((that._snapshotSizes[key] || that._snapshotSize) * 1024 * 1024, cb2);
  }, cb);
};

/**
 * Find out if each versioned collection is empty or not.
 *
 * @param {Function} cb  The first parameter will contain either an Error object or
 *                       null. The second parameter is a boolean indicating if
 *                       every versioned collection is empty.
 */
VersionedSystem.prototype.allVersionedCollectionsEmpty = function allVersionedCollectionsEmpty(cb) {
  var that = this;
  this._versionedCollectionCounts(function(err, counts) {
    if (err) { cb(err); return; }
    var empty = Object.keys(counts).every(function(key) {
      return !counts[key];
    });
    if (that.debug) { console.log('vs allVersionedCollectionsEmpty', empty); }
    cb(null, empty);
  });
};

/**
 * Fine the lowest timestamp in an array of timestamps.
 *
 * @param {Array} times  an array of mongodb.Timestamps
 * @return {mongodb.Timestamp|null} return the lowest timestamp or null
 */
VersionedSystem.minTimestamp = function minTimestamp(times) {
  if (!Array.isArray(times)) { throw new TypeError('times must be an array'); }

  var minTs = times.reduce(function(prev, curr) {
    var ts = curr || new Timestamp(0, 2147483647);
    return ts.greaterThan(prev) ? prev : ts;
  }, new Timestamp(0, 2147483647));

  if (minTs && minTs.equals(new Timestamp(0, 2147483647))) {
    minTs = null;
  }
  return minTs;
};

/**
 * Check if the system is still connected with the oplog and start tracking the
 * oplog and any versioned collections.
 *
 * @param {Boolean} [follow, default true]  whether or not to follow the oplog
 * @param {Function} cb  First parameter will be an error object or null. Called
 *                       after tracking stopped.
 */
VersionedSystem.prototype.start = function start(follow, cb) {
  if (typeof follow === 'function') {
    cb = follow;
    follow = true;
  }
  if (typeof follow !== 'boolean') { throw new TypeError('follow must be a boolean'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  var error;

  if (this._autoProcessing) { throw new Error('stop auto processing before starting'); }

  if (this._startRunning) {
    error = new Error('start already running');
    if (!this._hide) { console.error('vs start', error); }
    process.nextTick(function() {
      cb(error);
    });
    return;
  }
  this._startRunning = true;

  function finalize(err) {
    if (err) { cb(err); return; }
    that._stopAutoProcessing(function(err) {
      if (err) { cb(err); return; }
      that._startRunning = false;
      cb(null);
    });
  }

  function run() {
    that._startAutoProcessing(function(err) {
      if (err) { finalize(err); return; }
      that.trackOplogAndVCs(follow, function(err) {
        if (that.debug) { console.log('vs start trackOplogAndVCs returned'); }
        if (err) { finalize(err); return; }
        finalize();
      });
    });
  }

  // run tails if on track or all empty
  this.lastUsedOplogInOplog(function(err, onTrack) {
    if (err) { finalize(err); return; }
    if (onTrack) {
      run();
    } else {
      if (that.debug) { console.log('vs start not on track with oplog, checking if all vc\'s are empty'); }

      // if all versioned collections are empty, a new rebuild wil automatically start
      // otherwise an error is returned
      that.allVersionedCollectionsEmpty(function(err, empty) {
        if (err) { finalize(err); return; }

        if (!empty) {
          var error = new Error('not on track with oplog');
          finalize(error);
          return;
        }

        run();
      });
    }
  });
};

/**
 * Start tailing the oplog and versioned collections. Calls back if all tails are
 * closed or when an error occurred.
 *
 * @param {Boolean} [follow, default true]  whether or not to follow the oplog and
 *                                          vc's
 * @param {Function} cb  First parameter will be an error object or null.
 */
VersionedSystem.prototype.trackOplogAndVCs = function trackOplogAndVCs(follow, cb) {
  if (typeof follow === 'function') {
    cb = follow;
    follow = true;
  }
  if (typeof follow !== 'boolean') { throw new TypeError('follow must be a boolean'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var error;

  if (this._trackOplogAndVCsRunning) {
    error = new Error('trackOplogAndVCs already running');
    if (!this._hide) { console.error('vs trackOplogAndVCs', error); }
    process.nextTick(function() {
      cb(error);
    });
    return;
  }
  this._trackOplogAndVCsRunning = true;

  if (Object.keys(this._vcToVcTailStreams).length) {
    error = new Error('trackOplogAndVCs still some vc to vc tails running');
    if (!this._hide) { console.error('vs trackOplogAndVCs', error); }
    process.nextTick(function() {
      cb(error);
    });
    return;
  }

  var that = this;

  var finishedTails = {};
  finishedTails.oplog = false;

  function finalize(err) {
    if (err) { return cb(err); }

    var allClosed = Object.keys(finishedTails).every(function(key) {
      return finishedTails[key];
    });
    if (that.debug) { console.log('vs trackOplogAndVCs all tails closed', allClosed, JSON.stringify(finishedTails)); }
    if (allClosed) {
      that._trackOplogAndVCsRunning = false;
      cb(null);
    }
  }

  that.ensureSnapshotCollections(function(err) {
    if (err) { return finalize(err); }

    that.setupVcToVcConfigs(follow, function(err, configs) {
      if (err) { return finalize(err); }

      if (that.debug) { console.log('vs trackOplogAndVCs start oplog tail'); }
      that.trackOplog(follow, function(err) {
        finishedTails.oplog = true;
        finalize(err);
      });

      that.once('trackOplogStartTail', function() {
        if (that.debug) { console.log('vs trackOplogAndVCs start vc to vc tails', configs.length); }
        configs.forEach(function(cfg) {
          var fromName = cfg.from.databaseName + '.' + cfg.from.collectionName;
          var toName = cfg.to.databaseName + '.' + cfg.to.collectionName;
          var name = fromName + '+' + toName;

          finishedTails[name] = false;

          if (that.debug) { console.log('vs trackOplogAndVCs tail', fromName, toName, 'after', cfg.offset); }

          // save the stream handler of the tail for stopTrack
          that._vcToVcTailStreams[name] = that.trackVersionedCollections(cfg.from, cfg.to, cfg, function(err) {
            if (that.debug) { console.log('vs trackOplogAndVCs tail', fromName, toName, 'ended'); }
            finishedTails[name] = true;
            delete that._vcToVcTailStreams[name];
            finalize(err);
          });
        });
      });
    });
  });
};

/**
 * Setup configs for tails from one versioned collection to another, based on the
 * replication config.
 *
 * @param {Boolean} [follow, default true]  whether or not to follow the oplog
 * @param {Function} cb  First parameter will be an error object or null. Second
 *                       parameter will be an array of functions that start a tail.
 */
VersionedSystem.prototype.setupVcToVcConfigs = function setupVcToVcConfigs(follow, cb) {
  if (typeof follow === 'function') {
    cb = follow;
    follow = true;
  }
  if (typeof follow !== 'boolean') { throw new TypeError('follow must be a boolean'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  var splitReplicationConfig = Replicator.splitImportExport(that._replicate);

  Replicator.debug = this.debug;
  Replicator.getTailOptions(splitReplicationConfig, that._databaseCollections, function(err, vcConfigs) {
    if (err) { return cb(err); }

    var configs = [];
    Object.keys(vcConfigs).forEach(function(key) {
      var cfg = vcConfigs[key];

      var fromName = cfg.from.databaseName + '.' + cfg.from.collectionName;
      var toName = cfg.to.databaseName + '.' + cfg.to.collectionName;

      cfg.filter = cfg.filter || {};
      if (!Object.keys(cfg.filter).length) {
        if (that.debug) { console.log('vs setupVcToVcConfigs item filter empty', fromName, toName); }
      }

      cfg.offset = cfg.last;
      delete cfg.last;
      cfg.follow = follow;

      configs.push(cfg);
    });

    cb(null, configs);
  });
};

/**
 * Tail the first versioned collection into the other one.
 *
 * @param {VersionedCollection} from  source vc to tail from
 * @param {VersionedCollection} to  destination vc to tail into
 * @param {Object} [opts]  additional options
 * @param {Function} cb  First parameter will be an error object or null when tail
 *   is closed (if follow is false).
 * @return {Function} stream of the tail
 *
 * Options
 * - **filter** {Object} filter to apply before inputting into dst
 * - **importHooks**  {Array} hooks to run on each item before import
 * - **exportHooks** {Array} hooks to run on each item before export
 * - **transform** {Function|null}  transformation to run on each item
 * - **offset** {String}  the last version in the versioned collection
 * - **follow** {Boolean, default true}  whether to keep tailing the source
 *
 * Events:
 *  trackVersionedCollectionsCloseTail  emitted if the tail is closed
 */
VersionedSystem.prototype.trackVersionedCollections = function trackVersionedCollections(from, to, opts, cb) {
  /* jshint maxcomplexity: 18 */ /* might need some refactoring */

  if (!(from instanceof VersionedCollection)) { throw new TypeError('from must be a VersionedCollection'); }
  if (!(to instanceof VersionedCollection)) { throw new TypeError('to must be a VersionedCollection'); }

  if (typeof opts === 'function') {
    cb = opts;
    opts = {};
  }
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var filter = opts.filter || {};
  if (typeof filter !== 'object') { throw new TypeError('opts.filter must be an object'); }

  var importHooks = opts.importHooks || [];
  if (!Array.isArray(importHooks)) { throw new TypeError('opts.importHooks must be an array'); }

  var exportHooks = opts.exportHooks || [];
  if (!Array.isArray(exportHooks)) { throw new TypeError('opts.exportHooks must be an array'); }

  if (opts.transform && typeof opts.transform !== 'function') { throw new TypeError('opts.transform must be a function'); }

  var offset = opts.offset || '';
  if (typeof offset !== 'string') { throw new TypeError('opts.offset must be a string'); }

  var follow = true;
  if (opts.hasOwnProperty('follow')) { follow = opts.follow; }
  if (typeof follow !== 'boolean') { throw new TypeError('opts.follow must be a boolean'); }

  var first = true;
  if (!offset) { first = false; }

  var remoteName = from.databaseName;
  var fromKey = from.databaseName + '.' + from.collectionName;
  var toKey = to.databaseName + '.' + to.collectionName;

  var that = this;
  var error;
  var tailClosed = false;
  var running = 0;

  function finalize(err) {
    if (err) { return cb(err); }

    if (!tailClosed) { return; }
    if (running) { return; }

    if (follow) {
      // tailable cursor needs at least one item in the collection to stay open
      error = new Error('tailable cursor closed');
      if (!that._hide) { console.error('vs trackVersionedCollections', fromKey, toKey, error, offset); }
      cb(error);
      return;
    }

    if (that.debug) { console.log('vs trackVersionedCollections', fromKey, toKey, 'completed tail'); }
    return cb(null);
  }

  return from.tail(filter, offset, { follow: follow, hooks: exportHooks, hookOptions: opts, transform: opts.transform }, function(err, item, next) {
    if (err) {
      if (!that._hide) { console.error('vs trackVersionedCollections', fromKey, toKey, 'error', err); }
      finalize(err);
      return;
    }

    if (!item) {
      tailClosed = true;
      if (that.debug) { console.log('vs trackVersionedCollections', fromKey, toKey, 'closed tail'); }
      that.emit('trackVersionedCollectionsCloseTail');
      finalize();
      return;
    }

    if (first) {
      first = false;
      if (that.debug) { console.log('vs trackVersionedCollections', fromKey, toKey, 'first', item && JSON.stringify(item._id)); }
      if (item._id._v !== offset) {
        return finalize(new Error('first item does not equal requested start'));
      }
      next();
      return;
    }

    item._id._pe = remoteName;
    // run hooks
    running++;
    VersionedCollection.runHooks(importHooks, to._db, item, opts, function(err, newItem) {
      if (err) {
        console.error('vs trackVersionedCollections', fromKey, toKey, 'error import hook', err);
        return finalize(err, item);
      }

      if (!newItem) {
        if (that.debug) { console.log('vs trackVersionedCollections', fromKey, toKey, 'runImportHooks filtered', JSON.stringify(item._id), running); }
      }

      to.saveRemoteItem(newItem, function(err) {
        running--;
        if (err) {
          console.error('vs trackVersionedCollections', fromKey, toKey, 'to.saveRemoteItem error', err);
          return finalize(err);
        }
        // update offset
        offset = item._id._v;

        if (that.debug) { console.log('vs trackVersionedCollections', fromKey, toKey, 'saveRemoteItem', JSON.stringify(item._id), running); }
        finalize();
      }, next);
    });
  });
};



/////////////////////
//// PRIVATE API ////
/////////////////////



/**
 * Find the number of versions in each each versioned collection.
 *
 * @param {Function} cb  The first parameter will contain either an Error object or
 *                       null. The second parameter is an object with collection
 *                       names and versioned collection counts.
 */
VersionedSystem.prototype._versionedCollectionCounts = function _versionedCollectionCounts(cb) {
  var that = this;
  var result = {};
  async.each(Object.keys(this._databaseCollections), function(key, cb2) {
    that._databaseCollections[key]._snapshotCollection.count(function(err, snapshotCollectionCount) {
      if (err) { cb2(err); return; }

      result[key] = snapshotCollectionCount;
      cb2();
    });
  }, function(err) {
    if (that.debug) { console.log('vs _versionedCollectionCounts', result); }
    cb(err, result);
  });
};

/**
 * Find the oldest item with timestamp in the oplog.
 *
 * @param {Function} cb  this will be called after executing this method. The first
 *                       parameter will contain either an Error object or null. The
 *                       second parameter the oldest oplog item or null.
 */
VersionedSystem.prototype._getOldestOplogItem = function _getOldestOplogItem(cb) {
  this._oplogCollection.findOne({ ts: { $exists: true } }, { sort: { $natural: 1 } }, cb);
};

/**
 * Find the latest item with timestamp in the oplog.
 *
 * @param {Function} cb  this will be called after executing this method. The first
 *                       parameter will contain either an Error object or null. The
 *                       second parameter the latest oplog item.
 */
VersionedSystem.prototype._getLatestOplogItem = function _getLatestOplogItem(cb) {
  this._oplogCollection.findOne({ ts: { $exists: true } }, { sort: { $natural: -1 } }, cb);
};

/**
 * Start auto processing on every versioned collection.
 *
 * @param {Number} [autoProcessInterval]  possibility to overrule this._autoProcessInterval
 * @param {Function} [cb]  first parameter will be an error or null
 */
VersionedSystem.prototype._startAutoProcessing = function _startAutoProcessing(autoProcessInterval, cb) {
  if (typeof autoProcessInterval === 'function') {
    cb = autoProcessInterval;
    autoProcessInterval = null;
  }

  cb = cb || function() {};
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  this._autoProcessing = true;

  var that = this;
  that._stopAutoProcessing(function(err) {
    if (err) { return cb(err); }
    Object.keys(that._databaseCollections).forEach(function(key) {
      that._databaseCollections[key].startAutoProcessing(autoProcessInterval || that._autoProcessInterval);
    });
    cb(null);
  });
};

/**
 * Stop auto processing on every versioned collection.
 *
 * @param {Function} cb  First parameter will be an error or null.
 */
VersionedSystem.prototype._stopAutoProcessing = function _stopAutoProcessing(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  async.eachSeries(Object.keys(that._databaseCollections), function(key, cb2) {
    that._databaseCollections[key].stopAutoProcessing(cb2);
  }, function(err) {
    if (err) { return cb(err); }
    that._autoProcessing = false;
    cb(null);
  });
};

/**
 * Remove the last used collection oplog item.
 *
 * @param {Function} cb  this will be called after executing this method. The first
 *                       parameter will contain either an Error object or null.
 */
VersionedSystem.prototype._removeLastUsedCollectionOplogItem = function _removeLastUsedCollectionOplogItem(cb) {
  var that = this;
  this._localStorageCollection.remove({ _id: 'lastUsedCollectionOplogItem' }, function(err) {
    if (err) {
      if (!that._hide) { console.error('vs _removeLastUsedCollectionOplogItem', err); }
      return cb(err);
    }
    cb(null);
  });
};

/**
 * Remove the last used oplog item.
 *
 * @param {Function} cb  First parameter will be either an error object or null.
 */
VersionedSystem.prototype._removeLastUsedOplogItem = function _removeLastUsedOplogItem(cb) {
  var that = this;
  this._localStorageCollection.remove({ _id: 'lastUsedOplogItem' }, function(err) {
    if (err) {
      if (!that._hide) { console.error('vs _removeLastUsedOplogItem', err); }
      return cb(err);
    }
    cb(null);
  });
};

/**
 * Find a saved item by key.
 *
 * @param {String} key  the name of the item to fetch
 * @param {Function} cb  this will be called after executing this method. The first
 *                       parameter will contain either an Error object or null. The
 *                       second parameter the found Timestamp or null.
 */
VersionedSystem.prototype._getByKey = function _getByKey(key, cb) {
  var that = this;
  this._localStorageCollection.findOne({ _id: key }, function(err, item) {
    if (err) {
      if (!that._hide) { console.error('vs _getByKey', err); }
      return cb(err);
    }
    cb(err, item);
  });
};

/**
 * Find the most recently saved collection timestamp.
 *
 * @param {Function} cb  this will be called after executing this method. The first
 *                       parameter will contain either an Error object or null. The
 *                       second parameter the found Timestamp or null.
 */
VersionedSystem.prototype._getLastUsedCollectionOplogItem = function _getLastUsedCollectionOplogItem(cb) {
  this._getByKey('lastUsedCollectionOplogItem', cb);
};

/**
 * Find the most recently saved timestamp.
 *
 * @param {Function} cb  this will be called after executing this method. The first
 *                       parameter will contain either an Error object or null. The
 *                       second parameter the found Timestamp or null.
 */
VersionedSystem.prototype._getLastUsedOplogItem = function _getLastUsedOplogItem(cb) {
  this._getByKey('lastUsedOplogItem', cb);
};

/**
 * Save this item as the last processed collection oplog item.
 *
 * @param {Object} item  item from the oplog (it must contain a timestamp)
 * @param {Function} cb  this will be called after executing this method. The first
 *                       parameter will contain either an Error object or null.
 */
VersionedSystem.prototype._saveLastUsedCollectionOplogItem = function _saveLastUsedCollectionOplogItem(item, cb) {
  if (typeof item !== 'object') { throw new TypeError('item must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (!(item.ts instanceof Timestamp)) { throw new TypeError('item.ts must be a mongodb.Timestamp'); }

  if (this.debug) { console.log('vs _saveLastUsedCollectionOplogItem', JSON.stringify(item)); }

  item._id = 'lastUsedCollectionOplogItem';
  this._localStorageCollection.update({ _id: 'lastUsedCollectionOplogItem' }, item, { upsert: true, w: 1 }, cb);
};

/**
 * Save this item as the last processed oplog item.
 *
 * @param {Object} item  item from the oplog (it must contain a timestamp)
 * @param {Function} cb  this will be called after executing this method. The first
 *                       parameter will contain either an Error object or null.
 */
VersionedSystem.prototype._saveLastUsedOplogItem = function _saveLastUsedOplogItem(item, cb) {
  if (typeof item !== 'object') { throw new TypeError('item must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (!(item.ts instanceof Timestamp)) { throw new TypeError('item.ts must be a mongodb.Timestamp'); }

  if (this.debug) { console.log('vs _saveLastUsedOplogItem', JSON.stringify(item)); }

  // workaround "The dotted field '_id._id' in 'o.key._id._id' is not valid for storage." in case of saving items about indexes
  if (item.o && item.o.key) {
    delete item.o.key;
  }

  item._id = 'lastUsedOplogItem';
  this._localStorageCollection.update({ _id: 'lastUsedOplogItem' }, item, { upsert: true, w: 1 }, cb);
};

/**
 * Determine the last oplog item passed to a versioned collection.
 *
 * Note: depends on OplogResolver
 *
 * @param {mongodb.Timestamp, default Timestamp(0, 0)} [oplogOffset] minimum offset
 *   to use
 * @param {Object} vc  versioned collection
 * @param {Function} cb  First parameter will contain either an Error object or
 *                       null. The second parameter will be the matched oplog item
 *                       or null if the versioned collection is empty.
 */
VersionedSystem.prototype._resolveSnapshotToOplog = function _resolveSnapshotToOplog(oplogOffset, vc, cb) {
  if (typeof vc === 'function') {
    cb = vc;
    vc = oplogOffset;
    oplogOffset = null;
  }

  oplogOffset = oplogOffset || new Timestamp(0, 0);
  if (!(oplogOffset instanceof mongodb.Timestamp)) { throw new TypeError('oplogOffset must be a mongodb.Timestamp'); }
  if (!(vc instanceof VersionedCollection)) { throw new TypeError('vc must be a VersionedCollection'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  (new OplogResolver(this._oplogCollection, vc, { debug: this.debug, hide: this._hide, lowerBound: oplogOffset }, function(err, oplogItem) {
    if (err) { return cb(err); }

    if (that.debug) { console.log('vs _resolveSnapshotToOplog', vc.databaseName, vc.collectionName, JSON.stringify(oplogItem)); }

    return cb(null, oplogItem);
  })).start();
};

/**
 * Determine for each versioned collection at which point it has to start following
 * the oplog. This can be at the offset, the end, or somewhere in between. First
 * check if a timestamp is stored in local storage for a certain snapshot.
 *
 * @param {mongodb.Timestamp, default Timestamp(0, 0)} [oplogOffset] the last
 *   succesfully used oplog item
 * @param {Function} cb  this will be called after executing this method. The first
 *   parameter will contain either an Error object or null. The second parameter an
 *   object with a timestamp per versioned collection.
 */
VersionedSystem.prototype._resolveSnapshotOffsets = function _resolveSnapshotOffsets(oplogOffset, cb) {
  if (typeof oplogOffset === 'function') {
    cb = oplogOffset;
    oplogOffset = null;
  }
  oplogOffset = oplogOffset || new Timestamp(0, 0);
  if (!(oplogOffset instanceof mongodb.Timestamp)) { throw new TypeError('oplogOffset must be a mongodb.Timestamp'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  // find the offsets for every tracked snapshot collection
  var result = {};
  var that = this;
  var error;
  async.eachSeries(Object.keys(that._databaseCollections), function(databaseCollectionName, callback) {
    var vc = that._databaseCollections[databaseCollectionName];
    // make sure the versioned collection has items, otherwise set null
    vc._snapshotCollection.count(function(err, count) {
      if (err) { return callback(err); }

      if (count === 0) {
        if (that.debug) { console.log('vs _resolveSnapshotOffsets', databaseCollectionName, 'empty collection'); }
        result[databaseCollectionName] = null;
        return callback();
      }

      // if not all items are ackd, abort
      vc._snapshotCollection.count(function(err, ackCount) {
        if (err) { return callback(err); }

        if (ackCount !== count) {
          // TODO: support aborted instances
          error = new Error('not all items are ackd');
          if (!that._hide) { console.error('vs _resolveSnapshotOffsets', error, databaseCollectionName); }
          return callback(error);
        }

        // check if there is a predefined key
        that._getByKey(databaseCollectionName, function(err, item) {
          if (err) { return callback(err); }

          if (item && item.ts) {
            if (that.debug) { console.log('vs _resolveSnapshotOffsets pre-defined timestamp', databaseCollectionName, item.ts); }
            result[databaseCollectionName] = item.ts;
            return callback();
          }

          that._resolveSnapshotToOplog(oplogOffset, vc, function(err, oplogItem) {
            if (err) {
              if (err.message !== 'no modifications on snapshot found') {
                return callback(err);
              }
              if (that.debug) { console.log('vs _resolveSnapshotOffsets no last snapshot mod found', databaseCollectionName); }
            }

            if (oplogItem) {
              result[databaseCollectionName] = oplogItem.ts;
            } else {
              if (that.debug) { console.log('vs _resolveSnapshotOffsets', databaseCollectionName, 'using oplog offset', oplogOffset); }
              result[databaseCollectionName] = oplogOffset;
            }
            callback();
          });
        });
      });
    });
  }, function(err) {
    cb(err, result);
  });
};

/**
 * Determine the last used oplog item and the oplog offset per versioned
 * collection. First check if a timestamp is stored in the local storage for a
 * certain snapshot.
 *
 * @param {Function} cb  this will be called after executing this method. The first
 *                       parameter will contain either an Error object or null. The
 *                       second parameter is the found Timestamp.
 */
VersionedSystem.prototype._determineOplogOffsets = function _determineOplogOffsets(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  // use the last used collection oplog item as the minimum offset to resolve other offsets
  var that = this;
  that._getLastUsedCollectionOplogItem(function(err, lastUsedCollectionOplogItem) {
    if (err) { return cb(err); }

    var offset = new Timestamp(0, 0);
    if (lastUsedCollectionOplogItem) {
      offset = lastUsedCollectionOplogItem.ts;
    }

    // find the offset for each versioned collection
    that._resolveSnapshotOffsets(offset, function(err, offsets) {
      if (err) { return cb(err); }

      that._getLastUsedOplogItem(function(err, item) {
        if (err) { return cb(err); }

        if (!item) {
          // find max of the versioned collection offsets
          var maxTs = new Timestamp(0, 0);
          Object.keys(offsets).forEach(function(key) {
            if (offsets[key] && maxTs.lessThan(offsets[key])) {
              maxTs = offsets[key];
            }
            item = { ts: maxTs };
          });
        }

        offsets[that._localDbName + '.' + that._oplogCollectionName] = item.ts;

        if (that.debug) { console.log('vs _determineOplogOffsets offsets', JSON.stringify(offsets)); }

        cb(null, offsets);
      });
    });
  });
};

/**
 * Tail the oplog from a certain offset and pass items with the corresponding
 * versioned collection.
 *
 * @param {mongodb.Timestamp} ts  timestamp to start tailing after
 * @param {Boolean} tailable  whether or not to keep tailing
 * @param {Number} [tailableRetryInterval]  overrule this.tailableRetryInterval
 * @param {Function} cb  On error the first parameter will be an Error object or
 *                       null. The second parameter will be an oplog item or null
 *                       on normal close (without error). Third parameter will be
 *                       the versioned collection corresponding with the oplog
 *                       item, if any. The fourth parameter will be a next handler.
 * @return {mongodb.CursorStream} the tail that is running
 */
VersionedSystem.prototype._tailOplog = function _tailOplog(ts, tailable, tailableRetryInterval, cb) {
  if (typeof tailableRetryInterval === 'function') {
    cb = tailableRetryInterval;
    tailableRetryInterval = this._tailableRetryInterval;
  }

  tailableRetryInterval = tailableRetryInterval || this._tailableRetryInterval;

  if (!(ts instanceof Timestamp)) { throw new TypeError('ts must be a mongodb.Timestamp'); }
  if (typeof tailable !== 'boolean') { throw new TypeError('tailable must be a boolean'); }
  if (typeof tailableRetryInterval !== 'number') { throw new TypeError('tailableRetryInterval must be a number'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (this.debug) { console.log('vs _tailOplog ts', ts); }

  var mongoOpts = { sort: { '$natural': 1 }, comment: '_tailOplog' };
  if (tailable) {
    mongoOpts.tailable = true;
    mongoOpts.tailableRetryInterval = tailableRetryInterval;
  }
  var stream = this._oplogCollection.find({ ts: { $gt: ts } }, mongoOpts).stream();
  if (this.debug) { console.log('vs _tailOplog stream options', JSON.stringify(mongoOpts)); }

  var that = this;

  stream.on('data', function (data) {
    stream.pause();
    cb(null, data, that._databaseCollections[data.ns], function(err) {
      if (err) { throw err; }
      stream.resume();
    });
  });

  var error;
  stream.on('error', function(err) {
    if (!that._hide) { console.error('vs _tailOplog', err); }
    error = err;
    cb(err);
    stream.destroy();
  });

  stream.on('close', function() {
    if (that.debug) { console.log('vs _tailOplog stream closed. tailable:', tailable); }
    if (!error) { cb(null, null, null, null); }
  });

  return stream;
};
