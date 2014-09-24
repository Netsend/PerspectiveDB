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

var mongodb = require('mongodb');
var StateMachine = require('javascript-state-machine');

var VersionedCollection = require('./versioned_collection');

var Timestamp = mongodb.Timestamp;

/**
 * OplogResolver
 *
 * Map a versioned collection to an item in the oplog.
 *
 * TODO: currenly does not work with half synchronized snapshots
 *
 * @param {mongodb.Collection} oplogCollection  oplog collection
 * @param {VersionedCollection} vc  the versioned collection to map
 * @param {Object} [options]  additional options
 * @param {Function} cb  First parameter will contain either an Error object or
 *                       null. The second parameter will be the matched oplog item
 *                       or null if the versioned collection is empty.
 *
 * Options
 * - **fsm** {Object}, finite state machine definition for ack, default
 *                        config/fsm.json
 * - **fsmCbs** {Object}, event based callbacks
 * - **lowerBound** {mongodb.Timestamp, default Timestamp(0, 0)}, the minimum offset to use
 * - **debug** {Boolean, default: false}  whether to do extra console logging or not
 * - **hide** {Boolean, default: false}  whether to suppress errors or not (used
 *                                       in tests)
 */
function OplogResolver(oplogCollection, vc, options, cb) {
  if (!(oplogCollection instanceof mongodb.Collection)) { throw new TypeError('oplogCollection must be a mongdb.Collection'); }
  if (!(vc instanceof VersionedCollection)) { throw new TypeError('vc must be a VersionedCollection'); }

  if (typeof options === 'function') {
    cb = options;
    options = {};
  }

  if (typeof options !== 'object') { throw new TypeError('options must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  this._cb = cb;

  this._options = options;

  this._oplogCollection = oplogCollection;

  this._vc = vc;

  this._dbCollName = vc.databaseName + '.' + vc.collectionName;
  this._dbSnapshotCollName = vc.databaseName + '.' + vc.snapshotCollectionName;

  this.debug = !!this._options.debug;
  this._hide = !!this._options.hide;
  this._lowerBound = this._options.lowerBound || new Timestamp(0, 0);
  if (!(this._lowerBound instanceof Timestamp)) { throw new TypeError('options.lowerBound must be a mongodb.Timestamp'); }

  var that = this;

  if (this._options.fsm) {
    this._fsm = StateMachine.create({
      events: this._options.fsm,
      callbacks: this._options.fsmCbs,
      error: function(ev, from, to, args, errCode, errMsg, e) {
        if (!that._hide) { console.error(errCode, errMsg, e, ev, from, to, JSON.stringify(args)); }
        that._cb(new Error(errMsg));
      }
    });
  } else {
    // use separate json file with state machine so that it can be visualized with d3
    var events = require('../config/fsm.json');
    var callbacks = {
      onleavestate: function(ev, from, to) {
        if (that.debug) { console.log('or leave', from, ev, to); }

        // if not in initial state, verify
        if (from !== 'none' && from !== 'S') {
          return that._verifyOplogItemWithSnapshot.apply(that, arguments);
        }
        return true;
      },
      onenterstate: function(ev, from, to) {
        if (that.debug) { console.log('or enter', to, '(', from, ev, ')', that._fsm && that._fsm.current, JSON.stringify(that._finalOplogItem)); }

        // if not in initial state, try to finalize
        if (from !== 'none' && from !== 'S') {
          that._finalize();
        }
      },
      onSnapshotAck: that._walk.bind(this),
      onSnapshotInsert: that._walk.bind(this),
      onEinsert: function(ev, from, to, oplogItem) {
        that._finalOplogItem = oplogItem;
      },
      onEupdate: function(ev, from, to, oplogItem) {
        that._finalOplogItem = oplogItem;
      },
      onEupdate2: function(ev, from, to, oplogItem) {
        that._finalOplogItem = oplogItem;
      },
      onEdelete: function(ev, from, to, oplogItem) {
        that._finalOplogItem = oplogItem;
      }
    };
    this._fsm = StateMachine.create({
      initial: 'S',
      events: events,
      callbacks: callbacks,
      error: function(ev, from, to, args, errCode, err, e) {
        if (!that._hide) { console.error(errCode, err, e, ev, from, to, JSON.stringify(args)); }
        that._cb(new Error(err));
      }
    });
  }
}

module.exports = OplogResolver;

/**
 * Determine the last oplog item passed to the versioned collection: 
 * 0. find last DAG modification in oplog (either ack or insert, a not-so-tight upper bound)
 *   1. ack
 *     if _d false, _p 0: INSERT
 *       find all c i, c uf and s i
 *         if c uf, s i, c i originated by user
 *         if c i, s i originated by system
 *     if _d false, _p n: UPDATE
 *       find all c uf, s i, c u
 *         if c uf, s i, c uf originated by user
 *         if c uf, s i, c u originated by user
 *         if c uf, s i originated by system
 *     if _d true,  _p n: DELETE
 *       find all s i, c d
 *         if c d, s i originated by system
 *   2. insert
 *     if _d false, _p 0: INSERT
 *       find all c i
 *         if c i originated by user
 *         if nothing originated by system
 *     if _d false, _p n: UPDATE
 *       find all c uf and c u
 *         if c uf originated by user
 *         if c u originated by user
 *         if nothing originated by system
 *     if _d true,  _p n: DELETE
 *       find all c d
 *         if c d originated by user
 *         if nothing originated by system
 *
 * Note: depends on the inner workings of rebuild and vc._saveOplogItem and
 * vc._saveRemoteItem.
 *
 * @param {Function} cb  First parameter will contain either an Error object or
 *                       null. The second parameter will be the matched oplog item
 *                       or null if the versioned collection is empty.
 */
OplogResolver.prototype.start = function start() {
  var that = this;
  var error;

  // first set snapshot creation and try to tighten up the lowerbound
  that._setSnapshotCreationItemAndLowerBound(function(err) {
    if (err) { return that._cb(err); }

    that._lastSnapshotMod(that._lowerBound, function(err, oplogItem) {
      if (err) {
        if (!that._hide) { console.error('or start', that._dbCollName, err); }
        return that._cb(err);
      }

      if (!oplogItem) {
        error = new Error('no modifications on snapshot found');
        if (!that._hide) { console.error('or start', that._dbCollName, error); }
        return that._cb(error);
      }

      var ackOrInsert = that._determineAI(oplogItem);

      switch (ackOrInsert) {
      case 'ack':
        that._fsm.sack(oplogItem.o2._id._id, oplogItem.ts, oplogItem.o2._id._v);
        break;
      case 'insert':
        that._fsm.si(oplogItem.o._id._id, oplogItem.ts, oplogItem.o._id._v);
        break;
      default:
        return that._cb(new Error('could not determine last modification of vc'));
      }
    });
  });
};

/**
 * 
 * Save snapshot creation item in that._snapshotCreationItem and reset lower bound
 * if this creation item is newer than the current lower bound.
 *
 * @param {Function} cb  First parameter will be an error object or null.
 */
OplogResolver.prototype._setSnapshotCreationItemAndLowerBound = function _setSnapshotCreationItemAndLowerBound(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  that._lastSnapshotCreation(function(err, creationItem) {
    if (err) { cb(err); return; }

    that._snapshotCreationItem = creationItem;

    // reset only if it's higher than the original lower bound.
    if (creationItem && creationItem.ts.greaterThan(that._lowerBound)) {
      if (that.debug) { console.log('or start', that._dbCollName, 'old lower bound', that._lowerBound, 'new lower bound', creationItem.ts); }
      that._lowerBound = creationItem.ts;
    }
    cb(null);
  });
};

/**
 * If in a final position or if the stream is closed, call back.
 *
 * Note: deletes the callback to ensure it's called only once.
 */
OplogResolver.prototype._finalize = function _finalize() {
  if (this._finalized) {
    if (this.debug) { console.log('or _finalize already finalized', this._dbCollName); }
    return;
  }

  if (this.debug) { console.log('or _finalize running', this._dbCollName, '_fsmInTransition', this._fsmInTransition); }

  // callback if the stream is closed and not in transition
  if (!this._fsmInTransition && this._stream && !this._stream.readable) {
    if (this.debug) { console.log('or _finalize finalized', this._dbCollName); }
    this._finalized = true;
    if (this._finalOplogItem) {
      this._cb(null, this._finalOplogItem);
    } else {
      if (this.debug) { console.log('or _finalize cb with snapshotCreationItem', JSON.stringify(this._snapshotCreationItem)); }
      this._cb(null, this._snapshotCreationItem);
    }
    return;
  }

  // check the current state, if it's a final state, call back with oplog item
  // Eupdate is not always a final state, so only consider it final if the stream
  // is closed.
  if (~['Einsert', 'Eupdate2', 'Edelete'].indexOf(this._fsm.current)) {
    if (this.debug) { console.log('or _finalize finalized', this._dbCollName); }
    this._finalized = true;
    if (this._stream && this._stream.readable) {
      this._stream.destroy();
    }
    if (this._finalOplogItem) {
      this._cb(null, this._finalOplogItem);
    } else {
      if (this.debug) { console.log('or _finalize cb with snapshotCreationItem', JSON.stringify(this._snapshotCreationItem)); }
      this._cb(null, this._snapshotCreationItem);
    }
    return;
  }

  if (this.debug) { console.log('or _finalize not finalized', this._dbCollName); }
};

/**
 * Some version is ackd or inserted by the given oplog item id and version. Find
 * the next item.
 *
 * @param {String} ev  name of the fsm event
 * @param {String} from  name of the previous state
 * @param {String} to  name of the new state
 * @param {mixed} id  id of the oplog item
 * @param {mongodb.Timestamp} ts  oplog timestamp, upperbound
 * @param {String} version  version of the oplog item
 */
OplogResolver.prototype._walk = function _walk(ev, from, to, id, ts, version) {
  var that = this;

  that._fetchFromSnapshot(id, version, function(err, snapshotItem) {
    if (err) {
      if (!that._hide) { console.error('or _walk', that._dbCollName, err); }
      return that._cb(err);
    }

    var selector = {
      $or: [
        { // snapshot insert
          'ts': { $gte: that._lowerBound, $lt: ts },
          'op': 'i',
          'ns': that._dbSnapshotCollName,
          'o._id._id': id,
          'o._id._v': version,
          'o._id._pe': that._vc.localPerspective
        },
        { // collection insert or delete
          'ts': { $gte: that._lowerBound, $lt: ts },
          'op': { $in: [ 'i', 'd' ] },
          'ns': that._dbCollName,
          'o._id': id
        },
        { // collection update
          'ts': { $gte: that._lowerBound, $lt: ts },
          'op': 'u',
          'ns': that._dbCollName,
          'o2._id': id
        },
        { // snapshot creation
          'ts': { $gte: that._lowerBound, $lt: ts },
          'op': 'c',
          'ns': that._vc.databaseName + '.$cmd',
          'o.create': that._vc.snapshotCollectionName
        }
      ]
    };

    if (that.debug) { console.log('or _walk', that._dbCollName, 'selector', JSON.stringify(selector)); }

    that._stream = that._oplogCollection.find(selector, { sort: { $natural: -1 }, comment: '_walk' }).stream();

    that._stream.on('data', function(oplogItem) {
      if (that.debug) { console.log('or _walk', that._dbCollName, 'item', JSON.stringify(oplogItem)); }

      // stop if the creation or the snapshot is reached
      if (oplogItem.op === 'c') {
        if (that.debug) { console.log('or _walk', that._dbCollName, 'encountered creation, close', JSON.stringify(oplogItem)); }
        that._stream.destroy();
        return;
      }

      if (oplogItem.ns === that._dbCollName) {
        if (oplogItem.op === 'i') {
          that._fsmInTransition = true;
          that._fsm.ci(oplogItem, snapshotItem);
        }

        if (oplogItem.op === 'u') {
          that._fsmInTransition = true;
          that._fsm.cu(oplogItem, snapshotItem);
        }

        if (oplogItem.op === 'd') {
          that._fsmInTransition = true;
          that._fsm.cd(oplogItem, snapshotItem);
        }
      }

      if (oplogItem.ns === that._dbSnapshotCollName && oplogItem.op === 'i') {
        that._fsmInTransition = true;
        that._fsm.si(oplogItem, snapshotItem);
      }
    });

    that._stream.on('error', function(err) {
      if (!that._hide) { console.error('or _walk', that._dbCollName, err); }

      that._cb(err);
    });

    that._stream.on('close', function() {
      if (that.debug) { console.log('or _walk', that._dbCollName, 'stream closed'); }
      that._finalize();
    });
  });
};

/**
 * Check if the given oplog item is related to the given snapshot item.
 *
 * @param {Object} snapshotItem  item from the DAG
 * @param {Object} collectionItem  item from the collection to compare with
 * @return {Boolean|fsm.ASYNC} true if equal, false if match, async automatically transitions on success
 */
OplogResolver.prototype._verifyOplogItemWithSnapshot = function _verifyOplogItemWithSnapshot(ev, from, to, oplogItem, snapshotItem) {
  if (this.debug) { console.log('or _verifyOplogItemWithSnapshot', this._dbCollName); }

  var ret, clone;
  if (oplogItem.ns === this._dbSnapshotCollName) {
    // this is a snapshot insert, ignore _m3._ack since one might not be ackknowledged yet
    clone = this._vc.cloneDAGItem(snapshotItem);
    clone._m3._ack = oplogItem.o._m3._ack;
    ret = this._vc.compareDAGItems(clone, oplogItem.o);
    if (this.debug) { console.log('or _compareCollectionWithSnapshot snapshot insert', this._dbCollName, JSON.stringify(oplogItem.o), JSON.stringify(snapshotItem), 'equal', ret); }
  } else {
    // this is a collection update
    ret = this._compareCollectionWithSnapshot(oplogItem, snapshotItem);
  }

  if (typeof ret !== 'boolean') {
    // pause stream and expect event handler to resume
    if (this.debug) { console.log('or _verifyOplogItemWithSnapshot', this._dbCollName, 'ASYNC fsm transition, stream paused'); }
    this._stream.pause();
  }

  // need to manually update transition state
  this._fsmInTransition = false;
  return ret;
};

/**
 * Check if the given oplog item is related to the given snapshot item.
 *
 * @param {Object} snapshotItem  item from the DAG
 * @param {Object} collectionItem  item from the collection to compare with
 * @return {Boolean|fsm.ASYNC} true if equal, false if match, async automatically transitions on success
 */
OplogResolver.prototype._compareCollectionWithSnapshot = function _compareCollectionWithSnapshot(oplogItem, snapshotItem) {
  var that = this;
  var clone, error, ret;

  if (oplogItem.op === 'i') {
    // insert
    // ignore version
    clone = this._vc.cloneDAGItem(snapshotItem);
    clone._id._v = oplogItem.o[that._vc.versionKey];
    ret = this._vc.compareDAGItemWithCollectionItem(clone, oplogItem.o);
    if (that.debug) { console.log('or _compareCollectionWithSnapshot insert', this._dbCollName, JSON.stringify(oplogItem), JSON.stringify(snapshotItem), ret); }
    return ret;
  }

  if (oplogItem.op === 'd') {
    // delete
    if (snapshotItem._id._d) {
      ret = true;
    } else {
      ret = false;
    }
    if (that.debug) { console.log('or _compareCollectionWithSnapshot weak comparison, delete _id._d exists', this._dbCollName, JSON.stringify(oplogItem), JSON.stringify(snapshotItem), ret); }
    return ret;
  }

  if (oplogItem.op === 'u' && !VersionedCollection.oplogUpdateContainsModifier(oplogItem)) {
    // full doc update
    ret = this._vc.compareDAGItemWithCollectionItem(snapshotItem, oplogItem.o);
    if (that.debug) { console.log('or _compareCollectionWithSnapshot update full doc', this._dbCollName, JSON.stringify(oplogItem), JSON.stringify(snapshotItem), ret); }
    return ret;
  }

  if (that.debug) { console.log('or _compareCollectionWithSnapshot update by modifier', that._dbCollName, JSON.stringify(oplogItem)); }

  // update by modifier, compare with pervious version
  // 1. find previous version
  // 2. apply this oplog item
  // 3. see if the new item equals the next version
  if (snapshotItem._id._pa.length !== 1) {
    error = new Error('update modifier should have exactly one parent');
    if (!that._hide) { console.error('or _compareCollectionWithSnapshot', that._dbCollName, error); }
    return that._cb(error);
  }

  var selector = {
    '_id._id': snapshotItem._id._id,
    '_id._v':  snapshotItem._id._pa[0],
    '_id._pe': that._vc.localPerspective
  };
  that._vc._snapshotCollection.findOne(selector, { sort: { $natural: -1 }, comment: '_compareCollectionWithSnapshot' }, function(err, prev) {
    if (err) {
      if (!that._hide) { console.error('or _compareCollectionWithSnapshot', that._dbCollName, err); }
      return that._cb(err);
    }

    if (!prev) {
      error = new Error('parent of oplog update modifier not found');
      if (!that._hide) { console.error('or _compareCollectionWithSnapshot', that._dbCollName, error, JSON.stringify(oplogItem)); }
      return that._cb(error);
    }

    that._vc.createNewVersionByUpdateDoc(prev, oplogItem, function(err, newObj) {
      if (err) {
        if (!that._hide) { console.error('or _compareCollectionWithSnapshot', that._dbCollName, err); }
        return that._cb(err);
      }
      // sync version, increment, _lo and _m3
      newObj._id._v = snapshotItem._id._v;
      newObj._id._i = snapshotItem._id._i;
      newObj._id._lo = snapshotItem._id._lo;
      newObj._m3._ack = snapshotItem._m3._ack;

      var diff = VersionedCollection.diff(newObj, snapshotItem);
      if (Object.keys(diff).length) {
        if (that.debug) { console.log('or _compareCollectionWithSnapshot update modifier diff has items', that._dbCollName, JSON.stringify(diff)); }
        // need to manually update transition state
        that._fsmInTransition = false;
        that._fsm.transition.cancel();
        that._stream.resume();
      } else {
        if (that.debug) { console.log('or _compareCollectionWithSnapshot update modifier diff ok', that._dbCollName, JSON.stringify(diff)); }
        // need to manually update transition state
        that._fsmInTransition = false;
        that._fsm.transition();
        that._stream.resume();
      }
    });
  });
  return StateMachine.ASYNC;
};

/**
 * Search the oplog for the (last) creation of the snapshot.
 *
 * @param {Function} cb  First parameter will contain either an Error object or
 *                       null. The second parameter will be the found oplog item.
 */
OplogResolver.prototype._lastSnapshotCreation = function _lastSnapshotCreation(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  // find the last modification on the versioned collection
  var selector = {
    ns: this._vc.databaseName + '.$cmd',
    op: 'c',
    'o.create': this._vc.snapshotCollectionName
  };

  this._oplogCollection.findOne(selector, { sort: { '$natural': -1 }, comment: '_lastSnapshotCreation' }, function(err, item) {
    if (err) {
      if (!that._hide) { console.error('or _lastSnapshotCreation', that._dbCollName, err); }
      return cb(err);
    }

    if (that.debug) { console.log('or _lastSnapshotCreation', that._dbCollName, JSON.stringify(item)); }
    cb(null, item);
  });
};

/**
 * Search the oplog for the last modification of the snapshot.
 *
 * @param {mongodb.Timestamp} [offset]  optional offset to start at
 * @param {Function} cb  First parameter will contain either an Error object or
 *                       null. The second parameter will be the found oplog item.
 */
OplogResolver.prototype._lastSnapshotMod = function _lastSnapshotMod(offset, cb) {
  if (typeof offset === 'function') {
    cb = offset;
    offset = new Timestamp(0, 0);
  }

  if (!(offset instanceof mongodb.Timestamp)) { throw new TypeError('offset must be a mongdb.Timestamp'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  // find the last modification on the versioned collection
  var selector = {
    ns: this._dbSnapshotCollName,
    ts: { $gte: offset },
    op: { $in: ['i', 'u'] }
  };

  this._oplogCollection.findOne(selector, { sort: { '$natural': -1 }, comment: '_lastSnapshotMod' }, function(err, item) {
    if (err) {
      if (!that._hide) { console.error('or _lastSnapshotMod', that._dbCollName, err); }
      return cb(err);
    }

    if (that.debug) { console.log('or _lastSnapshotMod', that._dbCollName, JSON.stringify(selector), JSON.stringify(item)); }

    cb(err, item);
  });
};

/**
 * Determine whether a given oplog item is an ack or a snapshot insert.
 *
 * @param {Object} oplogItem  item from the oplog
 * @return {String|null} 'ack' if it is an acknowledgement, 'insert' if it is a
 *   snapshot isnert item or null if it can't be determined.
 */
OplogResolver.prototype._determineAI = function _determineAI(oplogItem) {
  if (typeof oplogItem !== 'object') { throw new TypeError('oplogItem must be an object'); }

  try {
    if (oplogItem.ns !== this._dbSnapshotCollName) { throw new Error('oplogItem not from snapshot'); }

    if (oplogItem.op === 'u' && oplogItem.o.$set['_m3._ack'] === true) {
      return 'ack';
    } else if (oplogItem.op === 'i') {
      return 'insert';
    } else {
      throw new Error('unrecognized last modification of versioned collection in oplog');
    }
  } catch(err) {
    if (!this._hide) { console.error('or _determineAI', this._dbCollName, err, JSON.stringify(oplogItem)); }
    return null;
  }
};

/**
 * Find the snapshot item for the given version and id.
 *
 * @param {mixed} id  the id of the snapshot item
 * @param {String} version  the version to find
 * @param {Function} cb  First parameter will contain either an Error object or
 *                       null. The second parameter will be either 'insert',
 *                       'update' or 'delete' item or null if the versioned
 *                       collection is empty. Third parameter will be the item from
 *                       the snapshot.
 */
OplogResolver.prototype._fetchFromSnapshot = function _fetchFromSnapshot(id, version, cb) {
  var that = this;
  var error;

  var selector = {
    '_id._id': id,
    '_id._v':  version,
    '_id._pe': that._vc.localPerspective
  };
  that._vc._snapshotCollection.findOne(selector, { sort: { $natural: -1 }, comment: '_fetchFromSnapshot' }, function(err, item) {
    if (err) {
      if (!that._hide) { console.error('or _fetchFromSnapshot', that._dbCollName, err); }
      return cb(err);
    }

    if (!item) {
      error = new Error('not found');
      if (!that._hide) { console.error('or _fetchFromSnapshot', that._dbCollName, error, id, version); }
      return cb(error);
    }

    cb(null, item);
  });
};
