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

'use strict';

var Transform = require('stream').Transform;
var util = require('util');

var isEqual = require('is-equal');

var Tree = require('tree');

var noop = function() {};

/**
 * MergeTree
 *
 * Accept objects from different perspectives. Merge other perspectives into the
 * local perspective.
 *
 * @param {LevelUP.db} db  database for persistent storage
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   local {String, default "_local"}  name of the local tree, should not exceed
 *                                     255 bytes
 *   staging {String, default "_staging"}  name of the staging tree, should not
 *                                         exceed 255 bytes
 *   perspectives {Array}  Names of different sources that should be merged to
 *                         the local tree. A name should not exceed 255 bytes.
 *   vSize {Number, default 6}  number of bytes used for the version. Should be:
 *                              0 < vSize <= 6
 *   iSize {Number, default 6}  number of bytes used for i. Should be:
 *                              0 < iSize <= 6
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 */
function MergeTree(db, opts) {
  if (typeof db !== 'object' || db === null) { throw new TypeError('db must be an object'); }

  opts = opts || {};
  if (typeof opts !== 'object' || opts === null || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }
  if (typeof opts.local !== 'undefined' && typeof opts.local !== 'string') { throw new TypeError('opts.local must be a string'); }
  if (typeof opts.staging !== 'undefined' && typeof opts.staging !== 'string') { throw new TypeError('opts.staging must be a string'); }
  if (typeof opts.perspectives !== 'undefined' && !Array.isArray(opts.perspectives)) { throw new TypeError('opts.perspectives must be an array'); }
  if (typeof opts.log !== 'undefined' && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }
  if (typeof opts.vSize !== 'undefined' && typeof opts.vSize !== 'number') { throw new TypeError('opts.vSize must be a number'); }
  if (typeof opts.iSize !== 'undefined' && typeof opts.iSize !== 'number') { throw new TypeError('opts.iSize must be a number'); }

  opts.objectMode = true;

  this._local = opts.local || '_local';
  this._staging = opts.staging || '_staging';
  this._perspectives = opts.perspectives || [];

  if (Buffer.byteLength(this._local) > 255) { throw new Error('opts.local must not exceed 255 bytes'); }
  if (Buffer.byteLength(this._staging) > 255) { throw new Error('opts.staging must not exceed 255 bytes'); }

  if (this._local === this._staging) { throw new Error('local and staging names can not be the same'); }

  this._perspectives.forEach(function(perspective) {
    if (Buffer.byteLength(perspective) > 255) { throw new Error('each perspective name must not exceed 255 bytes'); }
    if (perspective === that._local) { throw new Error('every perspective should have a name that differs from the local name'); }
    if (perspective === that._staging) { throw new Error('every perspective should have a name that differs from the staging name'); }
  });

  this._vSize = opts.vSize || 6;
  this._iSize = opts.iSize || 6;

  if (this._vSize < 0 || this._vSize > 6) { throw new Error('opts.vSize must be between 0 and 6'); }
  if (this._iSize < 0 || this._iSize > 6) { throw new Error('opts.iSize must be between 0 and 6'); }

  Transform.call(this, opts);

  this._db = db;

  this._log = opts.log || {
    emerg:   console.error,
    alert:   console.error,
    crit:    console.error,
    err:     console.error,
    warning: console.log,
    notice:  console.log,
    info:    console.log,
    debug:   console.log,
    debug2:  console.log,
    getFileStream: noop,
    getErrorStream: noop,
    close: noop
  };

  var that = this;

  // create a cache with the last seen head per tree
  this._lastSeenHead = {};

  // create trees
  this._trees = {};

  this._perspectives.forEach(function(perspective) {
    that._trees[perspective] = new Tree(db, perspective, opts);
  });

  this._trees[this._local]   = new Tree(db, this._local, opts);
  this._trees[this._staging] = new Tree(db, this._staging, opts);
}

util.inherits(MergeTree, Transform);
module.exports = MergeTree;

/**
 * Save a new version of a certain perspective in the appropriate Tree.
 *
 * Implementation of _transform method of Transform stream. This method is not
 * called directly. New items should have the following structure:
 * {
 *   id: {mixed} id of this item
 *   pe: {String} perspective
 *   [v]: {String} if this is a merged item it should be the version of that
 *                 merged item. if this is any new version, then version will be
 *                 ignored
 *   [op]: {String} "i", "u", "ui", "d"
 *   [pa]: {Array} parent versions
 *   [val]: {mixed} value to save
 *   [meta]: {mixed} extra info to store with this value
 *
 * @param {Object} item  item to save
 * @param {String} [encoding]  ignored
 * @param {Function} cb  Callback that is called once the item is saved. First
 *                       parameter will be an error object or null.
 *
 * cb can be called with any of the following errors:
 *   item is an invalid item (see Tree.invalidItem)
 *   item is not a new leaf
 */
MergeTree.prototype._transform = function(item, encoding, cb) {
  if (typeof item !== 'object') { throw new TypeError('item must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (typeof item.id === 'undefined' || item === null) { throw new TypeError('item.id must be defined'); }
  if (typeof item.pe !== 'string') { throw new TypeError('item.pe must be a string'); }

  var error;
  var that = this;

  var tree = this._trees[item.pe];

  if (!tree) {
    error = 'perspective not found';
    process.nextTick(function() {
      that._log.err('merge_tree %s %s', error, item.pe);
      cb(new Error(error));
    });
    return;
  }

  /**
   * If this is not the local tree, nothing special needs to be done. If this is
   * the local tree, determine parent and save it to staging.
   */
  if (item.pe !== this._local) {
    this._trees[item.pe].write(item, cb);
    return;
  }

  // use local and staging tree
  var local = this._trees[this._local];
  var stage = this._trees[this._staging];

  // the item is local, check if this is an ack in the stage or a new version of local
  stage.getByVersion(item.v, function(err, exists) {
    if (err) { cb(err); return; }

    if (exists && isEqual(exists, item)) {
      // if this item has zero or one parent, assume it exists in the perspective bound tree
      // if this item has more than one parent, assume the first parent is the version that exists in the persepective bound tree.
      // see mergeSrcWithLocal

      // ack, move everything up to this item to the local tree
      that._lastSrcInDst(that._local, item.pe, function(err, last) {
        if (err) { cb(err); return; }

        // set every copied item from src in conflict except this one from the stage, to
        // ensure all other heads are in conflict. this is to ensure only one
        // non-conflicting head in the local tree
        function transform(item2, cb2) {
          item2._h.c = true;
          cb2(null, item2);
        }

        // see mergeSrcWithLocal, if there is a parent, the first parent points to a version that exists in the perspective it came from
        var opts = { first: last.v, excludeFirst: true, last: item.v, excludeLast: true, transform: transform };
        that._copyTo(item.pe, that._local, opts, function(err) {
          if (err) { cb(err); return; }

          // copy last item from the stage
          that._local.write(item, cb);
        });
      });
    } else {
      // add new version, determine parent by last non-conflicting head (should always be one item in the local tree)
      if (item._pa) {
        error = 'did not expect local item to have a parent defined';
        that._log.err('merge_tree _transform %s %j', error, item);
        cb(new Error(error));
        return;
      }

      var p;
      var nonConflictingHeads = 0;
      local.getHeads({ id: item.id, skipConflicts: true }, function(head, next) {
        p = head;
        nonConflictingHeads++;
        next();
      }, function(err) {
        if (err) { cb(err); return; }

        if (nonConflictingHeads > 1) {
          error = 'more than one non-conflicting head in local tree';
          that._log.err('merge_tree _transform %s %j', error, item);
          cb(new Error(error));
          return;
        }

        if (p) {
          item._pa = [p];
        } else {
          that._log.debug('merge_tree _transform no parent found for %j', item);
        }

        local.write(item, cb);
      });
    }
  });
};

/**
 * Copy all items from src to the dst. Maintains insertion order of src.
 *
 * @param {String} src  name of the source tree to search
 * @param {String} dst  name of the destination tree
 * @param {Object} [opts]  object containing configurable parameters
 * @param {Function} cb  First parameter will be an error object or null.
 *
 * opts:
 *   first {base64 String}  first version that should be used
 *   last {base64 String}  last version to copy
 *   excludeFirst {Boolean, default false}  whether or not first should be
 *                                         excluded
 *   excludeLast {Boolean, default false}  whether or not last should be
 *                                         excluded
 *   transform {Function}  transformation function to run on each item
 *                         signature: function(item, cb2) cb2 should be called
 *                         with an optional error and a possibly transformed
 *                         item
 */
MergeTree.prototype._copyTo = function _copyTo(src, dst, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = null;
  }

  opts = opts || {};
  if (typeof src !== 'string') { throw new TypeError('src must be a string'); }
  if (typeof dst !== 'string') { throw new TypeError('dst must be a string'); }
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var stree = this._trees[src];
  var dtree = this._trees[dst];

  opts.transform = opts.transform || function(item, cb2) { cb2(null, item); };

  var first = true;
  stree.iterateInsertionOrder({ v: opts.first }, function(item, next) {
    if (first) {
      first = false;
      if (opts.first && item.v === opts.first && opts.excludeFirst) {
        next();
        return;
      }
    }

    if (opts.last && item.v === opts.last && opts.excludeLast) {
      next(false);
      return;
    }

    opts.transform(item, function(err, nitem) {
      if (err) { cb(err); return; }
      if (nitem === null || typeof nitem === 'undefined') {
        next();
        return;
      }

      dtree.write(nitem, function() {
        if (opts.last && opts.last === item._h.v) {
          next(false);
        } else {
          next();
        }
      });
    });
  }, cb);
};

/**
 * Merge src tree with the local tree. For every newly created merge, copy this
 * to staging and iterate. If there is a conflict, skip this head. If a merge is
 * a fast-forward for the local tree, nothing is done.
 *
 * On fast-forward (item with zero or one parent), copy head from src to staging
 * On merge (item has more than one parent), copy merge to staging. First parent
 * of the merge is the version that exists in the src tree, second parent is the
 * version that exists in the local tree.
 *
 * Note: no new items are inserted in the src or local trees, only merge items
 * in the staging tree.
 *
 * Note2: an item from src can be a merge that is a fast-forward for the local
 * tree. So a merge in stage does not mean it is locally created.
 *
 * @param {String} src  name of src tree to merge
 * @param {Function} iterator  function(merged, lhead, next) called with merged
 *                             local item, previous local item and next handler
 * @param {Function} cb  First parameter will be an error object or null.
 */
MergeTree.prototype.mergeSrcWithLocal = function mergeSrcWithLocal(src, iterator, cb) {
  if (typeof src !== 'string') { throw new TypeError('src must be a string'); }
  if (typeof iterator !== 'function') { throw new TypeError('iterator must be a function'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var stage = this._trees[this._staging];

  var that = this;

  function iter(shead, lhead, next) {
    var merge = that._merge(shead, lhead);
    if (Array.isArray(merge)) {
      // merge conflict, do nothing
      that._log.warn('mergeStageWithLocal merge conflict %s %j %j', merge, shead, lhead);
      next();
      return;
    }

    // insert merge in staging and iterate with the new merge and the old local perspective
    stage.write(merge[1], function() {
      iterator(merge[1], lhead, next);
    });
  }

  this.srcHeadsMissingInDst(src, this._local, iter, cb);
};

/**
 * Find every head in src that is missing in dst.
 *
 * Ensures insertion order of src.
 *
 * @param {String} src  name of src tree
 * @param {String} dst  name of dst tree
 * @param {Function} iterator  function(shead, dhead, next) called with src
 *                             head, dst head and next handler
 * @param {Function} cb  First parameter will be an error object or null.
 */
MergeTree.prototype.srcHeadsMissingInDst = function srcHeadsMissingInDst(src, dst, iterator, cb) {
  if (typeof src !== 'string') { throw new TypeError('src must be a string'); }
  if (typeof dst !== 'string') { throw new TypeError('dst must be a string'); }
  if (typeof iterator !== 'function') { throw new TypeError('iterator must be a function'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  var stree = this._trees[src];
  var dtree = this._trees[dst];

  // determine offset
  that._lastSrcInDst(src, dst, function(err, last) {
    if (err) { cb(err); return; }

    stree.iterateInsertionOrder({ first: last.v, excludeFirst: true }, function(sitem, snext) {
      // if this is a head, get all heads in dtree, try to merge with each one of them
      stree.getHeadVersions(sitem._h.id, function(err, heads) {
        if (err) { cb(err); return; }

        if (~heads.indexOf(sitem._h.v)) {
          // sitem is an ancestor
          snext();
          return;
        }

        // sitem is a head, find corresponding heads in dtree
        dtree.getHeads({ id: sitem._h.id }, function(dhead, dnext) {
          iterator(sitem, dhead, dnext);
        }, function(err) {
          if (err) { cb(err); return; }
          snext();
        });
      });
    }, cb);
  });
};

/**
 * Find the last item of src that is in dst.
 *
 * Relies on the property that all versions are added to each tree in the same
 * order.
 *
 * O(log(n))
 *
 * @param {String} src  name of the source tree to search
 * @param {String} dst  name of the destination tree to search
 * @param {Function} cb  First parameter will be an error object or null. Second
 *                       object will be the item found from src or null if dst
 *                       does not have any items from src.
 */
MergeTree.prototype._lastSrcInDst = function _lastSrcInDst(src, dst, cb) {
  if (typeof src !== 'string') { throw new TypeError('src must be a string'); }
  if (typeof dst !== 'string') { throw new TypeError('dst must be a string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  // TODO: do a binary search of versions in s and locate them in dst
  //cb(err, lastItem);
};
