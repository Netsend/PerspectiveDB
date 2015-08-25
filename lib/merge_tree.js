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

var through2 = require('through2');
var isEqual = require('is-equal');

var t = require('./core-util-is-fork');
var Tree = require('./tree');
var invalidLocalHeader = require('./invalid_local_header');

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
 *                                     254 bytes
 *   stage {String, default "_stage"}  name of the staging tree, should not
 *                                     exceed 254 bytes
 *   perspectives {Array}  Names of different sources that should be merged to
 *                         the local tree. A name should not exceed 254 bytes.
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
  if (!t.isUndefined(opts) && !t.isObject(opts)) { throw new TypeError('opts must be an object'); }
  if (!t.isUndefined(opts.local) && !t.isString(opts.local)) { throw new TypeError('opts.local must be a string'); }
  if (!t.isUndefined(opts.stage) && !t.isString(opts.stage)) { throw new TypeError('opts.stage must be a string'); }
  if (!t.isUndefined(opts.perspectives) && !t.isArray(opts.perspectives)) { throw new TypeError('opts.perspectives must be an array'); }
  if (!t.isUndefined(opts.log) && !t.isObject(opts.log)) { throw new TypeError('opts.log must be an object'); }
  if (!t.isUndefined(opts.vSize) && !t.isNumber(opts.vSize)) { throw new TypeError('opts.vSize must be a number'); }
  if (!t.isUndefined(opts.iSize) && !t.isNumber(opts.iSize)) { throw new TypeError('opts.iSize must be a number'); }

  opts.objectMode = true;

  this._localName = opts.local || '_local';
  this._stageName = opts.stage || '_stage';
  this._perspectives = opts.perspectives || [];

  if (Buffer.byteLength(this._localName) > 254) { throw new Error('opts.local must not exceed 254 bytes'); }
  if (Buffer.byteLength(this._stageName) > 254) { throw new Error('opts.stage must not exceed 254 bytes'); }

  if (this._localName === this._stageName) { throw new Error('local and stage names can not be the same'); }

  var that = this;

  this._perspectives.forEach(function(perspective) {
    if (Buffer.byteLength(perspective) > 254) { throw new Error('each perspective name must not exceed 254 bytes'); }
    if (perspective === that._localName) { throw new Error('every perspective should have a name that differs from the local name'); }
    if (perspective === that._stageName) { throw new Error('every perspective should have a name that differs from the stage name'); }
  });

  this._vSize = opts.vSize || 6;
  this._iSize = opts.iSize || 6;

  if (this._vSize < 0 || this._vSize > 6) { throw new Error('opts.vSize must be between 0 and 6'); }
  if (this._iSize < 0 || this._iSize > 6) { throw new Error('opts.iSize must be between 0 and 6'); }

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

  // create trees
  this._pe = {};

  this._perspectives.forEach(function(perspective) {
    that._pe[perspective] = new Tree(db, perspective, opts);
  });

  this._local = new Tree(db, this._localName, opts);
  this._stage = new Tree(db, this._stageName, opts);
}

module.exports = MergeTree;

/**
 * Save a new version of a certain perspective in the appropriate Tree.
 *
 * New items should have the following structure:
 * {
 *   h: {Object}  header containing the following values:
 *     id:  {mixed}  id of this h
 *     v:   {base64 String}  version
 *     pa:  {Array}  parent versions
 *     [d]: {Boolean}  true if this id is deleted
 *   m: {Object}  meta info to store with this document
 *     pe:  {String}  perspective
 *   [b]: {mixed}  document to save
 * }
 *
 * @param {Object} item  item to save
 * @param {Function} cb  Callback that is called once the item is saved. First
 *                       parameter will be an error object or null.
 */
MergeTree.prototype.createRemoteWriteStream = function createRemoteWriteStream() {
  var that = this;
  var error;

  return through2.obj(function(item, encoding, cb) {
    if (!t.isObject(item)) {
      process.nextTick(function() {
        cb(new TypeError('item must be an object'));
      });
      return;
    }
    if (!t.isFunction(cb)) {
      process.nextTick(function() {
        cb(new TypeError('cb must be a function'));
      });
      return;
    }

    var pe;

    try {
      pe = item.m.pe;
      if (!t.isString(pe)) {
        throw new TypeError();
      }
    } catch(err) {
      process.nextTick(function() {
        cb(new TypeError('item.m.pe must be a string'));
      });
      return;
    }

    if (pe === that._localName) {
      error = 'perspective should differ from local name';
      process.nextTick(function() {
        that._log.err('merge_tree %s %s', error, pe);
        cb(new Error(error));
      });
      return;
    }

    if (pe === that._stageName) {
      error = 'perspective should differ from stage name';
      process.nextTick(function() {
        that._log.err('merge_tree %s %s', error, pe);
        cb(new Error(error));
      });
      return;
    }

    var tree = that._pe[pe];

    if (!tree) {
      error = 'perspective not found';
      process.nextTick(function() {
        that._log.err('merge_tree %s %s', error, pe);
        cb(new Error(error));
      });
      return;
    }

    that._pe[pe].write(item, cb);
  });
};

/**
 * Save a new version from the local perspective or a confirmation of a
 * handled merge from a remote in the local tree.
 *
 * New items should have the following structure:
 * {
 *   h: {Object}  header containing the following values:
 *     id:  {mixed}  id of this item
 *     [v]: {base64 String}  supply version to confirm a handled merge
 *     [d]: {Boolean}  true if this id is deleted
 *   [m]: {mixed}  meta info to store with this document
 *   [b]: {mixed}  document to save
 * }
 *
 * @param {Object} item  item to save
 * @param {Function} cb  Callback that is called once the item is saved. First
 *                       parameter will be an error object or null.
 */
MergeTree.prototype.createLocalWriteStream = function createLocalWriteStream() {
  var that = this;

  return through2.obj(function(item, encoding, cb) {
    if (!t.isObject(item)) {
      process.nextTick(function() {
        cb(new TypeError('item must be an object'));
      });
      return;
    }
    if (!t.isFunction(cb)) {
      process.nextTick(function() {
        cb(new TypeError('cb must be a function'));
      });
      return;
    }

    var error = invalidLocalHeader(item.h);
    if (error) {
      process.nextTick(function() {
        cb(new TypeError('item.' + error));
      });
      return;
    }

    var header = item.h;
    var pe = that._localName;

    // use local and staging tree
    var local = that._local;
    var stage = that._stage;

    // the item is local, check if this is an ack in the stage or a new version of local
    stage.getByVersion(header.v, function(err, exists) {
      if (err) { cb(err); return; }

      if (exists && isEqual(exists, item)) {
        // if this item has zero or one parent, assume it exists in the perspective bound tree
        // if this item has more than one parent, assume the first parent is the version that exists in the persepective bound tree.
        // see mergeSrcWithLocal

        // ack, move everything up to this item to the local tree
        that._lastSrcInDst(that._local, pe, function(err, last) {
          if (err) { cb(err); return; }

          // set every copied item from src in conflict except this one from the stage, to
          // ensure all other heads are in conflict. this is to ensure only one
          // non-conflicting head in the local tree
          function transform(item2, cb2) {
            item2._h.c = true;
            cb2(null, item2);
          }

          // see mergeSrcWithLocal, if there is a parent, the first parent points to a version that exists in the perspective it came from
          var opts = { first: last.v, excludeFirst: true, last: header.v, excludeLast: true, transform: transform };
          that._copyTo(that._pe[pe], that._local, opts, function(err) {
            if (err) { cb(err); return; }

            // copy last item from the stage
            that._local.write(item, cb);
          });
        });
      } else {
        // add new version, determine parent by last non-conflicting head (should always be one item in the local tree)
        if (header.pa) {
          error = 'did not expect local item to have a parent defined';
          that._log.err('merge_tree createLocalWriteStream %s %j', error, item);
          cb(new Error(error));
          return;
        }

        var p;
        var nonConflictingHeads = 0;
        local.getHeads({ id: header.id, skipConflicts: true }, function(head, next) {
          p = head;
          nonConflictingHeads++;
          next();
        }, function(err) {
          if (err) { cb(err); return; }

          if (nonConflictingHeads > 1) {
            error = 'more than one non-conflicting head in local tree';
            that._log.err('merge_tree createLocalWriteStream %s %j', error, item);
            cb(new Error(error));
            return;
          }

          if (p) {
            header.pa = [p];
          } else {
            that._log.debug('merge_tree createLocalWriteStream no parent found for %j', item);
          }

          local.write(item, cb);
        });
      }
    });
  });
};

/**
 * Copy all items from stree to dtree. Maintains insertion order of stree.
 *
 * @param {Object} stree  source tree to search
 * @param {Object} dtree  destination tree
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
MergeTree.prototype._copyTo = function _copyTo(stree, dtree, opts, cb) {
  if (t.isFunction(opts)) {
    cb = opts;
    opts = null;
  }

  opts = opts || {};
  if (!t.isObject(stree)) { throw new TypeError('stree must be an object'); }
  if (!t.isObject(dtree)) { throw new TypeError('dtree must be an object'); }
  if (!t.isObject(opts)) { throw new TypeError('opts must be an object'); }
  if (!t.isFunction(cb)) { throw new TypeError('cb must be a function'); }

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
 * @param {Object} stree  source tree to merge
 * @param {Function} iterator  function(merged, lhead, next) called with merged
 *                             local item, previous local item and next handler
 * @param {Function} cb  First parameter will be an error object or null.
 */
MergeTree.prototype.mergeWithLocal = function mergeWithLocal(stree, iterator, cb) {
  if (!t.isObject(stree)) { throw new TypeError('stree must be an object'); }
  if (!t.isFunction(iterator)) { throw new TypeError('iterator must be a function'); }
  if (!t.isFunction(cb)) { throw new TypeError('cb must be a function'); }

  var stage = this._stage;

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

  this._headsMissing(stree, this._local, iter, cb);
};

/**
 * Find every head in src that is missing in dst.
 *
 * Ensures insertion order of src.
 *
 * @param {Object} stree  source tree
 * @param {Object} dtree  destination tree
 * @param {Function} iterator  function(shead, dhead, next) called with src
 *                             head, dst head and next handler
 * @param {Function} cb  First parameter will be an error object or null.
 */
MergeTree.prototype._headsMissing = function _headsMissing(stree, dtree, iterator, cb) {
  if (!t.isObject(stree)) { throw new TypeError('stree must be an object'); }
  if (!t.isObject(dtree)) { throw new TypeError('dtree must be an object'); }
  if (!t.isFunction(iterator)) { throw new TypeError('iterator must be a function'); }
  if (!t.isFunction(cb)) { throw new TypeError('cb must be a function'); }

  var that = this;

  // determine offset
  that._lastSrcInDst(stree.name, dtree, function(err, last) {
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
 * @param {Object} stree  source tree
 * @param {Object} dtree  destination tree
 * @param {Function} cb  First parameter will be an error object or null. Second
 *                       object will be the item found from src or null if dst
 *                       does not have any items from src.
 */
MergeTree.prototype._lastSrcInDst = function _lastSrcInDst(stree, dtree, cb) {
  if (!t.isObject(stree)) { throw new TypeError('stree must be an object'); }
  if (!t.isObject(dtree)) { throw new TypeError('dtree must be an object'); }
  if (!t.isFunction(cb)) { throw new TypeError('cb must be a function'); }


  // TODO: do a binary search of versions in s and locate them in dst
  //cb(err, lastItem);
};
