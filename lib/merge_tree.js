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
var findLCAs = require('./findLCAs');
var Tree = require('./tree');
var invalidLocalHeader = require('./invalid_local_header');

var noop = function() {};

/**
 * MergeTree
 *
 * Accept objects from different perspectives. Merge other perspectives into the
 * local perspective.
 *
 * If local updates can come in, as well as updates by other perspectives then a
 * mergeHandler should be provided. This function is called with every newly
 * created merge (either by fast-forward or three-way-merge). Make sure any
 * processed merges are written back to this merge tree (createLocalWriteStream)
 * in the same order as mergeHandler was called. It's ok to miss some items as
 * long as the order does not change.
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
 *   mergeHandler {Function}  function that should handle newly created merges
 *                            signature: function (merged, lhead, next)
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
  if (!t.isUndefined(opts.mergeHandler) && !t.isFunction(opts.mergeHandler)) { throw new TypeError('opts.mergeHandler must be a function'); }
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

  if (opts.mergeHandler) {
    this._mergeHandler = opts.mergeHandler;
  } else {
    // emediately confirm writes if no mergehandler is provided
    var writable = this.createLocalWriteStream();
    this._mergeHandler = function(merged, lhead, next) { writable(merged, next); };
  }

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
 *     pe:  {String}  perspective
 *     [d]: {Boolean}  true if this id is deleted
 *   [m]: {Object}  meta info to store with this document
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
      pe = item.h.pe;
      if (!t.isString(pe)) {
        throw new TypeError();
      }
    } catch(err) {
      process.nextTick(function() {
        cb(new TypeError('item.h.pe must be a string'));
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
 * handled merge (that originated from a remote) in the local tree.
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
        that._local.lastByPerspective(pe, 'base64', function(err, v) {
          if (err) { cb(err); return; }

          // set every copied item from src in conflict except this one from the stage, to
          // ensure all other heads are in conflict. this is to ensure only one
          // non-conflicting head in the local tree
          function transform(item2, cb2) {
            item2.h.c = true;
            cb2(null, item2);
          }

          // see mergeSrcWithLocal, if there is a parent, the first parent points to a version that exists in the perspective it came from
          var opts = { last: header.v, excludeLast: true, transform: transform };
          if (v) {
            opts.first = v;
            opts.excludeFirst = true;
          }
          MergeTree._copyTo(that._pe[pe], that._local, opts, function(err) {
            if (err) { cb(err); return; }

            // copy last item from the stage as well
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
 * Merge src tree with the local tree. For every newly created merge (ff or 3-
 * way), copy the merge to staging and call this._mergeHandler (provided as an
 * option to the constructor). If there is a conflict, skip the item.
 *
 * The mergeHandler is called with the new merge and the previous local version.
 *
 * It is assumed that each DAG in the local tree has exactly one head. Only
 * heads of src are attempted to be merged.
 *
 * Note: no new items are inserted in the src or local trees, only merge items
 * in the staging tree.
 *
 * Note2: an item from src can be a merge that is a fast-forward for the local
 * tree. So a merge in stage does not mean it is locally created.
 *
 * @param {Object} stree  source tree to merge
 * @param {Function} cb  First parameter will be an error object or null.
 */
MergeTree.prototype.mergeWithLocal = function mergeWithLocal(stree, cb) {
  if (!t.isObject(stree)) { throw new TypeError('stree must be an object'); }
  if (!t.isFunction(cb)) { throw new TypeError('cb must be a function'); }

  var stage = this._stage;

  var that = this;

  // get the corresponding head in the local tree, assume there is only one (non-
  // deleted and non-conflicting)
  var lastId, prev;
  function iterator(merge, sitem, ditem, next) {
    if (lastId !== ditem.h.id) {
      // new id, merge any previous items
      lastId = ditem.h.id;
      prev = [];
      that._log.err('mergeWithLocal more than one local head %j %j', lhead, dhead);
      next(new Error('multiple heads in local DAG encounterd'));
      return;
    }
    var lhead;
    that._local.getHeads(headOpts, function(dhead, dnext) {
      if (lhead) {
      }
      lhead = dhead;
      dnext();
    }, function(err) {
      if (err) { cb(err); return; }

      that._merge(sitem, lhead, stree, that._local, function(err, merged) {
        if (err) { cb(err); return; }

        if (Array.isArray(merged)) {
          // merge conflict, do nothing
          that._log.warn('mergeWithLocal merge conflict %s %j %j', merged, sitem, lhead);
          snext();
          return;
        }

        // insert merge in staging and call mergeHandler with the new merge and the old local perspective
        stage.write(merged[1], function(err) {
          if (err) {
            // write error in stage
            that._log.err('mergeWithLocal stage write error %j %j %j %s', sitem, lhead, merged, err);
            snext(err);
            return;
          }
          that._mergeHandler(merged[1], lhead, snext);
        });
      });
    });
  }

  MergeTree._mergeTrees(stree, this._local, iterator, cb);
};



/////////////////////
//// PRIVATE API ////
/////////////////////



/**
 * Merge src tree with dst tree. Find the last version of src tree that is in
 * dst tree and try to merge each head in src (in insertion order) with every
 * non-deleted head of the DAG in dst tree.
 *
 * @param {Object} stree  source tree to merge
 * @param {Object} dtree  dest tree to merge with
 * @param {Function} iterator  function(merge, spa, dpa, next) called with
 *                             merge, and both src and dst items this merge is
 *                             based upon. Fourth parameter is a next handler.
 * @param {Function} cb  First parameter will be an error object or null.
 */
MergeTree.prototype._mergeTrees = function _mergeTrees(stree, dtree, iterator, cb) {
  if (!t.isObject(stree)) { throw new TypeError('stree must be an object'); }
  if (!t.isObject(dtree)) { throw new TypeError('dtree must be an object'); }
  if (!t.isFunction(iterator)) { throw new TypeError('iterator must be a function'); }
  if (!t.isFunction(cb)) { throw new TypeError('cb must be a function'); }

  var that = this;

  var headOpts = { skipDeletes: true };

  MergeTree._iterateMissing(stree, dtree, function(sitem, snext) {
    headOpts.id = sitem.h.id;
    // is this a head in stree?
    stree.getHeads(headOpts, function(shead, shnext) {
      if (shead.h.v !== sitem.h.v) {
        shnext();
        return;
      }

      // get any corresponding head(s) in dtree
      dheads = [];
      dtree.getHeads(headOpts, function(dhead, dnext) {
        dheads.push(dhead);
        dnext();
      }, function(err) {
        if (err) { shnext(err); return; }

        if (!dheads.length) {
          // new item, fast-forward
          iterator(shead, null, null, shnext);
          return;
        }

        // merge all dheads
        var first = dheads.shift();
        var pa = [];
        pa.push(first.h.v);
        async.reduce(dheads, first, function(memo, head, cb2) {
          that._merge(memo, head, function(err, merge) {
            if (err) { cb2(err); return; }
            pa.push(head.h.v);
            iterator(merge, memo, head, function(err) {
              cb2(err, merge[0]);
            });
          });
        }, function(err, dmerge) {
          if (err) { shnext(err); return; }
          // merge result with shead
          that._merge(shead, dmerge[0], function(err, merge) {
            if (err) { shnext(err); return; }
            pa.push(shead.h.v);
            merge[0].h.pa = pa;
            merge[1].h.pa = pa;
            iterator(merge, shead, dmerge[0], shnext);
          });
        });
      });
    }, snext);
  }, cb);
};

/**
 * Start iterating over src starting at the last version of stree that is in dtree.
 *
 * Ensures insertion order of src.
 *
 * @param {Object} stree  source tree
 * @param {Object} dtree  destination tree
 * @param {Function} iterator  function(item, next) called with src item and
 *                             next handler
 * @param {Function} cb  First parameter will be an error object or null.
 */
MergeTree._iterateMissing = function _iterateMissing(stree, dtree, iterator, cb) {
  if (!t.isObject(stree)) { throw new TypeError('stree must be an object'); }
  if (!t.isObject(dtree)) { throw new TypeError('dtree must be an object'); }
  if (!t.isFunction(iterator)) { throw new TypeError('iterator must be a function'); }
  if (!t.isFunction(cb)) { throw new TypeError('cb must be a function'); }

  // determine offset
  dtree.lastByPerspective(stree.name, 'base64', function(err, v) {
    if (err) { cb(err); return; }

    var opts = {};
    if (v) {
      opts.first = v;
      opts.excludeFirst = true;
    }
    stree.iterateInsertionOrder(opts, iterator, cb);
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
MergeTree._copyTo = function _copyTo(stree, dtree, opts, cb) {
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
  stree.iterateInsertionOrder({ first: opts.first }, function(item, next) {
    if (first) {
      first = false;
      if (opts.first && item.h.v === opts.first && opts.excludeFirst) {
        next();
        return;
      }
    }

    if (opts.last && item.h.v === opts.last && opts.excludeLast) {
      next(null, false);
      return;
    }

    opts.transform(item, function(err, nitem) {
      if (err) { next(err); return; }
      if (nitem === null || typeof nitem === 'undefined') {
        next();
        return;
      }

      // allow one error without bubbling up
      dtree.once('error', function() {});

      dtree.write(nitem, function(err) {
        if (err) { next(err); return; }
        if (opts.last && opts.last === item.h.v) {
          next(null, false);
        } else {
          next();
        }
      });
    });
  }, cb);
};

/**
 * Merge two versions using a recursive three-way merge strategy.
 * 1. find the lowest common ancestor(s) (by perspective) if there is more than
 *    one lca, recurse
 * 2. do a three-way-merge of the two versions with the lca
 *
 * Note: if perspectives of the given items are different, two merged items will
 * The first merge matches the perspective of sX and the second merge matches
 * the perspective of sY.
 *
 * If a merged item has a h.v property, it's a fast-forward to an item that is
 * already saved. If it has a ._id._v but no ._m3 it's a
 * replayed fast-forward that is not saved yet. And if it misses both it's a
 * genuine merge that's not saved either.
 *
 * TODO: use memoization for recursively generated virtual merges
 *
 * @param {StreamTree} sX  readable stream that emits vertices from leaf to root
 * @param {StreamTree} sY  readable stream that emits vertices from leaf to root
 * @param {Object} [opts]  object containing configurable parameters
 * @param {Function} cb  first parameter will be an error object or null, second
 *                       parameter will be an array with merged items
 *
 * opts:
 *   fnv {Function}  function that extracts a vertice identifier (version) and
 *                   it's parents from a vertex in the format:
 *                     { v: ..., pa: [...] }
 *   fnb {Function}  function that extracts the data associated with an item
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 */
MergeTree.prototype._merge = function _merge(itemX, itemY, treeX, treeY, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = null;
  }

  if (sX == null || typeof sX !== 'object') { throw new TypeError('sX must be a StreamTree'); }
  if (sY == null || typeof sY !== 'object') { throw new TypeError('sY must be a StreamTree'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }
  if (opts.fnv != null && typeof opts.fnv !== 'function') { throw new TypeError('opts.fnv must be a function'); }
  if (opts.fnb != null && typeof opts.fnb !== 'function') { throw new TypeError('opts.fnb must be a function'); }
  if (opts.log != null && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }

  var fnv = opts.fnv;
  var fnb = opts.fnb;

  merge._log = opts.log || { emerg: noop, alert: noop, crit: noop, err: noop, warning: noop, notice: noop, info: noop, debug: noop, debug2: noop, getFileStream: noop, getErrorStream: noop, close: noop };

  var error;
  var that = this;

  if (!isEqual(objX._id._id, objY._id._id)) {
    error = new Error('merge id mismatch');
    merge._log.err('merge %s', err);
    process.nextTick(function() {
      cb(err, null);
    });
    return;
  }

  // find lca(s) and resolve versions to items
  var sX = treeX.createReadStream({ id: itemX.h.id, first: itemX.h.v, reverse: true });
  var sY = treeY.createReadStream({ id: itemY.h.id, first: itemY.h.v, reverse: true });

  findLCAs(sX, sY, function(err, lcas) {
    if (err) { cb(err); return; }

    if (lcas.length < 1) {
      error = new Error('no lca found');
      that._log.err('vc _merge %s');
      cb(error);
      return;
    }

    // recurse, untill one lca is left
    var firstLca = lcas.shift();
    async.reduce(lcas, firstLca, function(prevLca, currLca, cb2) {
      // get lcas from both trees
      treeX.getByVersion(prevLca, function(err, lcaX) {
        if (err) { cb2(err); return; }

        treeY.getByVersion(currLca, function(err, lcaY) {
          if (err) { cb2(err); return; }

          that._log.err('vc _merge recursive merge lcaX: %j, lcaY: %j, lcas: %j', lcaX, lcaY, lcas);
          that._merge(lcaX, lcaY, treeX, treeY, cb2);
        });
      });
    }, doMerge);
  });
};

/**
 * Merge two items by fast-forward or three-way-merge:
 *  - if both versions are equal, fast-forward with one item  
 *  - if the lca version equals the version of one of the items, fast-forward to the other item
 *    with one fast-forwarded item per perspective (by recreating one fast-forward)
 *  - in all other cases create two merged items (one per perspective)
 *
 * @param {StreamTree} sX  readable stream that emits vertices from leaf to root
 * @param {StreamTree} sY  readable stream that emits vertices from leaf to root
 */
MergeTree._doMerge = function _doMerge(itemX, itemY, lcaX, lcaY) {
  this._log.err('vc _doMerge itemX: %j, itemY: %j, lcaX: %j, lcaY: %j', itemX, itemY, lcaX, lcaY);

  var merged;

  // if versions are equal, fast-forward with original items
  if (itemX.h.v === itemY.h.v) {
    return [itemX, itemY];
  }

  // if lca equals one item, fast-forward to the other item and create a merged fast-forward for the missing perspective
  if (lcaX.id.v === itemX.id.v) {
    // ff to original itemY and recreate itemY from the other perspective
    merged = VersionedCollection._threeWayMerge(itemX.b, itemY.b, lcaX.b, lcaY.b);

    if (Array.isArray(merge)) {
      that._log.err('vc _merge error', merged, 'itemX', JSON.stringify(itemXid), 'itemY', JSON.stringify(itemYid));
      cb(new Error('merge conflict'), merged, perspectives);
      return;
    }

  } else if (lcaY.id.v === itemY.id.v) {
    // ff to original itemX and recreate itemX from the other perspective
    if (itemX._id._pe === lcaIds[0]._pe) {
      merge = VersionedCollection._threeWayMerge(itemY, itemX, lcas[1], lcas[0]);
    } else {
      merge = VersionedCollection._threeWayMerge(itemY, itemX, lcas[0], lcas[1]);
    }

    merged = [itemX, merge];

    if (Array.isArray(merge)) {
      that._log.err('vc _merge error', merged, 'itemX', JSON.stringify(itemXid), 'itemY', JSON.stringify(itemYid));
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
    that._log.err('vc _merge error', merged, 'itemX', JSON.stringify(itemXid), 'itemY', JSON.stringify(itemYid));
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
};
