/**
 * Copyright 2015 Netsend.
 *
 * This file is part of PersDB.
 *
 * PersDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PersDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PersDB. If not, see <https://www.gnu.org/licenses/>.
 */

/* jshint -W116 */

'use strict';

var async = require('async');

var findLCAs = require('./find_lcas');
var threeWayMerge = require('./three_way_merge');

var noop = require('./noop');

/**
 * MergeConflict, prototypally inherits from the Error constructor
 *
 * Optionally set a "conflict" property that lists all conflicting attributes.
 *
 * @param {Array} [conflict]  list of conflicting attributes
 */
function MergeConflict(conflict) {
  if (conflict != null && !Array.isArray(conflict)) { throw new TypeError('conflict must be an array if specifed'); }

  this.name = 'MergeConflict';
  this.message = 'merge conflict';
  this.conflict = conflict || [];
  this.stack = (new Error()).stack;
}
MergeConflict.prototype = Object.create(Error.prototype);
MergeConflict.prototype.constructor = MergeConflict;

/**
 * Merge two items by fast-forward or three-way-merge:
 *  - if both versions are equal, return original items
 *  - if the lca version equals the version of itemX or itemY, fast-forward to the other item.
 *    The original item (with h.i) and the newly created fast forwarded item (without h.i) is returned.
 *  - in all other cases create two merged items (without h.v and h.i), bound by lca
 *
 * @param {Object} itemX  item to merge with itemY
 * @param {Object} itemY  item to merge with itemX
 * @param {Object} lcaX  lca from the tree of itemX
 * @param {Object} lcaY  lca from the tree of itemY (possibly the same as the tree of itemX)
 * @return {Array} array with two items. item 1 is merge based on lcaX, item 2 is merge based on lcaY
 * @throws lca version mismatch
 * @throws Error if lca versions mismatch
 * @throws MergeConflict if there is a conflict
 */
function _doMerge(itemX, itemY, lcaX, lcaY) {
  if (itemX == null || typeof itemX !== 'object' || Array.isArray(itemX)) { throw new TypeError('itemX must be an object'); }
  if (itemY == null || typeof itemY !== 'object' || Array.isArray(itemY)) { throw new TypeError('itemY must be an object'); }
  if (lcaX == null || typeof lcaX !== 'object' || Array.isArray(lcaX)) { throw new TypeError('lcaX must be an object'); }
  if (lcaY == null || typeof lcaY !== 'object' || Array.isArray(lcaY)) { throw new TypeError('lcaY must be an object'); }

  // if lca versions don't match, error
  if (lcaX.h.v !== lcaY.h.v) {
    throw new Error('lca version mismatch');
  }

  var lcaVersion = lcaX.h.v;

  // if versions are equal, return original items
  if (itemX.h.v === itemY.h.v) {
    return [itemX, itemY];
  }

  var headerX, headerY, mergeX, mergeY;

  // create new headers
  headerX = {
    id: itemX.h.id,
    pa: []
  };

  headerY = {
    id: itemY.h.id,
    pa: []
  };

  // only set delete property on merged item if both itemX and itemY have it set
  if (itemX.h.d && itemY.h.d) {
    headerX.d = true;
    headerY.d = true;
  }

  // if lca version is set and equals one item, fast-forward to the other item and create a merged fast-forward for the missing perspective
  if (lcaVersion != null && lcaVersion === itemX.h.v) {
    // ff to itemY and recreate itemY from the other perspective
    mergeX = threeWayMerge(itemX.b || {}, itemY.b || {}, lcaX.b || {}, lcaY.b || {});

    if (Array.isArray(mergeX)) {
      throw new MergeConflict(mergeX);
    }

    // copy version
    headerX.v = itemY.h.v;

    // if deleted, copy property
    if (itemY.h.d) {
      headerX.d = true;
    }

    // copy parents from itemY
    Array.prototype.push.apply(headerX.pa, itemY.h.pa);

    return [{ h: headerX, b: mergeX }, itemY];
  }

  if (lcaVersion != null && lcaVersion === itemY.h.v) {
    // ff to itemX and recreate itemX from the other perspective
    mergeY = threeWayMerge(itemY.b || {}, itemX.b || {}, lcaY.b || {}, lcaX.b || {});

    if (Array.isArray(mergeY)) {
      throw new MergeConflict(mergeY);
    }

    // copy version
    headerY.v = itemX.h.v;

    // if deleted, copy property
    if (itemX.h.d) {
      headerY.d = true;
    }

    // copy parents from itemX
    Array.prototype.push.apply(headerY.pa, itemX.h.pa);

    return [itemX, { h: headerY, b: mergeY }];
  }

  // not a fast forward but a three-way-merge

  // use item versions as parent, if item has no version, it is a virtual merge, use it's parents instead
  if (itemX.h.v) {
    headerX.pa.push(itemX.h.v);
    headerY.pa.push(itemX.h.v);
  } else {
    Array.prototype.push.apply(headerX.pa, itemX.h.pa);
    Array.prototype.push.apply(headerY.pa, itemX.h.pa);
  }

  if (itemY.h.v) {
    headerX.pa.push(itemY.h.v);
    headerY.pa.push(itemY.h.v);
  } else {
    Array.prototype.push.apply(headerX.pa, itemY.h.pa);
    Array.prototype.push.apply(headerY.pa, itemY.h.pa);
  }

  mergeX = threeWayMerge(itemX.b || {}, itemY.b || {}, lcaX.b, lcaY.b);
  if (Array.isArray(mergeX)) {
    throw new MergeConflict(mergeX);
  }

  mergeY = threeWayMerge(itemY.b || {}, itemX.b || {}, lcaY.b, lcaX.b);
  if (Array.isArray(mergeY)) {
    throw new MergeConflict(mergeY);
  }

  return [{ h: headerX, b: mergeX }, { h: headerY, b: mergeY }];
}

/**
 * Merge two versions using a recursive three-way merge strategy.
 * 1. find the lowest common ancestor(s) for x and y
 * 2. if there is more than one lca, until there is one left
 * 3. do a three-way-merge of the two versions with the one lca
 *
 * If a merged item has a h.v and h.i property, it's a fast-forward to an item
 * that is already in the tree. If the item has only a h.v but no h.i, it's a
 * newly created merge that exists in the other tree (fast-forward). If a merge has
 * no h.v and no h.i, it is a newly created merge.
 *
 * @param {stream.Readable} sX  readable stream that emits vertices from leaf to root
 * @param {stream.Readable} sY  readable stream that emits vertices from leaf to root
 * @param {Object} [opts]  object containing configurable parameters
 * @param {Function} cb  first parameter will be an error object or null, second
 *                       parameter will be mergeX, third item will be mergeY.
 *
 * opts:
 *   fnv {Function}  function that extracts a vertice identifier and it's
 *                   parents from a vertex in the format: { v: ..., pa: [...] }
 *   rootX {Object}  root object for sX, instead of the first emitted object of sX
 *   rootY {Object}  root object for sY, instead of the first emitted object of sY
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 */
function merge(sX, sY, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = null;
  }

  if (sX == null || typeof sX !== 'object') { throw new TypeError('sX must be a stream.Readable'); }
  if (sY == null || typeof sY !== 'object') { throw new TypeError('sY must be a stream.Readable'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }
  if (opts.log != null && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }

  var log = opts.log || { emerg: noop, alert: noop, crit: noop, err: noop, warning: noop, notice: noop, info: noop, debug: noop, debug2: noop, getFileStream: noop, getErrorStream: noop, close: noop };

  var fnv = opts.fnv || function(item) { return { v: item.h.v, pa: item.h.pa }; };

  var error;

  // if no version exists, use a fixed randomly generated 95 bit key (16 characters out of a set of 62 characters)
  var tmpvX = 'kJzSOZShrdvSwhAd';
  var tmpvY = 'VZFBSVDYLDA6hA4R';

  var findLcaOpts = {
    log: log,
    fnv: fnv,
    rootX: opts.rootX,
    rootY: opts.rootY
  };

  findLCAs(sX, sY, findLcaOpts, function(err, lcas, lcasX, lcasY, itemX, itemY) {
    if (err) { cb(err); return; }

    if (lcas.length < 1) {
      error = new Error('no lca found');
      log.err('merge error %s', error);
      cb(error);
      return;
    }

    log.debug('merge lcas: %j, lcasX: %j, lcasY: %j, itemX: %j, itemY:', lcas, lcasX, lcasY, itemX, itemY);

    var prevLcaX;
    var prevLcaY;

    // merge with any previous lca, until one is left
    async.eachSeries(lcas, function(lca, cb2) {
      var lcaX = lcasX[lca];
      var lcaY = lcasY[lca];

      if (prevLcaX) {
        log.debug('merge recurse');

        var findLcaOpts2 = {
          log: log,
          fnv: fnv,
          rootX: {
            h: {
              v: prevLcaX.h.v || tmpvX,
              pa: prevLcaX.h.pa
            },
            b: prevLcaX.b
          },
          rootY: {
            h: {
              v: lcaY.h.v || tmpvY,
              pa: lcaY.h.pa
            },
            b: lcaY.b
          }
        };
        merge(sX.reopen(), sY.reopen(), findLcaOpts2, function(err, mergeX, mergeY) {
          if (err) { cb2(err); return; }
          log.debug('merge intermediate lca result x: %j, y: %j', mergeX.h, mergeY.h);
          prevLcaX = mergeX;
          prevLcaY = mergeY;
          cb2();
        });
      } else {
        prevLcaX = lcaX;
        prevLcaY = lcaY;
        process.nextTick(cb2);
      }
    }, function(err) {
      if (err) { cb(err); return; }
      var merged;
      try {
        log.debug('_doMerge itemX: %j, itemY: %j, lcaX: %j, lcaY: %j', itemX, itemY, prevLcaX, prevLcaY);
        merged = _doMerge(itemX, itemY, prevLcaX, prevLcaY);
        log.debug('merge result x: %j, y: %j', merged[0], merged[1]);
      } catch(err) {
        log.err('merge error: %s', err);
        cb(err);
        return;
      }
      cb(null, merged[0], merged[1], lcas);
    });
  });
}

merge._doMerge = _doMerge;

module.exports = merge;
