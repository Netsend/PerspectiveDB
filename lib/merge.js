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

/* jshint -W116 */

'use strict';

var async = require('async');
var isEqual = require('is-equal');

var findLCAs = require('./find_lcas');
var threeWayMerge = require('./three_way_merge');

var noop = function() {};

/**
 * Merge two items by fast-forward or three-way-merge:
 *  - if both versions are equal, return both items
 *  - if the lca version equals the version of itemX or itemY, fast-forward to the other item
 *  - in all other cases create two merged items
 *
 * @param {Object} itemX  item to merge with itemY
 * @param {Object} itemY  item to merge with itemX
 * @param {Object} lcaX  lca from the tree of itemX
 * @param {Object} lcaY  lca from the tree of itemY (possibly the same as the tree of itemX)
 * @param {Object} [opts]  object containing configurable parameters
 * @return {Array} array with two items. item 1 is merge based on lcaX, item 2 is merge based on lcaY
 * @throws lca version mismatch
 * @throws merge conflict
 */
function _doMerge(itemX, itemY, lcaX, lcaY, opts) {
  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }
  if (opts.log != null && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }

  var log = opts.log || { emerg: noop, alert: noop, crit: noop, err: noop, warning: noop, notice: noop, info: noop, debug: noop, debug2: noop, getFileStream: noop, getErrorStream: noop, close: noop };

  log.err('_doMerge itemX: %j, itemY: %j, lcaX: %j, lcaY: %j', itemX, itemY, lcaX, lcaY);

  var mergeX, mergeY;

  // if lca versions don't match, error
  if (lcaX.h.v !== lcaY.h.v) {
    throw new Error('lca version mismatch');
  }

  var lcaVersion = lcaX.h.v;

  // if versions are equal, return original items
  if (itemX.h.v === itemY.h.v) {
    return [itemX, itemY];
  }

  // if lca equals one item, fast-forward to the other item and create a merged fast-forward for the missing perspective
  if (lcaVersion === itemX.h.v) {
    // ff to itemY and recreate itemY from the other perspective
    mergeX = threeWayMerge(itemX.b, itemY.b, lcaX.b, lcaY.b);

    if (Array.isArray(mergeX)) {
      log.err('merge ff itemX to itemY, conflict %s', mergeX);
      throw new Error('merge conflict');
    }

    return [mergeX, itemY];
  }

  if (lcaVersion === itemY.id.v) {
    // ff to itemX and recreate itemX from the other perspective
    mergeY = threeWayMerge(itemY.b, itemX.b, lcaY.b, lcaX.b);

    if (Array.isArray(mergeY)) {
      log.err('merge ff itemY to itemX, conflict %s', mergeY);
      throw new Error('merge conflict');
    }

    return [itemX, mergeY];
  }

  // merge from both perspectives
  mergeX = threeWayMerge(itemX.b, itemY.b, lcaX.b, lcaY.b);
  if (Array.isArray(mergeX)) {
    log.err('merge three way merge, conflict %s', mergeX);
    throw new Error('merge conflict');
  }

  mergeY = threeWayMerge(itemY.b, itemX.b, lcaY.b, lcaX.b);
  if (Array.isArray(mergeY)) {
    log.err('merge three way merge, conflict %s', mergeY);
    throw new Error('merge conflict');
  }

  // create new headers
  var headerX = {
    id: itemX.h.id,
    pa: []
  };

  var headerY = {
    id: itemY.h.id,
    pa: []
  };

  // only set delete property on merged item if both itemX and itemY have it set
  if (itemX.h.d && itemY.h.d) {
    headerX.d = true;
    headerY.d = true;
  }

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

  return [{ h: headerX, b: mergeX }, { h: headerY, b: mergeY }];
}

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
 * If a merged item has a h.v property, it's a fast-forward to an item that
 * already exists in at least one of the trees. If no version is set it's a
 * newly created merge.
 *
 * @param {Object} itemX  item to merge with itemY
 * @param {Object} itemY  item to merge with itemX
 * @param {Tree} treeX  tree object that contains itemX
 * @param {Tree} treeY  tree object that contains itemY
 * @param {Object} [opts]  object containing configurable parameters
 * @param {Function} cb  first parameter will be an error object or null, second
 *                       parameter will be mergeX, third item will be mergeY.
 *
 * opts:
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 */
function merge(itemX, itemY, treeX, treeY, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = null;
  }

  if (itemX == null || typeof itemX !== 'object') { throw new TypeError('itemX must be an object'); }
  if (itemY == null || typeof itemY !== 'object') { throw new TypeError('itemY must be an object'); }
  if (treeX == null || typeof treeX !== 'object') { throw new TypeError('treeX must be an object'); }
  if (treeY == null || typeof treeY !== 'object') { throw new TypeError('treeY must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }
  if (opts.log != null && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }

  var log = opts.log || { emerg: noop, alert: noop, crit: noop, err: noop, warning: noop, notice: noop, info: noop, debug: noop, debug2: noop, getFileStream: noop, getErrorStream: noop, close: noop };

  log.debug('merge itemX: %j, itemY: %j', itemX.h, itemY.h);

  var error;

  if (!isEqual(itemX.h.id, itemY.h.id)) {
    error = new Error('merge id mismatch');
    log.err('merge %s', error);
    process.nextTick(function() {
      cb(error, null);
    });
    return;
  }

  // find lca(s) and resolve versions to items
  var sX = treeX.createReadStream({ id: itemX.h.id, last: itemX.h.v, reverse: true });
  var sY = treeY.createReadStream({ id: itemY.h.id, last: itemY.h.v, reverse: true });

  var findLcaOpts = {
    log: log,
    fnv: function(item) {
      return { v: item.h.v, pa: item.h.pa };
    }
  };
  findLCAs(sX, sY, findLcaOpts, function(err, lcas) {
    if (err) { cb(err); return; }

    if (lcas.length < 1) {
      error = new Error('no lca found');
      log.err('merge error %s', error);
      cb(error);
      return;
    }

    if (lcas.length > 1) {
      // recurse, untill one lca is left
      // TODO: fix
      async.reduce(lcas, lcas[0], function(prevLca, currLca, cb2) {
        if (!currLca) {
          cb2(new Error());
        }

        // get lcas from both trees
        treeX.getByVersion(prevLca, function(err, lcaX) {
          if (err) { cb2(err); return; }
          if (!lcaX) {
            error = new Error('could not resolve lca');
            log.err('merge tree x misses lca: %s', prevLca);
            cb2(error);
            return;
          }

          log.debug('merge recursive resolved lcaX: %j', lcaX);

          treeY.getByVersion(currLca, function(err, lcaY) {
            if (err) { cb2(err); return; }
            if (!lcaY) {
              error = new Error('could not resolve lca');
              log.err('merge tree y misses lca: %s', currLca);
              cb2(error);
              return;
            }

            log.debug('merge recursive resolved lcaY: %j', lcaY);

            if (prevLca !== currLca) {
              merge(lcaX, lcaY, treeX, treeY, function(err, mergeX, mergeY) {
                if (err) { cb2(err); return; }
                cb2(null, lcaX, lcaY);
              });
            } else {
              cb2(null, lcaX, lcaY);
            }
          });
        });
      }, function(err, lcaX, lcaY) {
        log.debug('merge recursive lcaX: %j, lcaY: %j, lcas: %j', lcaX, lcaY, lcas);
        console.log('JAA', lcaX, lcaY);
        if (err) { cb(err); return; }
        try {
          var merge = _doMerge(itemX, itemY, lcaX, lcaY);
          cb(null, merge[0], merge[1]);
        } catch(err) {
          cb(err);
        }
      });
    } else {
      // get lca from both trees
      treeX.getByVersion(lcas[0], function(err, lcaX) {
        if (err) { cb(err); return; }
        if (!lcaX) {
          error = new Error('could not resolve lca');
          log.err('merge tree x misses lca: %s', lcas);
          cb(error);
          return;
        }

        log.debug('merge resolved lcaX: %j', lcaX);

        treeY.getByVersion(lcas[0], function(err, lcaY) {
          if (err) { cb(err); return; }
          if (!lcaY) {
            error = new Error('could not resolve lca');
            log.err('merge tree y misses lca: %s', lcas);
            cb(error);
            return;
          }

          log.debug('merge resolved lcaY: %j', lcaY);

          try {
            var merge = _doMerge(itemX, itemY, lcaX, lcaY);
            cb(null, merge[0], merge[1]);
          } catch(err) {
            cb(err);
          }
        });
      });
    }
  });
}

module.exports = merge;