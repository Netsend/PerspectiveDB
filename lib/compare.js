/**
 * Copyright 2014, 2016 Netsend.
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

// Compare all items of one db with another db
'use strict';

var Transform = require('stream').Transform;
var util = require('util');

var xtend = require('xtend');

/**
 * Compare two items on equality (test certain attributes).
 *
 * @param {Object} item1  item
 * @param {Object} item2  another item
 * @param {Object} includeAttrs  keys to include
 * @return {Boolean} true if equal, false if inequal
 */
function compareItemsInclude(item1, item2, includeAttrs) {
  if (typeof includeAttrs !== 'object') { throw new TypeError('includeAttrs must be an object'); }

  function comparator(key) {
    if (!includeAttrs[key]) { return true; }
    return JSON.stringify(item1[key]) === JSON.stringify(item2[key]);
  }

  var keys1 = Object.keys(item1);
  var keys2 = Object.keys(item2);

  return keys1.every(comparator) && keys2.every(comparator);
}

/**
 * Compare two items on equality (ignore certain attributes).
 *
 * @param {Object} item1  item
 * @param {Object} item2  another item
 * @param {Object} excludeAttrs  keys to exclude
 * @return {Boolean} true if equal, false if inequal
 */
function compareItemsExclude(item1, item2, excludeAttrs) {
  if (typeof excludeAttrs !== 'object') { throw new TypeError('excludeAttrs must be an object'); }

  function comparator(key) {
    if (excludeAttrs[key]) { return true; }
    return JSON.stringify(item1[key]) === JSON.stringify(item2[key]);
  }

  var keys1 = Object.keys(item1);
  var keys2 = Object.keys(item2);

  return keys1.every(comparator) && keys2.every(comparator);
}

/**
 * Compare two trees.
 *
 * @param {Tree} tree1  first perspective
 * @param {Tree} tree2  second perspective
 * @param {Object} [opts]  options
 *
 * options:
 *   debug {Boolean}  whether or not to show debugging info
 *   includeAttrs {Object}  list of keys to include for comparison
 *   excludeAttrs {Object}  list of keys to exclude for comparison
 */
function Comparator(tree1, tree2, opts) {
  if (tree1 == null || typeof tree1 !== 'object') { throw new TypeError('tree1 must be an object'); }
  if (tree2 == null || typeof tree2 !== 'object') { throw new TypeError('tree2 must be an object'); }
  if (opts == null || typeof opts !== 'object') { throw new TypeError('opts must be an object'); }

  opts = opts || {};
  Transform.call(this, xtend(opts, { objectMode: true }));

  this._tree1 = tree1;
  this._tree2 = tree2;
  this._opts = opts;

  this._stats = { missing: [], inequal: [], equal: [], multiple: [] };

  this._includeAttrs = !!Object.keys(opts.includeAttrs).length;
  this._excludeAttrs = !!Object.keys(opts.excludeAttrs).length;

  if (this._includeAttrs && this._excludeAttrs) { throw new Error('includeAttrs and excludeAttrs can not be combined'); }
}
util.inherits(Comparator, Transform);

Comparator.prototype._transform = function _transform(item, encoding, cb) {
  // find this item in tree2 and compare
  var items = [];

  var that = this;
  this._tree2.getHeads({ id: item.h.id }, function(head, next) {
    items.push(head);
    next();
  }, function(err) {
    if (err) { cb(err); return; }

    if (that._opts.debug) { console.log('compare items tree2 %d', items.length); }

    if (items.length > 1) {
      that._stats.multiple.push({ item1: item, items2: items });
      cb();
      return;
    }

    if (items.length < 1) {
      that._stats.missing.push(item);
      cb();
      return;
    }

    var item2 = items[0];

    if (that._includeAttrs) {
      if (compareItemsInclude(item.b, item2.b, that._opts.includeAttrs)) {
        that._stats.equal.push({ item1: item, item2: item2 });
      } else {
        that._stats.inequal.push({ item1: item, item2: item2 });
      }
    } else {
      if (compareItemsExclude(item.b, item2.b, that._opts.excludeAttrs)) {
        that._stats.equal.push({ item1: item, item2: item2 });
      } else {
        that._stats.inequal.push({ item1: item, item2: item2 });
      }
    }

    cb();
  });
};

Comparator.prototype._flush = function _flush(cb) {
  if (this._opts.debug) { console.log('compare stats', this._stats); }
  this.push(this._stats);
  cb();
};

/**
 * Compare two trees.
 *
 * @param {Tree} tree1  first perspective
 * @param {Tree} tree2  second perspective
 * @param {Object} [opts]  options
 * @param {Function} cb  First parameter will be an error or null. Second parameter
 *                       will be an object containing missing, inequal, equal and
 *                       multiple arrays that contain the items from the trees.
 *
 * options:
 *   debug {Boolean}  whether or not to show debugging info
 *   includeAttrs {Object}  list of keys to include for comparison
 *   excludeAttrs {Object}  list of keys to exclude for comparison
 */
function compare(tree1, tree2, opts, cb) {
  if (tree1 == null || typeof tree1 !== 'object') { throw new TypeError('tree1 must be an object'); }
  if (tree2 == null || typeof tree2 !== 'object') { throw new TypeError('tree2 must be an object'); }

  if (typeof opts === 'function') {
    cb = opts;
    opts = {};
  }

  opts = opts || {};
  if (opts == null || typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  opts = xtend({
    includeAttrs: {},
    excludeAttrs: {},
  }, opts);

  if (typeof opts.includeAttrs !== 'object') { throw new TypeError('opts.includeAttrs must be an object'); }
  if (typeof opts.excludeAttrs !== 'object') { throw new TypeError('opts.excludeAttrs must be an object'); }

  var comparator = new Comparator(tree1, tree2, opts);

  //tree1.createReadStream().pipe(comparator);

  var error, stats;
  comparator.on('readable', function() {
    var results = comparator.read();
    if (results) {
      stats = results;
    } else {
      cb(error, stats);
    }
  });
  comparator.on('error', cb);

  tree1.getHeads(function(item, next) {
    if (comparator.write(item)) {
      next();
    } else {
      comparator.once('drain', next);
    }
  }, function(err) {
    if (err) { error = err; }
    comparator.end();
  });
}

module.exports = compare;
