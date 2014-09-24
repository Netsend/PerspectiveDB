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

// Compare all items of one collection with another collection
'use strict';

var mongodb = require('mongodb');

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
 * Compare two collections.
 *
 * @param {mongodb.Collection} coll1  first collection
 * @param {mongodb.Collection} coll2  second collection
 * @param {Object} [opts]  options
 * @param {Function} cb  first parameter will be an error or null. second parameter
 *                       will be an object containing missing, inequal, equal,
 *                       multiple and unknown arrays that contain the items from
 *                       the collections.
 *
 * options:
 *   debug {Boolean}  whether or not to show debugging info
 *   includeAttrs {Object}  list of keys to include for comparison
 *   excludeAttrs {Object}  list of keys to exclude for comparison
 *   matchAttrs {Object}  list of keys that should match
 */
function compare(coll1, coll2, opts, cb) {
  if (!(coll1 instanceof mongodb.Collection)) { throw new TypeError('coll1 must be a mongodb.Collection'); }
  if (!(coll2 instanceof mongodb.Collection)) { throw new TypeError('coll2 must be a mongodb.Collection'); }

  if (typeof opts === 'function') {
    cb = opts;
    opts = {};
  }

  opts = opts || {};
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  opts.includeAttrs = opts.includeAttrs || {};
  if (typeof opts.includeAttrs !== 'object') {
    throw new TypeError('opts.includeAttrs must be an object');
  }

  opts.excludeAttrs = opts.excludeAttrs || {};
  if (typeof opts.excludeAttrs !== 'object') {
    throw new TypeError('opts.excludeAttrs must be an object');
  }

  opts.matchAttrs = opts.matchAttrs || {};
  if (typeof opts.matchAttrs !== 'object') {
    throw new TypeError('opts.matchAttrs must be an object');
  }

  var includeAttrs = !!Object.keys(opts.includeAttrs).length;
  var excludeAttrs = !!Object.keys(opts.excludeAttrs).length;
  var matchAttrs = !!Object.keys(opts.matchAttrs).length;

  if (includeAttrs && excludeAttrs) { throw new Error('includeAttrs and excludeAttrs can not be combined'); }

  var stats = { missing: [], inequal: [], equal: [], multiple: [], unknown: [] };

  var processed = {};

  var stream = coll1.find().stream();

  stream.on('data', function(item) {
    stream.pause();

    var selector = {};

    if (matchAttrs) {
      Object.keys(opts.matchAttrs).forEach(function(key) {
        selector[key] = item[key];
      });
    } else if (includeAttrs) {
      Object.keys(item).forEach(function(key) {
        if (!opts.includeAttrs[key]) { return; }
        selector[key] = item[key];
      });
    } else {
      Object.keys(item).forEach(function(key) {
        if (opts.excludeAttrs[key]) { return; }
        selector[key] = item[key];
      });
    }

    if (!Object.keys(selector).length) {
      stats.unknown.push(item);
      stream.resume();
      return;
    }

    // find this item in coll2 and compare
    coll2.find(selector).toArray(function(err, items) {
      if (err) { return cb(err); }

      if (opts.debug) { console.log('compare selector', selector, 'items coll2', items.length); }

      if (items.length > 1) {
        stats.multiple.push({ item1: item, items2: items });
        stream.resume();
        return;
      }

      if (items.length < 1) {
        stats.missing.push(item);
        stream.resume();
        return;
      }

      var item2 = items[0];

      // check if this item was already found
      if (processed[item2._id]) {
        stats.multiple.push({ item1: item, items2: items });
        stream.resume();
        return;
      }

      processed[item2._id] = true;

      if (includeAttrs) {
        if (compareItemsInclude(item, item2, opts.includeAttrs)) {
          stats.equal.push({ item1: item, item2: item2 });
        } else {
          stats.inequal.push({ item1: item, item2: item2 });
        }
      } else {
        if (compareItemsExclude(item, item2, opts.excludeAttrs)) {
          stats.equal.push({ item1: item, item2: item2 });
        } else {
          stats.inequal.push({ item1: item, item2: item2 });
        }
      }

      stream.resume();
    });
  });

  stream.on('error', function(err) {
    if (opts.debug) { console.log('compare stats', stats); }
    cb(err, stats);
  });

  stream.on('close', function() {
    if (opts.debug) { console.log('compare stats', stats); }
    cb(null, stats);
  });
}

module.exports = compare;
