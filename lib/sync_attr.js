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

// Synchonize an attribute of items of one collection with values of corresponding items in another collection.

var mongodb = require('mongodb');
var async = require('async');

var compare = require('./compare');

var DEBUG;

/**
 * Copy equal items to a temp collection, then delete from and reinsert into
 * collection 1.
 *
 * @param {mongodb.Collection} coll  the collection to update
 * @param {mongodb.Collection} tmpColl  the temp. collection to use
 * @param {Array} equalItems  all the equal items of collection 1 and 2
 * @param {String} attr  the attribute to sync
 * @param {Function} cb  first parameter is error or null, second parameter the
 *                       number of updated documents.
 */
function _strategyTemp(coll, tmpColl, equalItems, attr, cb) {
  var itemsFrom1 = equalItems.map(function(items) { return items.item1; });

  var error;
  // ensure the temp. collection is empty
  tmpColl.drop(function(err) {
    if (err && err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) {
      console.error('sync_attr _strategyTemp', 1, err);
      return cb(err);
    }

    // copy the items from the collection into a temp. collection
    tmpColl.insert(itemsFrom1, { w: 1 }, function(err, inserted) {
      if (err) {
        console.error('sync_attr _strategyTemp', 2, err);
        return cb(err);
      }
      if (inserted.length !== itemsFrom1.length) {
        error = new Error('not all items are inserted');
        console.error('sync_attr _strategyTemp', 3, error, itemsFrom1.length, inserted.length);
        return cb(error);
      }

      // delete these id's from collection 1
      coll.remove({ _id: { $in: itemsFrom1.map(function(i) { return i._id; }) } }, { w: 1 }, function(err, removed) {
        if (err) {
          console.error('sync_attr _strategyTemp', 4, err);
          return cb(err);
        }
        if (removed !== itemsFrom1.length) {
          error = new Error('not all items are removed');
          console.error('sync_attr _strategyTemp', 5, error, itemsFrom1.length, removed);
          return cb(error);
        }

        if (DEBUG) { console.log('sync_attr _strategyTemp', 5, 'removed', removed); }

        // update attr on item1 with value of item2
        equalItems.forEach(function(items) {
          items.item1[attr] = items.item2[attr];
        });

        // reinsert item1 into collection 1 with attr synced
        coll.insert(itemsFrom1, { w: 1 }, function(err, inserted) {
          if (err) {
            console.error('sync_attr _strategyTemp', 6, err);
            return cb(err);
          }
          if (inserted.length !== itemsFrom1.length) {
            error = new Error('not all items are inserted');
            console.error('sync_attr _strategyTemp', 7, error, itemsFrom1.length, inserted.length);
            return cb(error);
          }
          cb(null, inserted.length);
        });
      });
    });
  });
}

/**
 * Set the attribute of the object directly
 *
 * Note: can not be done when syncing _id.
 *
 * @param {mongodb.Collection} coll  the collection to update
 * @param {Array} equalItems  all the equal items of collection 1 and 2
 * @param {String} attr  the attribute to sync
 * @param {Function} cb  first parameter is error or null, second parameter the
 *                       number of updated documents.
 */
function _strategyDirect(coll, equalItems, attr, cb) {
  var error;
  var updated = 0;
  async.eachSeries(equalItems, function(items, cb2) {
    if (items.item1[attr] === items.item2[attr]) { return process.nextTick(cb2); }

    if (DEBUG) { console.log('overwrite', items.item1[attr], 'with', items.item2[attr], 'on', items.item1._id); }

    var modifier = { $set: { } };
    modifier.$set[attr] = items.item2[attr];

    coll.update(items.item1, modifier, { w: 1 }, function(err, updates) {
      if (err) {
        console.error(err, JSON.stringify(items));
        return cb2(err);
      }
      if (updates !== 1) {
        error = new Error('item not updated');
        console.error(error, JSON.stringify(items), updates);
        return cb2(error);
      }
      updated++;
      cb2();
    });
  }, function(err) {
    cb(err, updated);
  });
}

/**
 * Synchonize attributes of items of one collection with attributes from
 * corresponding items in another collection.
 *
 * Determine correspondence by using either included or excluded attributes.
 *
 * Note: the complete history of collection 1 will be rebuild, collection 2 will be
 * left untouched.
 *
 * @param {mongodb.Collection} coll1  the collection to rebase
 * @param {mongodb.Collection} coll2  the collection to rebase on (use as parent)
 * @param {String} attr  the attribute to sync from items in collection 2
 * @param {mongodb.Collection} tmpColl  temporary collection to use
 * @param {Object} [opts]  options
 * @param {Function} cb  first parameter will be an error or null.
 *
 * options (see compare.js):
 *   debug {Boolean}  whether or not to show debugging info
 *   includeAttrs {Object}  list of keys to include
 *   excludeAttrs {Object}  list of keys to exclude
 *   matchAttrs {Object}  list of keys that should match
 */
function syncAttr(coll1, coll2, tmpColl, attr, opts, cb) {
  /* jshint maxcomplexity: 24 */ /* might need some refactoring */

  if (!(coll1 instanceof mongodb.Collection)) { throw new TypeError('coll1 must be a mongodb.Collection'); }
  if (!(coll2 instanceof mongodb.Collection)) { throw new TypeError('coll2 must be a mongodb.Collection'); }
  if (!(tmpColl instanceof mongodb.Collection)) { throw new TypeError('tmpColl must be a mongodb.Collection'); }
  if (typeof attr !== 'string') { throw new TypeError('attr must be a string'); }

  if (typeof opts === 'function') {
    cb = opts;
    opts = {};
  }

  opts = opts || {};
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  opts.debug = opts.debug || false;
  if (typeof opts.debug !== 'boolean') { throw new TypeError('opts.debug must be a boolean'); }

  DEBUG = opts.debug;

  opts.includeAttrs = opts.includeAttrs || {};
  if (typeof opts.includeAttrs !== 'object') { throw new TypeError('opts.includeAttrs must be an object'); }

  opts.excludeAttrs = opts.excludeAttrs || {};
  if (typeof opts.excludeAttrs !== 'object') { throw new TypeError('opts.excludeAttrs must be an object'); }

  opts.matchAttrs = opts.matchAttrs || {};
  if (typeof opts.matchAttrs !== 'object') { throw new TypeError('opts.matchAttrs must be an object'); }

  if (Object.keys(opts.includeAttrs).length) {
    if (opts.includeAttrs[attr]) { throw new Error('can not include attribute that is synced for comparison'); }
  } else {
    if (DEBUG) { console.log('exclude', attr); }
    opts.excludeAttrs[attr] = true;
  }

  if (Object.keys(opts.matchAttrs).length) {
    if (opts.matchAttrs[attr]) { throw new Error('can not match on attribute that is synced for comparison'); }
  }

  if (DEBUG) { console.log('attr', attr); }
  if (DEBUG) { console.log('opts', JSON.stringify(opts)); }

  // 1. find ids in collection 1 that are in collection 2 and only differ on included or excluded attributes
  compare(coll1, coll2, opts, function(err, stats) {
    if (err) { throw err; }

    if (DEBUG) { console.log('multiple', stats.multiple.length); }

    if (stats.multiple.length) {
      return cb(new Error('ambiguous elements in collection2'));
    }

    if (DEBUG) { console.log('equal', stats.equal.length); }

    if (!stats.equal.length) { return cb(null, 0); }

    if (attr === '_id') {
      if (DEBUG) { console.log('temp strategy'); }
      _strategyTemp(coll1, tmpColl, stats.equal, attr, cb);
    } else {
      if (DEBUG) { console.log('direct strategy'); }
      _strategyDirect(coll1, stats.equal, attr, cb);
    }
  });
}

module.exports = syncAttr;
module.exports._strategyTemp = _strategyTemp;
module.exports._strategyDirect = _strategyDirect;
