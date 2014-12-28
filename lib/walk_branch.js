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

/**
 * Travel branches towards the root, based on criteria given.
 *
 * Note: when we reach the end, the callback is called with null for all three
 * parameters.
 *
 * @param {Object} selector  requirements like _id._id and _id._pe
 * @param {String} head  certain version to start tracking from
 * @param {String} localPerspective  name of the perspective
 * @param {Object} collection  mongodb collection to walk through
 * @param {Function} cb  first parameter will be an Error or null. second parameter
 *                       will be the item found or null in case the stream is
 *                       closed. Third parameter will be the stream (so it can be
 *                       closed before we reach the end of the branch.
 */
function walkBranch(selector, head, localPerspective, collection, cb) {
  if (typeof selector !== 'object') { throw new TypeError('selector must be an object'); }
  if (typeof head !== 'string') { throw new TypeError('head must be a string'); }
  if (typeof localPerspective !== 'string') { throw new TypeError('localPerspective must be a string'); }
  if (typeof collection !== 'object') { throw new TypeError('collection must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  // follow parents, take advantage of the fact that the DAG is topologically sorted
  if (!selector) {
    process.nextTick(function() {
      cb(new Error('provide selector'), null);
    });
    return;
  }

  if (!selector['_id._id']) {
    process.nextTick(function() {
      cb(new TypeError('missing selector._id._id'), null);
    });
    return;
  }

  var stream;
  if (selector['_id._pe'] === localPerspective) {
    stream = collection.find(selector, { sort: { '_id._i': -1 }, comment: '_walkBranch' }).stream();
  } else {
    stream = collection.find(selector, { sort: { $natural: -1 }, comment: '_walkBranch' }).stream();
  }

  stream.on('error', cb);

  var nextParents = {};
  nextParents[head] = true;

  stream.on('data', function(item) {
    // if the current item is in nextParents, replace it by it's parents
    if (nextParents[item._id._v]) {
      delete nextParents[item._id._v];
      item._id._pa.forEach(function(p) {
        nextParents[p] = true;
      });

      cb(null, item, stream);
    }
  });

  stream.on('close', function() {
    cb(null, null, null);
  });
}

module.exports = walkBranch;
