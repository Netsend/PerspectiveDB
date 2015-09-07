/**
 * Copyright 2014, 2015 Netsend.
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
 * Get the elements that are in both given arrays.
 *
 * O(m+n) where m and n are the number of elements in arr1 and arr2 respectively.
 *
 * @param {Array} arr1  first array of elements
 * @param {Array} arr2  second array of elements
 * @param {Function} cb  First parameter will be an Error if any. Second parameter
 *                       is an array with items in arr1 that are also in arr2,
 *                       while maintaining order of arr1. Third parameter will be
 *                       false if both objects are not subsets of each other. -1 if
 *                       arr1 is a subset of arr2, or 1 if arr2 is a subset of arr1
 *                       and 0 if both sets are equal.
 */
function intersect(arr1, arr2, cb) {
  if (!arr1) {
    process.nextTick(function() {
      cb(new Error('provide arr1'), arr1);
    });
    return;
  }

  if (!arr2) {
    process.nextTick(function() {
      cb(new Error('provide arr2'), arr2);
    });
    return;
  }

  if (!Array.isArray(arr1)) {
    process.nextTick(function() {
      cb(new TypeError('arr1 must be an array'), arr1);
    });
    return;
  }

  if (!Array.isArray(arr2)) {
    process.nextTick(function() {
      cb(new TypeError('arr2 must be an array'), arr2);
    });
    return;
  }

  var intersection = [];
  var all1in2 = true;
  var all2in1 = true;

  // create object for constant lookup times
  var obj2 = {};
  arr2.forEach(function(el) {
    obj2[el] = true;
  });

  arr1.forEach(function(el) {
    if (obj2.hasOwnProperty(el)) {
      intersection.push(el);
      delete obj2[el];
    } else {
      all1in2 = false;
    }
  });

  if (Object.keys(obj2).length) {
    all2in1 = false;
  }

  var count = 0;
  if (all1in2) { count = -1; }
  if (all2in1) { count += 1; }

  var subset = false;
  if (all1in2 || all2in1) { subset = count; }

  process.nextTick(function() {
    cb(null, intersection, subset);
  });
  return;
}

module.exports = intersect;
