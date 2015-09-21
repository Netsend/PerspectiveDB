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

/**
 * Do a three-way-merge.
 *
 * @param {Object} itemA  version a
 * @param {Object} itemB  version b
 * @param {Object} lca  lowest common ancestor of itemA and itemB
 * @param {Object} [lcaB]  lowest common ancestor of itemB if perspectives differ
 *                         lca and itemA will always be leading in this case.
 * @return {Object|Array} merged item or an array with conflicting key names
 */
function threeWayMerge(itemA, itemB, lca, lcaB) {
/*
* w = lowest common ancestor

ouput each attribute that’s either

    common to all three sequences, or
    present in x but absent in y and w, or
    present in y but absent in x and w,

while we delete the attributes that are either

    present in y and w but absent in x, or
    present in x and w but absent in y.

auto merge when

    present in x and y but absent in w, or
    present in w but absent in x and y.

mark in conflict when

    present in x and same attibute present in y but with different values
*/
  lcaB = lcaB || lca;

  var keysLcaA = Object.keys(lca);
  var keysLcaB = Object.keys(lcaB);
  var keysItemA = Object.keys(itemA);
  var keysItemB = Object.keys(itemB);
  var mergedItem = {};
  var conflicts = [];


  var diffA = {}, diffB = {};

  // calculate diff of itemA
  // + = create
  // ~ = changed
  // - = removed
  // check for added and changed keys
  keysItemA.forEach(function(keyA) {
    // copy itemA while we're on it
    mergedItem[keyA] = itemA[keyA];

    // check if only one version is different from lca
    if (lca.hasOwnProperty(keyA)) {
      if (JSON.stringify(itemA[keyA]) !== JSON.stringify(lca[keyA])) {
        diffA[keyA] = '~';
      }
    } else {
      diffA[keyA] = '+';
    }
  });
  // check for deleted keys
  keysLcaA.forEach(function(keyA) {
    if (!itemA.hasOwnProperty(keyA)) {
      diffA[keyA] = '-';
    }
  });

  // calculate diff of itemB
  // check for added and changed keys
  keysItemB.forEach(function(keyB) {
    // check if only one version is different from lca
    if (lcaB.hasOwnProperty(keyB)) {
      if (JSON.stringify(itemB[keyB]) !== JSON.stringify(lcaB[keyB])) {
        diffB[keyB] = '~';
      }
    } else {
      diffB[keyB] = '+';
    }
  });
  // check for deleted keys, and keys created in diffA that were already in lcaB
  keysLcaB.forEach(function(keyB) {
    if (!itemB.hasOwnProperty(keyB)) {
      diffB[keyB] = '-';
    }
    if (diffA.hasOwnProperty(keyB) && diffA[keyB] === '+') {
      conflicts.push(keyB);
    }
  });

  // detect any conflicts
  Object.keys(diffB).forEach(function(delta) {
    if (diffA.hasOwnProperty(delta) && JSON.stringify(diffA[delta]) !== JSON.stringify(diffB[delta])) {
      conflicts.push(delta);
    } else {
      // handle other scenarios
      // either apply delta, or add conflict
      if (diffB[delta] === '-') {
        delete mergedItem[delta];
      } else if (diffA[delta] === '~' && JSON.stringify(itemA[delta]) !== JSON.stringify(itemB[delta])) {
        // both updated to different values
        conflicts.push(delta);
      } else if (diffA[delta] === '+' && JSON.stringify(itemA[delta]) !== JSON.stringify(itemB[delta])) {
        // both created with different values
        conflicts.push(delta);
      } else if (diffB[delta] === '+' && itemA.hasOwnProperty(delta) && JSON.stringify(itemA[delta]) !== JSON.stringify(itemB[delta])) {
        // created at B but already existed in A
        conflicts.push(delta);
      } else {
        mergedItem[delta] = itemB[delta];
      }
    }
  });

  if (conflicts.length) { return conflicts; }

  return mergedItem;
}

module.exports = threeWayMerge;
