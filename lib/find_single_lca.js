/**
 * Copyright 2014-2015, 2016 Netsend.
 *
 * This file is part of PerspectiveDB.
 *
 * PerspectiveDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PerspectiveDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PerspectiveDB. If not, see <https://www.gnu.org/licenses/>.
 */

'use strict';

/**
 * Find single lowest common ancestor of u and v. Walk from leaf to root without
 * preprocessing or requiring to visit the root.
 *
 * Assume each stream is a topologically sorted DAG.
 *
 * @param {Object} u  vertice u, should yield v and pa after optional transform
 * @param {Object} v  vertice v, should yield v and pa after optional transform
 * @param {stream.Readable} sU  readable stream that emits vertices from leaf to
 *                              root
 * @param {stream.Readable} sV  readable stream that emits vertices from leaf to
 *                              root
 * @param {Object} [opts]  object containing configurable parameters
 * @param {Function} cb  First parameter will be an Error or null. Second
 *                       parameter will be the single lca identifier, if found.
 *
 * opts:
 *   transform {Function}  function that extracts a vertice identifier and it's
 *     parents from a vertex in the format: { v: ..., pa: [...] }. Used on sU and
 *     sV as well as u and v.
 */
function findSingeLCA(u, v, sU, sV, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = null;
  }

  if (u == null || typeof u !== 'object') { throw new TypeError('u must be an object'); }
  if (v == null || typeof v !== 'object') { throw new TypeError('v must be an object'); }
  if (sU == null || typeof sU !== 'object') { throw new TypeError('sU must be a stream.Readable'); }
  if (sV == null || typeof sV !== 'object') { throw new TypeError('sV must be a stream.Readable'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  opts = opts || {};
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (opts.transform != null && typeof opts.transform !== 'function') { throw new TypeError('opts.transform must be a function'); }

  var slca, sUclosed, sVclosed, destroyed;
  var transform = opts.transform;

  if (transform) {
    u = transform(u);
    v = transform(v);
  }

  function close() {
    if (sUclosed && sVclosed && !destroyed) {
      destroyed = true;
      cb(null, slca);
    }
  }

  sU.on('error', cb);
  sV.on('error', cb);

  /**
   * init:
   * nextU <- u
   * nextV <- v
   * ancestorsU <- addParentGroup(u)
   * ancestorsV <- addParentGroup(v)
   */

  var nextU = {};
  var nextV = {};
  nextU[u.v] = true;
  nextV[v.v] = true;

  // Track each path starting in u or v. The index of the array is the distance
  // measured in the number of edges from u or v. Each element is the group of
  // parents in an encountered vertex, except for the element at index 0.
  var ancestorsU = [groupVertices([u.v])];
  var ancestorsV = [groupVertices([v.v])];

  sU.on('end', function() {
    sUclosed = true;
    //close();
  });

  sV.on('end', function() {
    sVclosed = true;
    //close();
  });

  sU.on('readable', function() {
    var vertice = sU.read();
    if (!vertice) {
      sUclosed = true;
      close();
      return;
    }
    vertice = transform ? transform(vertice) : vertice;
    slca = processVertice(vertice, nextU, ancestorsU, ancestorsV);
    if (slca) {
      sV.pause();
      sUclosed = true;
      sVclosed = true;
      close();
    }
  });

  sV.on('readable', function() {
    var vertice = sV.read();
    if (!vertice) {
      sVclosed = true;
      close();
      return;
    }
    vertice = transform ? transform(vertice) : vertice;
    slca = processVertice(vertice, nextV, ancestorsV, ancestorsU);
    if (slca) {
      sU.pause();
      sUclosed = true;
      sVclosed = true;
      close();
    }
  });
}

// V is array of vertices
// returns object with each vertice as key and "true" as value
function groupVertices(V) {
  var res = {};
  V.forEach(function(v) {
    res[v] = true;
  });
  return res;
}

/**
 * for each v in V
 *   if v in nextSame
 *     nextSame[v] = parents(v)
 *     if v in ancestorsOther
 *       v = single lca
 *     else
 *       for each parentGroup in ancestorsOther
 *         ca = []
 *         for each parent in parents(v)
 *           if p in parentGroup
 *             ca <- p
 *         if ca.length = 1
 *           ca[0] is single lca
 *         else
 *           ancestorsSame <- addParentGroup parents(v)
 *
 * @param {Object} v  vertice containing v for identifier and pa which is an array
 * @param {String} v.v  vertice identifier
 * @param {String[]} v.pa  vertice parent identifiers
 * @param {Object} next  the expected parents when walking the dag from leaf to
 *   root
 * @param {Object[]} ancestors  ancestors of v, grouped per vertice parent group
 * @param {Object[]} ancestorsOther  ancestors of "the other stream", also grouped
 *   per vertice parent group
 * @return {String} the found lca version or undefined if none found
 */
function processVertice(v, next, ancestors, ancestorsOther) {
  if (!next[v.v])
    return;

  // update next with parents of v
  delete next[v.v];
  v.pa.forEach(pa => {
    next[pa] = true;
  });

  // if v is in ancestorsOther, then v is the single lca
  if (ancestorsOther.some(parentGroup => parentGroup[v.v])) {
    return v.v;
  }

  // else if only one of the parents is in an ancestorOther group, then that
  // parent is the single lca
  var slca;
  ancestorsOther.some(parentGroup => {
    var ca = [];
    // check if exactly one parent of v is in the parent group
    v.pa.forEach(pa => {
      if (parentGroup[pa])
        ca.push(pa);
    });
    if (ca.length === 1)
      slca = ca[0];
    return slca;
  });

  // update ancestors with the parents of v as a new parent group
  ancestors.push(groupVertices(v.pa));

  return slca;
}

module.exports = findSingeLCA;
