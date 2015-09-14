/**
 * Copyright 2014-2015 Netsend.
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

var noop = function() {};

/**
 * Remove null values from an array.
 *
 * @param {Array} arr  subject
 * @return {Number} number of elements removed
 */
function _removeNull(arr) {
  var removed = 0;
  for (var i = 0; i < arr.length; i++) {
    if (arr[i] === null) {
      // remove null
      arr.splice(i, 1);
      i--;
      removed++;
    }
  }
  return removed;
}

/**
 * Determine if v is ine left of E and/or right of E.
 *
 * Assume E is topologically sorted, if subject is on the right, it will be
 * found before it's on the left.
 *
 * @param {Array} E  edges of E, topologically sorted
 * @param {String} v  vertice that is searched for 
 * @return {Object} { l: {Boolean}, r: {Boolean} } whether v exists in left of E
 *                  and right of E
 */
function _lr(E, v) {
  var l = false, r = false;

  // stop if left is found or end of E
  E.some(function(edge) {
    if (edge[0] === v) { l = true; }
    if (edge[1] === v) { r = true; }
    return l;
  });

  return { l: l, r: r };
}

/**
 * Find lowest common ancestor(s) of the given streams. The first emitted vertex
 * of each stream is used as the starting vertex. Assume each stream is a
 * topologically sorted DAG.
 *
 * @param {Tree.iterateDAG} sX  readable stream that emits vertices from leaf to root
 * @param {Tree.iterateDAG} sY  readable stream that emits vertices from leaf to root
 * @param {Object} [opts]  object containing configurable parameters
 * @param {Function} cb  First parameter will be an Error or null. Second parameter
 *                       will be an array with all lowest common ancestor versions
 *
 * opts:
 *   fnv {Function}  function that extracts a vertice identifier and it's
 *                   parents from a vertex in the format: { v: ..., pa: [...] }
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 */
function findLCAs(sX, sY, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = null;
  }

  if (sX == null || typeof sX !== 'object') { throw new TypeError('sX must be a stream.Readable'); }
  if (sY == null || typeof sY !== 'object') { throw new TypeError('sY must be a stream.Readable'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  opts = opts || {};
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }
  if (opts.fnv != null && typeof opts.fnv !== 'function') { throw new TypeError('opts.fnv must be a function'); }
  if (opts.log != null && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }

  var fnv = opts.fnv;

  findLCAs._log = opts.log || { emerg: noop, alert: noop, crit: noop, err: noop, warning: noop, notice: noop, info: noop, debug: noop, debug2: noop, getFileStream: noop, getErrorStream: noop, close: noop };

  var that = findLCAs;

  var lca = [];

  // contains each edge topologically sorted
  var xE = [];
  var yE = [];

  var outdegreeX = {};
  var indegreeX = {};

  var outdegreeY = {};
  var indegreeY = {};

  var sXclosed, sYclosed, destroyed;

  function close() {
    that._log.debug('find_lcas close: lca: %s, xE: %j, yE: %j', lca, xE, yE);
    if (sXclosed && sYclosed && !destroyed) {
      that._log.info('find_lcas close: cb');
      destroyed = true;
      cb(null, lca);
    }
  }

  // register end events
  sX.once('end', function() {
    sXclosed = true;
    close();
  });

  sY.once('end', function() {
    sYclosed = true;
    close();
  });

  sX.on('error', cb);
  sY.on('error', cb);

  /**
   * Remove a vertice and all single paths leading to it from a topologically
   * sorted DAG.
   *
   *   function removeVertice(E, v)
   *     for each e in E
   *       // delete every single path that leads to v
   *       if right(e) == v
   *         remove e from E
   *         if out-degree(left(e)) == 1
   *           removeVertice(E, indegree, outdegree, left(e))
   *       // delete every single path that originates from v
   *       if left(e) == v
   *         remove e from E
   *         if in-degree(right(e)) == 1
   *           removeVertice(E, indegree, outdegree, right(e))
   *
   * @param {Array} E  all edges topologically sorted
   * @param {Object} indegree  key is a vertice, value a number
   * @param {Object} outdegree  key is a vertice, value a number
   * @param {String} v  vertice that is to be removed
   * @return {Number} number of edges removed
   */
  function removeVertice(E, indegree, outdegree, v) {
    var removed = 0;

    that._log.debug('find_lcas removeVertice v: %s, E: %j', v, E);

    for (var i = 0; i < E.length; i++) {
      if (E[i] == null) {
        continue;
      }

      var l = E[i][0];
      var r = E[i][1];

      that._log.debug('find_lcas removeVertice e: l: %s, r: %s', l, r);

      if (r === v) {
        that._log.debug('find_lcas removeVertice remove');
        E[i] = null;
        removed++;
        outdegree[l]--;
        indegree[r]--;
        if (outdegree[l] === 0) {
          that._log.debug('find_lcas removeVertice recurse with l %s', l);
          removed += removeVertice(E, indegree, outdegree, l);
        }
      }
      if (l === v) {
        that._log.debug('find_lcas removeVertice remove');
        E[i] = null;
        removed++;
        outdegree[l]--;
        indegree[r]--;
        if (indegree[r] === 0) {
          that._log.debug('find_lcas removeVertice recurse with r %s', r);
          removed += removeVertice(E, indegree, outdegree, r);
        }
      }
    }

    return removed;
  }

  /**
   *  root is a boolean that is true if this is the first item of a stream
   *
   *   function processNext(v, E, indegree, outdegree, peerE, root)
   *     // either remove from E, add to E or do nothing
   *     if right(E) or a root
   *       if v in left(peerE) // other stream encountered this node before
   *         lca <- v
   *         remove v from peerE
   *         remove v from E
   *         if empty(peerE)
   *           return lca
   *         if empty(E)
   *           return lca
   *       else
   *         if leaf(v)
   *           E <- (v, null)
   *         else
   *           for each parent pa in v
   *             E <- (v, pa)
   *
   * @param {Object} obj  a vertice
   * @param {Array} E  all edges, topologically sorted
   * @param {Object} indegree  key is a vertice, value a number
   * @param {Object} outdegree  key is a vertice, value a number
   * @param {Array} peerE  all edges of the other DAG, topologically sorted
   * @param {Object} peerIndegree  key is a vertice, value a number
   * @param {Object} peerOutdegree  key is a vertice, value a number
   * @param {Boolean} root  whether or not this is the first vertice of the DAG
   */
  function processNext(obj, E, indegree, outdegree, peerE, peerIndegree, peerOutdegree, root) {
    obj = fnv ? fnv(obj) : obj;
    that._log.debug('find_lcas processNext obj: %j, lca: %s, E: %j, indegree: %j, outdegree: %j, peerE: %j, indegree: %j, outdegree: %j, root: %s', obj, lca, E, indegree, outdegree, peerE, peerIndegree, peerOutdegree, root);

    var v = obj.v;
    var pa = obj.pa;

    // if part of this graph
    var lr = _lr(E, v);
    if (lr.r || root) {
      // if v is in left(E), an lca is found
      var peerLr = _lr(peerE, v);
      if (peerLr.l) {
        that._log.debug('find_lcas processNext %s IN left(peerE), add lca and remove from E and peerE', v);
        // remove all edges connecting to this vertice and add lca
        lca.push(v);
        removeVertice(E, indegree, outdegree, v);
        removeVertice(peerE, peerIndegree, peerOutdegree, v);
        _removeNull(E);
        _removeNull(peerE);
        that._log.debug('E and peerE %j %j', peerE, E);

        // check if there is an edge left in E or peerE
        if (!E.length || !peerE.length) {
          that._log.debug('find_lcas processNext E or peerE empty, lca: %s', lca);
          sX.pause();
          sXclosed = true;

          sY.pause();
          sYclosed = true;

          close();
          return;
        }
      } else {
        that._log.debug('find_lcas processNext %s IN right(E) or root', v);
        if (!pa.length) {
          that._log.debug('find_lcas processNext no parents, add root edge %s', v);
          E.push([v, null]);
          outdegree[v] = outdegree[v] || 0;
          indegree[v] = indegree[v] || 0;
        } else {
          pa.forEach(function(p) {
            that._log.debug('find_lcas processNext parent %s, add edge', p);
            E.push([v, p]);
            outdegree[v] = outdegree[v] || 0;
            outdegree[v]++;
            indegree[p] = indegree[p] || 0;
            indegree[p]++;
          });
        }
      }
    }

    that._log.debug('find_lcas processNext end: lca: %s, E: %j, indegree: %j, outdegree: %j', lca, E, indegree, outdegree);
  }

  sX.once('data', function(root) {
    processNext(root, xE, indegreeX, outdegreeX, yE, indegreeY, outdegreeY, true);
    sX.on('data', function(vertice) {
      processNext(vertice, xE, indegreeX, outdegreeX, yE, indegreeY, outdegreeY);
    });
  });

  sY.once('data', function(root) {
    processNext(root, yE, indegreeY, outdegreeY, xE, indegreeX, outdegreeX, true);
    sY.on('data', function(vertice) {
      processNext(vertice, yE, indegreeY, outdegreeY, xE, indegreeX, outdegreeX);
    });
  });
}

findLCAs._lr = _lr;

module.exports = findLCAs;
