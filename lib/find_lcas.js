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

'use strict';

var async = require('./async');

var intersect = require('./intersect');

/**
 * Find lowest common ancestor(s) of x and y.
 *
 * @param {Object} itemX  item x
 * @param {Object} itemY  item y
 * @param {Function} cb  First parameter will be an Error or null. Second parameter
 *                       will be an array with all lowest common ancestor versions.
 */
function findLCAs(itemX, itemY, cb) {
  /* jshint maxcomplexity: 23 */ /* might need some refactoring */

  var that = this;
  if (!itemX) {
    process.nextTick(function() {
      var err = new Error('provide itemX');
      that._log.err('findLCAs error', err);
      cb(err, itemX);
    });
    return;
  }

  if (!itemY) {
    process.nextTick(function() {
      var err = new Error('provide itemY');
      that._log.err('findLCAs error', err);
      cb(err, itemY);
    });
    return;
  }

  if (!itemX._id) {
    process.nextTick(function() {
      var err = new TypeError('missing itemX._id');
      that._log.err('findLCAs error', err);
      cb(err, itemY);
    });
    return;
  }

  if (!itemY._id) {
    process.nextTick(function() {
      var err = new TypeError('missing itemY._id');
      that._log.err('findLCAs error', err);
      cb(err, itemY);
    });
    return;
  }

  this._log.info('findLCAs', JSON.stringify(itemX._id), JSON.stringify(itemY._id));

  if (typeof itemX._id !== 'object') {
    process.nextTick(function() {
      var err = new TypeError('itemX._id must be an object');
      that._log.err('findLCAs error', err);
      cb(err, itemY);
    });
    return;
  }

  if (typeof itemY._id !== 'object') {
    process.nextTick(function() {
      var err = new TypeError('itemY._id must be an object');
      that._log.err('findLCAs error', err);
      cb(err, itemY);
    });
    return;
  }

  // check if ids are equal
  // only use strict comparison on certain types
  if (~['string', 'number', 'boolean'].indexOf(typeof itemX._id._id)) {
    if (itemX._id._id !== itemY._id._id) {
      process.nextTick(function() {
        var err = new TypeError('itemX._id._id must equal itemY._id._id');
        that._log.err('findLCAs error', err, itemX._id._id, itemY._id._id);
        cb(err, itemY);
      });
      return;
    }
  } else {
    // use JSON.stringify comparison in all other cases
    if (JSON.stringify(itemX._id._id) !== JSON.stringify(itemY._id._id)) {
      process.nextTick(function() {
        var err = new TypeError('itemX._id._id must equal itemY._id._id');
        that._log.err('findLCAs error', err, itemX._id._id, itemY._id._id);
        cb(err, itemY);
      });
      return;
    }
  }

  if (!itemX._id._pe) {
    process.nextTick(function() {
      var err = new TypeError('missing itemX._id._pe');
      that._log.err('findLCAs error', err);
      cb(err, itemY);
    });
    return;
  }

  if (!itemY._id._pe) {
    process.nextTick(function() {
      var err = new TypeError('missing itemY._id._pe');
      that._log.err('findLCAs error', err);
      cb(err, itemY);
    });
    return;
  }

  var perspectiveX = itemX._id._pe;
  var perspectiveY = itemY._id._pe;

  var perspectives = [perspectiveX];
  if (perspectiveX !== perspectiveY) {
    perspectives.push(perspectiveY);
  }

  var cas = {};
  var lcas = []; // list of lowest common ancestors

  // init
  var headsX = {};
  var headsY = {};

  // if this is a virtual merge (an item without _id._v), use it's parents
  if (itemX._id._v) {
    headsX[itemX._id._v] = perspectiveX;
  } else {
    itemX._id._pa.forEach(function(p) {
      headsX[p] = perspectiveX;
    });
  }

  if (itemY._id._v) {
    headsY[itemY._id._v] = perspectiveY;
  } else {
    itemY._id._pa.forEach(function(p) {
      headsY[p] = perspectiveY;
    });
  }

  // shortcut case where one item is the parent of the other and both are from the same perspective
  // only do it with exact one parent and not on virtual merges (items without a version)
  // FIXME: check if this really has to be perspective bound
  if (perspectives.length === 1) {
    if (itemX._id._pa.length === 1 && itemY._id._v && itemX._id._pa[0] === itemY._id._v) { lcas.push(itemY._id._v); }
    if (itemY._id._pa.length === 1 && itemX._id._v && itemY._id._pa[0] === itemX._id._v) { lcas.push(itemX._id._v); }
    if (lcas.length) {
      that._log.info('findLCAs shortcut', lcas);
      process.nextTick(function() {
        cb(null, lcas);
      });
      return;
    }
  }

  var ancestorsX = [];
  var ancestorsY = [];

  // determin selector and sort
  var selectorPerspectives = { $in: [perspectiveX, perspectiveY] };
  var sort = { '$natural': -1 };
  if (perspectiveX === perspectiveY) {
    selectorPerspectives = perspectiveX;

    if (perspectiveX === this.localPerspective) {
      sort = { '_id._i': -1 };
    }
  }

  // go through the DAG from heads to root
  var selector = { '_id._id': itemX._id._id, '_id._pe': selectorPerspectives };
  var stream = this._snapshotCollection.find(selector, { sort: sort, comment: '_findLCAs' }).stream();

  stream.on('data', function(item) {
    var version = item._id._v;
    var perspective = item._id._pe;
    var parents = item._id._pa || [];

    that._log.debug2('findLCAs version:', version, perspective);
    that._log.debug2('findLCAs START HEADSX', headsX);
    that._log.debug2('findLCAs START HEADSY', headsY);
    that._log.debug2('findLCAs START ANCESTORSX', ancestorsX);
    that._log.debug2('findLCAs START ANCESTORSY', ancestorsY);
    that._log.debug2('findLCAs START LCAS', lcas);
    that._log.debug2('findLCAs START CAS', cas);

    // track branches of X and Y by updating heads by perspective on match and keep track of ancestors
    if (headsX[version] === perspective) {
      delete headsX[version];
      parents.forEach(function(p) {
        headsX[p] = perspectiveX;
      });
      ancestorsX.unshift(version);

      // now check if current item is in the ancestors of the other DAG, if so, we have a ca
      if (~ancestorsY.indexOf(version)) {
        if (!cas[version]) {
          lcas.push(version);
        }
        // make sure any of it's ancestors won't count as a ca (which makes this an lca)
        parents.forEach(function(p) {
          cas[p] = true;
        });
      }
    }

    // same with the heads of y
    if (headsY[version] === perspective) {
      delete headsY[version];
      parents.forEach(function(p) {
        headsY[p] = perspectiveY;
      });
      ancestorsY.unshift(version);

      if (~ancestorsX.indexOf(version)) {
        if (!cas[version]) {
          lcas.push(version);
        }
        parents.forEach(function(p) {
          cas[p] = true;
        });
      }
    }

    that._log.debug2('findLCAs END HEADSX', headsX);
    that._log.debug2('findLCAs END HEADSY', headsY);
    that._log.debug2('findLCAs END ANCESTORSX', ancestorsX);
    that._log.debug2('findLCAs END ANCESTORSY', ancestorsY);
    that._log.debug2('findLCAs END LCAS', lcas);
    that._log.debug2('findLCAs END CAS', cas);

    stream.pause();

    // as soon as both sets of open heads are equal, we have seen all lca(s)
    intersect(Object.keys(headsX), Object.keys(headsY), function(err, intersection, subset) {
      if (subset === 0) {
        // finish up
        // add any of the open heads that are not a common ancestor and are in the database
        async.eachSeries(Object.keys(headsX), function(head, callback) {
          if (cas[head]) { return process.nextTick(callback); }

          selector = { '_id._id': itemX._id._id, '_id._v': head, '_id._pe': selectorPerspectives};
          that._snapshotCollection.find(selector, { comment: '_findLCAs2' }).toArray(function(err, items) {
            if (err) { return callback(err); }
            if (items.length !== perspectives.length) {
              var msg = new Error('missing at least one perspective when fetching lca ' + head + '. perspectives: ' + perspectives.join(', '));
              return callback(msg);
            }

            lcas.push(head);
            callback();
          });
        }, function(err) {
          if (err) { return cb(err); }

          stream.destroy();
          return;
        });
      } else {
        stream.resume();
      }
    });
  });

  stream.on('close', function() {
    that._log.info('findLCAs found', lcas);
    cb(null, lcas);
  });

  stream.on('error', function(err) {
    that._log.err('findLCAs cursor stream error', err);
    cb(err);
  });
}

module.exports = findLCAs;
