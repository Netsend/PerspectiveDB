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

/* jshint -W018, -W030, -W116 */

'use strict';

var util = require('util');
var Readable = require('stream').Readable;

var async = require('async');
var bson = require('bson');
var BSON = new bson.BSONPure.BSON();

var Tree;

/**
 * Get a readable stream of the tree in the order of insertion into the tree.
 *
 * @param {Object} tree  tree object
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   id {String|Object}  limit stream to one specific DAG (maintain insertion
 *                       order)
 *   first {base64 String}  first version, offset
 *   last {base64 String}  last version
 *   excludeFirst {Boolean, default false}  whether or not first should be
 *                                          excluded
 *   excludeLast {Boolean, default false}  whether or not last should be
 *                                         excluded
 *   reverse {Boolean, default false}  if true, starts with last version
 *   raw {Boolean, default false}  whether to return a BSON serialized or
 *                                 deserialezed object (false).
 */
function StreamTree(tree, opts) {
  if (typeof tree !== 'object' || Array.isArray(opts)) { throw new TypeError('tree must be an object'); }

  Tree = tree.constructor;

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }

  this._id = opts.id;
  if (this._id != null && !Buffer.isBuffer(this._id)) {
    this._id = new Buffer(Tree._ensureString(this._id));
  }

  if (opts.first != null) {
    if (typeof opts.first !== 'number' && typeof opts.first !== 'string') { throw new TypeError('opts.first must be a base64 string or a number'); }
    if (typeof opts.first === 'string' && Buffer.byteLength(opts.first, 'base64') !== tree._vSize) { throw new Error('opts.first must be the same size as the configured vSize'); }
  }
  if (opts.last != null) {
    if (typeof opts.last !== 'number' && typeof opts.last !== 'string') { throw new TypeError('opts.last must be a base64 string or a number'); }
    if (typeof opts.last === 'string' && Buffer.byteLength(opts.last, 'base64') !== tree._vSize) { throw new Error('opts.last must be the same size as the configured vSize'); }
  }
  if (opts.reverse != null && typeof opts.reverse !== 'boolean') { throw new TypeError('opts.reverse must be a boolean'); }
  if (opts.raw != null && typeof opts.raw !== 'boolean') { throw new TypeError('opts.raw must be a boolean'); }
  if (opts.excludeOffset != null && typeof opts.excludeOffset !== 'boolean') { throw new TypeError('opts.excludeOffset must be a boolean'); }

  this.tree = tree;

  this._opts = opts;
  this._opts.objectMode = true;

  Readable.call(this, this._opts);
}

util.inherits(StreamTree, Readable);

// return a new stream with the same parameters
StreamTree.prototype.reopen = function() {
  return new StreamTree(this.tree, this._opts);
};

// create a range for id, first, last, exlucdeFirst and excludeLast
function determineRange(tree, opts, cb) {
  var first, last;

  var tasks = [function(cb) { process.nextTick(cb); }];

  if (opts.first) {
    // resolve v to i
    tasks.push(function(cb) {
      tree._resolveVtoI(opts.first, function(err, i) {
        if (err) { cb(err); return; }

        first = i;
        cb();
      });
    });
  }

  if (opts.last) {
    // resolve v to i
    tasks.push(function(cb) {
      tree._resolveVtoI(opts.last, function(err, i) {
        if (err) { cb(err); return; }

        last = i;
        cb();
      });
    });
  }

  async.series(tasks, function(err) {
    var r;
    if (opts.id) {
      // start reading the dskey index
      r = tree.getDsKeyRange({ id: opts.id, minI: first, maxI: last});
    } else {
      // start reading the ikey index
      r = tree.getIKeyRange({ minI: first, maxI: last });
    }
    tree._log.info('StreamTree range start: %j, end: %j', r.s.toString('hex'), r.e.toString('hex'));
    cb(err, r);
  });
}

function openStream(tree, r, opts, cb) {
  var streamOpts = {};

  if (opts.excludeFirst) {
    streamOpts.gt = r.s;
  } else {
    streamOpts.gte = r.s;
  }

  if (opts.excludeLast) {
    streamOpts.lt = r.e;
  } else {
    streamOpts.lte = r.e;
  }

  if (opts.reverse) {
    streamOpts.reverse = true;
  }

  tree._log.info('StreamTree streamOpts %j', streamOpts);

  var it = tree._db.createReadStream(streamOpts);

  cb(null, it);
}

StreamTree.prototype._read = function() {
  if (this._it) {
    this._it.resume();
    return;
  }

  // setup stream
  var that = this;
  var processing = 0;
  var ended = false;

  determineRange(this.tree, this._opts, function(err, r) {
    if (err) { that.emit('error', err); return; }

    openStream(that.tree, r, that._opts, function(err, it) {
      that._it = it;

      function processItem(item) {
        that.tree._log.debug('StreamTree item push %j', item);

        var cont = that.push(that._opts.raw ? item : BSON.deserialize(item));
        if (!cont) {
          it.pause();
        }
        processing--;
        if (ended && !processing) {
          that.push(null);
        }
      }

      it.on('data', function(obj) {
        processing++;

        that.tree._log.err('StreamTree item key %j', obj.key);

        if (that._id) {
          // dskey is used
          processItem(obj.value);
        } else {
          // ensure the same order
          it.pause();

          // ikey is used
          var key = Tree.parseKey(obj.key);
          var val = Tree.parseKey(obj.value, { decodeId: 'utf8' });
          var dsKey = that.tree._composeDsKey(val.id, key.i);

          that.tree._db.get(dsKey, function(err, item) {
            if (err) {
              that.tree._log.err('StreamTree %s', err);
              that.emit('error', err);
              return;
            }

            it.resume();
            processItem(item);
          });
        }
      });

      it.on('error', function(err) {
        that.emit('error', err);
      });

      it.on('end', function() {
        ended = true;
        if (!processing) {
          that.tree._log.debug('StreamTree end push null, processing:', processing);
          that.push(null);
        }
      });
    });
  });
};

module.exports = StreamTree;
