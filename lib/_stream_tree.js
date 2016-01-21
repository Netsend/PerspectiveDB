/**
 * Copyright 2015 Netsend.
 *
 * This file is part of PersDB.
 *
 * PersDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PersDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PersDB. If not, see <https://www.gnu.org/licenses/>.
 */

/* jshint -W018, -W030, -W116 */

'use strict';

var util = require('util');
var Transform = require('stream').Transform;

var async = require('async');
var BSON, bson = require('bson');
if (process.browser) {
  BSON = new bson();
} else {
  BSON = new bson.BSONPure.BSON();
}

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
 *                                 deserialized object (false).
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
  if (opts.excludeFirst != null && typeof opts.excludeFirst !== 'boolean') { throw new TypeError('opts.excludeFirst must be a boolean'); }
  if (opts.excludeLast != null && typeof opts.excludeLast !== 'boolean') { throw new TypeError('opts.excludeLast must be a boolean'); }

  this.tree = tree;

  this._opts = opts;
  this._opts.objectMode = true;

  Transform.call(this, this._opts);

  this._destroyed = false;

  this._setupSource();
}

util.inherits(StreamTree, Transform);

// return a new stream with the same parameters
StreamTree.prototype.reopen = function() {
  return new StreamTree(this.tree, this._opts);
};

// create a range for id, first, last, excludeFirst and excludeLast
function determineRange(tree, opts, cb) {
  var first, last;

  var tasks = [function(cb2) { process.nextTick(cb2); }];

  if (opts.first) {
    // resolve v to i
    tasks.push(function(cb2) {
      tree._resolveVtoI(opts.first, function(err, i) {
        if (err) { cb2(err); return; }

        first = i;
        cb2();
      });
    });
  }

  if (opts.last) {
    // resolve v to i
    tasks.push(function(cb2) {
      tree._resolveVtoI(opts.last, function(err, i) {
        if (err) { cb2(err); return; }

        last = i;
        cb2();
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
    tree._log.info('st range start: %j, end: %j', r.s.toString('hex'), r.e.toString('hex'));
    cb(err, r);
  });
}

// proxy destroy
StreamTree.prototype.destroy = function() {
  this.tree._log.debug('st destroy');
  this._it.destroy();
  this._destroyed = true;
};

StreamTree.prototype._setupSource = function() {
  var that = this;

  determineRange(this.tree, this._opts, function(err, r) {
    if (err) { that.emit('error', err); return; }

    var streamOpts = {};

    if (that._opts.excludeFirst) {
      streamOpts.gt = r.s;
    } else {
      streamOpts.gte = r.s;
    }

    if (that._opts.excludeLast) {
      streamOpts.lt = r.e;
    } else {
      streamOpts.lte = r.e;
    }

    if (that._opts.reverse) {
      streamOpts.reverse = true;
    }

    that.tree._log.debug('st streamOpts %j', streamOpts);

    that._it = that.tree._db.createReadStream(streamOpts);

    that._it.pipe(that);

    that._it.on('error', function(err) {
      that.emit('error', err);
    });
  });
};

StreamTree.prototype._transform = function(obj, enc, cb) {
  var that = this;

  this.tree._log.debug('st item key %j', obj.key);

  if (this._id) {
    // dskey is used
    this.tree._log.debug('st item push %j', obj.value);

    var cont = this.push(this._opts.raw ? obj.value : BSON.deserialize(obj.value));
    if (!cont) {
      this.tree._log.debug('st pause and wait for drain');
      this.pause();
      this.on('drain', function() {
        that.read(0);
      });
    }
    process.nextTick(cb);
  } else {
    // ikey is used
    var key = Tree.parseKey(obj.key);
    var val = Tree.parseKey(obj.value, { decodeId: 'utf8' });
    var dsKey = this.tree._composeDsKey(val.id, key.i);

    this.tree._db.get(dsKey, function(err, item) {
      if (err) {
        that.tree._log.err('st item err %j %s', obj.key, err);
        that.emit('error', err);
        return;
      }

      that.tree._log.debug('st item push %j', item);

      var cont = that.push(that._opts.raw ? item : BSON.deserialize(item));
      if (!cont) {
        that.tree._log.debug('st pause and wait for drain');
        that.pause();
        that.on('drain', function() {
          that.read(0);
        });
      }
      cb();
    });
  }
};

module.exports = StreamTree;
