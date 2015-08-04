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

var Writable = require('stream').Writable;
var util = require('util');

var async = require('async');
var BSON = require('bson').BSONPure.BSON;

var noop = function() {};

var DSKEY   = 0x01;
var IKEY    = 0x02;
var HEADKEY = 0x03;

/**
 * Tree
 *
 * Manage DAGs in a tree.
 *
 * @param {LevelUP.db} db  database for persistent storage
 * @param {String} name  name of this tree, should not exceed 255 bytes
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   vSize {Number, default 6}  number of bytes used for the version. Should be:
 *                              0 < vSize <= 6
 *   iSize {Number, default 6}  number of bytes used for i. Should be:
 *                              0 < iSize <= 6
 *   offset {String}  version that should be used as offset, if ommitted all
 *                    heads are served and what follows are any subsequent
 *                    updates
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 */
function Tree(db, name, opts) {
  if (typeof db !== 'object' || db === null) { throw new TypeError('db must be an object'); }
  if (typeof name !== 'string') { throw new TypeError('name must be a string'); }
  if (Buffer.byteLength(name) > 255) { throw new TypeError('name must not exceed 255 bytes'); }

  opts = opts || {};
  if (typeof opts !== 'object' || opts === null || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }
  if (typeof opts.log !== 'undefined' && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }
  if (typeof opts.vSize !== 'undefined' && typeof opts.vSize !== 'number') { throw new TypeError('opts.vSize must be a number'); }
  if (typeof opts.iSize !== 'undefined' && typeof opts.iSize !== 'number') { throw new TypeError('opts.iSize must be a number'); }

  opts.objectMode = true;

  Writable.call(this, opts);

  var that = this;

  this._name = name;

  this._db = db;

  // partition db in a data store, i index and head index
  // see the keyspec for details
  this._dsPrefix      = Tree.getPrefix(name, DSKEY);
  this._idxIPrefix    = Tree.getPrefix(name, IKEY);
  this._idxHeadPrefix = Tree.getPrefix(name, HEADKEY);

  // init _i
  this._i = 0;

  this._vSize = opts.vSize || 6;
  this._iSize = opts.iSize || 6;

  if (this._vSize < 0 || this._vSize > 6) { throw new TypeError('opts.vSize must be between 0 and 6'); }
  if (this._iSize < 0 || this._iSize > 6) { throw new TypeError('opts.iSize must be between 0 and 6'); }

  this._log = opts.log || {
    emerg:   console.error,
    alert:   console.error,
    crit:    console.error,
    err:     console.error,
    warning: console.log,
    notice:  console.log,
    info:    console.log,
    debug:   console.log,
    debug2:  console.log,
    getFileStream: noop,
    getErrorStream: noop,
    close: noop
  };
}

util.inherits(Tree, Writable);
module.exports = Tree;

/**
 * Create a key prefix for data store or an index. See keyspec for different key
 * types.
 *
 * @param {String} name  name of the prefix
 * @param {Number} type  valid subkey type
 * @return {Buffer} corresponding prefix
 */
Tree.getPrefix = function getPrefix(name, type) {
  if (typeof name !== 'string') { throw new TypeError('name must be a string'); }
  if (Buffer.byteLength(name) > 255) { throw new TypeError('name must not exceed 255 bytes'); }
  if (typeof type !== 'number') { throw new TypeError('type must be a number'); }
  if (type < 1 || type > 3) { throw new TypeError('type must be in the subkey range of 1 to 3'); }

  var prefix = new Buffer(1 + Buffer.byteLength(name) + 1 + 1);

  prefix[0] = Buffer.byteLength(name);
  prefix.write(name, 1);
  prefix[Buffer.byteLength(name) + 1] = 0x00;
  prefix[Buffer.byteLength(name) + 2] = type;

  return prefix;
}

/**
 * Check if the item contains a valid _h.id, _h.v, _h.pa and _b property. Where
 * _h is for header and _b is for body.
 *
 * @param {Object} item  item to check
 * @return {String} empty string if nothing is wrong or a problem description
 */
Tree.invalidItem = function invalidItem(item) {
  if (typeof item !== 'object' || Array.isArray(item) || item === null) {
    return 'item must be an object';
  }

  if (typeof item._h !== 'object' || Array.isArray(item._h) || item._h === null) {
    return 'item._h must be an object';
  }

  if (typeof item._b !== 'object' || Array.isArray(item._b) || item._b === null) {
    return 'item._b must be an object';
  }

  if (Object.keys(item).length !== 2) {
    return 'item should only contain _h and _b keys';
  }

  if (!item._h.hasOwnProperty('id')) {
    return 'item._h.id must be a buffer, a string or implement "toString"';
  }

  var id = item._h.id;
  if (typeof id === 'undefined' || id === null) {
    return 'item._h.id must be a buffer, a string or implement "toString"';
  }

  if (!Buffer.isBuffer(id) &&
      typeof id !== 'string' &&
      typeof id !== 'boolean' &&
      typeof id !== 'number' &&
      typeof id !== 'symbol' &&
      !('toString' in id && typeof id.toString === 'function')) {
    return 'item._h.id must be a buffer, a string or implement "toString"';
  }

  if (typeof item._h.v !== 'string') {
    return 'item._h.v must be a string';
  }

  if (!Array.isArray(item._h.pa)) {
    return 'item._h.pa must be an array';
  }

  return '';
};

/**
 * Get a range object with start and end points.
 *
 * @param {Buffer} prefix  prefix used for start and end
 * @param {Buffer} [start]  optional extra buffer appended to range
 * @param {Buffer} [end]  optional extra buffer appended to end range
 * @return {Object} a start and end buffer
 */
Tree.getRange = function getRange(prefix, start, end) {
  var slen, elen;

  if (!Buffer.isBuffer(prefix)) {
    throw new TypeError('prefix must be a buffer');
  }

  slen = prefix.length + 1;
  elen = prefix.length + 1;

  if (typeof start !== 'undefined' && start !== null) {
    if (!Buffer.isBuffer(start)) {
      throw new TypeError('start must be a buffer if provided');
    } else {
      slen += start.length;
      elen += start.length + 1;
    }
  }

  if (typeof end !== 'undefined' && end !== null) {
    if (!Buffer.isBuffer(end)) {
      throw new TypeError('end must be a buffer if provided');
    } else {
      elen += end.length + 1;
    }
  }

  var s, e;

  s = new Buffer(slen);
  e = new Buffer(elen);

  prefix.copy(s);
  prefix.copy(e);

  s[prefix.length] = 0x00;

  if (start) {
    start.copy(s, prefix.length + 1);

    e[prefix.length] = 0x00;
    start.copy(e, prefix.length + 1);
  }

  if (end) {
    e[prefix.length + 1 + start.length] = 0x00;
    end.copy(e, prefix.length + 1 + start.length + 1);
  }

  e[elen - 1] = 0xff;

  return { s: s, e: e };
};



/////////////////////
//// PRIVATE API ////
/////////////////////



/**
 * Save a new version in the appropriate DAG if it connects.
 *
 * Implementation of _write method of Writable stream. This method is not
 * called directly.
 *
 * @param {Object} item  item to save
 * @param {String} [encoding]  ignored
 * @param {Function} cb  Callback that is called once the item is saved. First
 *                       parameter will be an error object or null.
 */
Tree.prototype._write = function(item, encoding, cb) {
  if (typeof item !== 'object') { throw new TypeError('item must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var error;
  var that = this;

  var msg = Tree.invalidItem(item);
  if (msg) {
    process.nextTick(function() {
      that._log.err('tree %s %j', msg, item);
      cb(new Error(msg));
    });
    return;
  }

  // make sure we do not already have this item
  var headKey = this._composeHeadKey(item._h.id, item._h.v);
  that._log.debug('tree _write check if items already exists by key %j %j', headKey, item._h);

  this._db.get(headKey, function(err, sItem) {
    if (err && !err.notFound) {
      that._log.err('tree _write item lookup error %j %j', item._h, err);
      that.emit('error', err);
      cb(err);
      return;
    }

    if (sItem) {
      // emit item since we already have it
      that._log.info('tree _write item already exists %j', item._h);
      that.emit('data', item);
      cb();
      return;
    }

    // check if all parents are found and make sure all indices are updated
    that._validParents(item, function(err, valid) {
      if (err) { cb(err); return; }
      if (!valid) {
        error = new Error('item is not connected to the DAG');
        that._log.err('tree _write %j %j', error, item._h);
        cb(error);
        return;
      }

      var tra = [];
      that._nextI(function(err, i) {
        if (err) { cb(err); return; }

        item._h.i = i;

        // update indexes and add new version
        var iKey = that._composeIKey(i);
        var dsKey = that._composeDsKey(item._h);

        // delete all heads this new item replaces
        item._h.pa.forEach(function(pa) {
          tra.push({ type: 'del', key: that._composeHeadKey(item._h.id, pa) });
        });
        tra.push({ type: 'put', key: headKey, value: iKey });
        tra.push({ type: 'put', key: iKey, value: headKey });

        // update data
        tra.push({ type: 'put', key: dsKey, value: BSON.serialize(item) });
        that._db.batch(tra, function(err) {
          // emit item if successfully written, or an error otherwise
          if (err) {
            that._log.err('tree _write item not written %s %j %j', i, item._h, err);
            that.emit('error', err);
          } else {
            that._log.debug('tree _write item written %s %j', i, item._h);
            that.emit('data', item);
          }
          cb(err);
        });
      });
    });
  });
};

/**
 * Ensure input is a string or converted to a string.
 *
 * @param {mixed} x  item that is a string or implements "toString"
 * @return {String} return a string or throw an error if impossible
 */
Tree._ensureString = function _ensureString(x) {
  if (typeof x !== 'string') {
    return x.toString();
  }

  return x;
};

/**
 * Convert an id to a buffer.
 *
 * @param {mixed} id  valid id (item._h.id)
 * @return {Buffer} buffer of id
 */
Tree._idToB = function _idToB(id) {
  if (Buffer.isBuffer(id)) {
    return id;
  }

  return new Buffer(Tree._ensureString(x));
};

/**
 * Compose a key from an id and a number of a given size.
 *
 * @param {Buffer} prefix  prepended buffer
 * @param {mixed} id  valid id (item._h.id)
 * @param {Buffer|Number} n  valid number
 * @param {Number} size  length for n
 * @return {Buffer} preix || id || 0x00 || 1 || n (n padded in size)
 */
Tree._composeKey = function _composeKey(prefix, id, n, size) {
  if (!Buffer.isBuffer(prefix)) { throw new TypeError('n must be a number'); }

  // try to ensure id to be a buffer
  if (!Buffer.isBuffer(id)) {
    id = Tree._idToB(id);
  }

  if (typeof n !== 'number' && !Buffer.isBuffer(n)) { throw new TypeError('n must be a buffer or a number'); }
  if (typeof size !== 'number') { throw new TypeError('size must be a number'); }

  // then create and fill the new buffer
  var b = new Buffer(prefix.length + id.length + 2 + size);

  prefix.copy(b);
  id.copy(b, prefix.length);
  b[prefix.length + 1 + id.length] = 0x00;

  // write n
  if (Buffer.isBuffer(n)) {
    n.copy(b, prefix.length + 1 + id.length + 1);
  } else {
    b.writeUIntLE(n, prefix.length + 1 + id.length + 1, size);
  }

  return b;
};

/**
 * Get i from an ikey.
 *
 * @param {Buffer} b  key of subtype ikey
 * @return {Number} i  lbeint
 */
Tree.prototype._decomposeIKey = function _decomposeIKey(b) {
  if (!Buffer.isBuffer(b)) { throw new TypeError('b must be a buffer'); }
  if (b[this._idxIPrefix.length - 1] !== 0x02) { throw new TypeError('b key must be of key subtype 0x02'); }

  var len = b[this._idxIPrefix.length];

  if (len !== this._iSize) { throw new TypeError('ikey length differs from iSize'); }

  var offset = this._idxIPrefix.length + 1;
  return b.readUIntBE(offset, len);
};

/**
 * Get the key for the i index. A valid i is any number up to 48 bits.
 *
 * @param {Number} i  valid lbeint
 * @return {Buffer} key of subtype ikey
 */
Tree.prototype._composeIKey = function _composeIKey(i) {
  if (typeof i !== 'number') { throw new TypeError('i must be a number'); }

  var b = new Buffer(this._idxIPrefix.length + 1 + this._iSize);
  var offset = 0;

  this._idxIPrefix.copy(b);
  offset += this._idxIPrefix.length;

  b[offset] = this._iSize;
  offset++;

  b.writeUIntBE(i, this._idxIPrefix.length + 1, this._iSize);

  return b;
};

/**
 * Get the key for the head index. It is assumed that id is a string or an
 * object that implements the "toString" method. A valid v is a lbeint.
 *
 * @param {String|Object} id  id that is a string or implements "toString"
 * @param {String} v  base64 representation of a lbeint
 * @return {Buffer} valid key of subtype headkey
 */
Tree.prototype._composeHeadKey = function _composeHeadKey(id, v) {
  id = Tree._ensureString(id);
  if (typeof v !== 'string') { throw new TypeError('v must be a string'); }

  if (v.length * 6 !== this._vSize * 8) {
    this._log.err('tree _composeHeadKey base64 version length is %s bits instead of %s bits (vSize)', v.length * 6, this._vSize * 8);
    throw new Error('v is too short or too long');
  }
  this._log.debug('tree _composeHeadKey %s %s %s', v.length * 6, this._vSize * 8, v);

  var b = new Buffer(this._idxHeadPrefix.length + 1 + Buffer.byteLength(id) + 1 + 1 + this._vSize);
  var offset = 0;

  this._idxHeadPrefix.copy(b, offset);
  offset += this._idxHeadPrefix.length;

  b[offset] = Buffer.byteLength(id);
  offset++;

  (new Buffer(id)).copy(b, offset);
  offset += Buffer.byteLength(id);

  b[offset] = 0x00;
  offset++;

  b[offset] = this._vSize;
  offset++;

  b.write(v, offset, this._vSize, 'base64');

  return b;
};

/**
 * Get the key for the data store. It is assumed that id is a string or an
 * object that implements the "toString" method. A valid i is a lbeint.
 *
 * @param {String|Object} id  id that is a string or implements "toString"
 * @param {Number} i  valid lbeint
 * @return {Buffer} valid key of subtype dskey
 */
Tree.prototype._composeDsKey = function _composeDsKey(id, i) {
  id = Tree._ensureString(id);
  if (typeof i !== 'number') { throw new TypeError('i must be a number'); }

  var b = new Buffer(this._dsPrefix.length + 1 + Buffer.byteLength(id) + 1 + 1 + this._iSize);
  var offset = 0;

  this._dsPrefix.copy(b, offset);
  offset += this._dsPrefix.length;

  b[offset] = Buffer.byteLength(id);
  offset++;

  (new Buffer(id)).copy(b, offset);
  offset += Buffer.byteLength(id);

  b[offset] = 0x00;
  offset++;

  b[offset] = this._iSize;
  offset++;

  b.writeUIntBE(i, offset, this._iSize);

  return b;
};

/**
 * Get the next increment number.
 *
 * @param {Function} cb  The first parameter will contain either an Error object or
 *                       null, second parameter will be a Number.
 */
Tree.prototype._nextI = function _nextI(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  // first check if the last increment is known.
  if (this._i) {
    process.nextTick(function() {
      cb(null, ++that._i);
    });
    return;
  }

  // else find the last increment number
  var r = Tree.getRange(this._idxIPrefix);
  var it = this._db.createKeyStream({ reverse: true, lt: r.e, limit: 1 });

  var found = 0;

  var error;
  it.on('error', function(err) {
    that._log.err('tree _nextI %j', err);
    error = err;
  });

  it.on('data', function(i) {
    that._log.debug('tree _nextI i %s', i);
    found = that._decomposeIKey(i);
  });

  it.on('close', function() {
    that._log.debug('tree _nextI "end"');
    if (error) { cb(error); return; }

    that._i = found;
    cb(null, ++that._i);
  });
};

/**
 * Ensure that the item's parents exist in the given database and that if the
 * item has no parents that there does not exist a non-deleted or
 * non-conflicting head.
 *
 * @param {Object} item  item to inspect
 * @param {Function} cb  first parameter is an error object or null, second
 *                       parameter is a boolean whether the parents connect to
 *                       the saved DAG or in case the item is a root, that there
 *                       exists no other non-deleted and non-conflicting head.
 */
Tree.prototype._validParents = function _validParents(item, cb) {
  var error;
  var that = this;

  // first verify if all parents exist in the DAG
  async.every(item._h.pa, function(pa, cb2) {
    // search from head to older
    that._log.info('tree _validParents ds descending DAG %s search for %s', item._h.id, item._h.v);

    var r = Tree.getRange(that._dsPrefix, Tree._idToB(item._h.id));
    var it = that._db.createValueStream({ reverse: true, gte: r.s, lt: r.e });

    var parentFound = false;

    it.on('data', function(sItem) {
      sItem = BSON.deserialize(sItem);
      that._log.info('tree _validParents ds descend item %j', sItem._h);

      if (sItem._h.v === pa) {
        that._log.info('tree _validParents version equals needle %s', pa);
        // great success
        parentFound = true;
        it.destroy();
      } else {
        that._log.debug('tree _validParents version %s does not equal needle %s', sItem._h.v, pa);
      }
    });

    it.on('close', function() {
      cb2(parentFound);
    });
  }, function(parentsExist) {
    if (!parentsExist) {
      that._log.info('tree _validParents not all parents exist %j', item._h);
      cb(null, false);
      return;
    }

    // all heads are checked
    // if this is not a root, all is done.
    if (item._h.pa.length) {
      cb(null, true);
      return;
    }

    // search for all heads of this id
    var r = Tree.getRange(that._idxHeadPrefix, Tree._idToB(item._h.id));
    var it = that._db.createValueStream({ reverse: true, gte: r.s, lt: r.e });

    // since this is a new root, make sure no head already exists, unless all are in conflict or deleted
    var parentFound = false;

    it.on('error', cb);

    it.on('data', function(i) {
      i = that._decomposeIKey(i);
      that._log.debug('tree _validParents idxHeads %j %s', item._h, i);

      // resolve item
      it.pause();
      that._db.get(that._composeDsKey({ id: item._h.id, i: i }), function(err, sItem) {
        that._log.debug('tree _validParents resolved head %s for %j', i, item._h);

        if (err) {
          that._log.err('tree _validParents resolving head %j', err);
          error = err;
          it.destroy();
          return;
        }

        sItem = BSON.deserialize(sItem);

        if (!sItem._h.c && !sItem._h.d) {
          that._log.info('tree _validParents existing head %j for %j', sItem._h, item._h);
          // no conflict and not deleted
          // a head item for this root exists, not ok, stop searching any further
          parentFound = true;
          it.destroy();
        } else {
          that._log.debug('tree _validParents existing head was _c or _d %j continue...', sItem._h, item._h);
          it.resume();
        }
      });
    });

    it.on('close', function() {
      if (error) { cb(error); return; }

      if (parentFound) {
        that._log.info('tree _validParents active parent exists for new root item %j', item._h);
        cb(null, false);
        return;
      }

      cb(null, true);
    });
  });
};