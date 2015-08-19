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

/* jshint -W018 */

'use strict';

var Transform = require('stream').Transform;
var util = require('util');

var through2 = require('through2');
var async = require('async');
var BSON = require('bson').BSONPure.BSON;

var noop = function() {};

var DSKEY   = 0x01;
var IKEY    = 0x02;
var HEADKEY = 0x03;
var VKEY    = 0x04;

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

  Transform.call(this, opts);

  this._name = name;

  this._db = db;

  // partition db in a data store, i index and head index
  // see the keyspec for details
  this._dsPrefix      = Tree.getPrefix(name, DSKEY);
  this._idxIPrefix    = Tree.getPrefix(name, IKEY);
  this._idxHeadPrefix = Tree.getPrefix(name, HEADKEY);
  this._idxVPrefix    = Tree.getPrefix(name, VKEY);

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

util.inherits(Tree, Transform);
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
  if (type < 0x01 || type > 0x04) { throw new TypeError('type must be in the subkey range of 0x01 to 0x04'); }

  var prefix = new Buffer(1 + Buffer.byteLength(name) + 1 + 1);

  prefix[0] = Buffer.byteLength(name);
  prefix.write(name, 1);
  prefix[Buffer.byteLength(name) + 1] = 0x00;
  prefix[Buffer.byteLength(name) + 2] = type;

  return prefix;
};

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
 * Parse a key.
 *
 * @param {Buffer} key  any valid type: dskey, ikey or headkey, vkey
 * @param {String} [idDecode]  decode id as string with given encoding. Encoding
 *                             must be either "hex", "utf8" or "base64". If not
 *                             given a buffer will be returned.
 * @param {String} [vDecode]  decode v as string with given encoding. Encoding
 *                            must be either "hex", "utf8" or "base64". If not
 *                            given a number will be returned.
 * @return {Object} containing the members name and type and depending on the
 *                  type it can contain id, i and/or v.
 */
Tree.parseKey = function parseKey(key, idDecode, vDecode) {
  if (!Buffer.isBuffer(key)) { throw new TypeError('key must be a buffer'); }

  var name, type, ret;
  var offset, namelen, idlen, nlen;

  offset = 0;

  namelen = key[offset++];

  name = key.slice(offset, offset += namelen);
  if (key[offset++] !== 0x00) { throw new Error('expected a null byte after name'); }

  type = key[offset++];
  if (type < 0x01 || type > 0x04) { throw new TypeError('key is of an unknown type'); }

  ret = {
    name: name,
    type: type
  };

  switch (type) {
  case DSKEY:
    idlen = key[offset++];

    ret.id = key.slice(offset, offset += idlen);
    if (key[offset++] !== 0x00) { throw new Error('expected a null byte after id'); }
    if (ret.id && idDecode) { ret.id = ret.id.toString(idDecode); }

    nlen = key[offset++];
    if (!(nlen > 0)) { throw new Error('i must be at least one byte'); }
    ret.i = key.readUIntBE(offset, nlen);
    offset += nlen;

    if (offset !== key.length) { throw new Error('expected no bytes after i'); }
    break;
  case IKEY:
    nlen = key[offset++];
    if (!(nlen > 0)) { throw new Error('i must be at least one byte'); }
    ret.i = key.readUIntBE(offset, nlen);
    offset += nlen;

    if (offset !== key.length) { throw new Error('expected no bytes after i'); }
    break;
  case HEADKEY:
    idlen = key[offset++];

    ret.id = key.slice(offset, offset += idlen);
    if (key[offset++] !== 0x00) { throw new Error('expected a null byte'); }
    if (ret.id && idDecode) { ret.id = ret.id.toString(idDecode); }

    nlen = key[offset++];
    if (!(nlen > 0)) { throw new Error('v must be at least one byte'); }

    ret.v = vDecode ? key.slice(offset).toString(vDecode) : key.readUIntBE(offset, nlen);
    offset += nlen;

    if (offset !== key.length) { throw new Error('expected no bytes after v'); }
    break;
  case VKEY:
    nlen = key[offset++];
    if (!(nlen > 0)) { throw new Error('v must be at least one byte'); }
    ret.v = vDecode ? key.slice(offset).toString(vDecode) : key.readUIntBE(offset, nlen);
    offset += nlen;

    if (offset !== key.length) { throw new Error('expected no bytes after v'); }
    break;
  }

  return ret;
};

/**
 * Get a range object with start and end points for the ikey index.
 *
 * @param {Object} [opts]  object containing configurable parameters
 * @return {Object}  start and end buffer: { s: buffer, e: buffer }
 *
 * opts:
 *   minI {Number}  minimum offset, valid lbeint
 *   maxI {Number}  maximum value, valid lbeint
 */
Tree.prototype.getIKeyRange = function getIKeyRange(opts) {
  if (typeof opts !== 'undefined' && typeof opts !== 'object' || Array.isArray(opts) || opts === null) { throw new TypeError('opts must be an object'); }

  opts = opts || {};

  var minI, maxI;

  if (typeof opts.minI !== 'undefined') {
    if (typeof opts.minI !== 'number') { throw new TypeError('opts.minI must be a number'); }
    minI = opts.minI;
  }

  if (typeof opts.maxI !== 'undefined') {
    if (typeof opts.maxI !== 'number') { throw new TypeError('opts.maxI must be a number'); }
    maxI = opts.maxI;
  }

  var prefix = this._idxIPrefix;

  var s;
  if (minI) {
    s = new Buffer(prefix.length + 1 + this._iSize);
  } else {
    s = new Buffer(prefix.length);
  }

  var e = new Buffer(prefix.length + 1 + this._iSize + 1);

  var offset = 0;

  // fill s
  prefix.copy(s);
  prefix.copy(e);

  offset += prefix.length;

  if (minI) {
    s[offset] = this._iSize;
  }

  e[offset] = this._iSize + 1;

  offset++;

  if (minI) {
    s.writeUIntBE(minI, offset, this._iSize);
  }

  if (maxI) {
    e.writeUIntBE(maxI, offset, this._iSize);
    offset += this._iSize;
    e[offset] = 0xff;
  } else {
    e.fill(0xff, offset);
  }

  return { s: s, e: e };
};

/**
 * Get a range object with start and end points for the vkey index.
 *
 * @return {Object}  start and end buffer: { s: buffer, e: buffer }
 */
Tree.prototype.getVKeyRange = function getVKeyRange() {
  var prefix = this._idxVPrefix;

  var s = new Buffer(prefix.length);
  var e = new Buffer(prefix.length + 1 + this._vSize + 1);

  // fill s
  prefix.copy(s);
  prefix.copy(e);

  var offset = 0;
  offset += prefix.length;

  e[offset] = this._vSize + 1;
  offset++;

  e.fill(0xff, offset);

  return { s: s, e: e };
};

/**
 * Get a range object with start and end points for the headkey index.
 *
 * @param {Buffer} [id]  optional id to prefix start and end
 * @return {Object}  start and end buffer: { s: buffer, e: buffer }
 */
Tree.prototype.getHeadKeyRange = function getHeadKeyRange(id) {
  if (id && !Buffer.isBuffer(id)) { throw new TypeError('id must be a buffer if provided'); }

  var prefix = this._idxHeadPrefix;

  var len = prefix.length;
  if (id) {
    len += 1 + id.length;
  }

  var s = new Buffer(len);
  var e = new Buffer(len + 1);

  var offset = 0;

  // fill s
  prefix.copy(s);

  offset += prefix.length;

  if (id) {
    s[offset++] = id.length;
    id.copy(s, offset);
    offset += id.length;
  }

  s.copy(e);
  e[offset] = 0xff;

  return { s: s, e: e };
};

/**
 * Get a range object with start and end points for the dskey index.
 *
 * @param {Buffer} [id]  optional id to prefix start and end
 * @return {Object}  start and end buffer: { s: buffer, e: buffer }
 */
Tree.prototype.getDsKeyRange = function getDsKeyRange(id) {
  if (id && !Buffer.isBuffer(id)) { throw new TypeError('id must be a buffer if provided'); }

  var prefix = this._dsPrefix;

  var len = prefix.length;
  if (id) {
    len += 1 + id.length;
  }

  var s = new Buffer(len);
  var e = new Buffer(len + 1);

  var offset = 0;

  // fill s
  prefix.copy(s);

  offset += prefix.length;

  if (id) {
    s[offset++] = id.length;
    id.copy(s, offset);
    offset += id.length;
  }

  s.copy(e);
  e[offset] = 0xff;

  return { s: s, e: e };
};

/**
 * Get an iterator that iterates over all heads in order of id. Both values and
 * keys are emitted.
 *
 * @return {Readable Stream}  each head key by id and version and value is i.
 */
Tree.prototype.getHeads = function getHeads() {
  var r = this.getHeadKeyRange();
  this._log.info('getHeads %j', r);
  return this._db.createReadStream({ gte: r.s, lt: r.e });
};

/**
 * Get an iterator that iterates over all heads in order of id. Only Keys are
 * emitted.
 *
 * @return {Readable Stream}  each head key by id and version
 */
Tree.prototype.getHeadKeys = function getHeadKeys() {
  var r = this.getHeadKeyRange();
  this._log.info('getHeadKeys %j', r);
  return this._db.createKeyStream({ gte: r.s, lt: r.e });
};

/**
 * Get an item by version. A valid version is any number up to 48 bits.
 *
 * @param {Number|base64 String} version  valid lbeint or base64 int
 * @param {Function} cb  First parameter will be an error object or null. Second
 *                       parameter will be a found dskey buffer or null if
 *                       version is not found.
 */
Tree.prototype.getDsKeyByVersion = function getDsKeyByVersion(version, cb) {
  if (typeof version !== 'number' && typeof version !== 'string') { throw new TypeError('version must be a number or a base64 string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  this._log.debug('tree getDsKeyByVersion %s', version);

  var that = this;
  this._db.get(this._composeVKey(version), function(err, dsKey) {
    if (err) {
      if (err.notFound) {
        cb(null, null);
        return;
      }

      that._log.err('tree getDsKeyByVersion lookup error %j', err);
      that.emit('error', err);
      cb(err, null);
      return;
    }

    cb(null, dsKey);
  });
};

/**
 * Get a readable stream that emits items in the order as they were added to this
 * tree.
 *
 * @param {Number} [i]  optional offset, valid lbeint
 * @return {Readable Stream}  emits each item as a BSON serialized buffer
 */
Tree.prototype.createReadStream = function createReadStream(i) {
  var that = this;

  var readable = through2.obj(function(obj, enc, cb) {
    this.push(obj);
    cb();
  });

  // first start reading the ikey index
  var r = this.getIKeyRange({ minI: i });
  var it = this._db.createReadStream({ gte: r.s, lte: r.e });

  it.on('data', function(obj) {
    it.pause();

    var key = Tree.parseKey(obj.key);
    var val = Tree.parseKey(obj.value, 'utf8');
    var dsKey = that._composeDsKey(val.id, key.i);

    that._db.get(dsKey, function(err, item) {
      if (err) {
        that._log.err('tree createReadStream %s', err);
        it.destroy();
      }

      if (readable.push(item)) {
        it.resume();
      } else {
        readable.once('drain', function() {
          it.resume();
        });
      }
    });
  });

  // when the end of the ikey index is reached, pipe all data events directly to
  // the stream
  it.on('end', function() {
    that.pipe(readable);
  });

  return readable;
};



/////////////////////
//// PRIVATE API ////
/////////////////////



/**
 * Save a new version in the appropriate DAG if it connects.
 *
 * Implementation of _transform method of Transform stream. This method is not
 * called directly.
 *
 * @param {Object} item  item to save
 * @param {String} [encoding]  ignored
 * @param {Function} cb  Callback that is called once the item is saved. First
 *                       parameter will be an error object or null.
 *
 * cb can be called with any of the following errors:
 *   item is an invalid item (see Tree.invalidItem)
 *   item is not a new leaf
 */
Tree.prototype._transform = function(item, encoding, cb) {
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

    // check if this is a new leaf
  this._isNewChild(item, function(err, isNew) {
    if (err) { cb(err); return; }
    if (!isNew) {
      error = new Error('item is not a new leaf');
      that._log.err('tree _transform %s %j', error, item._h);
      cb(error);
      return;
    }

    var tra = [];
    that._nextI(function(err, i) {
      if (err) { cb(err); return; }

      item._h.i = i;

      // update indexes and add new version
      var iKey = that._composeIKey(i);
      var vKey = that._composeVKey(item._h.v);
      var headKey = that._composeHeadKey(item._h.id, item._h.v);
      var dsKey = that._composeDsKey(item._h.id, i);

      // delete all heads this new item replaces
      item._h.pa.forEach(function(pa) {
        that._log.debug('tree _transform delete old head', item._h.id, pa);
        tra.push({ type: 'del', key: that._composeHeadKey(item._h.id, pa) });
      });
      tra.push({ type: 'put', key: headKey, value: iKey });
      tra.push({ type: 'put', key: iKey, value: headKey });
      tra.push({ type: 'put', key: vKey, value: dsKey });

      // update data
      var bitem = BSON.serialize(item);
      tra.push({ type: 'put', key: dsKey, value: bitem });
      that._db.batch(tra, function(err) {
        // emit item if successfully written, or an error otherwise
        if (err) {
          that._log.err('tree _transform item not written %s %j %j', i, item._h, err);
          cb(err);
          return;
        }

        that._log.info('tree _transform item written %s %j', i, item._h);
        that.push(bitem);
        cb();
      });
    });
  });
};

/**
 * Ensure input is a string or converted to a string.
 *
 * @param {mixed} obj  item that is a string or implements "toString"
 * @return {String} return a string or throw an error if impossible
 */
Tree._ensureString = function _ensureString(obj) {
  if (typeof obj !== 'string') {
    return obj.toString();
  }

  return obj;
};

/**
 * Convert something to a buffer. Accepts numbers, strings arrays and anything
 * else that implements "toString".
 *
 * @param {mixed} obj  valid id (item._h.id)
 * @return {Buffer} buffer
 */
Tree._toBuffer = function _toBuffer(obj) {
  if (Buffer.isBuffer(obj)) {
    return obj;
  }

  if (typeof obj === 'number') {
    return new Buffer([obj]);
  }

  if (typeof obj === 'string') {
    return new Buffer(obj);
  }

  if (Array.isArray(obj)) {
    return new Buffer(obj);
  }

  return new Buffer(Tree._ensureString(obj));
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
 * Get the key for the v index. A valid v is any number up to 48 bits.
 *
 * @param {Number|base64 String} v  valid lbeint or base64 int
 * @return {Buffer} key of subtype vkey
 */
Tree.prototype._composeVKey = function _composeVKey(v) {
  if (typeof v !== 'number' && typeof v !== 'string') { throw new TypeError('v must be a number or a base64 string'); }

  if (typeof v === 'string' && Buffer.byteLength(v, 'base64') !== this._vSize) { throw new Error('v must be the same size as the configured vSize'); }

  var b = new Buffer(this._idxVPrefix.length + 1 + this._vSize);
  var offset = 0;

  this._idxVPrefix.copy(b);
  offset += this._idxVPrefix.length;

  b[offset] = this._vSize;
  offset++;

  if (typeof v === 'number') {
    b.writeUIntBE(v, offset, this._vSize);
  } else {
    b.write(v, offset, this._vSize, 'base64');
  }

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
  this._log.debug('tree _composeHeadKey %s %s', id, v);

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

  this._log.debug('tree _nextI first time, resolve max i');

  // else find the last increment number
  var r = this.getIKeyRange();
  var it = this._db.createKeyStream({ reverse: true, gt: r.s, lt: r.e, limit: 1 });

  var found = 0;

  var error;
  it.on('error', function(err) {
    that._log.err('tree _nextI %j', err);
    error = err;
  });

  it.on('data', function(i) {
    that._log.debug('tree _nextI i %s', i);
    found = Tree.parseKey(i).i;
  });

  it.on('close', function() {
    that._log.debug('tree _nextI close, max i found %s', found);
    if (error) { cb(error); return; }

    that._i = found;
    cb(null, ++that._i);
  });
};

/**
 * Check if the given item is a new child of an existing DAG. If there is no
 * existing DAG, or if all heads are in conflict or deleted, any new root will
 * be considered to be a new child. Otherwise only new leaves ("fast-forwards"
 * and forks) will be considered to be new childs. If the item (version) already
 * exists in the DAG it will not be considered to be a new child.
 *
 * @param {Object} item  item to inspect
 * @param {Function} cb  first parameter is an error object or null, second
 *                       parameter is a boolean whether this item is a new child
 */
Tree.prototype._isNewChild = function _isNewChild(item, cb) {
  var error;
  var that = this;

  // make sure this version does not exist
  var vKey = this._composeVKey(item._h.v);
  that._log.debug('tree _isNewChild check if this version already exists %j', item._h);

  this._db.get(vKey, function(err, dsKey) {
    if (err && !err.notFound) {
      that._log.err('tree _isNewChild item lookup error %j', err);
      that.emit('error', err);
      cb(err);
      return;
    }

    // an error is expected (not found) if the version does not exist
    if (!err) {
      var key = Tree.parseKey(dsKey, 'utf8');
      if (key.id !== item._h.id) {
        error = 'version exists for a different id';
        that._log.info('tree _isNewChild %j %s %s', error, key.id, item._h.v);
        that.emit('error', error);
        cb(error);
        return;
      } else {
        // the version does already exist
        that._log.debug('tree _isNewChild version already exists %s', item._h.v);
        cb(null, false);
        return;
      }
    }

    that._log.debug('tree _isNewChild version does not exist');

    // verify if all parents exist in the DAG
    async.every(item._h.pa, function(pa, cb2) {
      // search from head to older
      that._log.info('tree _isNewChild descend %s search for parent %s', item._h.id, pa);

      var r = that.getDsKeyRange(Tree._toBuffer(item._h.id));
      var it = that._db.createValueStream({ reverse: true, gte: r.s, lt: r.e });

      var parentFound = false;

      it.on('data', function(sItem) {
        sItem = BSON.deserialize(sItem);
        that._log.debug('tree _isNewChild descend item %j', sItem._h);

        if (sItem._h.v === pa) {
          that._log.info('tree _isNewChild version equals parent %s', pa);
          // great success
          parentFound = true;
          it.destroy();
        } else {
          that._log.debug('tree _isNewChild version %s does not equal parent %s', sItem._h.v, pa);
        }
      });

      it.on('close', function() {
        cb2(parentFound);
      });
    }, function(parentsExist) {
      if (!parentsExist) {
        that._log.info('tree _isNewChild not all parents exist %j', item._h);
        cb(null, false);
        return;
      }

      that._log.debug('tree _isNewChild all parents exist');

      // all heads are checked
      // if this is not a root, all is done.
      if (item._h.pa.length) {
        cb(null, true);
        return;
      }

      that._log.debug('tree _isNewChild since this is a new root, search for exising items');

      // search for all heads of this id
      var r = that.getHeadKeyRange(Tree._toBuffer(item._h.id));
      var it = that._db.createValueStream({ reverse: true, gte: r.s, lt: r.e });

      // since this is a new root, make sure no head already exists, unless all are in conflict or deleted
      var parentFound = false;

      it.on('error', cb);

      it.on('data', function(i) {
        i = Tree.parseKey(i).i;
        that._log.debug('tree _isNewChild idxHeads %j %s', item._h, i);

        // resolve item
        it.pause();
        that._db.get(that._composeDsKey(item._h.id, i), function(err, sItem) {
          that._log.debug('tree _isNewChild resolved head %s for %j', i, item._h);

          if (err) {
            that._log.err('tree _isNewChild resolving head %j', err);
            error = err;
            it.destroy();
            return;
          }

          sItem = BSON.deserialize(sItem);

          if (!sItem._h.c && !sItem._h.d) {
            that._log.info('tree _isNewChild existing head %j for %j', sItem._h, item._h);
            // no conflict and not deleted
            // a head item for this root exists, not ok, stop searching any further
            parentFound = true;
            it.destroy();
          } else {
            that._log.debug('tree _isNewChild existing head was _c or _d %j continue...', sItem._h);
            it.resume();
          }
        });
      });

      it.on('close', function() {
        if (error) { cb(error); return; }

        if (parentFound) {
          that._log.info('tree _isNewChild active parent exists for new root item %j', item._h);
          cb(null, false);
          return;
        }

        cb(null, true);
      });
    });
  });
};
