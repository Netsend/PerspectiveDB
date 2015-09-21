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

var Writable = require('stream').Writable;
var util = require('util');

var async = require('async');
var bson = require('bson');
var BSON = new bson.BSONPure.BSON();

var invalidItem = require('./invalid_item');
var StreamTree = require('./_stream_tree');

var noop = function() {};

var DSKEY   = 0x01;
var IKEY    = 0x02;
var HEADKEY = 0x03;
var VKEY    = 0x04;
var USKEY   = 0x05;

// head index value option masks
var CONFLICT = 0x01;
var DELETE = 0x02;

/**
 * Tree
 *
 * Manage DAGs in a tree.
 *
 * @param {LevelUP.db} db  database for persistent storage
 * @param {String} name  name of this tree, should not exceed 254 bytes
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
  if (Buffer.byteLength(name) > 254) { throw new TypeError('name must not exceed 254 bytes'); }

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }
  if (opts.log != null && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }
  if (opts.vSize != null && typeof opts.vSize !== 'number') { throw new TypeError('opts.vSize must be a number'); }
  if (opts.iSize != null && typeof opts.iSize !== 'number') { throw new TypeError('opts.iSize must be a number'); }

  opts.objectMode = true;

  Writable.call(this, opts);

  this.name = name;

  this._db = db;

  // partition db in a data store, i-, head-, v- and pe-index
  // see the keyspec for details
  this._dsPrefix      = Tree.getPrefix(name, DSKEY);
  this._idxIPrefix    = Tree.getPrefix(name, IKEY);
  this._idxHeadPrefix = Tree.getPrefix(name, HEADKEY);
  this._idxVPrefix    = Tree.getPrefix(name, VKEY);
  this._usPrefix      = Tree.getPrefix(name, USKEY);

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
  if (Buffer.byteLength(name) > 254) { throw new TypeError('name must not exceed 254 bytes'); }
  if (typeof type !== 'number') { throw new TypeError('type must be a number'); }
  if (type < 0x01 || type > 0x05) { throw new TypeError('type must be in the subkey range of 0x01 to 0x05'); }

  var prefix = new Buffer(1 + Buffer.byteLength(name) + 1 + 1);

  prefix[0] = Buffer.byteLength(name);
  prefix.write(name, 1);
  prefix[Buffer.byteLength(name) + 1] = 0x00;
  prefix[Buffer.byteLength(name) + 2] = type;

  return prefix;
};

/**
 * Parse a key.
 *
 * @param {Buffer} key  any valid type: dskey, ikey or headkey, vkey
 * @param {Object} [opts]  object containing configurable parameters
 * @return {Object} containing the members name and type and depending on the
 *                  type it can contain id, i and/or v.
 *
 *
 * opts:
 *   decodeId {String}  decode id as string with given encoding. Encoding must
 *                      be either "hex", "utf8" or "base64". If not given a
 *                      buffer will be returned.
 *   decodeV {String}  decode v as string with given encoding. Encoding must be
 *                     either "hex", "utf8" or "base64". If not given a number
 *                     will be returned.
 *   decodeUs {String}  decode pe as string with given encoding. Encoding must
 *                      be either "hex", "utf8" or "base64". If not given a
 *                      number will be returned.
 */
Tree.parseKey = function parseKey(key, opts) {
  if (!Buffer.isBuffer(key)) { throw new TypeError('key must be a buffer'); }

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }

  var idDecode = opts.decodeId;
  var vDecode  = opts.decodeV;
  var usDecode = opts.decodeUs;

  var name, type, ret;
  var offset, namelen, idlen, nlen;

  offset = 0;

  namelen = key[offset++];

  name = key.slice(offset, offset += namelen);
  if (key[offset++] !== 0x00) { throw new Error('expected a null byte after name'); }

  type = key[offset++];
  if (type < 0x01 || type > 0x05) { throw new TypeError('key is of an unknown type'); }

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
  case USKEY:
    nlen = key[offset++];
    if (!(nlen > 0)) { throw new Error('us must be at least one byte'); }
    ret.us = key.slice(offset, offset += nlen);
    if (key[offset++] !== 0x00) { throw new Error('expected a null byte'); }
    if (ret.us && usDecode) { ret.us = ret.us.toString(usDecode); }

    nlen = key[offset++];
    if (!(nlen > 0)) { throw new Error('i must be at least one byte'); }
    ret.i = key.readUIntBE(offset, nlen);
    offset += nlen;

    if (offset !== key.length) { throw new Error('expected no bytes after i'); }
    break;
  }

  return ret;
};

/**
 * Parse head index value.
 *
 * @param {Buffer} value  value of a headkey
 * @return {Object} containing the options and i
 */
Tree.parseHeadVal = function parseHeadVal(value) {
  if (!Buffer.isBuffer(value)) { throw new TypeError('value must be a buffer'); }

  var opts, ret;
  var offset, nlen;

  ret = {};

  offset = 0;

  // read option byte
  opts = value.readUIntBE(offset++, 1);

  if (opts & CONFLICT) {
    ret.c = true; 
  }
  if (opts & DELETE) {
    ret.d = true;
  }

  nlen = value[offset++];
  if (!(nlen > 0)) { throw new Error('i must be at least one byte'); }
  ret.i = value.readUIntBE(offset, nlen);
  offset += nlen;

  if (offset !== value.length) { throw new Error('unexpected length of value'); }

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
  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }

  opts = opts || {};

  var minI, maxI;

  if (opts.minI != null) {
    if (typeof opts.minI !== 'number') { throw new TypeError('opts.minI must be a number'); }
    minI = opts.minI;
  }

  if (opts.maxI != null) {
    if (typeof opts.maxI !== 'number') { throw new TypeError('opts.maxI must be a number'); }
    maxI = opts.maxI;
  }

  var prefix = this._idxIPrefix;

  var slen = prefix.length;
  var elen = prefix.length;

  if (minI) {
    slen += 1 + this._iSize;
  }

  if (maxI) {
    elen += 1 + this._iSize;
  } else {
    elen += 1; // 0xff byte
  }

  var s = new Buffer(slen);
  var e = new Buffer(elen);

  var offset = 0;

  // fill s
  prefix.copy(s);
  offset += prefix.length;

  s.copy(e);

  if (minI) {
    s[offset] = this._iSize;
    s.writeUIntBE(minI, offset + 1, this._iSize);
  }

  if (maxI) {
    e[offset] = this._iSize;
    e.writeUIntBE(maxI, offset + 1, this._iSize);
  } else {
    e[offset] = 0xff;
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
    len += 1 + id.length + 1;
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
    s[offset++] = 0x00;
  }

  s.copy(e);
  e[offset] = 0xff;

  return { s: s, e: e };
};

/**
 * Get a range object with start and end points for the dskey index.
 *
 * @param {Buffer} [id]  optional id to prefix start and end
 * @param {Object} [opts]  object containing configurable parameters
 * @return {Object}  start and end buffer: { s: buffer, e: buffer }
 *
 * opts:
 *   id {Buffer|String|Object}  id to prefix start and end, must be a buffer,
 *                              string or implement "toString"
 *   minI {Number}  minimum offset, valid lbeint (id required)
 *   maxI {Number}  maximum value, valid lbeint (id required)
 */
Tree.prototype.getDsKeyRange = function getDsKeyRange(opts) {
  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }

  var id = opts.id;
  if (id != null && !Buffer.isBuffer(id)) {
    id = new Buffer(Tree._ensureString(id));
  }

  var minI, maxI;

  if (opts.minI != null) {
    if (typeof opts.minI !== 'number') { throw new TypeError('opts.minI must be a number'); }
    minI = opts.minI;
  }

  if (opts.maxI != null) {
    if (typeof opts.maxI !== 'number') { throw new TypeError('opts.maxI must be a number'); }
    maxI = opts.maxI;
  }

  var prefix = this._dsPrefix;

  var slen = prefix.length;
  var elen = prefix.length;

  if (id) {
    slen += 1 + id.length + 1;
    elen += 1 + id.length + 1;

    if (minI) {
      slen += 1 + this._iSize;
    }

    if (maxI) {
      elen += 1 + this._iSize;
    }
  }

  var s = new Buffer(slen);
  var e = new Buffer(elen + 1);

  var offset = 0;

  // fill s
  prefix.copy(s);
  offset += prefix.length;

  if (id) {
    s[offset++] = id.length;
    id.copy(s, offset);
    offset += id.length;

    s[offset++] = 0x00;

    s.copy(e);

    if (minI) {
      s[offset] = this._iSize;
      s.writeUIntBE(minI, offset + 1, this._iSize);
    }

    if (maxI) {
      e[offset] = this._iSize;
      e.writeUIntBE(maxI, offset + 1, this._iSize);
      offset += 1;
      offset += this._iSize;
    }
  } else {
    s.copy(e);
  }
  e[offset] = 0xff;

  return { s: s, e: e };
};

/**
 * Get all head versions of a given id.
 *
 * @param {Buffer} id  id to find versions for
 * @param {Function} cb  First parameter will be an error object or null. Second
 *                       parameter will be an array with version strings.
 */
Tree.prototype.getHeadVersions = function getHeadVersions(id, cb) {
  if (!Buffer.isBuffer(id)) { throw new TypeError('id must be a buffer'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var r = this.getHeadKeyRange(id);
  this._log.info('tree getHeadVersions range %j', r);

  var it = this._db.createKeyStream({ gte: r.s, lt: r.e });

  var result = [];

  var that = this;
  it.on('data', function(headKey) {
    var key = Tree.parseKey(headKey, { decodeV: 'base64' });

    that._log.debug('tree getHeadVersions iterate key %j', key);
    result.push(key.v);
  });

  it.on('error', cb);

  it.on('end', function() {
    cb(null, result);
  });
};

/**
 * Get an iterator that iterates over all heads in order of id. Both values and
 * keys are emitted.
 *
 * @param {Object} [opts]  object containing configurable parameters
 * @param {Function} iterator  function(head, next) called with each head
 * @param {Function} cb  First parameter will be an error object or null.
 *
 * opts:
 *   id {Buffer|String|Object}  id to prefix start and end, must be a buffer,
 *                              string or implement "toString"
 *   skipConflicts {Boolean, default false}  whether to emit heads with the
 *                                           conflict bit set
 *   skipDeletes {Boolean, default false}  whether to emit heads with the delete
 *                                         bit set
 *   raw {Boolean, default false}  whether to return a BSON serialized or
 *                                 deserialezed object (false).
 */
Tree.prototype.getHeads = function getHeads(opts, iterator, cb) {
  if (typeof opts === 'function') {
    cb = iterator;
    iterator = opts;
    opts = null;
  }

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  var id = opts.id;
  if (id != null && !Buffer.isBuffer(id)) {
    id = new Buffer(Tree._ensureString(id));
  }
  if (opts.skipConflicts != null && typeof opts.skipConflicts !== 'boolean') { throw new TypeError('opts.skipConflicts must be a boolean'); }
  if (opts.skipDeletes != null && typeof opts.skipDeletes !== 'boolean') { throw new TypeError('opts.skipDeletes must be a boolean'); }
  if (opts.raw != null && typeof opts.raw !== 'boolean') { throw new TypeError('opts.raw must be a boolean'); }
  if (typeof iterator !== 'function') { throw new TypeError('iterator must be a function'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var r = this.getHeadKeyRange(id);
  this._log.info('tree getHeads range %j', r);

  var it = this._db.createReadStream({ gte: r.s, lt: r.e });

  var that = this;

  var processing = 0;
  var ended;

  it.on('data', function(headIdx) {
    processing++;

    var key = Tree.parseKey(headIdx.key, { decodeV: 'base64' });
    var val = Tree.parseHeadVal(headIdx.value);
    if (opts.skipConflicts && val.c) {
      processing--;
      return;
    }

    if (opts.skipDeletes && val.d) {
      processing--;
      return;
    }

    that._log.debug('tree getHeads iterate key %j val %j', key, val);

    that._db.get(that._composeDsKey(key.id, val.i), function(err, item) {
      if (err) {
        that._log.err('tree getHeads %s', err);
        it.pause();
        cb(err);
        return;
      }

      that._log.debug('tree getHeads iterate item %j', item);

      iterator(opts.raw ? item : BSON.deserialize(item), function(err, cont) {
        if (err) {
          it.pause();
          cb(err);
          return;
        }

        if (cont != null && !cont) {
          it.pause();
          cb();
          return;
        }

        processing--;
        if (ended && !processing) {
          cb();
        }
      });
    });
  });

  it.on('error', cb);

  it.on('end', function() {
    ended = true;
    if (!processing) {
      cb();
    }
  });
};

/**
 * Get an item by version. A valid version is any number up to 48 bits.
 *
 * @param {Number|base64 String} version  valid lbeint or base64 int
 * @param {Object} [opts]  object containing configurable parameters
 * @param {Function} cb  First parameter will be an error object or null. Second
 *                       parameter will be an item if found.
 *
 * opts:
 *   raw {Boolean, default false}  whether to return a BSON serialized or
 *                                 deserialezed object (false).
 */
Tree.prototype.getByVersion = function getByVersion(version, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = null;
  }

  if (typeof version !== 'number' && typeof version !== 'string') { throw new TypeError('version must be a number or a base64 string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }
  if (opts.raw != null && typeof opts.raw !== 'boolean') { throw new TypeError('opts.raw must be a boolean'); }

  this._log.debug('tree getByVersion %s', version);

  var that = this;
  this._getDsKeyByVersion(version, function(err, dsKey) {
    if (err) { cb(err); return; }

    if (!dsKey) {
      cb(null, null);
      return;
    }

    that._db.get(dsKey, function(err, item) {
      if (err) { cb(err); return; }
      cb(null, opts.raw ? item : BSON.deserialize(item));
    });
  });
};

/**
 * Get an iterator over each item in the order of insertion into the tree.
 *
 * @param {Object} [opts]  object containing configurable parameters
 * @param {Function} iterator  function (item, next) called with each item as a
 *                             BSON serialized buffer. Call next when done
 *                             handling the item. next(err, continue) where
 *                             continue defaults to true.
 * @param {Function} cb  First parameter will be an error object or null.
 *
 * opts:
 *   id {String|Object}  limit to one specific DAG
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
Tree.prototype.iterateInsertionOrder = function iterateInsertionOrder(opts, iterator, cb) {
  if (typeof opts === 'function') {
    cb = iterator;
    iterator = opts;
    opts = null;
  }

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }
  if (typeof iterator !== 'function') { throw new TypeError('iterator must be a function'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var s = new StreamTree(this, opts);

  s.on('data', function(item) {
    s.pause();
    iterator(item, function(err, cont) {
      if (err) { cb(err); return; }
      if (cont != null && !cont) {
        cb(err);
        return;
      }
      s.resume();
    });
  });

  s.on('error', cb);
  s.on('end', cb);
};

/**
 * Get a stream over each item in the order of insertion into the tree.
 *
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   id {String|Object}  limit to one specific DAG
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
Tree.prototype.createReadStream = function createReadStream(opts) {
  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }

  return new StreamTree(this, opts);
};

/**
 * Get last version of a certain perspective.
 *
 * Assume _writev writes the last accepted version per perspective.
 *
 * @param {Buffer|String} pe  perspective
 * @param {String} [decodeV]  decode v as string with given encoding. Encoding
 *                          must be either "hex", "utf8" or "base64". If not
 *                          given a number will be returned.
 * @param {Function} cb  First parameter will be an error object or null. Second
 *                       parameter will be a version or null.
 */
Tree.prototype.lastByPerspective = function lastByPerspective(pe, decodeV, cb) {
  if (!Buffer.isBuffer(pe)) {
    if (typeof pe !== 'string') { throw new TypeError('pe must be a buffer or a string'); }
    pe = new Buffer(pe);
  }
  if (typeof decodeV === 'function') {
    cb = decodeV;
    decodeV = null;
  }
  if (decodeV && typeof decodeV !== 'string') { throw new TypeError('decodeV must be a string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  // find the last version by perspective
  var usKey = this._composeUsKey(pe);
  this._db.get(usKey, function(err, v) {
    if (err) {
      if (err.notFound) {
        cb();
        return;
      } else {
        that._log.err('tree lastByPerspective lookup error %j', err);
        cb(err);
        return;
      }
    }

    if (decodeV) { v = v.toString(decodeV); }

    that._log.debug('tree lastByPerspective %s', v);
    cb(null, v);
  });
};



/////////////////////
//// PRIVATE API ////
/////////////////////



/**
 * Get a dsKey by version. A valid version is any number up to 48 bits.
 *
 * @param {Number|base64 String} version  valid lbeint or base64 int
 * @param {Function} cb  First parameter will be an error object or null. Second
 *                       parameter will be a found dskey buffer or null if
 *                       version is not found.
 */
Tree.prototype._getDsKeyByVersion = function _getDsKeyByVersion(version, cb) {
  if (typeof version !== 'number' && typeof version !== 'string') { throw new TypeError('version must be a number or a base64 string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  this._log.debug('tree _getDsKeyByVersion %s', version);

  var that = this;
  this._db.get(this._composeVKey(version), function(err, dsKey) {
    if (err) {
      if (err.notFound) {
        cb(null, null);
        return;
      }

      that._log.err('tree _getDsKeyByVersion lookup error %j', err);
      cb(err, null);
      return;
    }

    cb(null, dsKey);
  });
};

/**
 * Resolve v to i.
 *
 * @param {Number|base64 String} version  valid lbeint or base64 int
 * @param {Function} cb  First parameter will be an error object or null. Second
 *                       parameter will be a number.
 */
Tree.prototype._resolveVtoI = function _resolveVtoI(version, cb) {
  if (typeof version !== 'number' && typeof version !== 'string') { throw new TypeError('version must be a number or a base64 string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  var error;

  this._getDsKeyByVersion(version, function(err, dsKey) {
    if (err) { cb(err); return; }

    if (!dsKey) {
      error = new Error('version not found');
      that._log.err('tree _resolveVtoI %s version: %s', error, version);
      cb(error);
      return;
    }

    var i = Tree.parseKey(dsKey).i;
    that._log.debug('tree _resolveVtoI resolved v to i %s', i);
    cb(null, i);
  });
};

/**
 * Get max i.
 *
 * @param {Function} cb  First parameter will be an error object or null. Second
 *                       parameter will be the maximum i found (number).
 */
Tree.prototype._maxI = function _maxI(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  // find the last increment number
  var r = this.getIKeyRange();
  var it = this._db.createKeyStream({ reverse: true, gt: r.s, lt: r.e, limit: 1 });

  var that = this;
  var error;
  var found = null;

  it.on('error', function(err) {
    that._log.err('tree _maxI %j', err);
    error = err;
  });

  it.on('data', function(i) {
    that._log.debug('tree _maxI i %s', i);
    found = Tree.parseKey(i).i;
  });

  it.on('end', function() {
    that._log.debug('tree _maxI %s', found);
    cb(error, found);
  });
};

/**
 * Save a new version in the appropriate DAG if it connects.
 *
 * Implementation of _write method for the stream.Writable class.
 *
 * @param {Object} item  item to save
 * @param {String} [encoding]  ignored
 * @param {Function} cb  Callback that is called once the item is saved. First
 *                       parameter will be an error object or null.
 *
 * cb can be called with any of the following errors:
 *   item is an invalid item (see Tree.invalidItem)
 *   item is not a new child
 */
Tree.prototype._write = function(item, encoding, cb) {
  if (typeof item !== 'object') { throw new TypeError('item must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  this._writev([{ chunk: item, encoding: encoding }], cb);
};

/**
 * Save multiple new versions in the appropriate DAG if they connect.
 *
 * Implementation of _writev method for the stream.Writable class.
 *
 * Keep a record of last written version per perspective to assist
 * lastByPerspective.
 *
 * @param {Array} items  items to save
 * @param {Function} cb  Callback that is called once all items are saved. First
 *                       parameter will be an error object or null.
 *
 * cb can be called with any of the following errors:
 *   any item is an invalid item (see Tree.invalidItem)
 *   any item is not a new child
 */
Tree.prototype._writev = function(items, cb) {
  if (!Array.isArray(items)) { throw new TypeError('items must be an array'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var error;
  var that = this;

  // do parallel check of each item
  async.filter(items, function(chunk, cb2) {
    var item = chunk.chunk;
    var msg = invalidItem(item);
    if (msg) {
      process.nextTick(function() {
        that._log.err('tree %s %j', msg, item);
        cb(new Error(msg));
      });
      return;
    }

    // check if this item is connected and whether it already exists
    that._isConnected(item, function(err, connected, exists) {
      if (err) { cb(err); return; }

      // if it's not connected and not a root, chances are the parents are in the batch
      // TODO: do not assume if one parent is not in the DAG that every parent must be in the batch
      if (!connected && item.h.pa.length) {
        // check if each parent is in the batch
        if (item.h.pa.every(function(pa) {
          return items.some(function(oitem) {
            return oitem.chunk.h.v === pa;
          });
        })) {
          that._log.info('tree _writev parent is not yet in DAG but in batch, set connected %j', item.h);
          connected = true;
        }
      }
      if (!connected) {
        error = new Error('item is not a new child');
        that._log.err('tree _writev %s %j', error, item.h);
        cb(error);
        return;
      }
      if (exists) {
        // always accept existing items from remote (happens if a new remote is added)
        if (item.h.pe && item.h.pe !== that.name) {
          // update last written perspective version
          that._db.put(that._composeUsKey(item.h.pe), new Buffer(item.h.v, 'base64'), function(err) {
            if (err) {
              that._log.err('tree _writev could not update perspective in uskey %s %j', err, item.h);
              cb(err);
              return;
            }
            that._log.info('tree _writev updated last received perspective version of existing version %j', item.h);
            cb2(false);
          });
          return;
        } else {
          error = new Error('item is not a new child');
          that._log.err('tree _writev %s %j', error, item.h);
          cb(error);
          return;
        }
      }
      cb2(true);
    });
  }, function(fitems) {
    var tra = [];

    // create batch insert, maintain order
    async.eachSeries(fitems, function(chunk, cb2) {
      var item = chunk.chunk;
      that._nextI(function(err, i) {
        if (err) { cb2(err); return; }

        item.h.i = i;

        that._log.info('tree _writev new data item %j', item);

        // update indexes and add new version
        var iKey = that._composeIKey(i);
        var vKey = that._composeVKey(item.h.v);
        var headKey = that._composeHeadKey(item.h.id, item.h.v);
        var headVal = that._composeHeadVal(item);
        var dsKey = that._composeDsKey(item.h.id, i);

        // delete all heads this new item replaces
        item.h.pa.forEach(function(pa) {
          that._log.debug('tree _writev delete old head %s %s', item.h.id, pa);
          tra.push({ type: 'del', key: that._composeHeadKey(item.h.id, pa) });
        });
        tra.push({ type: 'put', key: headKey, value: headVal });
        tra.push({ type: 'put', key: iKey,  value: headKey });
        tra.push({ type: 'put', key: vKey,  value: dsKey });

        // update last written perspective version
        if (item.h.pe && item.h.pe !== that.name) {
          tra.push({ type: 'put', key: that._composeUsKey(item.h.pe), value: new Buffer(item.h.v, 'base64') });
        }

        // update data
        var bitem = BSON.serialize(item);
        tra.push({ type: 'put', key: dsKey, value: bitem });
        cb2();
      });
    }, function(err) {
      if (err) { cb(err); return; }

      // do batch insert
      that._db.batch(tra, function(err) {
        // emit item if successfully written, or an error otherwise
        if (err) {
          that._log.err('tree _writev items not written %d %s', tra.length, err);
          cb(err);
          return;
        }

        that._log.info('tree _writev items written %d', tra.length);
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
 * @param {mixed} obj  valid id (item.h.id)
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
 * Get the value for the head index.
 *
 * @param {Object} item  valid item
 * @return {Buffer} valid head index value
 */
Tree.prototype._composeHeadVal = function _composeHeadVal(item) {
  if (item === null || typeof item !== 'object') { throw new TypeError('item must be an object'); }
  try {
    if (typeof item.h.i !== 'number') { throw new TypeError('item.h.i must be a number'); }
  } catch(err) {
    throw new TypeError('item.h.i must be a number');
  }


  var b = new Buffer(1 + 1 + this._iSize);
  var offset = 0;

  var opts = 0x00;
  if (item.h.c) {
    opts = opts | CONFLICT;
  }
  if (item.h.d) {
    opts = opts | DELETE;
  }

  b[offset++] = opts;

  b[offset++] = this._iSize;

  b.writeUIntBE(item.h.i, offset, this._iSize);

  return b;
};

/**
 * Get the key for the user store. It is assumed that usKey is a string or an
 * object that implements the "toString" method.
 *
 * @param {Buffer|String|Object} usKey  usKey that is a buffer, string or
 *                                      implements "toString"
 * @return {Buffer} valid key of subtype pekey
 */
Tree.prototype._composeUsKey = function _composeUsKey(usKey) {
  if (!Buffer.isBuffer(usKey)) {
    usKey = new Buffer(Tree._ensureString(usKey));
  }

  var b = new Buffer(this._usPrefix.length + 1 + usKey.length + 1);
  var offset = 0;

  this._usPrefix.copy(b, offset);
  offset += this._usPrefix.length;

  b[offset] = usKey.length;
  offset++;

  usKey.copy(b, offset);
  offset += usKey.length;

  b[offset] = 0x00;

  return b;
};

/**
 * Get the key for the data store. It is assumed that id is a string or an
 * object that implements the "toString" method. A valid i is a lbeint.
 *
 * @param {Buffer|String|Object} id  id that is a buffer, string or implements
 *                                   "toString"
 * @param {Number} i  valid lbeint
 * @return {Buffer} valid key of subtype dskey
 */
Tree.prototype._composeDsKey = function _composeDsKey(id, i) {
  if (!Buffer.isBuffer(id)) {
    id = new Buffer(Tree._ensureString(id));
  }
  if (typeof i !== 'number') { throw new TypeError('i must be a number'); }

  var b = new Buffer(this._dsPrefix.length + 1 + id.length + 1 + 1 + this._iSize);
  var offset = 0;

  this._dsPrefix.copy(b, offset);
  offset += this._dsPrefix.length;

  b[offset] = id.length;
  offset++;

  id.copy(b, offset);
  offset += id.length;

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
  this._maxI(function(err, found) {
    that._log.debug('tree _nextI close, max i found %s', found);
    if (err) { cb(err); return; }

    that._i = found || 0;
    cb(null, ++that._i);
  });
};

/**
 * Check if the given item is connected to an existing DAG. If there is no
 * existing DAG, or if all heads are in conflict or deleted, any new root will
 * be considered connected.
 *
 * @param {Object} item  item to inspect
 * @param {Function} cb  First parameter is an error object or null. Second
 *                       parameter is a boolean about whether item is connected
 *                       or not, third parameter is a boolean whether this item
 *                       already exists or not.
 */
Tree.prototype._isConnected = function _isConnected(item, cb) {
  var error;
  var that = this;

  // make sure this version does not exist
  var vKey = this._composeVKey(item.h.v);
  that._log.debug('tree _isConnected check if this version already exists %j', item.h);

  this._db.get(vKey, function(err, dsKey) {
    if (err && !err.notFound) {
      that._log.err('tree _isConnected item lookup error %j', err);
      cb(err);
      return;
    }

    // an error is expected (not found) if the version does not exist
    if (!err) {
      var key = Tree.parseKey(dsKey, { decodeId: 'utf8' });
      if (key.id !== item.h.id) {
        error = 'version exists for a different id';
        that._log.info('tree _isConnected %j %s %s', error, key.id, item.h.v);
        cb(error);
        return;
      } else {
        // the version does already exist
        that._log.debug('tree _isConnected version already exists %s', item.h.v);
        cb(null, true, true);
        return;
      }
    }

    that._log.debug('tree _isConnected version does not exist');

    // verify if all parents exist in the DAG
    async.every(item.h.pa, function(pa, cb2) {
      // search from head to older
      that._log.info('tree _isConnected descend %s search for parent %s', item.h.id, pa);

      var r = that.getDsKeyRange({ id: Tree._toBuffer(item.h.id) });
      var it = that._db.createValueStream({ reverse: true, gte: r.s, lt: r.e });

      var parentFound = false;

      it.on('data', function(sItem) {
        sItem = BSON.deserialize(sItem);
        that._log.debug('tree _isConnected descend item %j', sItem.h);

        if (sItem.h.v === pa) {
          that._log.info('tree _isConnected version equals parent %s', pa);
          // great success
          parentFound = true;
          it.destroy();
        } else {
          that._log.debug('tree _isConnected version %s does not equal parent %s', sItem.h.v, pa);
        }
      });

      it.on('close', function() {
        cb2(parentFound);
      });
    }, function(parentsExist) {
      if (!parentsExist) {
        that._log.info('tree _isConnected not all parents exist %j', item.h);
        cb(null, false, false);
        return;
      }

      that._log.debug('tree _isConnected all parents exist');

      // all heads are checked
      // if this is not a root, all is done.
      if (item.h.pa.length) {
        cb(null, true, false);
        return;
      }

      that._log.debug('tree _isConnected since this is a new root, search for exising items');

      // search for all heads of this id
      var r = that.getHeadKeyRange(Tree._toBuffer(item.h.id));
      var it = that._db.createValueStream({ gte: r.s, lt: r.e });

      // since this is a new root, make sure no head already exists, unless all are deleted
      var parentFound = false;

      it.on('error', cb);

      it.on('data', function(i) {
        i = Tree.parseHeadVal(i).i;
        that._log.debug('tree _isConnected idxHeads %j %s', item.h, i);

        // resolve item
        it.pause();
        that._db.get(that._composeDsKey(item.h.id, i), function(err, sItem) {
          that._log.debug('tree _isConnected resolved head %s for %j', i, item.h);

          if (err) {
            that._log.err('tree _isConnected resolving head %j', err);
            error = err;
            it.destroy();
            return;
          }

          sItem = BSON.deserialize(sItem);

          if (sItem.h.d) {
            that._log.debug('tree _isConnected existing was deleted %j continue...', sItem.h);
            it.resume();
          } else {
            that._log.info('tree _isConnected existing head %j for %j', sItem.h, item.h);
            // not deleted
            // a head item for this root exists, not ok, stop searching any further
            parentFound = true;
            it.destroy();
          }
        });
      });

      it.on('close', function() {
        if (error) { cb(error); return; }

        if (parentFound) {
          that._log.info('tree _isConnected new root while other roots exist %j', item.h);
          cb(null, false, false);
          return;
        }

        cb(null, true, false);
      });
    });
  });
};
