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

var Writable = require('stream').Writable;
var util = require('util');

var async = require('async');
var BSON, bson = require('bson');
if (process.browser) {
  BSON = new bson();
} else {
  BSON = new bson.BSONPure.BSON();
}
var through2 = require('through2');

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
var DELETE   = 0x02;

// defined user store keys
// _composeUsKey(item.h.pe) => _composeVKey(item.h.v)

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
 *   skipValidation {Boolean, default false}  whether or not to check if new
 *                                            items connect 
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
  if (opts.skipValidation != null && typeof opts.skipValidation !== 'boolean') { throw new TypeError('opts.skipValidation must be a boolean'); }

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

  this._skipValidation = opts.skipValidation;

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
  this._log.info('t:%s getHeadVersions range %j', this.name, r);

  var it = this._db.createKeyStream({ gte: r.s, lt: r.e });

  var result = [];

  var that = this;
  it.on('data', function(headKey) {
    var key = Tree.parseKey(headKey, { decodeV: 'base64' });

    that._log.debug('t:%s getHeadVersions iterate key %j', that.name, key);
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
 * @param {Function} iterator  function(head, next) called with each head.
 *                             signature: next(err, continue) if continue is false
 *                             iteration will abort. if null or true, iteration
 *                             will continue to the next head.
 * @param {Function} cb  First parameter will be an error object or null, second
 *                       parameter will be the last iterator continue value.
 *
 * opts:
 *   id {Buffer|String|Object}  id to prefix start and end, must be a buffer,
 *                              string or implement "toString"
 *   skipConflicts {Boolean, default false}  whether to emit heads with the
 *                                           conflict bit set
 *   skipDeletes {Boolean, default false}  whether to emit heads with the delete
 *                                         bit set
 *   bson {Boolean, default false}  whether to BSON serialize the object or not
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
  if (opts.bson != null && typeof opts.bson !== 'boolean') { throw new TypeError('opts.bson must be a boolean'); }
  if (typeof iterator !== 'function') { throw new TypeError('iterator must be a function'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var r = this.getHeadKeyRange(id);
  this._log.debug('t:%s getHeads range %j', this.name, r);

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

    that._log.debug2('t:%s getHeads iterate key %j val %j', that.name, key, val);

    that._db.get(that._composeDsKey(key.id, val.i), function(err, item) {
      if (err) {
        that._log.err('t:%s getHeads %s', that.name, err);
        it.pause();
        cb(err);
        return;
      }

      that._log.debug2('t:%s getHeads iterate item %j', that.name, item);

      var value;
      if (opts.bson) {
        if (process.browser) {
          value = BSON.serialize(item);
        } else {
          value = item;
        }
      } else {
        if (process.browser) {
          value = item;
        } else {
          value = BSON.deserialize(item);
        }
      }
      iterator(value, function(err, cont) {
        if (err) {
          it.pause();
          cb(err);
          return;
        }

        if (cont != null && !cont) {
          it.pause();
          cb(null, cont);
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
 *   bson {Boolean, default false}  whether to BSON serialize the object or not
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
  if (opts.bson != null && typeof opts.bson !== 'boolean') { throw new TypeError('opts.bson must be a boolean'); }

  this._log.debug('t:%s getByVersion %s', this.name, version);

  var that = this;
  this._getDsKeyByVersion(version, function(err, dsKey) {
    if (err) { cb(err); return; }

    if (!dsKey) {
      cb(null, null);
      return;
    }

    that._db.get(dsKey, function(err, item) {
      if (err) { cb(err); return; }

      var value;
      if (opts.bson) {
        if (process.browser) {
          value = BSON.serialize(item);
        } else {
          value = item;
        }
      } else {
        if (process.browser) {
          value = item;
        } else {
          value = BSON.deserialize(item);
        }
      }
      cb(null, value);
    });
  });
};

/**
 * Get an iterator over each item in the order of insertion into the tree.
 *
 * @param {Object} [opts]  object containing configurable parameters
 * @param {Function} iterator  function(item, next) called with each item.
 *                             signature: next(err, continue) if continue is false
 *                             iteration will abort. if null or true, iteration
 *                             will continue to the next item.
 * @param {Function} cb  First parameter will be an error object or null, second
 *                       parameter will be the last iterator continue value.
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
 *   bson {Boolean, default false}  whether to BSON serialize the object or not
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

  var that = this;
  var ended, processing, cbCalled, lastCont;
  s.on('data', function(item) {
    s.pause();
    processing = true;
    iterator(item, function(err, cont) {
      if (err) {
        if (!cbCalled) {
          cbCalled = true;
          cb(err);
        } else {
          that._log.err('t:%s iterateInsertionOrder cb already called (iterator)', that.name, err);
        }
        return;
      }
      lastCont = cont;
      if (cont != null && !cont) {
        s.destroy();
        if (!cbCalled) {
          cbCalled = true;
          cb(null, lastCont);
        }
        return;
      }
      processing = false;
      if (ended && !cbCalled) {
        cbCalled = true;
        cb(null, lastCont);
        return;
      }
      s.resume();
    });
  });

  s.on('error', function(err) {
    if (!cbCalled) {
      cbCalled = true;
      cb(err);
    } else {
      that._log.err('t:%s iterateInsertionOrder cb already called', that.name, err);
    }
  });
  s.on('end', function() {
    ended = true;
    if (!processing && !cbCalled) {
      cbCalled = true;
      cb(null, lastCont);
    }
  });
};

/**
 * Get a stream over each item in the order of insertion into the tree. If tail is
 * true, the returned stream will have a "close" method.
 *
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   id {String|Object}  limit to one specific DAG
 *   first {base64 String}  first version, offset
 *   last {base64 String}  last version (mutually exclusive with tail)
 *   excludeFirst {Boolean, default false}  whether or not first should be
 *                                          excluded
 *   excludeLast {Boolean, default false}  whether or not last should be
 *                                         excluded (mutually exclusive with tail)
 *   reverse {Boolean, default false}  if true, starts with last version (mutually
 *                                     exclusive with tail)
 *   bson {Boolean, default false}  whether to BSON serialize the object or not
 *   tail {Boolean, default false}  if true, keeps the stream open
 *   tailRetry {Number, default 1000}  reopen readers every tailRetry ms
 */
Tree.prototype.createReadStream = function createReadStream(opts) {
  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }
  if (opts.tail != null && typeof opts.tail !== 'boolean') { throw new TypeError('opts.tail must be a boolean'); }
  if (opts.tailRetry != null && typeof opts.tailRetry !== 'number') { throw new TypeError('opts.tailRetry must be a number'); }
  if (opts.tail) {
    if (opts.last) { throw new Error('opts.last is mutually exclusive with opts.tail'); }
    if (opts.excludeLast) { throw new Error('opts.excludeLast is mutually exclusive with opts.tail'); }
    if (opts.reverse) { throw new Error('opts.reverse is mutually exclusive with opts.tail'); }
  }

  var first        = opts.first;
  var excludeFirst = opts.excludeFirst;
  var that         = this;

  var running, timeout, ws, rs;

  function scheduleNextOnEnd() {
    that._log.debug('t:%s createReadStream schedule next', that.name);
    running = false;
    rs.unpipe(ws);
    timeout = setTimeout(setupNewReaderToWriter, opts.tailRetry || 1000);
  }

  function scheduleEndOnEnd() {
    that._log.debug('t:%s createReadStream schedule end', that.name);
    running = false;
    rs.unpipe(ws);
    ws.end();
  }

  // create a new rs, pipe it to ws
  function setupNewReaderToWriter() {
    that._log.debug('t:%s createReadStream open reader: %j', that.name, opts);
    rs = new StreamTree(that, {
      id:            opts.id,
      first:         first,
      excludeFirst:  excludeFirst,
      bson:          opts.bson
    });

    running = true;
    rs.once('end', scheduleNextOnEnd);
    rs.once('error', function(err) {
      // since this is a retry tail, skip version not found errors, maybe the requested version will exist later
      if (err.message === 'version not found') {
        that._log.info('t:%s createReadStream unknown version requested', that.name);
      } else {
        that._log.err('t:%s createReadStream %j', that.name, err);
        rs.emit('error', err);
      }
    });
    rs.pipe(ws, { end: false });
  }

  if (opts.tail) {
    // track last emitted version
    ws = through2.obj(function(obj, enc, cb) { first = obj.h.v; cb(null, obj); });
    setupNewReaderToWriter();
    // exclude first on subsequent iterations
    excludeFirst = true;

    // implement way to stop tailing
    ws.close = function() {
      that._log.debug('t:%s createReadStream close', that.name);
      if (running) {
        rs.removeListener('end', scheduleNextOnEnd);
        rs.once('end', scheduleEndOnEnd);
      } else {
        that._log.debug('t:%s createReadStream end now', that.name);
        clearTimeout(timeout);
        ws.end();
      }
    };
    return ws;
  } else {
    return new StreamTree(that, opts);
  }
};

/**
 * Get last inserted version.
 *
 * @param {String} [decodeV]  decode v as string with given encoding. Encoding
 *                            must be either "hex", "utf8" or "base64". If not
 *                            given a number will be returned.
 * @param {Function} cb  First parameter will be an error object or null. Second
 *                       parameter will be a version or null.
 */
Tree.prototype.lastVersion = function lastVersion(decodeV, cb) {
  if (typeof decodeV === 'function') {
    cb = decodeV;
    decodeV = null;
  }
  if (decodeV != null && typeof decodeV !== 'string') { throw new TypeError('decodeV must be a string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  // find the last version
  var r = this.getIKeyRange();
  var it = this._db.createValueStream({ reverse: true, gt: r.s, lt: r.e, limit: 1 });

  var that = this;
  var error;
  var v = null;

  it.on('error', function(err) {
    that._log.err('t:%s lastVersion %j', that.name, err);
    error = err;
  });

  it.on('data', function(val) {
    v = Tree.parseKey(val, { decodeV: decodeV }).v;
  });

  it.on('end', function() {
    if (error) { cb(error, null); return; }

    that._log.debug('t:%s lastVersion %s', that.name, v);
    cb(null, v);
  });
};

/**
 * Get last version of a certain perspective.
 *
 * Assume _writev writes the last accepted version per perspective.
 *
 * @param {Buffer|String} pe  perspective
 * @param {String} [decodeV]  decode v as string with given encoding. Encoding
 *                            must be either "hex", "utf8" or "base64". If not
 *                            given a number will be returned.
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
  this._db.get(usKey, function(err, vkey) {
    if (err) {
      if (err.notFound) {
        that._log.info('t:%s lastByPerspective not found %s %j', that.name, pe, usKey);
        cb(null, null);
        return;
      } else {
        that._log.err('t:%s lastByPerspective lookup error %s %j', that.name, pe, err);
        cb(err);
        return;
      }
    }

    var v = Tree.parseKey(vkey, { decodeV: decodeV }).v;

    that._log.debug('t:%s lastByPerspective %s %s', that.name, pe, v);
    cb(null, v);
  });
};

/**
 * Delete item from the tree. Warning: this may leave the tree in inconsistent
 * state and is only possible if skipValidation is true.
 *
 * Note: will not reset any user store key.
 *
 * @param {Object} item  item to delete
 * @param {Function} cb  First parameter will be an error object or null. Second
 *                       parameter will be a version or null.
 */
Tree.prototype.del = function del(item, cb) {
  if (item == null || typeof item !== 'object') { throw new TypeError('item must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (!this._skipValidation) {
    process.nextTick(function() {
      cb(new Error('del is only available if skipValidation is set to true'));
    });
    return;
  }

  var that = this;

  var version = item.h.v;

  this._resolveVtoI(version, function(err, i) {
    if (err) { cb(err); return; }

    // remove from each index
    var tra = [];
    tra.push({ type: 'del', key: that._composeIKey(i) });
    tra.push({ type: 'del', key: that._composeVKey(version) });
    tra.push({ type: 'del', key: that._composeHeadKey(item.h.id, version) });
    tra.push({ type: 'del', key: that._composeDsKey(item.h.id, i) });

    that._db.batch(tra, cb);
  });
};

/**
 * Get statistics from the tree.
 *
 * @param {Function} cb  First parameter will be an error object or null. Second
 *                       parameter will be an object.
 */
Tree.prototype.stats = function stats(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var headCount = 0;
  var conflict = 0;
  var deleted = 0;

  this.getHeads(function(head, next) {
    headCount++;
    if (head.h.c) { conflict++; }
    if (head.h.d) { deleted++; }
    next();
  }, function(err) {
    if (err) { cb(err); return; }
    cb(null, {
      heads: {
        count: headCount,
        conflict: conflict,
        deleted: deleted
      }
    });
  });
};

/**
 * Set conflict by version. A valid version is any number up to 48 bits.
 *
 * @param {Number|base64 String} version  valid lbeint or base64 int
 * @param {Function} cb  First parameter will be an error object or null.
 */
Tree.prototype.setConflictByVersion = function setConflictByVersion(version, cb) {
  if (typeof version !== 'number' && typeof version !== 'string') { throw new TypeError('version must be a number or a base64 string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  this._log.debug('t:%s setConflict %s', this.name, version);

  var error;
  var that = this;

  // update by dskey and headkey
  this.getByVersion(version, function(err, item) {
    if (err) { cb(err); return; }

    if (!item) {
      error = new Error('version not found');
      that._log.err('t:%s setConflict %s: %s', that.name, error, version);
      cb(error);
      return;
    }

    if (item.h.c) {
      cb();
      return;
    }

    item.h.c = true;

    // update by dsKey
    var dsKey = that._composeDsKey(item.h.id, item.h.i);
    that._db.put(dsKey, process.browser ? item : BSON.serialize(item), function(err) {
      if (err) { cb(err); return; }

      var headKey = that._composeHeadKey(item.h.id, item.h.v);

      // if a head, update by headKey
      that._db.get(headKey, function(err) {
        if (err) {
          if (err.notFound) {
            cb();
            return;
          }
          cb(err);
          return;
        }

        var headVal = that._composeHeadVal(item);
        that._db.put(headKey, headVal, cb);
      });
    });
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

  this._log.debug2('t:%s _getDsKeyByVersion %s', this.name, version);

  var that = this;
  this._db.get(this._composeVKey(version), function(err, dsKey) {
    if (err) {
      if (err.notFound) {
        cb(null, null);
        return;
      }

      that._log.err('t:%s _getDsKeyByVersion lookup error %j', that.name, err);
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
      that._log.err('t:%s _resolveVtoI %s: %s', that.name, error, version);
      cb(error);
      return;
    }

    var i = Tree.parseKey(dsKey).i;
    that._log.debug('t:%s _resolveVtoI %s => %s', that.name, version, i);
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
    that._log.err('t:%s _maxI %j', that.name, err);
    error = err;
  });

  it.on('data', function(i) {
    that._log.debug2('t:%s _maxI i %s', that.name, i);
    found = Tree.parseKey(i).i;
  });

  it.on('end', function() {
    that._log.debug2('t:%s _maxI %s', that.name, found);
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

  // check each item
  async.filterSeries(items, function(chunk, cb2) {
    var item = chunk.chunk;
    var msg = invalidItem(item);
    if (msg) {
      process.nextTick(function() {
        that._log.err('t:%s %s %j', that.name, msg, item);
        cb(new Error(msg));
      });
      return;
    }

    if (that._skipValidation) {
      // keep it in the set to write
      cb2(true);
      return;
    }

    // check if this item is valid or only misses some parents that are still in the batch
    that._validNewItem(item, items, function(err, valid, missingParents, exists) {
      if (err) { cb(err); return; }

      // always accept existing items from remote (happens if a new remote is added)
      if (!valid && exists && item.h.pe && item.h.pe !== that.name) {
        // update last written perspective version
        that._db.put(that._composeUsKey(item.h.pe), that._composeVKey(item.h.v), function(err) {
          if (err) {
            that._log.err('t:%s _writev could not update perspective in uskey %s %j', that.name, err, item.h);
            cb(err);
            return;
          }
          that._log.info('t:%s _writev updated last received perspective version of existing version %j', that.name, item.h);
          cb2(false);
        });
        return;
      }

      if (!valid) {
        error = new Error('not a valid new item');
        that._log.err('t:%s _writev %s %j', that.name, error, item.h);
        cb(error);
        return;
      }

      // keep it in the set to write
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

        that._log.info('t:%s _writev new data item %j', that.name, item);

        // update indexes and add new version
        var iKey = that._composeIKey(i);
        var vKey = that._composeVKey(item.h.v);
        var headKey = that._composeHeadKey(item.h.id, item.h.v);
        var headVal = that._composeHeadVal(item);
        var dsKey = that._composeDsKey(item.h.id, i);

        that._log.debug2('t:%s _writev iKey: %s, vKey: %s, headKey: %s, dsKey: %s', that.name, iKey.toString('hex'), vKey.toString('hex'), headKey.toString('hex'), dsKey.toString('hex'));

        // delete all heads this new item replaces
        item.h.pa.forEach(function(pa) {
          that._log.debug('t:%s _writev delete old head %s %s', that.name, item.h.id, pa);
          tra.push({ type: 'del', key: that._composeHeadKey(item.h.id, pa) });
        });
        tra.push({ type: 'put', key: headKey, value: headVal });
        tra.push({ type: 'put', key: iKey,  value: headKey });
        tra.push({ type: 'put', key: vKey,  value: dsKey });

        // update last written perspective version
        if (item.h.pe && item.h.pe !== that.name) {
          tra.push({ type: 'put', key: that._composeUsKey(item.h.pe), value: that._composeVKey(item.h.v) });
        }

        // update data
        var bitem = process.browser ? item : BSON.serialize(item);
        tra.push({ type: 'put', key: dsKey, value: bitem });
        cb2();
      });
    }, function(err) {
      if (err) { cb(err); return; }

      // do batch insert
      that._db.batch(tra, function(err) {
        // emit item if successfully written, or an error otherwise
        if (err) {
          that._log.err('t:%s _writev items not written %d %s', that.name, tra.length, err);
          cb(err);
          return;
        }

        that._log.debug2('t:%s _writev items written %d', that.name, tra.length);
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
    this._log.err('t:%s _composeHeadKey base64 version length is %s bits instead of %s bits (vSize)', this.name, v.length * 6, this._vSize * 8);
    throw new Error('v is too short or too long');
  }
  this._log.debug2('t:%s _composeHeadKey %s %s', this.name, id, v);

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

  this._log.debug2('t:%s _nextI first time, resolve max i', that.name);

  // else find the last increment number
  this._maxI(function(err, found) {
    if (err) { cb(err); return; }
    that._log.debug2('t:%s _nextI close, max i found %s', that.name, found);

    that._i = found || 0;
    cb(null, ++that._i);
  });
};

/**
 * Check if the item has a valid connection to the tree. This means making sure the
 * version does not already exist, and furthermore:
 *   - for root items, check if there does not already exist a DAG for this id, and
 *     return an array of existing heads
 *   - for non-root items, check if all parents do exist and return an extra
 *     array that contains all parents that do not exist.
 *
 * @param {Object} item  item to inspect
 * @param {Array} batch  optional batch with new items that are not yet in the tree
 *                       the batch must be topologically sorted and item must be in
 *                       this batch as well (used with _writev)
 * @param {Function} cb  The first parameter is an error object or null. The second
 *                       parameter is a boolean about whether this item would be a
 *                       valid new version in the tree. If this is not a valid new
 *                       version the third parameter will be an array of
 *                       problematic versions.
 *                       If this as a root item, the third parameter is an array
 *                       of existing heads.
 *                       If this is not a root item, then the third parameter will
 *                       be an array with all missing parents. The fourth parameter
 *                       is a boolean about whether this version already exists.
 */
Tree.prototype._validNewItem = function _validNewItem(item, batch, cb) {
  var error;
  var that = this;

  if (typeof batch === 'function') {
    cb = batch;
    batch = null;
  }
  if (batch == null) { batch = []; }

  var id = item.h.id;
  var version = item.h.v;
  var parents = item.h.pa;

  this._log.debug('t:%s _validNewItem id: %s, version: %s, parents: %s', that.name, id, version, parents);

  // make sure this version does not exist
  var vKey = this._composeVKey(version);

  // for non-root items, check if all parents do exist and return an extra
  // array that contain's all missing parents.
  if (parents.length) {
    // first check if this item already exists
    this._db.get(vKey, function(err, dsKey) {
      if (err && !err.notFound) {
        that._log.err('t:%s _validNewItem item lookup error %j', that.name, err);
        cb(err);
        return;
      }

      // db.get returns a notFound error if the version does not exist
      if (!err) {
        var key = Tree.parseKey(dsKey, { decodeId: 'utf8' });
        if (key.id !== id) {
          error = 'version exists for a different id';
          that._log.err('t:%s _validNewItem %j %s %s', that.name, error, key.id, version);
          cb(error);
          return;
        } else {
          // the version does already exist
          that._log.debug('t:%s _validNewItem version already exists %s', that.name, version);
          // call back with missing parents
          cb(null, false, [], true);
          return;
        }
      }

      that._log.debug('t:%s _validNewItem good, this version does not exist in the tree', that.name);

      // check which parents do not exist in the tree
      var missingParents = [];
      async.each(parents, function(pa, cb2) {
        that._log.debug2('t:%s _validNewItem parent lookup %s', that.name, pa);

        // check if this parent exists
        that._db.get(that._composeVKey(pa), function(err, dsKey) {
          if (err && !err.notFound) {
            that._log.err('t:%s _validNewItem parent lookup error %j', that.name, err);
            cb2(err);
            return;
          }

          if (err && err.notFound) {
            that._log.debug('t:%s _validNewItem missing parent %s', that.name, pa);
            missingParents.push(pa);
            cb2();
            return;
          }

          var key = Tree.parseKey(dsKey, { decodeId: 'utf8' });
          if (key.id !== id) {
            error = 'parent exists for a different id';
            that._log.err('t:%s _validNewItem %j %s %s', that.name, error, key.id, pa);
            cb2(error);
            return;
          }

          that._log.debug('t:%s _validNewItem parent exists %s', that.name, pa);
          cb2();
        });
      }, function(err) {
        if (err) { cb(err); return; }

        // if there are missing parents and there is a batch with new items, chances are the parents are in the batch
        // TODO: check if any parents found in the batch are connected to the DAG
        // TODO: rewrite to process whole batch at once incrementally: examineItems(items, cb(err, newItems, existingItems, invalidItems)
        if (missingParents.length && batch.length) {
          if (missingParents.every(function(pa) {
            return batch.some(function(oitem) {
              return oitem.chunk.h.v === pa;
            });
          })) {
            that._log.info('t:%s _validNewItem parent(s) are not in DAG but in batch, set valid %j', that.name, item.h);
            missingParents = [];
          } else {
            that._log.info('t:%s _validNewItem parent(s) are not in DAG and not in batch %j', that.name, item.h);
          }
        }

        cb(null, !missingParents.length, missingParents, false);
      });
    });
  } else {
    // for root items, make sure no heads exist for this id
    var r = that.getHeadKeyRange(Tree._toBuffer(id));
    var it = that._db.createKeyStream({ gte: r.s, lt: r.e });

    var heads = {};

    it.on('error', cb);

    it.on('readable', function() {
      var headKey = it.read();
      if (headKey) {
        // record this head version
        heads[Tree.parseKey(headKey, { decodeV: 'base64' }).v] = true;
        return;
      }

      // done iterating tree, check batch with new items
      batch.some(function(item) {
        item = item.chunk;

        if (item.h.v === item.h.v) {
          // done
          return true;
        }

        if (item.h.id !== item.h.id) {
          // different DAG, skip
          return false;
        }

        // ensure existing head is replaced
        item.h.pa.forEach(function(pa) {
          delete heads[pa];
        });
        heads[item.h.v] = true;

        // continue
        return false;
      });

      // finally check if this root item itself already exists
      that._db.get(vKey, function(err, dsKey) {
        if (err && !err.notFound) {
          that._log.err('t:%s _validNewItem item lookup error %j', that.name, err);
          cb(err);
          return;
        }

        heads = Object.keys(heads);
        if (heads.length) {
          that._log.info('t:%s _validNewItem invalid root item %j, head(s) already exist in the tree %s', that.name, item.h, heads);
        }

        // db.get returns a notFound error if the version does not exist
        if (!err) {
          var key = Tree.parseKey(dsKey, { decodeId: 'utf8' });
          if (key.id !== id) {
            error = 'version exists for a different id';
            that._log.err('t:%s _validNewItem %j %s %s', that.name, error, key.id, version);
            cb(error);
            return;
          } else {
            // the version does already exist
            that._log.debug('t:%s _validNewItem version already exists %s', that.name, version);
            // call back with existing heads
            cb(null, !heads.length, heads, true);
            return;
          }
        }

        that._log.debug('t:%s _validNewItem good, this version does not exist in the tree', that.name);

        cb(null, !heads.length, heads, false);
      });
    });
  }
};
