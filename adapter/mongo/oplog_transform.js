/**
 * Copyright 2014, 2015, 2016 Netsend.
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

/* jshint -W116 */

'use strict';

var bson = require('bson');
var BSONStream = require('bson-stream');
var Transform = require('stream').Transform;
var util = require('util');
var xtend = require('xtend');

var BSON = new bson.BSONPure.BSON();

var noop = function() {};

/**
 * OplogTransform
 *
 * Transform oplog items into new versions.
 *
 * @param {Object} oplogDb  connection to the oplog database
 * @param {String} oplogCollName  oplog collection name, defaults to oplog.$main
 * @param {String} ns  namespace of the database.collection to follow
 * @param {Object} controlWrite  request stream to ask for latest versions
 * @param {Object} controlRead  response stream to recieve latest versions
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   tmpCollName {String, default _pdb._tmp.[ns]}  temporary collection
 *   bson {Boolean, default false}  whether to return raw bson or parsed objects
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 */
function OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, opts) {
  if (oplogDb == null || typeof oplogDb !== 'object') { throw new TypeError('oplogDb must be an object'); }
  if (!oplogCollName || typeof oplogCollName !== 'string') { throw new TypeError('oplogCollName must be a non-empty string'); }
  if (!ns || typeof ns !== 'string') { throw new TypeError('ns must be a non-empty string'); }
  if (controlWrite == null || typeof controlWrite !== 'object') { throw new TypeError('controlWrite must be an object'); }
  if (controlRead == null || typeof controlRead !== 'object') { throw new TypeError('controlRead must be an object'); }

  if (opts == null) opts = opts || {};
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }

  var nsParts = ns.split('.');
  if (nsParts.length < 2) { throw new TypeError('ns must contain at least two parts'); }
  if (!nsParts[0].length) { throw new TypeError('ns must contain a database name'); }
  if (!nsParts[1].length) { throw new TypeError('ns must contain a collection name'); }

  Transform.call(this, xtend(opts, { objectMode: true }));

  if (opts.bson != null && typeof opts.bson !== 'boolean') { throw new TypeError('opts.bson must be a boolean'); }
  if (opts.log != null && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }

  this._oplogColl = oplogDb.collection(oplogCollName);
  this._ns = ns;
  this._opts = opts;

  // ensure bson option is set
  this._bson = false;
  if (opts.bson != null) { this._bson = opts.bson; }

  // write ld-json to the request stream
  this._controlWrite = controlWrite;

  // expect bson on the response stream
  this._controlRead = controlRead.pipe(new BSONStream());

  this._tmpCollection = oplogDb.collection('_pdb.' + ns);

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

util.inherits(OplogTransform, Transform);
module.exports = OplogTransform;

/**
 * Ask for last version and start reading the oplog after that. Convert oplog items
 * to new versions.
 *
 * Ask last version
 *  * open oplog after that item
 *  * for each oplog item
 *    * if insert, delete or update by full doc, emit
 *    * if update by update modifier:
 *    * ask last version for h.id
 *      * save in tmp collection
 *      * apply update doc
 *      * emit
 */
OplogTransform.prototype.startStream = function startStream() {
  var that = this;

  // listen for response, expect it to be the last stored version
  this._controlRead.once('readable', function() {
    var obj = that._controlRead.read();
    if (!obj) {
      // eof, request stream closed
      that._log.notice('ot startStream control read stream closed before opening the oplog');
      return;
    }

    // expect the last version in the DAG with an oplog offset
    var offset;
    try {
      var err = new Error('unable to determine offset');
      offset = obj.m._op;
      if (!offset) {
        throw err;
      }
    } catch(e) {
      that._log.err('ot startStream %j %j', err, e);
      that.emit('error', err);
      return;
    }

    // handle new oplog items via this._transform
    var or = that._oplogReader(xtend(that._opts, { offset: offset, bson: false, tailable: true }));
    or.on('end', function() {
      that._log.err('ot startStream unexpected end of tailable oplog cursor');
    });
    or.pipe(that);
  });

  // ask for last version (not id restricted)
  // write version requests in ld-json
  this._controlWrite.write(JSON.stringify({ id: null }) + '\n');
};

/**
 * Read oplog, scoped to the namespace.
 *
 * @param {Object} [opts]  object containing optional parameters
 *
 * opts:
 *   filter {Object}  extra filter to apply apart from namespace
 *   offset {Object}  mongodb.Timestamp to start at
 *   bson {Boolean, default true}  whether to return raw bson or parsed objects
 *   includeOffset {Boolean, default false}  whether to include or exclude offset
 *   tailable {Boolean, default false}  whether or not to keep the cursor open and follow the oplog
 *   tailableRetryInterval {Number, default 1000}  set tailableRetryInterval
 */
OplogTransform.prototype._oplogReader = function _oplogReader(opts) {
  if (opts == null) opts = {};
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (opts.filter != null && typeof opts.filter !== 'object') { throw new TypeError('opts.filter must be an object'); }
  if (opts.offset != null && typeof opts.offset !== 'object') { throw new TypeError('opts.offset must be an object'); }
  if (opts.bson != null && typeof opts.bson !== 'boolean') { throw new TypeError('opts.bson must be a boolean'); }
  if (opts.includeOffset != null && typeof opts.includeOffset !== 'boolean') { throw new TypeError('opts.includeOffset must be a boolean'); }
  if (opts.tailable != null && typeof opts.tailable !== 'boolean') { throw new TypeError('opts.tailable must be a boolean'); }
  if (opts.tailableRetryInterval != null && typeof opts.tailableRetryInterval !== 'number') { throw new TypeError('opts.tailableRetryInterval must be a number'); }

  // setup CursorStream
  var selector = { ns: this._ns };
  if (opts.offset) {
    if (opts.includeOffset) {
      selector.ts = { $gte: opts.offset };
    } else {
      selector.ts = { $gt: opts.offset };
    }
  }
  if (opts.filter) {
    selector = { $and: [selector, opts.filter] };
  }

  var mongoOpts = {
    raw: true,
    sort: { '$natural': 1 },
    comment: 'oplog_reader'
  };
  if (typeof opts.bson === 'boolean') { mongoOpts.raw = opts.bson; }
  if (opts.tailable) { mongoOpts.tailable = true; }
  mongoOpts.tailableRetryInterval = opts.tailableRetryInterval || 1000;

  this._log.info('ot oplogReader offset: %s %s, selector: %j, opts: %j', opts.offset, opts.includeOffset ? 'include' : 'exclude', selector, mongoOpts);

  return this._oplogColl.find(selector, mongoOpts);
};

/**
 * Create a new version of a document by the given oplog item. For update modifiers
 * apply the oplog item on the last version.
 *
 * @param {Object} oplogItem  item from the oplog
 * @param {Function} cb  On error the first parameter will be the Error object and
 *                       the second parameter will be undefined. On success the
 *                       first parameter will be null and the second parameter will
 *                       be the new version of the document.
 */
OplogTransform.prototype._transform = function _transform(oplogItem, enc, cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (OplogTransform._invalidOplogItem(oplogItem)) {
    this._log.err('ot _transform invalid oplog item: %j', oplogItem);
    process.nextTick(function() {
      cb(new Error('invalid oplogItem'));
    });
    return;
  }

  // determine the type of operator
  var operator = oplogItem.op;

  // if updating in non-modifier mode (by full document), do an update insert.
  if (operator === 'u' && !OplogTransform._oplogUpdateContainsModifier(oplogItem)) {
    // ensure _id
    try {
      if (!oplogItem.o2._id) { throw new Error('missing oplogItem.o2._id'); }
    } catch(err) {
      process.nextTick(function() {
        cb(err);
      });
      return;
    }
    oplogItem.o._id = oplogItem.o2._id;
    operator = 'uf';
  }

  switch (operator) {
  case 'i':
    this._applyOplogFullDoc(oplogItem, cb);
    break;
  case 'uf':
    this._applyOplogFullDoc(oplogItem, cb);
    break;
  case 'u':
    this._applyOplogUpdateModifier(oplogItem, cb);
    break;
  case 'd':
    this._applyOplogDeleteItem(oplogItem, cb);
    break;
  default:
    process.nextTick(function() {
      cb(new Error('unsupported operator: ' + operator));
    });
    return;
  }
};

/**
 * If the first character of the first key of the object equals "$" then this item
 * contains one or more modifiers.
 *
 * @param {Object} oplogItem  the oplog item.
 * @return {Boolean} true if the object contains any modifiers, false otherwise.
 */
OplogTransform._oplogUpdateContainsModifier = function _oplogUpdateContainsModifier(oplogItem) {
  if (typeof oplogItem.o !== 'object' || Array.isArray(oplogItem)) { return false; }

  var keys = Object.keys(oplogItem.o);
  if (keys[0] && keys[0][0] === '$') {
    return true;
  }

  return false;
};

/**
 * Create a new version by applying the update in a temporary collection.
 *
 * @param {Object} dagItem  item from the snapshot
 * @param {Object} oplogItem  the update item from the oplog.
 * @param {Function} cb  On error the first parameter will be the Error object and
 *                       the second parameter will be undefined. On success the
 *                       first parameter will be null and the second parameter will
 *                       be the new version of the document.
 */
OplogTransform.prototype._createNewVersionByUpdateDoc = function _createNewVersionByUpdateDoc(dagItem, oplogItem, cb) {
  try {
    if (oplogItem.op !== 'u') { throw new Error('oplogItem op must be "u"'); }
    if (!oplogItem.o2._id) { throw new Error('missing oplogItem.o2._id'); }
    if (oplogItem.o._id) { throw new Error('oplogItem contains o._id'); }
  } catch(err) {
    this._log.err('ot _createNewVersionByUpdateDoc %s, %j', err, oplogItem);
    process.nextTick(function() {
      cb(err);
    });
    return;
  }

  var that = this;
  var error;

  var bson = this._bson;

  var selector = { _id: dagItem.h.id };

  // save the previous head and apply the update modifiers to get the new version of the doc
  that._tmpCollection.replaceOne(selector, dagItem.b, { w: 1, upsert: true, comment: '_createNewVersionByUpdateDoc' }, function(err, result) {
    if (err) { cb(err); return; }

    if (result.modifiedCount !== 1) {
      error = new Error('new version not inserted in tmp collection');
      that._log.err('ot _createNewVersionByUpdateDoc %s %j %j', error, dagItem, result);
      cb(error);
      return;
    }

    // update the just created copy
    that._log.info('ot _createNewVersionByUpdateDoc selector %j', selector);

    that._tmpCollection.findOneAndUpdate(selector, oplogItem.o, { returnOriginal: false }, function(err, result) {
      if (err) {
        that._log.err('ot _createNewVersionByUpdateDoc', err);
        cb(err);
        return;
      }
      if (!result.ok) {
        that._log.err('ot _createNewVersionByUpdateDoc new doc not created %j %j %j', dagItem, selector, oplogItem.o);
        cb(new Error('new doc not created'));
        return;
      }

      var newObj = result.value;

      // remove object from collection
      that._tmpCollection.deleteOne(selector, function(err, result) {
        if (err || result.deletedCount !== 1) {
          that._log.err('ot _createNewVersionByUpdateDoc remove update ', err, result);
          cb(err);
          return;
        }

        var obj = {
          h: { id: dagItem.h.id },
          m: { _op: oplogItem.ts },
          b: newObj
        };
        cb(null, bson ? BSON.serialize(obj) : obj);
      });
    });
  });
};

/**
 * Create a new version with only the id and the body straight from the oplog item.
 * Supports oplog by full doc and oplog insert items.
 *
 * @param {Object} oplogItem  the update item from the oplog.
 * @param {Function} cb  On error the first parameter will be the Error object and
 *                       the second parameter will be undefined. On success the
 *                       first parameter will be null and the second parameter will
 *                       be the new version of the document.
 */
OplogTransform.prototype._applyOplogFullDoc = function _applyOplogFullDoc(oplogItem, cb) {
  this._log.info('ot _applyOplogFullDoc', JSON.stringify(oplogItem));

  try {
    if (oplogItem.op !== 'u' && oplogItem.op !== 'i') { throw new Error('oplogItem.op must be "u" or "i"'); }
    if (!oplogItem.o._id) { throw new Error('missing oplogItem.o._id'); }
  } catch(err) {
    process.nextTick(function() {
      cb(err);
    });
    return;
  }

  var bson = this._bson;
  process.nextTick(function() {
    var obj = {
      h: { id: oplogItem.o._id },
      m: { _op: oplogItem.ts },
      b: oplogItem.o
    };
    cb(null, bson ? BSON.serialize(obj) : obj);
  });
};

/**
 * Update an existing version of a document by applying an oplog update item.  
 *
 * Request the current version on the control read stream. Insert it into a temporary
 * collection to update it and pass the result back on the data stream.
 *
 * Every mongodb update modifier is supported since the update operation is executed
 * by the database engine.
 *
 * @param {Object} oplogItem  the update item from the oplog.
 * @param {Function} cb  On error the first parameter will be the Error object and
 *                       the second parameter will be undefined. On success the
 *                       first parameter will be null and the second parameter will
 *                       be the new version of the document.
 */
OplogTransform.prototype._applyOplogUpdateModifier = function _applyOplogUpdateModifier(oplogItem, cb) {
  this._log.info('ot _applyOplogUpdateModifier', JSON.stringify(oplogItem));

  var that = this;

  // listen for response, expect it to be the last stored version
  this._controlRead.once('readable', function() {
    var head = that._controlRead.read();
    if (!head || !head.h) return void cb(new Error('previous version of doc not found'));

    that._createNewVersionByUpdateDoc(head, oplogItem, cb);
  });

  // ask for last version of this id
  this._controlWrite.write(JSON.stringify({ id: oplogItem.o2._id }) + '\n');
};

/**
 * Create a new version with only the id, h.d set to true and no body.
 *
 * @param {Object} oplogItem  the delete item from the oplog.
 * @param {Function} cb  On error the first parameter will be the Error object and
 *                       the second parameter will be undefined. On success the
 *                       first parameter will be null and the second parameter will
 *                       be the new version of the document.
 */
OplogTransform.prototype._applyOplogDeleteItem = function _applyOplogDeleteItem(oplogItem, cb) {
  this._log.info('ot _applyOplogDeleteItem', JSON.stringify(oplogItem));

  try {
    if (oplogItem.op !== 'd') { throw new Error('oplogItem.op must be "d"'); }
    if (!oplogItem.o._id) { throw new Error('missing oplogItem.o._id'); }
  } catch(err) {
    process.nextTick(function() {
      cb(err);
    });
    return;
  }

  var bson = this._bson;
  process.nextTick(function() {
    var obj = {
      h: {
        id: oplogItem.o._id,
        d: true,
      },
      m: { _op: oplogItem.ts }
    };
    cb(null, bson ? BSON.serialize(obj) : obj);
  });
};

/**
 * Check if given oplog item has the following attributes:
 * - has "o", "ts", "ns" and "op" properties. 
 * - "op" is one of "i", "u" or "d".
 *
 * @param {Object} data  object that needs to be tested
 * @return {String} empty string if nothing is wrong or a problem description
 */
OplogTransform._invalidOplogItem = function _invalidOplogItem(item) {
  // check if all fields are present
  if (!item)    { return 'missing item'; }
  if (!item.o)  { return 'missing item.o'; }
  if (!item.ts) { return 'missing item.ts'; }
  if (!item.ns) { return 'missing item.ns'; }
  if (!item.op) { return 'missing item.op'; }

  // ignore if operation is not "i", "u" or "d"
  if (item.op !== 'i' && item.op !== 'u' && item.op !== 'd') { return 'invalid item.op'; }

  return '';
};
