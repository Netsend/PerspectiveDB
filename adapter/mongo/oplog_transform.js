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

var Transform = require('stream').Transform;
var util = require('util');

var BSONStream = require('bson-stream');

var noop = function() {};

/**
 * OplogTransform
 *
 * Transform oplog items into new versions.
 *
 * @param {Object} oplogDb  connection to the oplog database
 * @param {String} oplogCollName  oplog collection name, defaults to oplog.$main
 * @param {String} ns  namespace of the database.collection to follow
 * @param {Object} reqChan  request channel to get latest versions
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   tmpCollName {String, default _pdb._tmp.[ns]}  temporary collection
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 *
 * @class represents a OplogTransform of a database and collection
 */
function OplogTransform(oplogDb, oplogCollName, ns, reqChan, opts) {
  if (oplogDb == null || typeof oplogDb !== 'object') { throw new TypeError('oplogDb must be an object'); }
  if (!oplogCollName || typeof oplogCollName !== 'string') { throw new TypeError('oplogCollName must be a non-empty string'); }
  if (!ns || typeof ns !== 'string') { throw new TypeError('ns must be a non-empty string'); }
  if (reqChan == null || typeof reqChan !== 'object') { throw new TypeError('reqChan must be an object'); }

  if (opts == null) opts = opts || {};
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }

  var nsParts = ns.split('.');
  if (nsParts.length < 2) { throw new TypeError('ns must contain at least two parts'); }
  if (!nsParts[0].length) { throw new TypeError('ns must contain a database name'); }
  if (!nsParts[1].length) { throw new TypeError('ns must contain a collection name'); }

  opts.objectMode = true;

  Transform.call(this, opts);

  if (opts.log != null && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }

  this._oplogColl = oplogDb.collection(oplogCollName);
  this._ns = ns;

  this._reqChan = reqChan;

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
 * Start reading the oplog and converting items to new versions.
 *
 * ask last version
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

  // expect bson responses from the request channel
  this._resChan = this._reqChan.pipe(new BSONStream());

  // ask for last version (not id restricted)
  // write version requests in ld-json
  this._reqChan.write(JSON.stringify({ id: null }) + '\n');

  // listen for response, expect it to be the last stored version
  this._resChan.once('readable', function() {
    var obj = that._resChan.read();
    if (!obj) {
      // eof, request channel closed
      throw new Error('response channel closed unexpectedly');
    }

    // expect the last version in the DAG with an oplog offset
    var offset = obj.m.op;
    if (!offset) {
      throw new Error('unable to determine offset');
    }

    // open oplog reader starting at this offset
    var opts2 = {
      tailable: true,
      offset: offset,
      log: that._log
    };

    // handle new oplog items via this._transform
    var or = that._oplogReader(opts2);
    or.pipe(that);
  });
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
 *                       the second parameter will be the original document. On 
 *                       success the first parameter will be null and the second
 *                       parameter will be the new version of the document.
 */
OplogTransform.prototype._transform = function _transform(oplogItem, enc, cb) {
  if (oplogItem == null || typeof oplogItem !== 'object') { throw new TypeError('oplogItem must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (OplogTransform._invalidOplogItem(oplogItem)) {
    process.nextTick(function() {
      cb(new Error('invalid oplogItem'), oplogItem);
    });
    return;
  }

  try {
    if (!oplogItem.o) { throw new Error('missing oplogItem.o'); }
  } catch(err) {
    process.nextTick(function() {
      cb(err, oplogItem);
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
        cb(err, oplogItem);
      });
      return;
    }
    oplogItem.o._id = oplogItem.o2._id;
    operator = 'uf';
  }

  switch (operator) {
  case 'i':
    this._applyOplogInsertItem(oplogItem, cb);
    break;
  case 'uf':
    this._applyOplogUpdateFullDoc(oplogItem, cb);
    break;
  case 'u':
    this._applyOplogUpdateModifier(oplogItem, cb);
    break;
  case 'd':
    this._applyOplogDeleteItem(oplogItem, cb);
    break;
  default:
    process.nextTick(function() {
      cb(new Error('unsupported operator: ' + operator), oplogItem);
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
  //if (!oplogItem) { return false; }
  //if (!oplogItem.o) { return false; }
  //if (typeof oplogItem.o !== 'object' || Array.isArray(oplogItem)) { return false; }

  var keys = Object.keys(oplogItem.o);
  if (keys[0] && keys[0][0] === '$') {
    return true;
  }

  return false;
};

/**
 * Create a new version by applying an update document in a temporary collection.
 *
 * @param {Object} dagItem  item from the snapshot
 * @param {Object} oplogItem  the update item from the oplog.
 * @param {Function} cb  On error the first parameter will be the Error object and
 *                       the second parameter will be the original document. On 
 *                       success the first parameter will be null and the second
 *                       parameter will be the new version of the document.
 */
OplogTransform.prototype._createNewVersionByUpdateDoc = function _createNewVersionByUpdateDoc(dagItem, oplogItem, cb) {
  if (typeof dagItem !== 'object') { throw new TypeError('dagItem must be an object'); }
  if (typeof oplogItem !== 'object') { throw new TypeError('oplogItem must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  try {
    if (oplogItem.op !== 'u') { throw new Error('oplogItem op must be "u"'); }
    if (!oplogItem.o2._id) { throw new Error('missing oplogItem.o2._id'); }
    if (oplogItem.o._id) { throw new Error('oplogItem contains o._id'); }
  } catch(err) {
    process.nextTick(function() {
      cb(err, oplogItem);
    });
    return;
  }

  var that = this;
  var error;

  // save the previous head and apply the update modifiers to get the new version of the doc
  that._tmpCollection.insert(dagItem.b, {w: 1, comment: '_createNewVersionByUpdateDoc'}, function(err, inserted) {
    if (err) { cb(err, oplogItem); return; }

    if (inserted.length !== 1) {
      error = new Error('new version not inserted in tmp collection');
      that._log.err('vc _createNewVersionByUpdateDoc %s %j %j', error, dagItem, inserted);
      cb(error);
      return;
    }

    // update the just created copy
    var selector = { '_id': dagItem.h.id };
    that._log.info('vc _createNewVersionByUpdateDoc selector %j', selector);

    that._tmpCollection.findAndModify(selector, [], oplogItem.o, {w: 0, new: true}, function(err, newObj) {
      if (err) {
        that._log.err('vc _createNewVersionByUpdateDoc', err);
        cb(err, oplogItem);
        return;
      }
      if (!newObj) {
        that._log.err('vc _createNewVersionByUpdateDoc new doc not created %j %j %j', dagItem, selector, oplogItem.o);
        cb(new Error('new doc not created'), oplogItem);
        return;
      }

      // clear tmp collection
      that._tmpCollection.remove(function(err) {
        if (err) {
          that._log.err('vc _createNewVersionByUpdateDoc clear _tmpCollection', err);
          cb(err, oplogItem);
          return;
        }

        cb(null, newObj);
      });
    });
  });
};

/**
 * Insert a new root element into the DAG. The root element can be inserted in the
 * collection first (locally created), in which case it might still need a new
 * version, or in the DAG first (from a remote) and then in the collection.
 *
 * @param {Object} oplogItem  the item from the oplog.
 * @param {Function} cb  On error the first parameter will be the Error object and
 *                       the second parameter will be the original document. On 
 *                       success the first parameter will be null and the second
 *                       parameter will be the saved versioned document.
 */
OplogTransform.prototype._applyOplogInsertItem = function _applyOplogInsertItem(oplogItem, cb) {
  this._log.info('vc _applyOplogInsertItem', JSON.stringify(oplogItem));
  this._applyOplogUpdateFullDoc(oplogItem, cb);
};

/**
 * Update an existing version of a document by applying an oplog update item with
 * full doc. Insert a new document in the DAG.
 *
 * @param {Object} oplogItem  the update item from the oplog.
 * @param {Function} cb  On error the first parameter will be the Error object and
 *                       the second parameter will be the original document. On 
 *                       success the first parameter will be null and the second
 *                       parameter will be the new version of the document.
 */
OplogTransform.prototype._applyOplogUpdateFullDoc = function _applyOplogUpdateFullDoc(oplogItem, cb) {
  this._log.info('vc _applyOplogUpdateFullDoc', JSON.stringify(oplogItem));

  try {
    if (oplogItem.op !== 'u' && oplogItem.op !== 'i') { throw new Error('oplogItem.op must be "u" or "i"'); }
    if (!oplogItem.o._id) { throw new Error('missing oplogItem.o._id'); }
  } catch(err) {
    process.nextTick(function() {
      cb(err, oplogItem);
    });
    return;
  }

  cb(null, {
    h: {
      id: oplogItem.o._id
    },
    m: {
      _op: oplogItem.ts
    },
    b: oplogItem.o
  });
};

/**
 * Update an existing version of a document by applying an oplog update item.  
 *
 * Every mongodb update modifier is supported since the update operation is executed
 * by the database engine in a temporary collection.
 *
 * @param {Object} oplogItem  the update item from the oplog.
 * @param {Function} cb  On error the first parameter will be the Error object and
 *                       the second parameter will be the original document. On 
 *                       success the first parameter will be null and the second
 *                       parameter will be the new version of the document.
 */
OplogTransform.prototype._applyOplogUpdateModifier = function _applyOplogUpdateModifier(oplogItem, cb) {
  this._log.info('vc _applyOplogUpdateModifier', JSON.stringify(oplogItem));

  if (typeof cb !== 'function') { throw new Error('cb must be a function'); }

  var that = this;

  // copy the parent of this item from the DAG to a temporary collection
  // update it there and insert it back into the DAG

  // ask for last version of this id
  this._reqChan.write(JSON.stringify({ id: oplogItem.o._id }) + '\n');

  // listen for response, expect it to be the last stored version
  this._resChan.once('readable', function() {
    var head = that._resChan.read();
    if (!head) return void cb(new Error('previous version of doc not found'), oplogItem);

    that._createNewVersionByUpdateDoc(head, oplogItem, cb);
  });
};

/**
 * Save a new document with only the _id of the doc, _d: true and a reference to
 * it's parent.
 *
 * @param {Object} oplogItem  the delete item from the oplog.
 * @param {Function} cb  On error the first parameter will be the Error object and
 *                       the second parameter will be the original document. On 
 *                       success the first parameter will be null and the second
 *                       parameter will be the new version of the document.
 */
OplogTransform.prototype._applyOplogDeleteItem = function _applyOplogDeleteItem(oplogItem, cb) {
  this._log.info('vc _applyOplogDeleteItem', JSON.stringify(oplogItem));

  if (typeof cb !== 'function') { throw new Error('cb must be a function'); }

  try {
    if (oplogItem.op !== 'd') { throw new Error('oplogItem.op must be "d"'); }
    if (!oplogItem.o._id) { throw new Error('missing oplogItem.o._id'); }
  } catch(err) {
    process.nextTick(function() {
      cb(err, oplogItem);
    });
    return;
  }

  cb(null, {
    h: {
      id: oplogItem.o._id,
      d: true,
    },
    m: {
      _op: oplogItem.ts
    },
    b: oplogItem.o
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
