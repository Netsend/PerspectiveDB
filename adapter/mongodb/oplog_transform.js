/**
 * Copyright 2014, 2015, 2016 Netsend.
 *
 * This file is part of PerspectiveDB.
 *
 * PerspectiveDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PerspectiveDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PerspectiveDB. If not, see <https://www.gnu.org/licenses/>.
 */

/* jshint -W116 */

'use strict';

var stream = require('stream');
var util = require('util');

var async = require('async');
var bson = require('bson');
var BSONStream = require('bson-stream');
var mongodb = require('mongodb');
var xtend = require('xtend');

var binsearch = require('../../lib/binsearch').inArray;
var isEqual = require('../../lib/is_equal');
var findAndDelete = require('./find_and_delete');
var noop = require('../../lib/noop');

var BSON = new bson.BSONPure.BSON();
var Timestamp = mongodb.Timestamp;
var Transform = stream.Transform;
var Writable = stream.Writable;

/**
 * OplogTransform
 *
 * Transform oplog items into new versions.
 *
 * @param {Object} oplogDb  connection to the oplog database
 * @param {String} oplogCollName  oplog collection name
 * @param {String} dbName  database to follow
 * @param {mongodb.Collection[]} collections  track these collections
 * @param {Object} controlWrite  request stream to ask for latest versions
 * @param {Object} controlRead  response stream to recieve latest versions
 * @param {Object} expected  Array with new objects that were updated by this
 *                           adapter and are thus expected to echo back via the
 *                           oplog
 * @param {Object} [opts]  object containing configurable parameters
 *
 * Options:
 *   updateRetry {Number, defaults to 5000}  retry once more if previous head
 *     not found
 *   conflicts {mongodb.Collection, defaults to conflicts}  collection to monitor
 *     for conflicts
 *   blacklist {String[]}  collections to not track, takes precedence over
 *     collections, by default contains the mongo system collections and the tmp
 *     and conflict collection
 *   tmpStorage {mongodb.Collection, default _pdbtmp}  temporary collection to
 *     compute update modifiers
 *   bson {Boolean, default false}  whether to return raw bson or parsed objects
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 */
function OplogTransform(oplogDb, oplogCollName, dbName, collections, controlWrite, controlRead, expected, opts) {
  if (oplogDb == null || typeof oplogDb !== 'object') { throw new TypeError('oplogDb must be an object'); }
  if (!oplogCollName || typeof oplogCollName !== 'string') { throw new TypeError('oplogCollName must be a non-empty string'); }
  if (!dbName || typeof dbName !== 'string') { throw new TypeError('dbName must be a non-empty string'); }
  if (!Array.isArray(collections)) { throw new TypeError('collections must be an array'); }
  if (controlWrite == null || typeof controlWrite !== 'object') { throw new TypeError('controlWrite must be an object'); }
  if (controlRead == null || typeof controlRead !== 'object') { throw new TypeError('controlRead must be an object'); }
  if (!Array.isArray(expected)) { throw new TypeError('expected must be an array'); }
  if (opts != null && typeof opts !== 'object') { throw new TypeError('opts must be an object'); }

  Transform.call(this, xtend(opts, { objectMode: true }));

  opts = opts || {};
  if (opts.bson != null && typeof opts.bson !== 'boolean') { throw new TypeError('opts.bson must be a boolean'); }
  if (opts.log != null && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }

  this._databaseName = dbName;

  this._oplogColl = oplogDb.collection(oplogCollName);

  this._expected = expected;
  this._opts = xtend(opts);

  this._db = oplogDb.db(this._databaseName);

  // write ld-json to the request stream
  this._controlWrite = controlWrite;

  // expect bson on the response stream
  this._controlRead = controlRead.pipe(new BSONStream());

  this._conflicts = opts.conflicts || this._db.collection('conflicts');
  this._tmpStorage = opts.tmpStorage || this._db.collection('_pdbtmp');
  this._updateRetry = opts.updateRetry || 5000;

  // blacklist mongo system collections
  this._blacklist = opts.blacklist || ['system.users', 'system.profile', 'system.indexes', this._tmpStorage.collectionName, this._conflicts.collectionName];

  // exclude blacklisted collections
  this._collections = collections.filter(coll => !~this._blacklist.indexOf(coll.collectionName));

  // set ns and collection map by namespace
  this._ns = [];
  this._collectionMap = {};

  this._collections.forEach(collection => {
    this._ns.push(this._databaseName + '.' + collection.collectionName);
    this._collectionMap[this._databaseName + '.' + collection.collectionName] = collection.collectionName;
  });

  this._ns.sort(); // sort so binary searches can be used

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

// add collection to accept and track
OplogTransform.prototype.addCollection = function addCollection(collection) {
  this._ns.push(this._databaseName + '.' + collection.collectionName);
  this._collectionMap[this._databaseName + '.' + collection.collectionName] = collection.collectionName;
  this._ns.sort();
};

/**
 * Ask for last version and start reading the oplog after that. Convert oplog items
 * to new versions.
 *
 * Ask last version
 *  * if no last version, sent the whole collection to upstream
 *  * else open oplog after that item
 *    * for each oplog item
 *      * if insert, delete or update by full doc, emit
 *      * if update by update modifier:
 *      * ask last version for h.id
 *        * save in tmp collection
 *        * apply update doc
 *        * emit
 */
OplogTransform.prototype.startStream = function startStream() {
  var that = this;
  var error;

  this._reopenOplog;

  // handle new oplog items via this._transform, use this._lastTs as offset
  function openOplog(reopen) {
    that._or = that._oplogReader(that._lastTs, { tailable: true, awaitData: true });
    // proxy error
    that._or.once('error', function(err) {
      // workaround shutdown mongo errors
      if (that._stop && err.message === 'cursor does not exist, was killed or timed out') {
        return;
      }
      that.emit('error', err);
      return;
    });
    that._or.once('end', function() {
      that._log.debug2('ot startStream end of tailable oplog cursor');
      that._or.unpipe(that);
      that._or = null;
      if (reopen && !that._stop) {
        that._reopenOplog = setTimeout(function() {
          openOplog(reopen);
        }, 1000);
      }
    });
    that._or.pipe(that, { end: false });
  }

  this._booting = true;
  this._boot(function(err, offset) {
    that._booting = false;
    if (err) {
      that.emit('error', err);
      return;
    }
    if (!offset) {
      error = new Error('could not determine oplog offset');
      that._log.err('ot startStream %s', error);
      that.emit('error', error);
      return;
    }
    that._lastTs = offset;

    // handle new oplog items via this._transform
    openOplog(true);
  });
};

/**
 * Stop oplog reader and signal to stop reopening.
 *
 * @param {Function} cb  First parameter will be an error object or null.
 */
OplogTransform.prototype.close = function close(cb) {
  this._stop = true;

  if (this._booting) {
    this._log.info('ot close still booting, wait a second and retry');
    setTimeout(() => this.close(cb), 1000);
    return;
  }

  var that = this;

  if (this._reopenOplog) {
    this._log.info('ot oplogReader cancel opening new oplog reader');
    clearTimeout(this._reopenOplog);
  }
  if (this._or) { // expect this does only exist if an oplog reader is not ended
    this._log.info('ot oplogReader close reader');
    // wait for end so that the killed cursor is unregistered within the driver (prevents MongoError: server 127.0.0.1:27017 sockets closed)
    this._or.once('end', function() {
      that.end(cb);
    });
    this._or.close();
  } else {
    this._log.info('ot oplogReader close');
    this.end(cb);
  }
};

/**
 * Make sure all collections are up to date with the latest oplog item, set name
 * spaces and collection maps.
 *
 * 1. Get the last known oplog item processed.
 * 2. For each collection, make sure it is bootstrapped
 * 3. zip up all collections with their offsets until the latest oplog item and
 *    proceed from there
 */
OplogTransform.prototype._boot = function _boot(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  var tasks = [];

  // lookup last known offset in the tree, if any
  var lastOffset;
  tasks.push(function(cb2) {
    that._controlRead.once('readable', function() {
      var head = that._controlRead.read();
      if (Object.keys(head).length) {
        try {
          lastOffset = head.m._op;
        } catch(err) {
          that.emit('error', new Error('unable to determine offset'));
        }
      }
      that._log.debug2('ot _boot lastOffset %s', lastOffset);
      cb2();
    });
    // ask for last version in the tree
    that._controlWrite.write(JSON.stringify({}) + '\n');
  });

  // make sure all collections have an offset, bootstrap if not
  var offsetColl = {};
  tasks.push(function(cb2) {
    async.eachSeries(that._collections, function(collection, cb3) {
      that._prefixExists(collection.collectionName + '\x01', function(err, exists) {
        if (err) { cb3(err); return; }

        // assume it is up to date, if boot procedures are not interupted, this is the case
        if (exists) {
          // if there exists a prefix in the tree, assume a last known offset could be determined
          offsetColl[lastOffset.toString()] = collection;
          cb3();
          return;
        }

        // else bootstrap this collection
        that._bootstrapColl(collection, function(err, newTs) {
          if (err) { cb3(err); return; }
          offsetColl[newTs.toString()] = collection;
          cb3();
        });
      });
    }, cb2);
  });

  // finally zip all up
  tasks.push(function(cb2) {
    that._log.debug2('ot _boot tracked collections %j', that._ns);
    that._zipUp(offsetColl, function(err, offset) {
      if (err) { cb2(err); return; }
      lastOffset = offset;
      cb2();
    });
  });

  async.series(tasks, function(err) {
    if (err) { cb(err); return; }
    cb(null, lastOffset);
  });
};

// lookup any head for the given prefix
OplogTransform.prototype._prefixExists = function _prefixExists(prefix, cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  var error;

  this._controlRead.once('readable', function() {
    var obj = that._controlRead.read();
    if (!obj) {
      // eof, request stream closed
      error = new Error('control read stream closed');
      that._log.err('ot _prefixExists %s', error);
      cb(error);
      return;
    }

    that._log.debug('ot _prefixExists %j', obj.h);
    cb(null, !!Object.keys(obj).length);
  });

  // ask for a head by prefix to see if there is any head at all for the given collection
  // write version requests in ld-json
  this._controlWrite.write(JSON.stringify({ prefixExists: prefix }) + '\n');
};

/**
 * Create an id for upstream based on the collection name and id.
 *
 * @param {mixed} id  the id to use, must contain a toString method if not a string
 */
OplogTransform.prototype._createUpstreamId = function _createUpstreamId(ns, id) {
  if (typeof id === 'object') { id = JSON.stringify(id); } // convert ObjectIDs and other objects to strings
  var collectionName = this._collectionMap[ns];
  if (!collectionName) {
    this._log.err('ot _createUpstreamId invalid collection name %s for id %s', ns, id);
    throw new Error('invalid collection name');
  }
  return collectionName + '\x01' + id;
}

/**
 * Bootstrap, read all documents from the given collection.
 *
 * @param {mongodb.Collection} collection  collection to bootstrap
 * @param {Function} cb  first item will be an error object or null, second item
 *   the last timestamp in the oplog before the bootstrap started.
 */
OplogTransform.prototype._bootstrapColl = function _bootstrapColl(coll, cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  // use current last offset and send every object in the collections upstream
  this._oplogColl.find().sort({ '$natural': -1 }).limit(1).project({ ts: 1 }).next(function(err, oplogItem) {
    if (err) { cb(err); return; }
    if (!oplogItem) { cb(new Error('no oplog item found')); return; }

    var collName = coll.collectionName;

    var i = 0;
    var transformer = new Transform({
      objectMode: true,
      transform: function(item, enc, cb2) {
        i++;
        that._log.debug2('ot _bootstrapColl item: %j', item);

        // enclose item in an oplog like item
        cb2(null, {
          o: item,
          ts: oplogItem.ts,
          ns: that._databaseName + '.' + collName,
          op: 'i'
        });
      }
    });

    transformer.on('end', function() {
      that._log.debug('ot bootstrapped %s %d items (since: %s)', collName, i, oplogItem.ts);
      cb(null, oplogItem.ts);
    });

    var c = coll.find();
    c.comment('bootstrap_oplog_reader');
    c.sort('_id');
    c.pipe(transformer).pipe(that, { end: false });
  });
};

/**
 * Zip up, read all documents in the oplog from the given collections starting at
 * the given offset up until the latest oplog item.
 *
 * @param {mongodb.Timestamp} offset  timestamp
 * @param {String[]} collectionNames  collections to process
 * @param {Function} cb  first item will be an error object or null, second item
 *   the last timestamp in the oplog when ended.
 */
OplogTransform.prototype._zipUp = function _zipUp(offsetCollections, cb) {
  // sort timestamps
  var timestamps = Object.keys(offsetCollections).sort();

  if (!timestamps.length) {
    process.nextTick(function() {
      var offset =  new Timestamp(0, (new Date()).getTime() / 1000);
      cb(null, offset);
    });
    return;
  }

  // start with first collection and lowest timestamp
  var offset = Timestamp.fromString(timestamps[0]);

  // list of namespaces that are currently tracked
  var zipping = [];
  zipping.push(this._databaseName + '.' + offsetCollections[timestamps.shift()].collectionName);

  var that = this;
  this._oplogReader(offset).pipe(new Writable({
    objectMode: true,
    write: function(oplogItem, enc, cb2) {
      // record last seen
      offset = oplogItem.ts;

      var ts = offset.toString();
      while (ts >= timestamps[0])
        zipping.push(that._databaseName + '.' + offsetCollections[timestamps.shift()].collectionName);

      // check if this item belongs to one of the tracked collections
      if (!oplogItem.ns || !~zipping.indexOf(oplogItem.ns)) {
        cb2();
        return;
      }
      that._log.debug('ot zipUp new item %j', oplogItem);

      that.write(oplogItem, cb2);
    }
  })).on('finish', function() {
    cb(null, offset);
  }).on('error', cb);
};

/**
 * Read oplog.
 *
 * @param {mongodb.Timestamp} offset  where to start reading the oplog
 * @param {Object} [opts]  object containing optional parameters
 * @return {mongodb.Cursor} the result of find which is a mongodb cursor
 *
 * opts:
 *   filter {Object}  extra filter to apply apart from namespace
 *   includeOffset {Boolean, default false}  whether to include or exclude offset
 */
OplogTransform.prototype._oplogReader = function _oplogReader(offset, opts) {
  if (offset == null || typeof offset !== 'object') { throw new TypeError('offset must be an object'); }

  if (opts == null) opts = {};
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (opts.filter != null && typeof opts.filter !== 'object') { throw new TypeError('opts.filter must be an object'); }
  if (opts.includeOffset != null && typeof opts.includeOffset !== 'boolean') { throw new TypeError('opts.includeOffset must be a boolean'); }

  if (opts.bson != null) { throw new TypeError('opts.bson is no longer supported'); }

  // setup CursorStream
  var selector = {};
  if (opts.includeOffset) {
    selector.ts = { $gte: offset };
  } else {
    selector.ts = { $gt: offset };
  }
  if (opts.filter) {
    selector = { $and: [selector, opts.filter] };
  }

  var mongoOpts = xtend({
    sort: { '$natural': 1 },
    comment: 'oplog_reader2'
  }, opts);

  this._log.debug2('ot oplogReader selector: %j, opts: %j', selector, mongoOpts);

  var c = this._oplogColl.find(selector);

  if (mongoOpts.sort)
    c.sort(mongoOpts.sort);
  if (mongoOpts.comment)
    c.comment(mongoOpts.comment);
  if (mongoOpts.tailable)
    c.addCursorFlag('tailable', true)
  if (mongoOpts.awaitData)
    c.addCursorFlag('awaitData', true)

  return c;
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

  var msg = OplogTransform._invalidOplogItem(oplogItem);
  if (msg) {
    // use timestamp and return if op is unknown
    if (msg === 'unknown item.op') {
      this._lastTs = oplogItem.ts;
      process.nextTick(cb);
      return;
    }

    this._log.err('ot _transform invalid oplog item: %s %j', msg, oplogItem);
    process.nextTick(function() {
      cb(new Error('invalid oplogItem'));
    });
    return;
  }

  this._lastTs = oplogItem.ts;

  // check if this item belongs to one of the tracked collections
  if (!binsearch(this._ns, oplogItem.ns)) {
    process.nextTick(cb);
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

  this._log.debug2('ot _transform oplog item: %j', oplogItem);

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

  var bson = this._opts.bson;

  var selector = { _id: oplogItem.o2._id };

  // restore id
  dagItem.b._id = dagItem.m._id;

  // save the previous head and apply the update modifiers to get the new version of the doc
  that._tmpStorage.replaceOne(selector, dagItem.b, { w: 1, upsert: true, comment: '_createNewVersionByUpdateDoc' }, function(err, result) {
    if (err) { cb(err); return; }

    if (!result.result.ok) {
      error = new Error('new version not inserted in tmp collection');
      that._log.err('ot _createNewVersionByUpdateDoc %s %j %j', error, dagItem, result);
      cb(error);
      return;
    }

    // update the just created copy
    that._log.info('ot _createNewVersionByUpdateDoc selector %j', selector);

    that._tmpStorage.findOneAndUpdate(selector, oplogItem.o, { returnOriginal: false }, function(err, result) {
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
      that._tmpStorage.deleteOne(selector, function(err, result) {
        if (err || result.deletedCount !== 1) {
          that._log.err('ot _createNewVersionByUpdateDoc remove update ', err, result);
          cb(err);
          return;
        }

        // put id in meta info and remove it from the body
        delete newObj._id;
        var obj = {
          n: {
            h: { id: dagItem.h.id },
            m: { _op: oplogItem.ts, _id: oplogItem.o2._id },
            b: newObj
          }
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
  try {
    if (oplogItem.op !== 'u' && oplogItem.op !== 'i') { throw new Error('oplogItem.op must be "u" or "i"'); }
    if (!oplogItem.o._id) { throw new Error('missing oplogItem.o._id'); }
  } catch(err) {
    process.nextTick(function() {
      cb(err);
    });
    return;
  }

  var that = this;
  var opts = this._opts;

  var upstreamId = that._createUpstreamId(oplogItem.ns, oplogItem.o._id);

  // check if this is a confirmation by the adapter or a third party update
  // copy without b._id (not saved in level)
  var copy = xtend(oplogItem.o);
  delete copy._id;

  var obj = findAndDelete(that._expected, function(item) {
    return item.n.h.id === upstreamId && isEqual(copy, item.n.b);
  });

  if (obj) {
    that._log.debug2('ot _applyOplogFullDoc ACK %j (%d)', obj.n.h, that._expected.length);
  } else {
    that._log.debug2('ot _applyOplogFullDoc NEW %s (%d)', oplogItem.o._id, that._expected.length);
    obj = {
      n: {
        h: { id: upstreamId },
        b: copy
      }
    };
  }
  obj.n.m = { _op: oplogItem.ts, _id: oplogItem.o._id };

  process.nextTick(function() {
    cb(null, opts.bson ? BSON.serialize(obj) : obj);
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
 * If a version is not found, it will be retried once more after a five second delay.
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

  // ask for last version of this id
  var upstreamId = this._createUpstreamId(oplogItem.ns, oplogItem.o2._id);

  // listen for response, expect it to be the last stored version
  this._controlRead.once('readable', function() {
    var head = that._controlRead.read();

    if (head && head.h) {
      that._createNewVersionByUpdateDoc(head, oplogItem, cb);
      return;
    }

		/* give it a bit more time and try again */
    that._log.warning('ot _applyOplogUpdateModifier previous version of doc not found (first try)', JSON.stringify(oplogItem), upstreamId);
    setTimeout(function() {
      that._controlRead.once('readable', function() {
        head = that._controlRead.read();

        if (head && head.h) {
          that._createNewVersionByUpdateDoc(head, oplogItem, cb);
          return;
        }

        that._log.err('ot _applyOplogUpdateModifier previous version of doc not found (second and last try)', JSON.stringify(oplogItem), upstreamId);
        cb(new Error('previous version of doc not found'));
        return;
      });

      that._controlWrite.write(JSON.stringify({ id: upstreamId }) + '\n');
    }, that._updateRetry);
  });

  this._controlWrite.write(JSON.stringify({ id: upstreamId }) + '\n');
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

  var that = this;
  var opts = this._opts;

  var upstreamId = that._createUpstreamId(oplogItem.ns, oplogItem.o._id);

  // check if this is a confirmation by the adapter or a third party update
  var obj = findAndDelete(that._expected, function(item) {
    return item.n.h.id === upstreamId && item.n.h.d;
  });

  if (obj) {
    that._log.debug2('ot _applyOplogDeleteItem ACK %j (%d)', obj.n.h, that._expected.length);
  } else {
    that._log.debug2('ot _applyOplogDeleteItem NEW %s (%d)', oplogItem.o._id, that._expected.length);
    obj = {
      n: {
        h: {
          id: upstreamId,
          d: true,
        },
      }
    };
  }
  obj.n.m = { _op: oplogItem.ts, _id: oplogItem.o._id };

  process.nextTick(function() {
    cb(null, opts.bson ? BSON.serialize(obj) : obj);
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
  if (!item || item == null || typeof item !== 'object') { return 'missing item'; }
  if (!item.hasOwnProperty('o'))  { return 'missing item.o'; }
  if (!item.hasOwnProperty('ts')) { return 'missing item.ts'; }
  if (!item.hasOwnProperty('ns')) { return 'missing item.ns'; }
  if (!item.hasOwnProperty('op')) { return 'missing item.op'; }

  // ignore if operation is not "i", "u" or "d"
  if (item.op !== 'i' && item.op !== 'u' && item.op !== 'd') { return 'unknown item.op'; }

  return '';
};
