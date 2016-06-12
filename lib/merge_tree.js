/**
 * Copyright 2015, 2016 Netsend.
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

var crypto = require('crypto');
var stream = require('stream');

var async = require('async');
var bson = require('bson');
var isEqual = require('is-equal');
var match = require('match-object');
var xtend = require('xtend');

var Tree = require('./tree');
var merge = require('./merge');
var noop = require('./noop');
var invalidLocalHeader = require('./invalid_local_header');
var ConcatReadStream = require('./concat_read_stream');
var runHooks = require('./run_hooks');

var Transform = stream.Transform;
var Writable = stream.Writable;

var BSON;
if (process.browser) {
  BSON = new bson();
} else {
  BSON = new bson.BSONPure.BSON();
}

/**
 * MergeTree
 *
 * Accept objects from different perspectives. Merge other perspectives into the
 * local perspective.
 *
 * If both local and remote updates can come in make sure to use
 * mergeTree.mergeAll and optionally mergeTree.createLocalWriteStream.
 *
 * @param {LevelUP.db} db  database for persistent storage
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   local {String, default "_local"}  name of the local tree, should not exceed
 *                                     254 bytes
 *   stage {String, default "_stage"}  name of the staging tree, should not
 *                                     exceed 254 bytes
 *   perspectives {Array}  Names of different sources that should be merged to
 *                         the local tree. A name should not exceed 254 bytes.
 *   vSize {Number, default 6}  number of bytes used for the version. Should be:
 *                              0 < vSize <= 6
 *   iSize {Number, default 6}  number of bytes used for i. Should be:
 *                              0 < iSize <= 6
 *   transform {Function}  transformation function to run on each item
 *                         signature: function(item, cb) cb should be called
 *                         with an error or a (possibly transformed) item
 *   mergeHandler {Function}  function to handle newly created merges
 *                            signature: function (merged, lhead, next)
 *   conflictHandler {Function}  function to handle merges with conflicts
 *                               signature: function (attrs, shead, lhead, next)
 *                               call next with a new item or null if not resolved.
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 */
function MergeTree(db, opts) {
  if (typeof db !== 'object' || db === null) { throw new TypeError('db must be an object'); }

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }

  if (opts.local != null && typeof opts.local !== 'string') { throw new TypeError('opts.local must be a string'); }
  if (opts.stage != null && typeof opts.stage !== 'string') { throw new TypeError('opts.stage must be a string'); }
  if (opts.perspectives != null && !Array.isArray(opts.perspectives)) { throw new TypeError('opts.perspectives must be an array'); }
  if (opts.log != null && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }
  if (opts.vSize != null && typeof opts.vSize !== 'number') { throw new TypeError('opts.vSize must be a number'); }
  if (opts.iSize != null && typeof opts.iSize !== 'number') { throw new TypeError('opts.iSize must be a number'); }
  if (opts.transform != null && typeof opts.transform !== 'function') { throw new TypeError('opts.transform must be a function'); }

  opts.objectMode = true;

  this.localName = opts.local || '_local';
  this._stageName = opts.stage || '_stage';
  this._perspectives = opts.perspectives || [];
  this._transform = opts.transform || function(item, cb) { cb(null, item); };

  if (Buffer.byteLength(this.localName) > 254) { throw new Error('opts.local must not exceed 254 bytes'); }
  if (Buffer.byteLength(this._stageName) > 254) { throw new Error('opts.stage must not exceed 254 bytes'); }

  if (this.localName === this._stageName) { throw new Error('local and stage names can not be the same'); }

  var that = this;

  this._vSize = opts.vSize || 6;
  this._iSize = opts.iSize || 6;

  if (this._vSize < 0 || this._vSize > 6) { throw new Error('opts.vSize must be between 0 and 6'); }
  if (this._iSize < 0 || this._iSize > 6) { throw new Error('opts.iSize must be between 0 and 6'); }

  this._db = db;

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

  // create trees
  this._pe = {};

  this._treeOpts = {
    vSize: this._vSize,
    iSize: this._iSize,
    log: this._log
  };

  // signal updates per perspective
  this._updatedPerspectives = {};

  this._perspectives.forEach(function(perspective) {
    that.addPerspective(perspective);
  });

  this._local = new Tree(db, this.localName, this._treeOpts);
  this._stage = new Tree(db, this._stageName, {
    vSize: this._vSize,
    iSize: this._iSize,
    log: this._log,
    skipValidation: true
  });

  // use new merge handler if given
  this.ensureMergeHandler(opts.mergeHandler);

  // use new conflict handler if given
  this._ensureConflictHandler(opts.conflictHandler);

  this._opts = opts;
}

module.exports = MergeTree;

/**
 * Dynamically add a new perspective.
 *
 * @param {Number|base64 String} version  valid lbeint or base64 int
 */
MergeTree.prototype.addPerspective = function addPerspective(perspective) {
  if (Buffer.byteLength(perspective) > 254) { throw new Error('each perspective name must not exceed 254 bytes'); }
  if (perspective === this.localName) { throw new Error('every perspective should have a name that differs from the local name'); }
  if (perspective === this._stageName) { throw new Error('every perspective should have a name that differs from the stage name'); }
  if (this._pe[perspective]) { throw new Error('perspective already exists'); }

  this._pe[perspective] = new Tree(this._db, perspective, this._treeOpts);
  // signal update on init
  this._updatedPerspectives[perspective] = true;
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
 *   bson {Boolean, default false}  whether to return a BSON serialized or
 *                                  deserialized object (false).
 */
MergeTree.prototype.getByVersion = function getByVersion(version, opts, cb) {
  return this._local.getByVersion(version, opts, cb);
};

/**
 * Get local tree.
 *
 * @return {Object} the local tree
 */
MergeTree.prototype.getLocalTree = function getLocalTree() {
  return this._local;
};

/**
 * Get stage tree.
 *
 * @return {Object} the stage tree
 */
MergeTree.prototype.getStageTree = function getStageTree() {
  return this._stage;
};

/**
 * Get all remote trees.
 *
 * @return {Object} key is the perspecive name, value the Tree.
 */
MergeTree.prototype.getRemoteTrees = function getRemoteTrees() {
  return this._pe;
};

/**
 * Get a readable stream over each item in the order of insertion into the tree.
 *
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   bson {Boolean, default false}  whether to return a BSON serialized or
 *                                  deserialized object (false).
 *   filter {Object}  conditions a document should hold
 *   first {base64 String}  first version, offset
 *   last {base64 String}  last version
 *   excludeFirst {Boolean, default false}  whether or not first should be
 *                                          excluded
 *   excludeLast {Boolean, default false}  whether or not last should be
 *                                         excluded
 *   reverse {Boolean, default false}  if true, starts with last version
 *   hooks {Array}  array of asynchronous functions to execute, each hook has the following signature: db, object, options,
 *                  callback and should callback with an error object, the new item and possibly extra data.
 *   hooksOpts {Object}  options to pass to a hook
 *   tail {Boolean, default false}  if true, keeps the stream open
 *   tailRetry {Number, default 1000}  reopen readers every tailRetry ms
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 */
MergeTree.prototype.createReadStream = function createReadStream(opts) {
  if (opts != null && typeof opts !== 'object') { throw new TypeError('opts must be an object'); }

  opts = xtend({
    bson: false,
    filter: {},
    excludeFirst: false,
    excludeLast: false,
    reverse: false,
    hooks: [],
    hooksOpts: {},
    tail: false,
    tailRetry: 1000,
    log: this._log
  }, opts);

  if (opts.bson != null && typeof opts.bson !== 'boolean') { throw new TypeError('opts.bson must be a boolean'); }
  if (opts.filter != null && typeof opts.filter !== 'object') { throw new TypeError('opts.filter must be an object'); }
  if (opts.first != null) {
    if (typeof opts.first !== 'number' && typeof opts.first !== 'string') { throw new TypeError('opts.first must be a base64 string or a number'); }
    if (typeof opts.first === 'string' && Buffer.byteLength(opts.first, 'base64') !== this._vSize) { throw new Error('opts.first must be the same size as the configured vSize'); }
  }
  if (opts.excludeFirst != null && typeof opts.excludeFirst !== 'boolean') { throw new TypeError('opts.excludeFirst must be a boolean'); }
  if (opts.hooks != null && !Array.isArray(opts.hooks)) { throw new TypeError('opts.hooks must be an array'); }
  if (opts.hooksOpts != null && typeof opts.hooksOpts !== 'object') { throw new TypeError('opts.hooksOpts must be an object'); }
  if (opts.tail != null && typeof opts.tail !== 'boolean') { throw new TypeError('opts.tail must be a boolean'); }
  if (opts.tailRetry != null && typeof opts.tailRetry !== 'number') { throw new TypeError('opts.tailRetry must be a number'); }
  if (opts.log != null && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }

  this._log.notice('createReadStream opts: %j', opts);

  var that = this;

  // shortcut
  var filter = opts.filter;
  var hooks = opts.hooks;
  var hooksOpts = opts.hooksOpts;

  /**
   * Call back with array of connected parents for item.
   *
   * findConnectedParents(v)
   *   parents <- {}
   *   for every pa in parents(v)
   *     item <- lookup(pa)
   *     if match(item, crit)
   *       parents set pa
   *     else
   *       parents set parents.findConnectedParents(pa)
   *   return parents
   */
  function findConnectedParents(item, crit, cb) {
    that._log.debug('createReadStream findConnectedParents %s', item.h.pa);
    var parents = {};
    async.eachSeries(item.h.pa, function(pav, cb2) {
      that._local.getByVersion(pav, function(err, pa) {
        if (err) { cb2(err); return; }

        // descend if not all criteria hold on this item
        if (!match(crit, pa.b)) {
          that._log.debug('createReadStream findConnectedParents crit does not hold, descend');
          findConnectedParents(pa, crit, function(err, nParents) {
            if (err) { cb2(err); return; }
            Object.keys(nParents).forEach(function(pa2) {
              parents[pa2] = true;
            });
            cb2();
          });
        } else {
          runHooks(hooks, that._db, pa, hooksOpts, function(err, afterItem) {
            if (err) { cb2(err); return; }

            // descend if hooks filter out the item
            if (afterItem) {
              that._log.debug('createReadStream findConnectedParents add to parents %s', pa.h.v);
              parents[pa.h.v] = true;
              cb2();
            } else {
              that._log.debug('createReadStream findConnectedParents crit holds, but hooks filter, descend');
              findConnectedParents(pa, crit, function(err, nParents) {
                if (err) { cb2(err); return; }
                Object.keys(nParents).forEach(function(pa2) {
                  parents[pa2] = true;
                });
                cb2();
              });
            }
          });
        }
      });
    }, function(err) {
      if (err) { cb(err); return; }
      cb(null, parents);
    });
  }

  var transformer = new Transform({
    readableObjectMode: !opts.bson,
    writableObjectMode: true,
    transform: function (item, enc, cb) {
      that._log.debug('createReadStream data %j', item.h);

      // don't emit if not all criteria hold on this item
      if (!match(filter, item.b)) {
        that._log.info('createReadStream filter %j', filter);
        cb();
        return;
      }

      runHooks(hooks, that._db, item, hooksOpts, function(err, afterItem) {
        if (err) { that.emit(err); return; }

        // skip if hooks filter out the item
        if (!afterItem) {
          that._log.info('createReadStream hook filtered %j', item.h);
          cb();
          return;
        }

        // else find parents
        findConnectedParents(afterItem, filter, function(err, parents) {
          if (err) { that.emit(err); return; }

          afterItem.h.pa = Object.keys(parents);

          // remove perspective and local state
          delete afterItem.h.pe;
          delete afterItem.h.i;
          delete afterItem.m;

          // push the bson or native object out to the reader, and resume if not flooded
          that._log.info('createReadStream push %j', afterItem.h);
          cb(null, opts.bson ? BSON.serialize(afterItem) : afterItem);
        });
      });
    }
  });

  // always read objects instead of bson
  var rw = this._local.createReadStream(xtend(opts, { bson: false }));
  rw.pipe(transformer);

  // proxy finish
  transformer.on('finish', function() {
    rw.end();
  });
  return transformer;
};

/**
 * Save new versions of a certain perspective in the appropriate Tree.
 *
 * New items should have the following structure:
 * {
 *   h: {Object}  header containing the following values:
 *     id:  {mixed}  id of this h
 *     v:   {base64 String}  version
 *     pa:  {Array}  parent versions
 *     [d]: {Boolean}  true if this id is deleted
 *   [m]: {Object}  meta info to store with this document
 *   [b]: {mixed}  document to save
 * }
 *
 * @param {String} remote  name of the remote, used to set h.pe
 * @param {Object} [opts]  object containing configurable parameters
 * @return {Object} stream.Writable
 *
 * opts:
 *   filter {Object}  conditions a document should hold
 *   hooks {Array}  array of asynchronous functions to execute, each hook has the following signature: db, object, options,
 *                  callback and should callback with an error object, the new item and possibly extra data.
 *   hooksOpts {Object}  options to pass to a hook
 */
MergeTree.prototype.createRemoteWriteStream = function createRemoteWriteStream(remote, opts) {
  if (typeof remote !== 'string') { throw new TypeError('remote must be a string'); }
  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }
  if (opts.filter != null && typeof opts.filter !== 'object') { throw new TypeError('opts.filter must be an object'); }
  if (opts.hooks != null && !Array.isArray(opts.hooks)) { throw new TypeError('opts.hooks must be an array'); }
  if (opts.hooksOpts != null && typeof opts.hooksOpts !== 'object') { throw new TypeError('opts.hooksOpts must be an object'); }

  var that = this;
  var error;

  if (remote === this.localName) {
    error = 'perspective should differ from local name';
    this._log.err('merge_tree createRemoteWriteStream %s %s', error, remote);
    throw new Error(error);
  }

  if (remote === this._stageName) {
    error = 'perspective should differ from stage name';
    this._log.err('merge_tree createRemoteWriteStream %s %s', error, remote);
    throw new Error(error);
  }

  var tree = this._pe[remote];

  if (!tree) {
    error = 'perspective not found';
    that._log.err('merge_tree createRemoteWriteStream %s %s', error, remote);
    throw new Error(error);
  }

  this._log.info('merge_tree createRemoteWriteStream opened tree %s', remote);

  var filter = opts.filter || {};
  var hooks = opts.hooks || [];
  var hooksOpts = opts.hooksOpts || [];
  var db = that._db;

  return new Writable({
    objectMode: true,
    write: function(item, encoding, cb) {
      try {
        item.h.pe = remote;
      } catch(err) {
        that._log.err('merge_tree createRemoteWriteStream %s %s', err, remote);
        process.nextTick(function() {
          cb(err);
        });
        return;
      }

      if (!match(filter, item.b)) {
        that._log.info('merge_tree createRemoteWriteStream filter %j', filter);
        cb();
        return;
      }

      runHooks(hooks, db, item, hooksOpts, function(err, afterItem) {
        if (err) { cb(err); return; }

        if (afterItem) {
          // push the item out to the reader
          that._log.debug('merge_tree createRemoteWriteStream write %s %j', remote, afterItem.h);
          tree.write(afterItem, function(err) {
            if (err) { tree.once('error', noop); cb(err); return; }
            that._updatedPerspectives[remote] = true;
            cb();
          });
        } else {
          // if hooks filter out the item don't push
          that._log.debug('merge_tree createRemoteWriteStream filter %s %j', remote, item.h);
          cb();
        }
      });
    }
  });
};

/**
 * Save a new version from the local perspective or a confirmation of a handled
 * merge (that originated from a remote and exists in the stage) in the local tree.
 *
 * New items should have the following structure:
 * {
 *   h: {Object}  header containing the following values:
 *     id:  {mixed}  id of this item
 *     [v]: {base64 String}  supply version to confirm a handled merge or
 *                           deterministically set the version. If not set, a
 *                           new random version will be generated.
 *     [d]: {Boolean}  true if this id is deleted
 *   [m]: {mixed}  meta info to store with this document
 *   [b]: {mixed}  document to save
 * }
 *
 * @param {Object} item  item to save
 * @param {Function} cb  Callback that is called once the item is saved. First
 *                       parameter will be an error object or null.
 */
MergeTree.prototype.createLocalWriteStream = function createLocalWriteStream() {
  var that = this;

  return new Writable({
    objectMode: true,
    write: function(item, encoding, cb) {
      var error = invalidLocalHeader(item.h);
      if (error) {
        process.nextTick(function() {
          cb(new TypeError('item.' + error));
        });
        return;
      }

      if (item.h.pa) {
        error = new Error('did not expect local item to have a parent defined');
        that._log.err('merge_tree createLocalWriteStream %s %j', error, item);
        process.nextTick(function() {
          cb(error);
        });
        return;
      }

      // use local and staging tree
      var local = that._local;
      var stage = that._stage;

      function handleStageItem(err, exists) {
        if (err) { cb(err); return; }

        if (exists && isEqual(exists.b, item.b)) {
          // see mergeWithLocal
          that._log.info('merge_tree createLocalWriteStream ack %j move with ancestors to local', item.h);

          // use header from item in stage and copy body and any meta info from the user supplied item
          exists.m = item.m;
          exists.b = item.b;
          item = exists;

          // ack, move everything up to this item to the local tree
          stage.insertionOrderStream({ id: item.h.id, last: item.h.v }).pipe(new Writable({
            objectMode: true,
            write: function(stageItem, enc, next) {
              // move item from stage to local
              local.write(stageItem, function(err) {
                if (err) { local.once('error', noop); next(err); return; }
                stage.del(stageItem, next);
              });
            }
          })).on('error', cb).on('finish', cb);
        } else {
          that._log.debug('merge_tree createLocalWriteStream new item %j determine parent by last non-conflicting head in local', item.h);

          var heads = [];
          local.getHeads({ id: item.h.id, skipConflicts: true }, function(head, next) {
            heads.push(head.h.v);
            next();
          }, function(err) {
            if (err) { cb(err); return; }

            that._log.debug('merge_tree createLocalWriteStream found heads %s', heads);

            if (heads.length > 1) {
              error = 'more than one non-conflicting head in local tree';
              that._log.err('merge_tree createLocalWriteStream %s %j', error, item);
              cb(new Error(error));
              return;
            }

            var newItem = {
              h: {
                id: item.h.id,
                pa: heads
              }
            };

            if (item.b) {
              newItem.b = item.b;
            }

            if (item.m) {
              newItem.m = item.m;
            }

            if (item.h.v) {
              newItem.h.v = item.h.v;
            } else {
              // generate new random version because the items content might already exist
              newItem.h.v = MergeTree._generateRandomVersion(that._vSize);
            }

            if (item.h.d) {
              newItem.h.d = true;
            }

            that._log.debug('merge_tree createLocalWriteStream write new head in local %j', newItem.h);

            local.write(newItem, function(err) {
              if (err) { local.once('error', noop); cb(err); return; }
              cb();
            });
          });
        }
      }
      if (item.h.v) {
        // check if this version is in the stage or not
        stage.getByVersion(item.h.v, handleStageItem);
      } else {
        handleStageItem();
      }
    }
  });
};

/**
 * Copy all items in stree that are not in the local tree to staging. Then merge
 * stage with local.
 *
 * Only mutates the staging tree.
 *
 * Transform can be provided to the constructor.
 *
 * @param {Object} stree  source tree to merge
 * @param {Function} cb  First parameter will be an error object or null.
 */
MergeTree.prototype.mergeWithLocal = function mergeWithLocal(stree, cb) {
  if (typeof stree !== 'object' || Array.isArray(stree)) { throw new TypeError('stree must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var local = this._local;
  var stage = this._stage;
  var that = this;

  this._log.info('mergeWithLocal %s -> %s (via %s)', stree.name, local.name, stage.name);

  this._copyMissingToStage(stree, local, function(err) {
    if (err) { cb(err); return; }

    that._mergeStageWithLocal(cb);
  });
};

/**
 * Get last version of a certain perspective.
 *
 * Proxy Tree.lastByPerspective
 *
 * @param {Buffer|String} pe  perspective
 * @param {String} [decodeV]  decode v as string with given encoding. Encoding
 *                            must be either "hex", "utf8" or "base64". If not
 *                            given a number will be returned.
 * @param {Function} cb  First parameter will be an error object or null. Second
 *                       parameter will be a version or null.
 */
MergeTree.prototype.lastByPerspective = function lastByPerspective(pe, decodeV, cb) {
  this._local.lastByPerspective(pe, decodeV, cb);
};

/**
 * Get last received version from a certain perspective.
 *
 * @param {String} remote  name of the perspective
 * @param {String} [decodeV]  decode v as string with given encoding. Encoding
 *                            must be either "hex", "utf8" or "base64". If not
 *                            given a number will be returned.
 * @param {Function} cb  First parameter will be an error object or null. Second
 *                       parameter will be a version or null.
 */
MergeTree.prototype.lastReceivedFromRemote = function lastReceivedFromRemote(remote, decodeV, cb) {
  if (typeof remote !== 'string') { throw new TypeError('remote must be a string'); }
  if (typeof decodeV === 'function') {
    cb = decodeV;
    decodeV = null;
  }
  if (decodeV != null && typeof decodeV !== 'string') { throw new TypeError('decodeV must be a string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var tree = this._pe[remote];

  if (!tree) {
    var error = 'remote not found';
    this._log.err('merge_tree %s %s', error, remote);
    throw new Error(error);
  }

  tree.lastVersion(decodeV, cb);
};

/**
 * Set a mergeHandler. Use if both createLocalWriteStream and
 * createRemoteWriteStream are in use. This function is called with every newly
 * created merge (either by fast-forward or three-way-merge). If no function is
 * specified, a default merge handler is created that will directly write to the
 * local write stream.
 *
 * This way, by using an intermediate stage, remote updates can be saved locally in
 * an asynchronous way. Make sure any processed merges are written back to the
 * local write stream in the same order as mergeHandler was called. It's ok to miss
 * some items as long as the order does not change.
 *
 * @param {Function} [mergeHandler]  function to handle newly created merges
 *                                   signature: function (merged, lhead, next)
 */
MergeTree.prototype.ensureMergeHandler = function ensureMergeHandler(mergeHandler) {
  if (mergeHandler != null && typeof mergeHandler !== 'function') { throw new TypeError('mergeHandler must be a function'); }

  if (mergeHandler) {
    this._mergeHandler = mergeHandler;
  }
  // ensure a mergeHandler is set
  if (!this._mergeHandler) {
    var writable = this.createLocalWriteStream();
    this._mergeHandler = function(merged, lhead, next) {
      // local items should not have a parent, it is determined by the local write stream
      delete merged.h.pa;
      writable.write(merged, function(err) {
        if (err) { writable.once('error', noop); next(err); return; }
        next();
      });
    };
  }
};

/**
 * Do a merge of every updated perspective with local. If an interval is given a
 * loop is started that can be interrupted by setting mergeTree.stopAutoMerge to
 * true.
 *
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   interval {Number, default 0}  time in ms to wait and repeat a merge, 0 is off.
 *   mergeHandler {Function}  function that should handle newly created merges
 *                            signature: function (merged, lhead, next)
 *   conflictHandler {Function}  function to handle merges with conflicts
 *                               signature: function (attrs, shead, lhead, next)
 *                               call next with a new item or null if not resolved.
 *   done {Function}  Called after last merge is executed. First parameter will be
 *                    an error object or null. Only called if repeat is false or
 *                    closed.
 */
MergeTree.prototype.mergeAll = function mergeAll(opts) {
  if (opts == null) { opts = {}; }

  if (opts.interval != null && typeof opts.interval !== 'number') { throw new TypeError('opts.interval must be a number'); }
  if (opts.mergeHandler != null && typeof opts.mergeHandler !== 'function') { throw new TypeError('opts.mergeHandler must be a function'); }
  if (opts.conflictHandler != null && typeof opts.conflictHandler !== 'function') { throw new TypeError('opts.conflictHandler must be a function'); }
  if (opts.done != null && typeof opts.done !== 'function') { throw new TypeError('opts.done must be a function'); }

  this._log.debug2('merge_tree mergeAll');

  this._mergeInterval = opts.interval || 0;

  // use new mergeHandler if given
  this.ensureMergeHandler(opts.mergeHandler);

  // use new conflictHandler if given
  this._ensureConflictHandler(opts.conflictHandler);

  var done = opts.done || noop;

  var that = this;
  setTimeout(function() {
    async.eachSeries(that._perspectives, function(pe, cb2) {
      if (that._updatedPerspectives[pe]) {
        that.mergeWithLocal(that._pe[pe], function(err) {
          that._updatedPerspectives[pe] = false;
          cb2(err);
        });
      } else {
        cb2();
      }
    }, function(err) {
      if (err) { done(err); return; }

      if (that._mergeInterval && !that.stopAutoMerge) {
        // don't pass on merge or conflict handler to allow ensureMergeHandler to register new handlers
        that.mergeAll({
          interval: opts.interval,
          done: opts.done
        });
      } else {
        done();
      }
    });
  }, this._mergeInterval);
};

/**
 * Close open read and write streams, merge once more and call back.
 *
 * TODO: should wait for all writers to finish, for now, just wait at least 100ms
 *
 * @param {Function} [cb]  Called after all is closed. First parameter will be an
 *                         error object or null.
 */
MergeTree.prototype.close = function close(cb) {
  this.stopAutoMerge = true;
  setTimeout(cb, (this._mergeInterval || 0) + 100); // wait at least 100ms
};

/**
 * Get statistics of all trees.
 *
 * @param {Function} cb  First parameter will be an error object or null. Second
 *                       parameter will be an object.
 */
MergeTree.prototype.stats = function stats(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var res = {};
  var that = this;
  that._local.stats(function(err, stats) {
    if (err) { cb(err); return; }
    res.local = stats;
    that._stage.stats(function(err, stats) {
      if (err) { cb(err); return; }
      res.stage = stats;

      async.each(that._perspectives, function(pe, cb2) {
        that._pe[pe].stats(function(err, stats) {
          if (err) { cb2(err); return; }
          res[pe] = stats;
          cb2();
        });
      }, function(err) {
        cb(err, res);
      });
    });
  });
};



/////////////////////
//// PRIVATE API ////
/////////////////////



/**
 * Set a conflictHandler. Use if both createLocalWriteStream and
 * createRemoteWriteStream are in use. This function is called with every newly
 * created merge that has a conflict. Call tree.getConflicts to get conflicting
 * merges.
 *
 * @param {Function} [conflictHandler]  function to handle newly conflicting merges
 *                                      signature:
 *                                      function (attrs, shead, lhead, next)
 *                                      call next with a new item or null if not
 *                                      resolved.
 */
MergeTree.prototype._ensureConflictHandler = function _ensureConflictHandler(conflictHandler) {
  if (conflictHandler != null && typeof conflictHandler !== 'function') { throw new TypeError('conflictHandler must be a function'); }

  if (conflictHandler) {
    this._conflictHandler = conflictHandler;
  }
  // ensure a conflictHandler is set
  if (!this._conflictHandler) {
    this._conflictHandler = function(attrs, shead, lhead, cb) {
      cb();
    };
  }
};

/**
 * Copy all items in stree that are not in dtree to stage. Run transform before
 * saving.
 *
 * Ensures insertion order of stree.
 *
 * @param {Object} stree  source tree to merge
 * @param {Object} dtree  dest tree to compare with
 * @param {Function} cb  First parameter will be an error object or null.
 */
MergeTree.prototype._copyMissingToStage = function _copyMissingToStage(stree, dtree, cb) {
  if (typeof stree !== 'object') { throw new TypeError('stree must be an object'); }
  if (typeof dtree !== 'object') { throw new TypeError('dtree must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  // determine offset
  dtree.lastByPerspective(stree.name, 'base64', function(err, v) {
    if (err) { cb(err); return; }

    var opts = {};
    if (v) {
      opts.first = v;
      opts.excludeFirst = true;
    }
    stree.insertionOrderStream(opts).pipe(new Writable({ // use a stream for proper caching
      objectMode: true,
      write: function(sitem, enc, cb2) {
        that._stage.getByVersion(sitem.h.v, function(err, inStage) {
          if (err) { cb2(err); return; }

          if (inStage) {
            that._log.debug('merge_tree _copyMissingToStage already in stage %s', sitem.h.v);
            cb2();
            return;
          }

          that._transform(sitem, function(err, nitem) {
            if (err) {
              that._log.err('_copyMissingToStage transform error %s %j', err, sitem);
              cb2(err);
              return;
            }

            // filter item
            if (nitem == null) {
              cb2();
              return;
            }

            that._log.debug('merge_tree _copyMissingToStage %s', sitem.h.v);
            nitem.h.pe = stree.name;
            that._stage.write(nitem, cb2);
          });
        });
      }
    })).on('error', cb).on('finish', cb);
  });
};

/**
 * Merge stage with local. Merge every non-conflicting head in stage in insertion
 * order with the one non-conflicting head in local. Call merge handler with the
 * head from stage, the head from local and the merged result. If there is a merge
 * conflict, the conflict handler is invoked with an array of conflicting
 * attributes and both heads.
 *
 * merge handler is called at most "number of heads in stage" times.
 *
 * @param {Function} cb  First parameter will be an error object or null.
 */
MergeTree.prototype._mergeStageWithLocal = function _mergeStageWithLocal(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var stage = this._stage;
  var local = this._local;
  var that  = this;

  // find all heads in insertion order
  stage.insertionOrderStream().pipe(new Writable({
    objectMode: true,
    write: function(sitem, enc, snext) {
      that._log.debug('_mergeStageWithLocal process %s', sitem.h.v);
      var headOpts = {
        id: sitem.h.id,
        skipConflicts: true
      };
      stage.getHeads(headOpts, function(shead, shnext) {
        if (shead.h.v !== sitem.h.v) {
          that._log.debug('_mergeStageWithLocal skip head %s, not this item %s', shead.h.v, sitem.h.v);
          shnext();
          return;
        }

        that._log.debug('_mergeStageWithLocal item %s is a head', sitem.h.v);

        // merge with the one non-conflicting head in local
        var lheads = [];
        local.getHeads(headOpts, function(lhead, lnext) {
          lheads.push(lhead);
          lnext();
        }, function(err) {
          if (err) { shnext(err); return; }

          if (lheads.length > 1) {
            var error = new Error('more than one head in local');
            that._log.err('_mergeStageWithLocal error %s %j', error, lheads);
            shnext(error);
            return;
          }

          if (!lheads.length) {
            // new item not in local yet, fast-forward
            that._mergeHandler(shead, null, shnext);
            return;
          }

          var lhead = lheads[0];

          that._log.debug('_mergeStageWithLocal merge %j with %j', sitem.h, lhead.h);

          // merge with previous head in stage
          var sX = new ConcatReadStream([ stage.createReadStream({ id: shead.h.id, reverse: true }), local.createReadStream({ id: lhead.h.id, reverse: true }) ]);
          var sY = local.createReadStream({ id: lhead.h.id, reverse: true });

          var opts = {
            rootX: shead,
            rootY: lhead
          };
          merge(sX, sY, opts, function(err, smerge, lmerge) {
            if (err) {
              if (err.name !== 'MergeConflict') {
                that._log.err('_mergeStageWithLocal merge error %s %j %j', err, shead, lhead, err.stack);
                shnext(err);
                return;
              }
              that._conflictHandler(err.conflict, shead.b, lhead.b, function(item) {
                that._log.notice('_mergeStageWithLocal merge conflict %s %j %j', err.conflict, shead, lhead);
                if (item) {
                  // use new item body as new version
                  var newItem = {
                    h: {
                      id: shead.h.id,
                      v: '',
                      pa: [shead.h.v, lhead.h.v].sort()
                    },
                    b: item
                  };
                  newItem.h.v = MergeTree._versionContent(item, that._vSize); // merkle-tree
                  // schedule next run
                  that._updatedPerspectives[shead.h.pe] = true;
                  stage.write(newItem, shnext);
                } else {
                  // mark shead conflict for later processing and proceed
                  stage.setConflictByVersion(shead.h.v, shnext);
                }
              });
              return;
            }

            // this item either exists in both trees, in only one of the trees (fast-forward) or in none of the trees (genuine merge)
            // if a fast-forward for stage, ignore
            if (!lmerge.h.v) {
              // merge

              // create a version based on content
              smerge.h.pa.sort();
              smerge.h.v = MergeTree._versionContent(smerge); // merkle-tree

              that._log.info('_mergeStageWithLocal merge %j', smerge.h);

              // save to stage and call merge handler
              stage.write(smerge, function(err) {
                if (err) { shnext(err); return; }
                that._mergeHandler(smerge, lhead, shnext);
              });
            } else if (!lmerge.h.i) {
              // merge by fast-forward, already in stage, call merge handler
              that._log.info('_mergeStageWithLocal merge ff %j', smerge.h);
              if (smerge.h.i) {
                that._mergeHandler(smerge, lhead, shnext);
              } else {
                that._log.info('_mergeStageWithLocal save merge %j', smerge.h);
                stage.write(smerge, function(err) {
                  if (err) { shnext(err); return; }
                  that._mergeHandler(smerge, lhead, shnext);
                });
              }
            } else {
              // lmerge already exists in local tree (has both h.v and h.i)
              that._log.info('_mergeStageWithLocal exists in local ff %j', lmerge.h);
              shnext();
            }
          });
        });
      }, snext);
    }
  })).on('error', cb).on('finish', cb);
};

/**
 * Create a content based version number. Based on the first vSize bytes of the
 * sha256 hash of the item encoded in BSON.
 *
 * @param {Object} item  item to create version for
 * @param {Number} [vSize]  version size in bytes, default to parent version size
 * @return {String} base64 encoded version of vSize bytes
 */
MergeTree._versionContent = function _versionContent(item, vSize) {
  if (typeof item !== 'object') { throw new TypeError('item must be an object'); }

  if (vSize == null) {
    // determine vSize from first parent, assume this is base64
    try {
      vSize = item.h.pa[0].length * 6 / 8;
    } catch(err) {
      throw new Error('cannot determine vSize');
    }
  }

  if (vSize < 1) {
    throw new Error('version too small');
  }
  var h = crypto.createHash('sha256');
  h.update(BSON.serialize(item), 'base64');
  // read first vSize bytes for version
  return h.digest().toString('base64', 0, vSize);
};

/**
 * Generate a random byte string.
 *
 * By default generates a 48 bit base64 number (string of 8 characters)
 *
 * @param {Number, default: 6} [size]  number of random bytes te generate
 * @return {String} base64 encoded version of size bytes
 */
MergeTree._generateRandomVersion = function _generateRandomVersion(size) {
  return crypto.randomBytes(size || 6).toString('base64');
};
