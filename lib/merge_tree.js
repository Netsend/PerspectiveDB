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

/* jshint -W116 */

'use strict';

var crypto = require('crypto');

var async = require('async');
var BSON, bson = require('bson');
if (process.browser) {
  BSON = new bson();
} else {
  BSON = new bson.BSONPure.BSON();
}

var through2 = require('through2');
var isEqual = require('is-equal');
var match = require('match-object');

var Tree = require('./tree');
var merge = require('./merge');
var invalidLocalHeader = require('./invalid_local_header');
var streamify = require('./streamify');
var ConcatReadStream = require('./concat_read_stream');
var StreamMergeTree = require('./_stream_merge_tree');
var runHooks = require('./run_hooks');

var noop = function() {};

// wrapper around streamify that supports "reopen" recursively
function streamifier(dag) {
  var result = streamify(dag);
  result.reopen = function() {
    return streamifier(dag);
  };
  return result;
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

  this._localName = opts.local || '_local';
  this._stageName = opts.stage || '_stage';
  this._perspectives = opts.perspectives || [];
  this._transform = opts.transform || function(item, cb) { cb(null, item); };

  if (Buffer.byteLength(this._localName) > 254) { throw new Error('opts.local must not exceed 254 bytes'); }
  if (Buffer.byteLength(this._stageName) > 254) { throw new Error('opts.stage must not exceed 254 bytes'); }

  if (this._localName === this._stageName) { throw new Error('local and stage names can not be the same'); }

  var that = this;

  this._perspectives.forEach(function(perspective) {
    if (Buffer.byteLength(perspective) > 254) { throw new Error('each perspective name must not exceed 254 bytes'); }
    if (perspective === that._localName) { throw new Error('every perspective should have a name that differs from the local name'); }
    if (perspective === that._stageName) { throw new Error('every perspective should have a name that differs from the stage name'); }
  });

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

  var treeOpts = {
    vSize: this._vSize,
    iSize: this._iSize,
    log: this._log
  };

  this._perspectives.forEach(function(perspective) {
    that._pe[perspective] = new Tree(db, perspective, treeOpts);
  });

  // signal updates per perspective
  this._updatedPerspectives = {};

  this._local = new Tree(db, this._localName, treeOpts);
  this._stage = new Tree(db, this._stageName, {
    vSize: this._vSize,
    iSize: this._iSize,
    log: this._log,
    skipValidation: true
  });

  this._opts = opts;
}

module.exports = MergeTree;

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
 * Get a stream over each item in the order of insertion into the tree.
 *
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   raw {Boolean, default false}  whether to return a BSON serialized or
 *                                 deserialezed object (false).
 *   filter {Object}  conditions a document should hold
 *   first {base64 String}  first version, offset
 *   excludeFirst {Boolean, default false}  whether or not first should be
 *                                          excluded
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
  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }

  opts.log = opts.log || this._log;
  return new StreamMergeTree(this, opts);
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

  if (remote === this._localName) {
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

  return through2.obj(function(item, encoding, cb) {
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
        that._log.info('merge_tree createRemoteWriteStream write %s %j', remote, afterItem.h);
        tree.write(afterItem, function(err) {
          if (err) { tree.once('error', noop); cb(err); return; }
          that._updatedPerspectives[remote] = true;
          cb();
        });
      } else {
        // if hooks filter out the item don't push
        that._log.info('merge_tree createRemoteWriteStream filter %s %j', remote, item.h);
        cb();
      }
    });
  });
};

/**
 * Save a new version from the local perspective or a confirmation of a
 * handled merge (that originated from a remote) in the local tree.
 *
 * New items should have the following structure:
 * {
 *   h: {Object}  header containing the following values:
 *     id:  {mixed}  id of this item
 *     [v]: {base64 String}  supply version to confirm a handled merge or
 *                           deterministically set the version. If not set, a
 *                           new version will be generated based on content and
 *                           parents.
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

  return through2.obj(function(item, encoding, cb) {
    if (typeof item !== 'object') {
      process.nextTick(function() {
        cb(new TypeError('item must be an object'));
      });
      return;
    }
    if (typeof cb !== 'function') {
      process.nextTick(function() {
        cb(new TypeError('cb must be a function'));
      });
      return;
    }

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
        that._log.info('merge_tree createLocalWriteStream ack %j move with ancestors to local', item.h);

        // use header from item in stage and copy body and any meta info from the user supplied item
        exists.m = item.m;
        exists.b = item.b;
        item = exists;

        // ack, move everything up to this item to the local tree
        stage.iterateInsertionOrder({ id: item.h.id, last: item.h.v }, function(stageItem, next) {
          var pe = stageItem.h.pe;

          if (pe) {
            // copy from perspective up to this item from stage
            local.lastByPerspective(pe, 'base64', function(err, v) {
              if (err) { cb(err); return; }

              // see mergeWithLocal, if there is a parent, the first parent might point to a version that exists in the perspective it came from
              var opts = { last: item.h.v, excludeLast: true, transform: that._transform };
              if (v) {
                opts.first = v;
                opts.excludeFirst = true;
              }
              that._log.info('merge_tree createLocalWriteStream _copyTo from %s to local', pe);
              MergeTree._copyTo(that._pe[pe], local, opts, function(err) {
                if (err) { cb(err); return; }

                // move item from stage to local
                local.write(stageItem, function(err) {
                  if (err) {
                    that._log.err('merge_tree createLocalWriteStream item not copied to local %s %j', err, stageItem.h);
                    local.once('error', noop);
                    next(err);
                    return;
                  }
                  stage.del(stageItem, function(err) {
                    if (err) {
                      that._log.err('merge_tree createLocalWriteStream item not removed from stage %s %j', err, stageItem.h);
                      next(err);
                      return;
                    }
                    that._log.info('merge_tree createLocalWriteStream item moved from stage to local %j', stageItem.h);
                    next();
                  });
                });
              });
            });
          } else {
            // move item from stage to local
            local.write(stageItem, function(err) {
              if (err) { local.once('error', noop); next(err); return; }
              stage.del(stageItem, function(err) {
                if (err) { next(err); return; }
                that._log.info('merge_tree createLocalWriteStream item moved from stage to local %j', stageItem.h);
                next();
              });
            });
          }
        }, cb);
      } else {
        // determine parent by last non-deleted and non-conflicting head in local
        var heads = [];
        local.getHeads({ id: item.h.id, skipDeletes: true, skipConflicts: true }, function(head, next) {
          heads.push(head.h.v);
          next();
        }, function(err) {
          if (err) { cb(err); return; }

          if (heads.length > 1) {
            error = 'more than one non-deleted and non-conflicting head in local tree';
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

          // generate new version based on content
          if (item.h.v) {
            newItem.h.v = item.h.v;
          } else {
            newItem.h.v = MergeTree._versionContent(item, that._vSize);
          }

          if (item.h.d) {
            newItem.h.d = true;
          }

          that._log.info('merge_tree createLocalWriteStream write new head in local %j', newItem.h);

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
  });
};

/**
 * Merge src tree with the local tree using an intermediate staging tree. Merge
 * every new head with any previous head in stage. Call merge handler with the
 * new head and the previous head.
 * If there is a conflict, add the original shead to stage with h.c set, but
 * don't call merge handler with it. This is to ensure insertion order of stree.
 *
 * Runs transform on each item before saving to staging.
 *
 * Merge handler and transform can be provided to the constructor.
 *
 * @param {Object} stree  source tree to merge
 * @param {Function} [mergeHandler]  function to handle newly created merges
 *                                   signature: function (merged, lhead, next)
 * @param {Function} cb  First parameter will be an error object or null.
 */
MergeTree.prototype.mergeWithLocal = function mergeWithLocal(stree, mergeHandler, cb) {
  if (typeof stree !== 'object' || Array.isArray(stree)) { throw new TypeError('stree must be an object'); }
  if (cb == null) {
    cb = mergeHandler;
    mergeHandler = null;
  }
  if (mergeHandler != null && typeof mergeHandler !== 'function') { throw new TypeError('mergeHandler must be a function'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var stage = this._stage;
  var that = this;
  var error;

  // use new mergeHandler if given
  this._ensureMergeHandler(mergeHandler);

  this._log.info('mergeWithLocal _mergeTrees %s -> %s (via %s)', stree.name, this._local.name, this._stage.name);

  MergeTree._mergeTrees(stree, this._local, function iterator(smerge, dmerge, shead, dhead, next) {
    if (shead.h.pe !== stree.name) {
      error = new Error('shead and stree perspective mismatch');
      that._log.err('mergeWithLocal %s %j', error, shead);
      next(error);
      return;
    }

    that._transform(shead, function(err, nitem) {
      if (err) {
        that._log.err('mergeWithLocal transform shead error %s %j', err, shead);
        next(err);
        return;
      }

      shead = nitem;

      // if there was a conflict, save shead in stage with conflict flag set
      if (Array.isArray(smerge)) {
        // set conflict flag
        shead.h.c = true;
        stage.write(shead, function(err) {
          if (err) { stage.once('error', noop); next(err); return; }
          next();
        });
        return;
      }

      // merge given head with non-deleted and non-conflicting head in stage
      function mergeNewHeadWithStage(newHead) {
        var headOpts = {
          id: newHead.h.id,
          skipDeletes: true,
          skipConflicts: true
        };
        var heads = [];
        stage.getHeads(headOpts, function(head, snext) {
          heads.push(head);
          snext();
        }, function(err) {
          if (err) { next(err); return; }

          that._log.debug('mergeWithLocal head in stage %j', heads);

          if (heads.length > 1) {
            next(new Error('more than one head in stage'));
            return;
          }

          // if no previous head in stage, insert new head in staging and call merge handler with the new head and the current head in the local tree
          if (!heads.length) {
            stage.write(newHead, function(err) {
              if (err) {
                // write error in stage
                that._log.err('mergeWithLocal stage write error %s %j', err, newHead);
                stage.once('error', noop);
                next(err);
                return;
              }
              that._log.debug('mergeWithLocal write first new head %j', newHead);
              that._mergeHandler(newHead, dhead, next);
            });
            return;
          }

          // merge with previous head
          var sX = new ConcatReadStream([streamifier(heads), stree.createReadStream({ id: newHead.h.id, reverse: true })]);
          var sY = new ConcatReadStream([streamifier(heads), stree.createReadStream({ id: newHead.h.id, reverse: true })]);

          var opts = {
            rootX: newHead,
            rootY: heads[0]
          };
          merge(sX, sY, opts, function(err, merged) {
            // if conflict, write new head with conflict flag set, don't call merge handler
            if (err) {
              that._log.err('mergeWithLocal staging merge error %s %j %j', err, newHead, heads[0]);
              // set conflict flag
              newHead.h.c = true;
              stage.write(newHead, function(err) {
                if (err) { stage.once('error', noop); next(err); return; }
                next();
              });
              return;
            }

            // if fast-forward, write new head and call merge handler with the new merged head and the previous head from stage
            if (merged.h.v && merged.h.v === newHead.h.v) {
              stage.write(newHead, function(err) {
                if (err) {
                  that._log.err('mergeWithLocal stage newHead write error %s %j %j', err, newHead);
                  stage.once('error', noop);
                  next(err);
                  return;
                }
                that._log.debug('mergeWithLocal write new head by fast-forward %j %j', newHead, merged);
                that._mergeHandler(newHead, heads[0], next);
              });
              return;
            }

            // else insert new head and new merge and call merge handler with the new merged head and the previous head from stage
            that._log.debug('mergeWithLocal write new head %j and merge %j', newHead, merged);
            stage.write(newHead, function(err) {
              if (err) {
                that._log.err('mergeWithLocal stage newHead write error %s %j %j', err, newHead);
                stage.once('error', noop);
                next(err);
                return;
              }

              merged.h.v = MergeTree._versionContent(merged);

              stage.write(merged, function(err) {
                if (err) {
                  that._log.err('mergeWithLocal stage merge write error %s %j %j', err, newHead, merged);
                  stage.once('error', noop);
                  next(err);
                  return;
                }
                that._mergeHandler(merged, heads[0], next);
              });
            });
          });
        });
      }

      // determine new item, merge, merge ff
      if (!smerge) {
        // merge ff
        mergeNewHeadWithStage(shead);
        return;
      }

      // merge
      that._transform(smerge, function(err, nitem) {
        if (err) {
          that._log.err('mergeWithLocal transform smerge error %s %j', err, smerge);
          next(err);
          return;
        }

        mergeNewHeadWithStage(nitem);
      });
    });
  }, cb);
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
 *   done {Function}  Called after last merge is executed. First parameter will be
 *                    an error object or null. Only called if repeat is false or
 *                    closed.
 */
MergeTree.prototype.mergeAll = function mergeAll(opts) {
  if (opts == null) { opts = {}; }

  if (opts.interval != null && typeof opts.interval !== 'number') { throw new TypeError('opts.interval must be a number'); }
  if (opts.mergeHandler != null && typeof opts.mergeHandler !== 'function') { throw new TypeError('opts.mergeHandler must be a function'); }
  if (opts.done != null && typeof opts.done !== 'function') { throw new TypeError('opts.done must be a function'); }

  this._mergeInterval = opts.interval || 0;

  // use new mergeHandler if given
  this._ensureMergeHandler(opts.mergeHandler);
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

      //that._log.debug('merge_tree mergeAll done');

      if (that._mergeInterval && !that.stopAutoMerge) {
        // don't pass on mergeHandler to allow _ensureMergeHandler to register new handlers
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
 * Set a mergeHandler. Use if both createLocalWriteStream and
 * createRemoteWriteStream are in use. This function is called with every newly
 * created merge (either by fast-forward or three-way-merge). If no function is
 * specified, a default merge handler is created that will directly write to the
 * local write stream.
 *
 * This way local updates can be saved async by using an intermediate stage. Make
 * sure any processed merges are written back to the local write stream in the same
 * order as mergeHandler was called. It's ok to miss some items as long as the
 * order does not change.
 *
 * @param {Function} [mergeHandler]  function to handle newly created merges
 *                                   signature: function (merged, lhead, next)
 */
MergeTree.prototype._ensureMergeHandler = function _ensureMergeHandler(mergeHandler) {
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
 * Merge src tree with dst tree. Find the last version of src tree that is in
 * dst tree and try to merge each head in src (in insertion order) with every
 * head of the DAG in dst tree. Call back with the head from src, the head from
 * dst and the merged result. If there is a merge conflict, smerge in the
 * iterator will be an array of conflicting attributes and no dmerge is set.
 *
 * @param {Object} stree  source tree to merge
 * @param {Object} dtree  dest tree to merge with
 * @param {Function} iterator  function(smerge, dmerge, shead, dhead, next) called with
 *                             merges from both perspectives and both heads a
 *                             merge is based on. merges are only created if not
 *                             a fast-forward
 *                             Fifth parameter is a next handler.
 * @param {Function} cb  First parameter will be an error object or null.
 */
MergeTree._mergeTrees = function _mergeTrees(stree, dtree, iterator, cb) {
  if (typeof stree !== 'object') { throw new TypeError('stree must be an object'); }
  if (typeof dtree !== 'object') { throw new TypeError('dtree must be an object'); }
  if (typeof iterator !== 'function') { throw new TypeError('iterator must be a function'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  MergeTree._iterateMissing(stree, dtree, function(sitem, snext) {
    // is this new version a head in stree?
    stree.getHeads({ id: sitem.h.id }, function(shead, shnext) {
      if (shead.h.v !== sitem.h.v) {
        shnext();
        return;
      }

      // merge with all head(s) in dtree
      var dheads = [];
      dtree.getHeads({ id: sitem.h.id }, function(dhead, dnext) {
        dheads.push(dhead);
        dnext();
      }, function(err) {
        if (err) { shnext(err); return; }

        if (!dheads.length) {
          // new item not in dtree yet, fast-forward
          iterator(null, null, shead, null, shnext);
          return;
        }

        // merge with all dheads
        async.eachSeries(dheads, function(dhead, cb2) {
          var sX = stree.createReadStream({ id: shead.h.id, reverse: true });
          var sY = dtree.createReadStream({ id: dhead.h.id, reverse: true });
          var opts = {
            rootX: shead,
            rootY: dhead
          };
          merge(sX, sY, opts, function(err, smerge, dmerge) {
            if (err) {
              // signal merge conflict
              iterator(err.conflict, null, shead, dhead, cb2);
              return;
            }

            // this is either a new item, an existing item, or a merge by fast-forward
            if (!dmerge.h.v) {
              // merge, create a version based on content
              var version = MergeTree._versionContent(smerge);
              dmerge.h.v = version;
              smerge.h.v = version;
              iterator(smerge, dmerge, shead, dhead, cb2);
              return;
            } else if (!smerge.h.i || !dmerge.h.i) {
              if (smerge.h.v !== dmerge.h.v) {
                cb2(new Error('unexpected version mismatch'));
                return;
              }
              // merge by fast-forward
              iterator(smerge, dmerge, shead, dhead, cb2);
            } else {
              // existing item with h.v and h.i
              iterator(null, null, shead, dhead, cb2);
            }
          });
        }, shnext);
      });
    }, snext);
  }, cb);
};

/**
 * Start iterating over src starting at the last version of stree that is in dtree.
 *
 * Ensures insertion order of src.
 *
 * @param {Object} stree  source tree
 * @param {Object} dtree  destination tree
 * @param {Function} iterator  function(item, next) called with src item and
 *                             next handler
 * @param {Function} cb  First parameter will be an error object or null.
 */
MergeTree._iterateMissing = function _iterateMissing(stree, dtree, iterator, cb) {
  if (typeof stree !== 'object') { throw new TypeError('stree must be an object'); }
  if (typeof dtree !== 'object') { throw new TypeError('dtree must be an object'); }
  if (typeof iterator !== 'function') { throw new TypeError('iterator must be a function'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  // determine offset
  dtree.lastByPerspective(stree.name, 'base64', function(err, v) {
    if (err) { cb(err); return; }

    var opts = {};
    if (v) {
      opts.first = v;
      opts.excludeFirst = true;
    }
    stree.iterateInsertionOrder(opts, iterator, cb);
  });
};

/**
 * Copy all items from stree to dtree. Maintains insertion order of stree.
 *
 * @param {Object} stree  source tree to search
 * @param {Object} dtree  destination tree
 * @param {Object} [opts]  object containing configurable parameters
 * @param {Function} cb  First parameter will be an error object or null.
 *
 * opts:
 *   first {base64 String}  first version that should be used
 *   last {base64 String}  last version to copy
 *   excludeFirst {Boolean, default false}  whether or not first should be
 *                                         excluded
 *   excludeLast {Boolean, default false}  whether or not last should be
 *                                         excluded
 *   transform {Function}  transformation function to run on each item
 *                         signature: function(item, cb2) cb2 should be called
 *                         with an optional error and a possibly transformed
 *                         item
 */
MergeTree._copyTo = function _copyTo(stree, dtree, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = null;
  }

  opts = opts || {};
  if (typeof stree !== 'object') { throw new TypeError('stree must be an object'); }
  if (typeof dtree !== 'object') { throw new TypeError('dtree must be an object'); }
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  opts.transform = opts.transform || function(item, cb2) { cb2(null, item); };

  var first = true;
  stree.iterateInsertionOrder({ first: opts.first }, function(item, next) {
    if (first) {
      first = false;
      if (opts.first && item.h.v === opts.first && opts.excludeFirst) {
        next();
        return;
      }
    }

    if (opts.last && item.h.v === opts.last && opts.excludeLast) {
      next(null, false);
      return;
    }

    opts.transform(item, function(err, nitem) {
      if (err) { next(err); return; }
      if (nitem === null || typeof nitem === 'undefined') {
        next();
        return;
      }

      // allow one error without bubbling up
      dtree.once('error', function() {});

      dtree.write(nitem, function(err) {
        if (err) { dtree.once('error', noop); next(err); return; }
        if (opts.last && opts.last === item.h.v) {
          next(null, false);
        } else {
          next();
        }
      });
    });
  }, cb);
};

/**
 * Create a content based version number. Based on the first vSize bytes of the
 * sha256 hash of the item encoded in BSON.
 *
 * @param {Object} item  item to create version for
 * @param {Number} [vSize]  version size in bytes, default to parent version size
 * @param {String} base64 encoded version of vSize bytes
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
