/**
 * Copyright 2015, 2016 Netsend.
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

var crypto = require('crypto');
var stream = require('stream');

var async = require('async');
var bson = require('bson');
var match = require('match-object');
var xtend = require('xtend');

var Tree = require('./tree');
var merge = require('./merge');
var noop = require('./noop');
var invalidLocalHeader = require('./invalid_local_header');
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
 * mergeTree.startMerge and optionally mergeTree.createLocalWriteStream.
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

  this._perspectives.forEach(function(perspective) {
    that._pe[perspective] = new Tree(that._db, perspective, that._treeOpts);
  });

  this._local = new Tree(db, this.localName, this._treeOpts);
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
 * Dynamically add a new perspective and setup a reader for the merge stream.
 *
 * @param {Number|base64 String} version  valid lbeint or base64 int
 */
MergeTree.prototype.addPerspective = function addPerspective(perspective) {
  if (Buffer.byteLength(perspective) > 254) { throw new Error('each perspective name must not exceed 254 bytes'); }
  if (perspective === this.localName) { throw new Error('every perspective should have a name that differs from the local name'); }
  if (perspective === this._stageName) { throw new Error('every perspective should have a name that differs from the stage name'); }
  if (this._pe[perspective]) { throw new Error('perspective already exists'); }

  this._pe[perspective] = new Tree(this._db, perspective, this._treeOpts);
  // start merging
  var that = this;
  this._local.lastByPerspective(perspective, 'base64', function(err, v) {
    if (err) {
      that._log.err('addPerspective %s', err);
      throw err;
    }

    var opts = { tail: true };
    if (v) {
      opts = xtend({
        first: v,
        excludeFirst: true
      }, opts);
    }
    that._setupMerge(perspective, opts, function(err) {
      if (err) { that._log.info('mt: %s error: %s', perspective, err); }
    });
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
 * Get a remote tree.
 *
 * @param {String} name of the remote
 * @return {Object} the remote Tree if found
 */
MergeTree.prototype.getRemoteTree = function getRemoteTree(name) {
  if (typeof name !== 'string') { throw new TypeError('name must be a string'); }
  return this._pe[name];
};

/**
 * Get the current local head by id.
 *
 * @param {Object} id  id of the object to fetch
 * @param {Function} cb  first parameter will be an error or null, second parameter
 *   will be the found head or undefined
 */
MergeTree.prototype.getLocalHead = function getLocalHead(id, cb) {
  // get the current local head, if any, and make sure it matches toBeResolved as well
  var that = this;
  var heads = [];
  this.getLocalTree().getHeads({ id: id }, function(head, next) {
    heads.push(head);
    next();
  }, function(err) {
    if (err) { cb(err); return; }
    if (heads.length > 1) {
      var error = new Error('local tree has multiple heads');
      that._log.err('mt getLocalHead %s %d for %s', error, heads.length, id);
      cb(error);
      return;
    }
    cb(null, heads[0]);
  });
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

  this._log.debug('mt createReadStream opts: %j', opts);

  // keys with undefined values will override these defaults
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

  var that = this;

  // shortcut
  var filter = opts.filter || {};
  var hooks = opts.hooks || [];
  var hooksOpts = opts.hooksOpts || {};

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
    that._log.debug2('mt createReadStream findConnectedParents %s', item.h.pa);
    var parents = {};
    async.eachSeries(item.h.pa, function(pav, cb2) {
      that._local.getByVersion(pav, function(err, pa) {
        if (err) { cb2(err); return; }

        // descend if not all criteria hold on this item
        if (!match(crit, pa.b)) {
          that._log.debug2('mt createReadStream findConnectedParents crit does not hold, descend');
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
              that._log.debug2('mt createReadStream findConnectedParents add to parents %s', pa.h.v);
              parents[pa.h.v] = true;
              cb2();
            } else {
              that._log.debug2('mt createReadStream findConnectedParents crit holds, but hooks filter, descend');
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
      that._log.debug2('mt createReadStream data %j', item.h);

      // don't emit if not all criteria hold on this item
      if (!match(filter, item.b)) {
        that._log.debug2('mt createReadStream filter %j', filter);
        cb();
        return;
      }

      runHooks(hooks, that._db, item, hooksOpts, function(err, afterItem) {
        if (err) { that.emit(err); return; }

        // skip if hooks filter out the item
        if (!afterItem) {
          that._log.debug2('mt createReadStream hook filtered %j', item.h);
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
          that._log.debug('mt createReadStream push %j', afterItem.h);
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
    this._log.err('mt createRemoteWriteStream %s %s', error, remote);
    throw new Error(error);
  }

  if (remote === this._stageName) {
    error = 'perspective should differ from stage name';
    this._log.err('mt createRemoteWriteStream %s %s', error, remote);
    throw new Error(error);
  }

  var tree = this._pe[remote];

  if (!tree) {
    error = 'perspective not found';
    that._log.err('mt createRemoteWriteStream %s %s', error, remote);
    throw new Error(error);
  }

  this._log.info('mt createRemoteWriteStream for %s', remote);

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
        that._log.err('mt createRemoteWriteStream %s %s', err, remote);
        process.nextTick(function() {
          cb(err);
        });
        return;
      }

      if (!match(filter, item.b)) {
        that._log.info('mt createRemoteWriteStream filter %j', filter);
        cb();
        return;
      }

      runHooks(hooks, db, item, hooksOpts, function(err, afterItem) {
        if (err) { cb(err); return; }

        if (afterItem) {
          // push the item out to the reader
          that._log.debug2('mt createRemoteWriteStream write %s %j', remote, afterItem.h);
          tree.write(afterItem, function(err) {
            if (err) { tree.once('error', noop); cb(err); return; }
            cb();
          });
        } else {
          // if hooks filter out the item don't push
          that._log.debug('mt createRemoteWriteStream filter %s %j', remote, item.h);
          cb();
        }
      });
    }
  });
};

/**
 * Save a new local version or a confirmation of a merge with a remote into the
 * local tree.
 *
 * New items should have the following structure:
 * {
 *   n: new version
 * }
 *
 * Merges should have the following structure:
 * {
 *   n: new version
 *   l: expected local version
 *   lcas: list of lca's
 *   pe: perspective of remote
 * }
 * all other properties are ignored.
 *
 * @return {stream.Writable}
 */
MergeTree.prototype.createLocalWriteStream = function createLocalWriteStream() {
  var that = this;

  if (this._localWriteStream) {
    throw new Error('local write stream already created');
  }

  var error;
  var local = this._local;

  /**
   * 1. New local version
   * Save as a local fast-forward.
   */
  function handleNewLocalItem(lhead, item, cb) {
    that._log.debug('mt createLocalWriteStream NEW %j, local: %j', item.h, lhead);

    error = invalidLocalHeader(item.h);
    if (error) {
      process.nextTick(function() {
        cb(new TypeError('item.' + error));
      });
      return;
    }

    if (item.h.pa) {
      error = new Error('did not expect local item to have a parent defined');
      that._log.err('mt createLocalWriteStream %s %j', error, item);
      process.nextTick(function() {
        cb(error);
      });
      return;
    }

    // parent is current local head
    var newItem = {
      h: {
        id: item.h.id,
        pa: lhead ? [lhead] : []
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
      newItem.h.v = MergeTree.generateRandomVersion(that._vSize);
    }

    if (item.h.d) {
      newItem.h.d = true;
    }

    local.write(newItem, function(err) {
      if (err) { local.once('error', noop); cb(err); return; }
      cb();
    });
  }

  /**
   * 2. Merge confirmation (either a fast-forward by a remote or a merge with a
   *    remote).
   *
   * If new version is from a perspective, it's a fast-forward, copy everything up
   * to this new version from the remote tree to the local tree.
   * If the new version has no perspective, it's a merge between a local and a
   * remote version. The merge must have exactly two parents and one of them must
   * point to the local head. Copy everything up until the other parent from the
   * remote tree to the local tree.
   *
   * Handles normal merges and conflict resolving merges.
   *
   * @param {String} lhead  version number of the local head
   * @param {Object} obj  new object
   * @param {Function} cb  First parameter will be an error object or null.
   */
  function handleMerge(lhead, obj, cb) {
    // move with ancestors to local, see startMerge
    that._log.debug('mt createLocalWriteStream ACK %j, lhead: %j', obj.n.h, lhead);

    // make sure this is a merge of the current local head
    if (!lhead && obj.l || lhead && !obj.l) {
      // unless one of the parents directly references the head
      if (!~obj.n.h.pa.indexOf(lhead)) {
        error = 'not a merge of lhead';
        that._log.debug('mt createLocalWriteStream error %s, obj.l: %j', error, obj.l && obj.l.h || obj.l);
        cb(new Error(error));
        return;
      }
    } else if (lhead != (obj.l && obj.l.h.v)) {
      // unless one of the parents directly references the head
      if (!~obj.n.h.pa.indexOf(lhead)) {
        error = 'lhead differs from obj.l';
        that._log.debug('mt createLocalWriteStream error %s, obj.l: %j', error, obj.l && obj.l.h || obj.l);
        cb(new Error(error));
        return;
      }
    }

    // for now, only support one lca
    if (obj.lcas.length > 1) {
      cb(new Error('can not merge with more than one lca'));
      return;
    }

    // copy all items from the remote tree to the local tree in one atomic operation to ensure one head
    var items = [];

    var firstInRemote = obj.lcas[0];
    var lastInRemote;

    if (obj.n.h.pe) { // this is a fast-forward to an item in a remote tree
      lastInRemote = obj.n.h.v;
    } else { // this is a merge between a remote and the local head
      // expect one parent to point to the local head
      if (obj.n.h.pa.length !== 2) {
        cb(new Error('merge must have exactly one remote parent and one local parent'));
        return;
      }
      if (obj.n.h.pa[0] !== lhead && obj.n.h.pa[1] !== lhead) {
        cb(new Error('merge must have the local head as one of it\'s parents'));
        return;
      }

      if (obj.n.h.pa[0] === lhead) {
        lastInRemote = obj.n.h.pa[1];
      } else {
        lastInRemote = obj.n.h.pa[0];
      }

      // put the merge at the top of the array as this can not be found in the remote tree
      items.push(obj.n);
    }

    var opts = {
      id: obj.n.h.id,
      excludeFirst: true,
      first: firstInRemote,
      last:  lastInRemote,
      reverse: true // backwards in case a merge is the new head
    };
    that._pe[obj.pe].createReadStream(opts).on('readable', function() {
      var item = this.read();
      if (item) items.unshift(item); // unshift in case a merge is the new head
    }).on('error', cb).on('end', function() {
      that._log.debug('mt createLocalWriteStream copy after %s from %s (%d)', firstInRemote, obj.pe, items.length);
      // ensure meta info of the acknowledged head is on the new head
      if (obj.n.m) {
        items[items.length - 1].m = obj.n.m;
      }
      local.write(items, cb); // atomic write of the new head and items leading to it
    });
  }

  this._localWriteStream = new Writable({
    objectMode: true,
    write: function(obj, encoding, cb) {
      // determine local head for this id
      var id = obj.n.h.id;
      var heads = [];
      local.createHeadReadStream({ id: id }).on('readable', function() {
        var head = this.read();

        if (head) {
          heads.push(head.h.v);
          return;
        }

        // end of stream

        if (heads.length > 1) {
          error = 'more than one local head';
          that._log.err('mt createLocalWriteStream %s %s %j', error, id, heads);
          cb(new Error(error));
          return;
        }

        // deletegate to merge or local version handler
        if (obj.lcas) {
          handleMerge(heads[0], obj, cb);
        } else {
          handleNewLocalItem(heads[0], obj.n, cb);
        }
      }).on('error', cb);
    }
  });
  return this._localWriteStream;
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
    this._log.err('mt %s %s', error, remote);
    throw new Error(error);
  }

  tree.lastVersion(decodeV, cb);
};

/**
 * Generate a random byte string.
 *
 * By default generates a 48 bit base64 number (string of 8 characters)
 *
 * @param {Number, default: 6} [size]  number of random bytes te generate
 * @return {String} base64 encoded version of size bytes
 */
MergeTree.generateRandomVersion = function generateRandomVersion(size) {
  return crypto.randomBytes(size || 6).toString('base64');
};

/**
 * Start merging, first clear the stage, than determine offsets for each perspective
 * finally start merge procedure (which utilizes the stage).
 *
 * @param {Object} [opts]  object containing configurable parameters for _mergeAll
 * @return {stream.Readable} a readable stream with newly merged items, of the
 * following form:
 * {
 *   n: {}   // new merged object or in case of a conflict, the remote head
 *   l: {}   // previous head
 *   lcas: [] // array with version numbers of each lca for n and l
 *   pe: pe  // name of the remote tree
 *   c: []   // name of keys with conflicts in case of a merge conflict
 * }
 *
 * opts:
 *   all tree.createReadStream options
 *   tail {Boolean, default true}  keep tailing trees to merge
 *   ready {Function}  called when all streams are started
 */
MergeTree.prototype.startMerge = function startMerge(opts) {
  opts = xtend({
    tail: true
  }, opts);
  if (opts != null && typeof opts !== 'object') { throw new TypeError('opts must be an object'); }

  if (this._mergeStream) {
    throw new Error('merge already running');
  }

  this._mergeStream = this._createMergeStream();

  var that = this;

  // first clear the stage
  this._stage.createReadStream().pipe(new Writable({
    objectMode: true,
    write: function(item, enc, cb2) {
      that._stage.del(item, cb2);
    }
  })).on('finish', function() {
    // for each perspective determine last version in the local tree and start a tailing readstream
    async.eachSeries(that._perspectives, function(pe, cb2) {
      that._local.lastByPerspective(pe, 'base64', function(err, v) {
        if (err) { cb2(err); return; }
        var opts2 = xtend(opts);
        if (v) {
          opts2 = xtend({
            first: v,
            excludeFirst: true
          }, opts2);
        }
        that._setupMerge(pe, opts2, function(err) {
          if (err) { that._log.info('%s error: %s', pe, err); }
        });
        cb2();
      });
    }, opts.ready || noop);
  });

  return this._mergeStream;
};

/**
 * Close trees and merge stream.
 *
 * @param {Function} [cb]  Called after all is closed. First parameter will be an
 *                         error object or null.
 */
MergeTree.prototype.close = function close(cb) {
  var that = this;
  var tasks = [];
  that._perspectives.forEach(function(pe) {
    tasks.push(function(cb2) {
      that._log.debug('mt closing %s tree', pe);
      that._pe[pe].end(cb2);
    });
  });
  if (this._mergeStream) {
    tasks.push(function(cb2) {
      that._log.debug('mt closing merge stream');
      that._mergeStream.end(cb2);
    });
  }

  // create some arbitrary time to let any readers timeout
  tasks.push(function(cb2) {
    that._log.debug('mt wait...');
    setTimeout(cb2, 100);
  });

  if (this._localWriteStream) {
    tasks.push(function(cb2) {
      that._log.debug('mt closing local write stream');
      that._localWriteStream.end(cb2);
    });
  }

  tasks.push(function(cb2) {
    that._log.debug('mt closing stage tree');
    that._stage.end(cb2);
  });
  tasks.push(function(cb2) {
    that._log.debug('mt closing local tree');
    that._local.end(cb2);
  });

  async.series(tasks, cb);
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

/**
 * Stop merging.
 *
 * @param {Function} cb  called if a stream ends or an error occurs
 */
MergeTree.prototype.stopMerge = function stopMerge(cb) {
  cb = cb || noop;

  if (this._mergeStream) {
    this._mergeStream.end(cb);
  } else {
    process.nextTick(cb)
  }
};



/////////////////////
//// PRIVATE API ////
/////////////////////



/**
 * Start merging a perspective.
 *
 * @param {String} name of the perspective to start merging
 * @param {Object} [opts]  tree.createReadStream options
 * @param {Function} cb  called if a stream ends or an error occurs
 */
MergeTree.prototype._setupMerge = function _setupMerge(pe, opts, cb) {
  cb = cb || noop;

  var that = this;
  var reader = that._pe[pe].createReadStream(opts)
  reader.pipe(new Transform({
    objectMode: true,
    transform: function(sitem, enc, cb2) {
      delete sitem.h.i;
      cb2(null, sitem);
    }
  })).on('finish', cb).pipe(that._mergeStream);

  // close reader if merge stream closes
  that._mergeStream.on('finish', function() {
    that._log.debug('mt _setupMerge end reader %s', pe);
    reader.end();
  });
};

/**
 * Merge items with local.
 *
 * Merge every remote item with the head in local, if any. Call back with the
 * merged result and the head from local. If there is a merge conflict call back
 * with the remote item and the local head and an array of conflicting key names.
 *
 * If the item is already in local, don't call back, just update the last version
 * by perspective in local.
 */
MergeTree.prototype._createMergeStream = function _createMergeStream() {
  var local = this._local;
  var that  = this;
  var error;

  var transformer = new Transform({
    objectMode: true,
    transform: function(ritem, enc, cb) {
      var rtree = that._pe[ritem.h.pe];
      if (!rtree) {
        error = new Error('remote tree could not be determined');
        that._log.err('mt _createMergeStream %s %j', error, ritem.h);
        cb(error);
        return;
      }

      // clone object to prevent side effects
      ritem = xtend(ritem);
      ritem.h = xtend(ritem.h);

      that._log.debug2('mt _createMergeStream %s', ritem.h.v);

      // merge with the head in local, if any
      var lheads = [];
      var headOpts = {
        id: ritem.h.id
      };
      local.createHeadReadStream(headOpts).pipe(new Writable({
        objectMode: true,
        write: function(lhead, enc, cb2) {
          lheads.push(lhead);
          cb2();
        }
      })).on('finish', function() {
        if (lheads.length > 1) {
          error = new Error('more than one head in local');
          that._log.err('mt _createMergeStream error %s %j', error, lheads);
          cb(error);
          return;
        }

        if (!lheads.length) {
          that._log.debug('mt _createMergeStream fast-forward %j', ritem.h);

          // new item not in local yet, fast-forward
          cb(null, {
            n: ritem,
            l: null,
            lcas: [],
            pe: rtree.name,
            c: null
          });
          return;
        }

        var lhead = lheads[0];

        that._log.debug('mt _createMergeStream merge %j with %j', ritem.h, lhead.h);

        // merge remote item with local head
        var sX = rtree.createReadStream({ id: ritem.h.id, last: ritem.h.v, reverse: true });
        var sY = local.createReadStream({ id: lhead.h.id, last: lhead.h.v, reverse: true });

        merge(sX, sY, function(err, rmerge, lmerge, lcas) {
          if (err) {
            if (err.name !== 'MergeConflict') {
              that._log.err('mt _createMergeStream merge error %s %j %j', err, ritem, lhead, err.stack);
              cb(err);
              return;
            }

            // merge conflict
            that._log.notice('mt _createMergeStream merge conflict %s %j %j', err.conflict, ritem, lhead);
            cb(null, {
              n: ritem,
              l: lhead,
              lcas: lcas,
              pe: rtree.name,
              c: err.conflict
            });
            return;
          }

          // this item either exists in both trees, in only one of the trees (fast-forward) or in none of the trees (genuine merge)
          // if a fast-forward for rtree, update last by perspective in local
          if (!lmerge.h.v) {
            // merge

            // create a version based on content
            rmerge.h.pa.sort();
            rmerge.h.v = MergeTree._versionContent(rmerge); // merkle-tree

            that._log.info('mt _createMergeStream merge %j', rmerge.h);

            cb(null, {
              n: rmerge,
              l: lhead,
              lcas: lcas,
              pe: rtree.name,
              c: null
            });
          } else if (!lmerge.h.i) {
            // merge by fast-forward
            that._log.info('mt _createMergeStream fast-forward %j', rmerge.h);
            cb(null, {
              n: ritem,
              l: lhead,
              lcas: lcas,
              pe: rtree.name,
              c: null
            });
          } else {
            // version already in local tree (has both h.v and h.i)
            that._log.debug('mt _createMergeStream set last by %s = %s', ritem.h.pe, ritem.h.v);
            local.write(ritem, cb);
          }
        });
      }).on('error', cb);
    }
  });
  return transformer;
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
