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

/* jshint -W116 */

'use strict';

var Readable = require('stream').Readable;
var util = require('util');

var async = require('async');
var bson = require('bson');
var BSON = new bson.BSONPure.BSON();
var match = require('match-object');

var runHooks = require('./run_hooks');

var noop = function() {};

/**
 * StreamMergeTree
 *
 * Create readable streams on a merge tree.
 *
 * @param {Object} mt  merge tree
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   raw {Boolean, default false}  whether to return a BSON serialized or
 *                                 deserialezed object (false).
 *   filter {Object}  conditions a document should hold
 *   first {base64 String}  first version, offset
 *   excludeFirst {Boolean, default true}  whether or not first should be
 *                                          excluded
 *   follow {Boolean, default: true}  whether to keep the tail open or not
 *   hooks {Array}  array of asynchronous functions to execute, each hook has the following signature: db, object, options,
 *                  callback and should callback with an error object, the new item and possibly extra data.
 *   hooksOpts {Object}  options to pass to a hook
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 *
 * @event "data" {Object}  emits one object at a time
 * @event "end"  emitted once the underlying cursor is closed
 *
 * @class represents a StreamMergeTree for a certain database.collection
 */
function StreamMergeTree(mt, opts) {
  if (typeof mt !== 'object') { throw new TypeError('mt must be an object'); }

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }
  if (opts.raw != null && typeof opts.raw !== 'boolean') { throw new TypeError('opts.raw must be a boolean'); }
  if (opts.filter != null && typeof opts.filter !== 'object') { throw new TypeError('opts.filter must be an object'); }
  if (opts.first != null) {
    if (typeof opts.first !== 'number' && typeof opts.first !== 'string') { throw new TypeError('opts.first must be a base64 string or a number'); }
    if (typeof opts.first === 'string' && Buffer.byteLength(opts.first, 'base64') !== mt._vSize) { throw new Error('opts.first must be the same size as the configured vSize'); }
  }
  if (opts.excludeFirst != null && typeof opts.excludeFirst !== 'boolean') { throw new TypeError('opts.excludeFirst must be a boolean'); }
  if (opts.follow != null && typeof opts.follow !== 'boolean') { throw new TypeError('opts.follow must be a boolean'); }
  if (opts.hooks != null && !Array.isArray(opts.hooks)) { throw new TypeError('opts.hooks must be an array'); }
  if (opts.hooksOpts != null && typeof opts.hooksOpts !== 'object') { throw new TypeError('opts.hooksOpts must be an object'); }
  if (opts.log != null && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }

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

  this._mt = mt;
  this._opts = opts;

  this._db = mt._db;

  var filter = opts.filter || {};
  var hooks = opts.hooks || [];

  if (typeof opts.follow === 'boolean') {
    this._follow = opts.follow;
  } else {
    this._follow = true;
  }

  Readable.call(this, { objectMode: !opts.raw });

  var sOpts = {
    first: opts.first,
    excludeFirst: opts.excludeFirst
  };
  this._log.notice('smt sOpts %j, filter %j', sOpts, opts.filter);
  this._source = mt._local.createReadStream(sOpts);

  var that = this;

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
    that._log.debug('smt findConnectedParents %s', item.h.pa);
    var parents = {};
    async.eachSeries(item.h.pa, function(pav, cb2) {
      mt._local.getByVersion(pav, function(err, pa) {
        if (err) { cb2(err); return; }

        // descend if not all criteria hold on this item
        if (!match(crit, pa.b)) {
          that._log.debug('smt findConnectedParents crit does not hold, descend');
          findConnectedParents(pa, crit, function(err, nParents) {
            if (err) { cb2(err); return; }
            Object.keys(nParents).forEach(function(pa2) {
              parents[pa2] = true;
            });
            cb2();
          });
        } else {
          runHooks(hooks, that._db, pa, opts.hooksOpts, function(err, afterItem) {
            if (err) { cb2(err); return; }

            // descend if hooks filter out the item
            if (afterItem) {
              that._log.debug('smt findConnectedParents add to parents %s', pa.h.v);
              parents[pa.h.v] = true;
              cb2();
            } else {
              that._log.debug('smt findConnectedParents crit holds, but hooks filter, descend');
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

  var str = that._source;
  var endEmitted, handlingData;

  function handleData(item) {
    str.pause();
    handlingData = true;

    that._log.debug('smt data %j', item.h);

    // don't emit if not all criteria hold on this item
    if (!match(filter, item.b)) {
      that._log.info('smt filter %j', filter);
      handlingData = false;
      str.resume();
      if (endEmitted) {
        that.push(null);
      }
      return;
    }

    runHooks(hooks, that._db, item, opts.hooksOpts, function(err, afterItem) {
      if (err) { that.emit(err); return; }

      // skip if hooks filter out the item
      if (!afterItem) {
        that._log.info('smt hook filtered %j', item.h);
        handlingData = false;
        str.resume();
        if (endEmitted) {
          that.push(null);
        }
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

        // push the raw or parsed item out to the reader, and resume if not flooded
        that._log.info('smt push %j', afterItem.h);
        var proceed = that.push(opts.raw ? BSON.serialize(afterItem) : afterItem);
        handlingData = false;
        if (proceed) { str.resume(); }
        if (endEmitted) {
          that.push(null);
        }
      });
    });
  }

  this._source.on('data', handleData);

  // proxy error
  this._source.on('error', function(err) {
    that._log.crit('smt stream error %s', err);
    that.emit('error', err);
  });

  this._source.on('end', function() {
    that._log.notice('smt stream ended');
    endEmitted = true;
    if (!handlingData) {
      that.push(null);
    }
  });
}
util.inherits(StreamMergeTree, Readable);

module.exports = StreamMergeTree;

// return a new stream with the same parameters
StreamMergeTree.prototype.reopen = function() {
  return new StreamMergeTree(this._mt, this._opts);
};



/////////////////////
//// PRIVATE API ////
/////////////////////



/**
 * Implementation of _read method of Readable stream. This method is not called
 * directly. In this implementation size of read buffer is ignored
 */
StreamMergeTree.prototype._read = function() {
  this._source.resume();
};
