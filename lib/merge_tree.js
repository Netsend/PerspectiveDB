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

'use strict';

var Transform = require('stream').Transform;
var util = require('util');

var async = require('async');
var BSON = require('bson').BSONPure.BSON;

var Tree = require('tree');

var noop = function() {};

/**
 * MergeTree
 *
 * Accept objects from different perspectives. Merge other perspectives into the
 * local perspective.
 *
 * @param {LevelUP.db} db  database for persistent storage
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   localPerspective {String, default "_local"}  name of the local tree, should
 *                                                not exceed 255 bytes
 *   perspectives {Array}  Names of different sources that should be merged. A
 *                         name should not exceed 255 bytes.
 *   vSize {Number, default 6}  number of bytes used for the version. Should be:
 *                              0 < vSize <= 6
 *   iSize {Number, default 6}  number of bytes used for i. Should be:
 *                              0 < iSize <= 6
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 */
function MergeTree(db, opts) {
  if (typeof db !== 'object' || db === null) { throw new TypeError('db must be an object'); }

  opts = opts || {};
  if (typeof opts !== 'object' || opts === null || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }
  if (typeof opts.localPerspective !== 'undefined' && typeof opts.localPerspective !== 'string') { throw new TypeError('opts.localPerspective must be a string'); }
  if (typeof opts.perspectives !== 'undefined' && !Array.isArray(opts.perspectives)) { throw new TypeError('opts.perspectives must be an array'); }
  if (typeof opts.log !== 'undefined' && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }
  if (typeof opts.vSize !== 'undefined' && typeof opts.vSize !== 'number') { throw new TypeError('opts.vSize must be a number'); }
  if (typeof opts.iSize !== 'undefined' && typeof opts.iSize !== 'number') { throw new TypeError('opts.iSize must be a number'); }

  opts.objectMode = true;

  this._localPerspective = opts.localPerspective || '_local';
  this._perspectives = opts.perspectives || [];

  if (Buffer.byteLength(this._localPerspective) > 255) { throw new TypeError('opts.localPerspective must not exceed 255 bytes'); }

  this._perspectives.forEach(function(perspective) {
    if (Buffer.byteLength(perspective) > 255) { throw new TypeError('each perspective name must not exceed 255 bytes'); }
  });

  this._vSize = opts.vSize || 6;
  this._iSize = opts.iSize || 6;

  if (this._vSize < 0 || this._vSize > 6) { throw new TypeError('opts.vSize must be between 0 and 6'); }
  if (this._iSize < 0 || this._iSize > 6) { throw new TypeError('opts.iSize must be between 0 and 6'); }

  Transform.call(this, opts);

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
  this._trees = {};

  var that = this;

  this._perspectives.forEach(function(perspective) {
    that._trees[perspective] = new Tree(db, opts);
  });
}

util.inherits(MergeTree, Transform);
module.exports = MergeTree;

/**
 * Save a new version of a certain perspective in the appropriate Tree.
 *
 * Implementation of _transform method of Transform stream. This method is not
 * called directly. New items should have the following structure:
 * {
 *   id: {mixed} id of this item
 *   pe: {String} perspective
 *   [op]: {String} "i", "u", "ui", "d"
 *   [pa]: {Array} parent versions
 *   [val]: {mixed} value to save
 *   [meta]: {mixed} extra info to store with this value
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
MergeTree.prototype._transform = function(item, encoding, cb) {
  if (typeof item !== 'object') { throw new TypeError('item must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (typeof item.id === 'undefined' || item === null) { throw new TypeError('item.id must be defined'); }
  if (typeof item.pe !== 'string') { throw new TypeError('item.pe must be a string'); }

  var error;
  var that = this;

  if (!this._trees[item.pe]) {
    error = 'perspective not found';
    process.nextTick(function() {
      that._log.err('merge_tree %s %s', error, item.pe);
      cb(new Error(error));
    });
    return;
  }

  /**
   * If this is not the local tree, nothing special needs to be done. If this is
   * the local tree, determine parents and try to merge the item (fast forward
   * or real merge).
   */
  if (item.pe !== this._localPerspective) {
    this._trees[item.pe].write(item, encoding, cb);
    return;
  }

  // check if this is an ack or a new version 
};
