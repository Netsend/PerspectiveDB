/**
 * Copyright 2014 Netsend.
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

var util = require('util');
var Transform = require('stream').Transform;

var runHooks = require('./run_hooks');

/**
 * RemoteTransform
 *
 * Set remote on h.pe of each incoming object, unless it contains an error key
 * only.
 *
 * Note: only supports object mode
 *
 * @param {String} remote  name of the remote to set on each incoming item
 * @param {Object} [opts] object containing optional parameters
 *
 * opts:
 *   db {Object}  mongodb database connection that is passed to each hook
 *   hooks {Array}  array of asynchronous functions to execute, each hook has the
 *                  following signature: db, object, options, callback and should
 *                  callback with an error object, the new item, or no item to
 *                  filter it out
 *   hooksOpts {Object}  options to pass to each hook
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 */
function RemoteTransform(remote, opts) {
  if (typeof remote !== 'string') { throw new TypeError('remote must be a string'); }

  opts = opts || {};
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (typeof opts.db !== 'undefined' && typeof opts.db !== 'object') { throw new TypeError('opts.db must be an object'); }
  if (typeof opts.hooks !== 'undefined' && !Array.isArray(opts.hooks)) { throw new TypeError('opts.hooks must be an array'); }
  if (typeof opts.hooksOpts !== 'undefined' && typeof opts.hooksOpts !== 'object') { throw new TypeError('opts.hooksOpts must be an object'); }
  if (typeof opts.log !== 'undefined' && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }

  opts.objectMode = true;
  Transform.call(this, opts);

  this._log = opts.log || {
    emerg:   console.error,
    alert:   console.error,
    crit:    console.error,
    err:     console.error,
    warning: console.log,
    notice:  console.log,
    info:    console.log,
    debug:   console.log,
    debug2:  console.log
  };

  this._remote = remote;

  this._db = opts.db;
  this._hooks = opts.hooks || [];
  this._hooksOpts = opts.hooksOpts || {};
}
util.inherits(RemoteTransform, Transform);

module.exports = RemoteTransform;

RemoteTransform.prototype._transform = function _transform(obj, encoding, cb) {
  // heavy debug when the object contains binary blobs
  this._log.debug2('rt %s: _transform before %j', this._remote, obj);

  var that = this;

  try {
    if (obj.error) {
      if (Object.keys(obj).length === 1) {
        throw new Error(obj.error);
      }
    }

    obj.h.pe = this._remote;
    delete obj.h.i;

    runHooks(that._hooks, that._db, obj, that._hooksOpts, function(err, afterItem) {
      if (err) { cb(err); return; }

      if (afterItem) {
        // push the obj out to the reader
        that._log.info('rt %s: _transform after %j', that._remote, afterItem);
        that.push(afterItem);
      } else {
        // if hooks filter out the object don't push
        that._log.debug('rt %s: _transform hook filtered %j', that._remote, obj.h);
      }
      cb();
    });
  } catch(err) {
    this._log.err(err, obj);
    cb(err);
  }
};
