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

/**
 * RemoteTransform
 *
 * Set remote on _id._pe of each incoming object, unless it contains an error key
 * only.
 *
 * Note: only supports object mode
 *
 * @param {String} remote  name of the remote to set on each incoming item
 * @param {Object} [opts] object containing optional parameters
 *
 * opts:
 * debug {Boolean, default false} whether to do extra console logging or not
 * hide {Boolean, default false} whether to suppress errors or not (used in tests)
 */
function RemoteTransform(remote, opts) {
  if (typeof remote !== 'string') { throw new TypeError('remote must be a string'); }
  if (typeof opts !== 'undefined' && typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  opts = opts || {};

  if (typeof opts.debug !== 'undefined' && typeof opts.debug !== 'boolean') { throw new TypeError('opts.debug must be a boolean'); }
  if (typeof opts.hide !== 'undefined' && typeof opts.hide !== 'boolean') { throw new TypeError('opts.hide must be a boolean'); }

  opts.objectMode = true;
  Transform.call(this, opts);

  this._remote = remote;
  this._debug = opts.debug || false;
  this._hide = !!opts.hide;
}
util.inherits(RemoteTransform, Transform);

module.exports = RemoteTransform;

RemoteTransform.prototype._transform = function _transform(obj, encoding, cb) {
  if (this._debug) { console.log('_transform', obj); }

  try {
    if (obj.error) {
      if (Object.keys(obj).length === 1) {
        throw new Error(obj.error);
      }
    }

    obj._id._pe = this._remote;
    // push the obj out to the reader
    this.push(obj);
    cb();
  } catch(err) {
    if (!this._hide) { console.error(err, obj); }
    cb(err);
  }
};
