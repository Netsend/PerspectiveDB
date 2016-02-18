/**
 * Copyright 2014-2016 Netsend.
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

/**
 * oplogReader
 *
 * Read oplog, scoped to a certain namespace.
 *
 * @param {Object} oplogColl  oplog collection
 * @param {String} ns  namespace to read
 * @param {Object} [opts]  object containing optional parameters
 *
 * opts:
 *   filter {Object}  extra filter to apply apart from namespace
 *   offset {Object}  mongodb.Timestamp to start at
 *   includeOffset {Boolean, default false}  whether to include or exclude offset
 *   tailable {Boolean, default false}  whether or not to keep the cursor open and follow the oplog
 *   tailableRetryInterval {Number, default 1000}  set tailableRetryInterval
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 */
module.exports = function oplogReader(oplogColl, ns, opts) {
  /* jshint maxcomplexity: 23 */ /* lots of parameter type checking */

  if (oplogColl == null || typeof oplogColl !== 'object') { throw new TypeError('oplogColl must be an object'); }
  if (typeof ns !== 'string') { throw new TypeError('ns must be a string'); }

  var nsParts = ns.split('.');
  if (nsParts.length < 2) { throw new TypeError('ns must contain at least two parts'); }
  if (!nsParts[0].length) { throw new TypeError('ns must contain a database name'); }
  if (!nsParts[1].length) { throw new TypeError('ns must contain a collection name'); }

  if (opts == null) opts = {};
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (opts.filter != null && typeof opts.filter !== 'object') { throw new TypeError('opts.filter must be an object'); }
  if (opts.offset != null && typeof opts.offset !== 'object') { throw new TypeError('opts.offset must be an object'); }
  if (opts.includeOffset != null && typeof opts.includeOffset !== 'boolean') { throw new TypeError('opts.includeOffset must be a boolean'); }
  if (opts.tailable != null && typeof opts.tailable !== 'boolean') { throw new TypeError('opts.tailable must be a boolean'); }
  if (opts.tailableRetryInterval != null && typeof opts.tailableRetryInterval !== 'number') { throw new TypeError('opts.tailableRetryInterval must be a number'); }
  if (opts.log != null && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }

  var log = opts.log || {
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

  // setup CursorStream
  var selector = { ns: ns };
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
  if (opts.tailable) { mongoOpts.tailable = true; }
  mongoOpts.tailableRetryInterval = opts.tailableRetryInterval || 1000;

  log.info('or offset: %s %s, selector: %j, opts: %j', opts.offset, opts.includeOffset ? 'include' : 'exclude', selector, mongoOpts);

  return oplogColl.find(selector, mongoOpts);
};
