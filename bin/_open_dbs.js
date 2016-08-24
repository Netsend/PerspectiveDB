/**
 * Copyright 2016 Netsend.
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

'use strict';

var async = require('async');
var xtend = require('xtend');

var openDb = require('./_open_db');

/**
 * Open one or more databases.
 *
 * @param {Object} config  database configuration, should contain a dbs key
 * @param {Object} [opts]  options
 * @param {Function} iterator  signature: db, dbCfg, next
 * @param {Function} cb  called when all dbs are iterated
 *
 * Options:
 *  - keepOpen {Boolean, default: false}  don't close the database
 */
function openDbs(config, opts, iterator, cb) {
  if (config == null || typeof config !== 'object') { throw new TypeError('config must be an object'); }
  if (typeof opts === 'function') {
    cb = iterator;
    iterator = opts;
    opts = {}
  }
  opts = opts || {};
  if (opts == null || typeof config !== 'object') { throw new TypeError('config must be an object'); }
  if (typeof iterator !== 'function') { throw new TypeError('config must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be an object'); }

  if (!Array.isArray(config.dbs)) { throw new Error('expected dbs to be an array'); }

  opts = xtend({
    keepOpen: false
  }, opts);

  async.eachSeries(config.dbs, function(dbCfg, cb2) {
    dbCfg.dbroot = config.dbroot;

    // map perspective names
    if (dbCfg.perspectives) {
      dbCfg.perspectives = dbCfg.perspectives.map(function(peCfg) {
        return peCfg.name;
      });
    }
    // open database
    var db = openDb(dbCfg);
    iterator(db, dbCfg, function(err) {
      if (!opts.keepOpen) {
        db.close();
      }
      cb2(err);
    });
  }, cb);
}
module.exports = openDbs;
