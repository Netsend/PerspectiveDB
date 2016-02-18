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

var mongodb = require('mongodb');

/**
 * Open a database connection using the provided configuration.
 *
 * @param {Object} config  database configuration containing dbName, dbHost and 
 *                         dbPort.
 * @param {Function} cb  first parameter is an error object or null, second
 *                       parameter is the database connection.
 *
 * config:
 *  dbName
 *  dbHost, defaults to 127.0.0.1
 *  dbPort, defaults to 27017
 *  dbUser
 *  dbPass
 *  authDb
 */
module.exports = function(config, cb) {
  if (typeof config !== 'object') { throw new TypeError('config must be an object'); }
  if (typeof config.dbName !== 'string') { throw new TypeError('config.dbName must be a string'); }

  config.dbHost = config.dbHost || '127.0.0.1';
  if (typeof config.dbHost !== 'string') { throw new TypeError('config.dbHost must be a string'); }

  config.dbPort = config.dbPort || 27017;
  if (typeof config.dbPort !== 'number') { throw new TypeError('config.dbPort must be a number'); }

  var db = new mongodb.Db(config.dbName, new mongodb.Server(config.dbHost, config.dbPort), { w: 1 });

  db.open(function(err) {
    if (err) { return cb(err); }

    if (config.dbUser || config.dbPass) {
      var authDb = db.db(config.authDb || config.dbName);
      authDb.authenticate(config.dbUser, config.dbPass, function(err) {
        cb(err, db);
      });
    } else {
      cb(null, db);
    }
  });
};
