/**
 * Copyright 2016 Netsend.
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

'use strict';

var async = require('async');
var level = require('level-packager')(require('leveldown'));

// config should contain a dbs key
// iterator will be called with db, dbCfg and next for each db
function openDbs(config, iterator, cb) {
  if (config.dbs && config.dbs.length) {
    async.eachSeries(config.dbs, function(dbCfg, cb2) {
      var chroot = dbCfg.chroot || '/var/persdb';
      var data = dbCfg.data || 'data';

      if (dbCfg.perspectives) {
        dbCfg.perspectives = dbCfg.perspectives.map(function(peCfg) {
          return peCfg.name;
        });
      }
      // open database
      level(chroot + '/' + data, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, db) {
        if (err) { cb2(err); return; }
        iterator(db, dbCfg, function(err) {
          db.close();
          cb2(err);
        });
      });
    }, cb);
  } else {
    // open database
    var newRoot = config.chroot || '/var/persdb';

    var path = config.path || '/data';
    // ensure leading slash
    if (path[0] !== '/') {
      path = '/' + path;
    }

    console.log(newRoot, path);

    level(newRoot + path, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, db) {
      if (err) { cb(err); return; }
      iterator(db, {}, function(err) {
        db.close();
        cb(err);
      });
    });
  }
}
module.exports = openDbs;
