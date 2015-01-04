#!/usr/bin/env node

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

var program = require('commander');
var Timestamp = require('mongodb').Timestamp;
var properties = require('properties');
var fs = require('fs');

var _db = require('./_db');

program
  .version('0.0.1')
  .usage('[-r] -f config [database.collection]')
  .description('set last used oplog item global or for a collection')
  .option('-r, --remove', 'remove the timestamp')
  .option('-f, --config  <config>', 'an ini config file')
  .option('-v, --verbose', 'verbose')
  .parse(process.argv);

if (!program.config) { program.help(); }

var config = program.config

// if relative, prepend current working dir
if (config[0] !== '/') {
  config = process.cwd() + '/' + config;
}

config = properties.parse(fs.readFileSync(config, { encoding: 'utf8' }), { sections: true, namespaces: true });

var id = program.args[0] || 'lastUsedOplogItem';

// update last used oplog item
function run(db) {
  var localDb = db.db(config.database.name || 'local');
  if (program.remove) {
    localDb.collection('m3').remove({ _id: id }, function(err, removed) {
      if (err) {
        console.error('error:', err.message);
        process.exit(1);
      }
      if (removed < 1) {
        console.log('item not found', id);
        process.exit(0);
      }
      if (program.verbose) { console.log('removed', id, removed); }
      localDb.close();
      db.close();
    });
  } else {
    var ts = new Timestamp(0, (new Date()).getTime() / 1000);
    localDb.collection('m3').update({ _id: id }, { $set: { ts: ts } }, { upsert: true }, function(err, updated) {
      if (err) {
        console.error('error:', err.message);
        process.exit(1);
      }
      if (updated !== 1) {
        console.error('error: while setting timestamp');
        process.exit(1);
      }
      if (program.verbose) { console.log('set', id, ts); }
      localDb.close();
      db.close();
    });
  }
}

var database = config.database;
var dbCfg = {
  dbName: database.name || 'local',
  dbHost: database.path || database.host,
  dbPort: database.port,
  dbUser: database.username,
  dbPass: database.password,
  adminDb: database.adminDb
};

// open database
_db(dbCfg, function(err, db) {
  if (err) { throw err; }
  run(db);
});
