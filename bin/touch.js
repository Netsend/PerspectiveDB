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
  .usage('[-f config] database.collection [timestamp]')
  .description('set last oplog item or given timestamp on last item in given collection')
  .option('-f, --config <config>', 'ini config file with database access credentials')
  .option('-v, --verbose', 'verbose')
  .parse(process.argv);

var ns = program.args[0];
if (!ns) {
  program.help();
}

var parts = ns.split('.');
if (parts.length < 2) {
  program.help();
}

var dbName = parts[0];
var collName = 'm3.' + parts.slice(1).join('.');

var config = { database: { dbName: dbName } };
var dbCfg = { dbName: dbName };

// if relative, prepend current working dir
if (program.config) {
  config = program.config;
  if (config[0] !== '/') {
    config = process.cwd() + '/' + config;
  }

  config = properties.parse(fs.readFileSync(config, { encoding: 'utf8' }), { sections: true, namespaces: true });

  if (config.database) {
    dbCfg = {
      dbName: dbName,
      dbHost: config.database.path || config.database.host,
      dbPort: config.database.port,
      dbUser: config.database.user,
      dbPass: config.database.pass,
      authDb: config.database.authDb
    };
  }
}

// update last used oplog item
function run(db) {
  function setOp(ts) {
    var coll = db.collection(collName);
    var update = { $set: { '_m3._op': ts } };
    // fetch last id
    coll.findOne({}, { sort: { $natural: -1 } }, function(err, lastItem) {
      if (err) { throw err; }
      if (!lastItem) {
        console.log('no item in collection: %s', dbName, collName);
        process.exit(1);
        return;
      }

      db.collection(collName).update({ _id: lastItem._id }, update, function(err, updated) {
        if (err) {
          console.error('error', err, ts);
          process.exit(1);
        }
        if (updated !== 1) {
          console.error('error: while setting timestamp');
          process.exit(1);
        }
        console.log(dbName, collName, 'set', ts);
        db.close();
      });
    });
  }

  var ts = program.args[1];
  if (ts) {
    ts = new Timestamp(ts);
    setOp(ts);
  } else {
    var oplogColl = db.db(config.database.oplogDb || 'local').collection(config.database.oplogCollection || 'oplog.$main');
    oplogColl.findOne({}, { sort: { $natural: -1 } }, function(err, item) {
      setOp(item.ts);
    });
  }
}

// open database
_db(dbCfg, function(err, db) {
  if (err) { throw err; }
  run(db);
});
