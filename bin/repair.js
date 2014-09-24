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

var _db = require('./_db');
var VersionedCollection = require('../lib/versioned_collection');

program
  .version('0.0.1')
  .usage('[-vn] -d database -c collection config.json')
  .description('Make sure all items in the collection are in the snapshot as the latest version. Run multi_head first!')
  .option('-d, --database <database>', 'name of the database')
  .option('-c, --collection <collection>', 'name of the collection to repair')
  .option('-v, --verbose', 'verbose')
  .parse(process.argv);

if (!program.args[0]) {
  program.help();
}

var config = program.args[0];

// if relative, prepend current working dir
if (config[0] !== '/') {
  config = process.cwd() + '/' + config;
}

config = require(config);

config.dbName = program.database || config.dbName;
config.collection = program.collection || config.collection;

if (!config.dbName) { program.help(); }
if (!config.collection) { program.help(); }

var debug = !!program.verbose;

function run(db) {
  var vc = new VersionedCollection(db, config.collection, { debug: debug });

  vc.copyCollectionOverSnapshot(function(err) {
    if (err) {
      console.error('error:', err.message);
      process.exit(1);
    }

    db.close();
  });
}

// open database
_db(config, function(err, db) {
  if (err) { throw err; }
  run(db);
});
