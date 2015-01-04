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

var _db = require('./_db');

var program = require('commander');
var VersionedSystem = require('../lib/versioned_system');

program
  .version(require('../package.json').version)
  .description('copy versioned collections to another database')
  .option('-v, --verbose', 'verbose')
  .option('-c, --config <file>', 'configuration file to use')
  .option('-d, --database <database>', 'database to copy to, defaults to "dump"')
  .parse(process.argv);

// get config path
var config;
if (program.config) {
  config = process.cwd() + program.config;
} else {
  config = '../config/development.json';
}

if (!program.database) { program.database = 'dump'; }

var c = require(config);

function start(db) {
  var opts = { debug: program.verbose, autoProcessInterval: 0 };
  var vs = new VersionedSystem(db, c.databases, c.collections, opts);
  vs.dump(program.database, function(err) {
    if (err) {
      console.error(err);
      process.exit(1);
    }
    db.close();
  });
}

// open database
_db(c, function(err, db) {
  if (err) { throw err; }
  start(db);
});
