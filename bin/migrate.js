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

var util = require('util');

var async = require('async');
var commander = require('commander');

var _db = require('./_db');

commander
  .version('0.0.2')
  .usage('[-v] script dbconfig [ dbconfig2 ... ]')
  .description('run custom javascript code on the given databases')
  .option('  script', 'Filename of a migration that exports a function named run')
  .option('        ', 'A MongoDB connection is passed as the first parameter. Second parameter is the casing monitor config.')
  .option('        ', 'Make sure you run db.close when done.')
  .option('  dbconfig', 'list of database config files')
  .option('-v, --verbose', 'verbose')
  .parse(process.argv);

if (!commander.args[0]) {
  commander.help();
}

var script = commander.args[0];
if (script[0] !== '/') {
  script = process.cwd()+'/'+script;
}

var dbConfigs;
if (commander.args[1]) {
  dbConfigs = commander.args.slice(1).map(function(entry) {
    // if relative, prepend current working dir
    if (entry[0] !== '/') {
      return process.cwd()+'/'+entry;
    }
    return entry;
  });
}

if (!dbConfigs.length) {
  util.error('No dbs found or given');
  process.exit(0);
}

var migration = require(script);

async.eachLimit(dbConfigs, 10, function(dbConfig, cb) {
  util.puts('reading '+ dbConfig);
  var config = require(dbConfig);

  // open database connection
  _db(config, function(err, db) {
    if (err) { throw err; }

    migration.run(db, config);

    db.on('close', cb);
  });
}, function(err) {
  if (err) { console.error(err); }
  console.log('done');
});
