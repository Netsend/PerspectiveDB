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
var properties = require('properties');
var fs = require('fs');

var syncAttr = require('../lib/sync_attr');

program
  .version('0.0.1')
  .usage('[-v] -a database [-b database] -c collection [-d collection] -f config attr')
  .description('synchronize attr on items in a.c from corresponding items in b.d')
  .option('-a, --database1 <database>', 'name of the database for collection1')
  .option('-b, --database2 <database>', 'name of the database for collection2 if different from database1')
  .option('-c, --collection1 <collection>', 'name of the collection to report about')
  .option('-d, --collection2 <collection>', 'name of the collection to compare against if different from collection1')
  .option('-f, --config <config>', 'an ini config file')
  .option('-m, --match <attrs>', 'comma separated list of attributes to should match', function(val) { return val.split(','); })
  .option('-i, --include <attrs>', 'comma separated list of attributes to include in comparison', function(val) { return val.split(','); })
  .option('-e, --exclude <attrs>', 'comma separated list of attributes to exclude in comparison', function(val) { return val.split(','); })
  .option('-v, --verbose', 'verbose')
  .parse(process.argv);


console.log(program.config);
// get config path from environment
if (!program.config) {
  program.help();
}

var config = program.config;

// if relative, prepend current working dir
if (config[0] !== '/') {
  config = process.cwd() + '/' + config;
}

config = properties.parse(fs.readFileSync(config, { encoding: 'utf8' }), { sections: true, namespaces: true });

if (!program.database1) { program.help(); }
if (!program.database2) { program.database2 = program.database1; }
if (!program.collection1) { program.help(); }
if (!program.collection2) { program.collection2 = program.collection1; }

if (!program.args[0]) { program.help(); }

var attr = program.args[0];

var excludeAttrs = {};
(program.exclude || []).forEach(function(attr) {
  excludeAttrs[attr] = true;
});

var includeAttrs = {};
(program.include || []).forEach(function(attr) {
  includeAttrs[attr] = true;
});

var matchAttrs = {};
(program.match || []).forEach(function(attr) {
  matchAttrs[attr] = true;
});

var debug = !!program.verbose;

if (debug && Object.keys(includeAttrs).length) { console.log('include:', program.include); }
if (debug && Object.keys(excludeAttrs).length) { console.log('exclude:', program.exclude); }
if (debug && Object.keys(matchAttrs).length) { console.log('match:', program.match); }

//// phase 1: sync version numbers on equal objects if the rest of the object has equal attribute values
//// phase 2: treat all items in collection 2 with equal ids and different objects as parents of collection 1 
function run(db) {
  var coll1 = db.db(program.database1).collection(program.collection1);
  var coll2 = db.db(program.database2).collection(program.collection2);
  var tmpColl = db.db(program.database1).collection(program.collection1 + '.tmp');

  var opts = {
    includeAttrs: includeAttrs,
    excludeAttrs: excludeAttrs,
    matchAttrs: matchAttrs,
    debug: debug
  };

  syncAttr(coll1, coll2, tmpColl, attr, opts, function(err, updated) {
    if (err) { throw err; }
    console.log('updated', updated);
    db.close();
  });
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
