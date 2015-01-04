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
var properties = require('properties');
var fs = require('fs');

program
  .version('0.0.1')
  .usage('-c collection -f config <hook.js>')
  .description('run a hook on a collection')
  .option('-d, --database <database>', 'name of the database')
  .option('-c, --collection <collection>', 'name of the collection')
  .option('-f, --config <config>', 'an ini config file')
  .option('    <hook.js>', 'path to the hook to use')
  .parse(process.argv);

var hook = program.args[0];

if (!program.config) { program.help(); }
if (!program.collection) { program.help(); }
if (!hook) { program.help(); }

var config = program.config

// if relative, prepend current working dir
if (config[0] !== '/') {
    config = process.cwd() + '/' + config;
}

config = properties.parse(fs.readFileSync(config, { encoding: 'utf8' }), { sections: true, namespaces: true });

// if relative, prepend current working dir
if (hook[0] !== '/') {
  hook = process.cwd() + '/' + hook;
}

hook = require(hook);

function start(db) {
  var coll = db.db(config.database.name || 'local').collection(program.collection);

  // start at the end
  var stream = coll.find().stream();

  stream.on('data', function(item) {
    hook(db, {}, item, {}, function(err, keep) {
      if (err) {
        console.error(err);
        process.exit(1);
      }
      if (keep) {
        console.log(JSON.stringify(item));
      }
    });
  });

  stream.on('error', function(err) {
    console.error(err);
    process.exit(1);
  });

  stream.on('close', function() {
    process.exit();
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
  start(db);
});
