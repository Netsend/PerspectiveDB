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
  .version('0.1.0')
  .usage('[-g] -c collection -f config')
  .description('tail the given collection')
  .option('-d, --database <database>', 'name of the database')
  .option('-c, --collection <collection>', 'name of the collection')
  .option('-f, --config <config>', 'an ini config file')
  .option('-o, --oplog', 'shortcut for "-d local -c oplog.$main"')
  .option('-n, --number <number>', 'the number of last items to show, defaults to 10')
  .option('-g, --follow', 'keep the cursor open (only on capped collections)')
  .parse(process.argv);

if (!program.config) { program.help(); }
if (!program.collection) { program.help(); }

var config = program.config;
var collection = program.collection;

// if relative, prepend current working dir
if (config[0] !== '/') {
  config = process.cwd() + '/' + config;
}

config = properties.parse(fs.readFileSync(config, { encoding: 'utf8' }), { sections: true, namespaces: true });

program.number = program.number || 10;

if (program.oplog) {
  config.database.name = 'local';
  collection = 'oplog.$main';
}

function tail(db) {
  var coll = db.db(config.database.name || 'local').collection(collection);

  // start at the end
  coll.count(function(err, counter) {
    if (err) {
      console.error(err);
      process.exit(1);
    }

    console.log('offset', counter);

    var opts = { skip: counter - program.number };
    if (program.follow) { opts.tailable = true; }

    var stream = coll.find({}, opts).stream();


    // if tailable, skip won't work so do this manually
    if (opts.tailable) {
      console.log('seeking...');
      var i = 0;
      var offsetReached;
      stream.on('data', function(item) {
        if (!offsetReached) {
          i++;
          if (i <= opts.skip) { return; }
          offsetReached = true;
        }
        console.log(JSON.stringify(item));
      });
    } else {
      stream.on('data', function(item) {
        console.log(JSON.stringify(item));
      });
    }

    stream.on('error', function() {
      process.exit(1);
    });
    stream.on('close', function() {
      process.exit();
    });
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
  tail(db);
});
