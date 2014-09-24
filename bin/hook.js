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

program
  .version('0.0.1')
  .usage('-d database -c collection <hook.js>')
  .description('run a hook on a collection')
  .option('-d, --database <database>', 'name of the database')
  .option('-c, --collection <collection>', 'name of the collection')
  .option('    <hook.js>', 'path to the hook to use')
  .parse(process.argv);

// get config path from environment
var config = require(process.env.CONFIG || '../config/development.json');

var hook = program.args[0];

if (!program.database) { program.help(); }
if (!program.collection) { program.help(); }
if (!hook) { program.help(); }

// if relative, prepend current working dir
if (hook[0] !== '/') {
  hook = process.cwd() + '/' + hook;
}

hook = require(hook);

function start(db) {
  var coll = db.db(program.database).collection(program.collection);

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

// open database
_db(config, function(err, db) {
  if (err) { throw err; }
  start(db);
});
