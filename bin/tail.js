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
  .version('0.1.0')
  .usage('[-f] -d database -c collection')
  .description('tail the given collection')
  .option('-d, --database <database>', 'name of the database')
  .option('-c, --collection <collection>', 'name of the collection')
  .option('-o, --oplog', 'shortcut for "-d local -c oplog.$main"')
  .option('-n, --number <number>', 'the number of last items to show, defaults to 10')
  .option('-f, --follow', 'keep the cursor open (only on capped collections)')
  .parse(process.argv);

// get config path from environment
var config = require(process.env.CONFIG || '../config/development.json');

if (program.oplog) {
  program.database = 'local';
  program.collection = 'oplog.$main';
}

if (!program.database) { program.help(); }
if (!program.collection) { program.help(); }

program.number = program.number || 10;

function tail(db) {
  var coll = db.db(program.database).collection(program.collection);

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

// open database
_db(config, function(err, db) {
  if (err) { throw err; }
  tail(db);
});
