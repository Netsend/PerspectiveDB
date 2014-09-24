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
var rebase = require('../lib/rebase');
var VersionedSystem = require('../lib/versioned_system');

program
  .version('0.0.1')
  .usage('[-v] -c <file> -r <file> database.collection')
  .option('-d, --debug', 'debug')
  .option('-c, --config <file>', 'configuration file')
  .option('-r, --replicate <file>', 'replication config')
  .option('    --restore', 'restore last aborted rebase in provided collection')
  .parse(process.argv);

if (!program.args[0]) { program.help(); }
var dbColl = program.args[0];

if (!program.config) {
  console.error('provide config file');
  program.help();
}

if (!program.replicate) {
  console.error('provide replicate file');
  program.help();
}

var config = require(process.cwd() + '/' + program.config);

var replicate = require(process.cwd() + '/' + program.replicate);

function run(db, cb) {
  var opts = { debug: !!program.debug, snapshotSizes: config.snapshotSizes, replicate: replicate };
  var vs = new VersionedSystem(db, config.databases, config.collections, opts);

  if (!vs._databaseCollections.hasOwnProperty(dbColl)) {
    return cb(new Error(dbColl + ' does not exist'));
  }

  var vc = vs._databaseCollections[dbColl];

  var tmpColl = db.collection('m3.rebase');

  if (program.restore) {
    console.log('restoring');


    vc._collection.drop(function(err) {
      if (err) {
        console.error('rebase restore drop', err);
        return cb(err);
      }

      var s = tmpColl.find().stream();
      s.on('data', function(item) {
        vc._collection.insert(item, { w: 1 }, function(err, inserted) {
          if (err) {
            console.error('rebase restore', err);
            return cb(err);
          }

          if (inserted.length !== 1) {
            var error = new Error('item not inserted into collection');
            console.error('rebase restore', error);
            return cb(error);
          }
        });
      });

      s.on('error', cb);
      s.on('close', cb);
    });
  } else {
    console.log('start rebase');
    rebase(vs, vc, tmpColl, opts, cb);
  }
}

// open database
_db(config, function(err, db) {
  if (err) { throw err; }

  run(db, function(err) {
    if (err) {
      console.error.apply(this, arguments);
      console.trace();
      var d = new Date();
      console.error(d.getTime(), d);
      process.exit(1);
    }
    db.close();
    console.log('db closed');
  });
});
