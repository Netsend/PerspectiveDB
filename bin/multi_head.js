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
var async = require('async');
var properties = require('properties');
var fs = require('fs');

var _db = require('./_db');
var VersionedCollection = require('../lib/versioned_collection');

program
  .version('0.0.1')
  .usage('[-vm] -c collection -f config')
  .description('find all ids that are not merged because of multiple heads')
  .option('-c, --collection <collection>', 'name of the collection to repair')
  .option('-f, --config <config>', 'an ini config file')
  .option('-m, --merge', 'try to merge multiple heads')
  .option('-a, --ack', 'ack ancestors of ackd items, implies -m')
  .option('-v, --verbose', 'verbose')
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

var debug2;
var debug = !!program.verbose;

if (program.ack && !program.merge) {
  console.log('use of --ack implies --merge');
  program.merge = true;
}

function run(db) {
  var vc = new VersionedCollection(db, collection, { debug: debug });

  var multiHeads = [];
  var error;

  vc.allHeads(function(id, heads) {
    if (heads.length > 1) {
      multiHeads.push({ id: id, heads: heads });
      console.log(id, heads);
    } else if (debug2) {
      console.log(id, heads);
    }
  }, function(err) {
    if (err) {
      console.error('error:', err.message);
      process.exit(1);
    }

    console.log('unmerged ids', multiHeads.length);

    if (program.merge) {
      // try to merge
      async.eachSeries(multiHeads, function(obj, cb) {
        var id = obj.id;
        var heads = obj.heads;
        vc._snapshotCollection.find({ '_id._id': id, '_id._pe': vc.localPerspective, '_id._v': { $in: heads } }).toArray(function(err, items) {
          if (err) {
            console.error(err);
            cb(err);
            return;
          }

          if (items.length !== heads.length) {
            error = new Error('could not resolve all heads');
            console.error(error);
            cb(error);
            return;
          }

          vc.mergeAndSave(items, function(err) {
            if (err) {
              console.error('error merging', err, id, heads);
            } else {
              console.log('merged', id, heads);
            }
            cb(err);
          });
        });
      }, function(err) {
        if (err) {
          console.error(err);
          process.exit(2);
        }

        if (program.ack) {
          vc.ackAncestorsAckd(function(err) {
            if (err) {
              console.error(err);
              process.exit(2);
            }

            console.log('set all ackd');
            db.close();
          });
        } else {
          db.close();
        }
      });
    } else {
      db.close();
    }
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
