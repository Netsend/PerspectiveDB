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
  .usage('[-v] -c collection -f config version1 version2 [versionLCA]')
  .description('merge two given versions, optionally using a third specified version as lca')
  .option('-d, --database <database>', 'name of the database')
  .option('-c, --collection <collection>', 'name of the collection to repair')
  .option('-f, --config <config>', 'an ini config file')
  .option('-p, --parents', 'set version1 and version2 to be the parents of the merge')
  .option('-s, --save', 'insert the new item in the snapshot')
  .option('-v, --verbose', 'verbose')
  .parse(process.argv);

if (!program.args[1]) {
  program.help();
}

if (!program.config) { program.help(); }
if (!program.collection) { program.help(); }

var config = program.config
var collection = program.collection;

// if relative, prepend current working dir
if (config[0] !== '/') {
    config = process.cwd() + '/' + config;
}

config = properties.parse(fs.readFileSync(config, { encoding: 'utf8' }), { sections: true, namespaces: true });

var v1 = program.args[0];
var v2 = program.args[1];
var lca = program.args[2];

var debug = !!program.verbose;

function run(db) {
  var vc = new VersionedCollection(db, collection, { debug: debug });

  var selector = { '_id._v': { $in: [ v1, v2 ] }, '_id._pe': vc.localPerspective };
  vc._snapshotCollection.find(selector).toArray(function(err, items) {
    if (err) {
      console.error('error:', err.message);
      process.exit(1);
    }

    if (items.length !== 2) {
      console.error('error: not exactly two items found');
      process.exit(1);
    }

    var task;
    if (lca) {
      if (debug) { console.log('merge with given lca'); }
      task = function(cb) {
        var selector = { '_id._v': lca, '_id._pe': vc.localPerspective };
        vc._snapshotCollection.findOne(selector, function(err, item) {
          if (err) { cb(err); return; }
          if (!item) { cb(new Error('lca not found')); return; }
          var merge = VersionedCollection._threeWayMerge(items[0], items[1], item);
          if (Array.isArray(merge)) {
            cb(new Error('merge conflict'), merge);
            return;
          }
          if (program.parents) {
            merge._id._pa = [v1, v2];
            merge._id._v = VersionedCollection._generateRandomVersion();
          }
          cb(null, [merge]);
        });
      };
    } else {
      if (debug) { console.log('merge from snapshot'); }
      task = function(cb) {
        vc._merge(items[0], items[1], cb);
      };
    }

    async.series([task], function(err, result) {
      if (err) {
        console.error('error:', err.message);
        process.exit(1);
      }

      var merge = result[0][0];

      console.log(JSON.stringify(merge));

      if (program.save) {
        vc._addAllToDAG([{ item: merge }], function(err) {
          if (err) {
            console.error('error:', err.message);
            process.exit(1);
          }
          db.close();
        });
      } else {
        db.close();
      }
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
  run(db);
});
