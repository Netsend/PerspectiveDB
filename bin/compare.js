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
var properties = require('properties');
var fs = require('fs');

var _db = require('./_db');
var compare = require('../lib/compare');
var VersionedCollection = require('../lib/versioned_collection');

program
  .version('0.1.0')
  .usage('[-v] -a database [-b database] -c collection [-d collection] -f config')
  .description('compare each item in collection1 with collection2')
  .option('-a, --database1 <database>', 'name of the database for collection1')
  .option('-b, --database2 <database>', 'name of the database for collection2 if different from database1')
  .option('-c, --collection1 <collection>', 'name of the collection to report about')
  .option('-d, --collection2 <collection>', 'name of the collection to compare against if different from collection1')
  .option('-f, --config <config>', 'an ini config file')
  .option('-m, --match <attrs>', 'comma separated list of attributes to should match', function(val) { return val.split(','); })
  .option('-i, --include <attrs>', 'comma separated list of attributes to include in comparison', function(val) { return val.split(','); })
  .option('-e, --exclude <attrs>', 'comma separated list of attributes to exclude in comparison', function(val) { return val.split(','); })
  .option('-s  --showid', 'print id of missing and unequal items')
  .option('-p, --patch', 'show a patch with differences between unequal items')
  .option('    --columns', 'print comma separated list of column names that have differences')
  .option('-v, --verbose', 'verbose')
  .parse(process.argv);

if (!program.database1) { program.help(); }
if (!program.database2) { program.database2 = program.database1; }
if (!program.collection1) { program.help(); }
if (!program.collection2) { program.collection2 = program.collection1; }
if (!program.config) { program.help(); }

var config = program.config

// if relative, prepend current working dir
if (config[0] !== '/') {
  config = process.cwd() + '/' + config;
}

config = properties.parse(fs.readFileSync(config, { encoding: 'utf8' }), { sections: true, namespaces: true });

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

// group patch by type
function groupPatch(patch) {
  var result = { '+': [], '-': [], '~': [] };
  Object.keys(patch).forEach(function(key) {
    result[patch[key]].push(key);
  });
  return result;
}

// count columns
function diffColumnCount(item1, item2, diffColumns) {
  var diff = VersionedCollection.diff(item1, item2);
  Object.keys(diff).forEach(function(key) {
    diffColumns[key] = diffColumns[key] || 0;
    diffColumns[key]++;
  });
  return diff;
}

function run(db) {
  var coll1 = db.db(program.database1).collection(program.collection1);
  var coll2 = db.db(program.database2).collection(program.collection2);

  var opts = {
    includeAttrs: includeAttrs,
    excludeAttrs: excludeAttrs,
    matchAttrs: matchAttrs,
    debug: debug
  };

  compare(coll1, coll2, opts, function(err, stats) {
    if (err) { throw err; }

    var diffColumns = {};

    if (debug) {
      console.log('missing');
      stats.missing.forEach(function(item) {
        console.log(item);
      });
      console.log('end of missing');

      console.log('unequal');
      stats.inequal.forEach(function(item) {
        console.log(1, item.item1);
        console.log(2, item.item2);
      });
      console.log('end of unequal');

      console.log('multiple');
      stats.multiple.forEach(function(item) {
        console.log(1, item.item1);
        console.log(2, JSON.stringify(item.items2));
      });
      console.log('end of multiple');

      console.log('unknown');
      stats.unknown.forEach(function(item) {
        console.log(item);
      });
      console.log('end of unknown');
    } else if (program.showid) {
      console.log('missing');
      stats.missing.forEach(function(item) {
        console.log(item._id);
      });
      console.log('end of missing');

      console.log('unequal');
      stats.inequal.forEach(function(item) {
        console.log(item.item1._id);
      });
      console.log('end of unequal');

      console.log('multiple');
      stats.multiple.forEach(function(item) {
        console.log(item.item1._id);
      });
      console.log('end of multiple');

      console.log('unknown');
      stats.unknown.forEach(function(item) {
        console.log(item);
      });
      console.log('end of unknown');
    } else if (program.patch) {
      console.log('missing');
      stats.missing.forEach(function(item) {
        console.log(item._id);
      });
      console.log('end of missing');

      console.log('unequal');
      stats.inequal.forEach(function(item) {
        var diff = diffColumnCount(item.item1, item.item2, diffColumns);
        console.log(item.item1._id, JSON.stringify(groupPatch(diff)));
      });
      console.log('end of unequal');

      console.log('multiple');
      stats.multiple.forEach(function(item) {
        item.items2.forEach(function(subItem) {
          var diff = diffColumnCount(item.item1, subItem, diffColumns);
          console.log(item.item1._id, JSON.stringify(groupPatch(diff)));
        });
      });
      console.log('end of multiple');

      console.log('unknown');
      stats.unknown.forEach(function(item) {
        console.log(item);
      });
      console.log('end of unknown');
    } else if (program.columns) {
      console.log('missing');
      stats.missing.forEach(function(item) {
        console.log(item._id);
      });
      console.log('end of missing');

      console.log('unequal');
      stats.inequal.forEach(function(item) {
        var diff = diffColumnCount(item.item1, item.item2, diffColumns);
        console.log(item.item1._id, JSON.stringify(groupPatch(diff)));
      });
      console.log('end of unequal');

      console.log('multiple');
      stats.multiple.forEach(function(item) {
        item.items2.forEach(function(subItem) {
          var diff = diffColumnCount(item.item1, subItem, diffColumns);
          console.log(item.item1._id, JSON.stringify(groupPatch(diff)));
        });
      });
      console.log('end of multiple');

      console.log('unknown');
      stats.unknown.forEach(function(item) {
        console.log(item);
      });
      console.log('end of unknown');
    }

    if (program.columns) {
      // show diffColumns sorted
      var sorted = Object.keys(diffColumns).map(function(key) {
        return {
          name: key,
          val: diffColumns[key]
        };
      }).sort(function(a, b) {
        if (a.val < b.val) {
          return -1;
        } else if (a.val === b.val) {
          return 0;
        } else {
          return 1;
        }
      }).map(function(key) {
        var obj = {};
        obj[key.name] = key.val;
        return obj;
      });
      console.log('columns', sorted);
    }

    console.log('missing',  stats.missing.length);
    console.log('unequal',  stats.inequal.length);
    console.log('equal',    stats.equal.length);
    console.log('multiple', stats.multiple.length);
    console.log('unknown', stats.unknown.length);

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
