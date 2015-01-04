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
var mongodb = require('mongodb');
var async = require('async');
var VersionedCollection = require('../lib/versioned_collection');
var properties = require('properties');
var fs = require('fs');

var program = require('commander');

program
  .version('0.1.0')
  .usage('[-n] -d database -c collection -f config')
  .description('tail the given collection')
  .option('-d, --database <database>', 'name of the database')
  .option('-c, --collection <collection>', 'name of the collection')
  .option('    --id <id>', 'show the log of one string based id')
  .option('    --oid <id>', 'show the log of one object id')
  .option('-f, --config <config>', 'an ini config file')
  .option('-s, --show', 'show complete objects')
  .option('    --sync', 'only show versions that are in sync')
  .option('    --nsync', 'only show versions that are not in sync')
  .option('-p, --patch', 'show patch compared to previous version')
  .option('    --pe <perspective>', 'perspective')
  .option('-n, --number <number>', 'number of latest versions to show, defaults to 10, 0 means unlimited')
  .parse(process.argv);

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

if (!program.database) { program.help(); }
if (!program.collection) { program.help(); }

if (program.sync && program.nsync) {
  console.error('error: --sync and --nsync are mutally exclusive');
  process.exit(1);
}

if (program.number === '0') {
  program.number = 0;
} else {
  program.number = Number(program.number) || 10;
}

var id;
if (program.id) {
  id = program.id;
} else if (program.oid) {
  id = new mongodb.ObjectID(program.oid);
}

// shows old values of deleted and changed attributes
function fmtPatch(patch, oldItem) {
  var result = {};
  Object.keys(patch).forEach(function(key) {
    if (patch[key] !== '+') {
      result[key] = oldItem[key];
    }
  });
  return JSON.stringify(result);
}

function fmtItem(item, parents) {
  parents = parents || [];

  var out = '';
  if (program.show) {
    // print tree
    if (parents.length) {
      out += '\u250f ';
    } else {
      out += '\u257a ';
    }
    out += ' ' + JSON.stringify(item);
    if (parents.length) {
      out += '\n';
    }

    var i = 0;
    parents.forEach(function(p) {
      i++;
      if (i === parents.length) {
        out += '\u2517\u2501 ';
      } else {
        out += '\u2523\u2501 ';
      }
      var diff = VersionedCollection.diff(item, p);
      delete diff._id;
      out += ' ' + p._id._id + ' ' + p._id._v + ' diff: ' + JSON.stringify(diff) + ' ' + fmtPatch(diff, p);
      if (i !== parents.length) {
        out += '\n';
      }
    });
  } else {
    out += item._id._id;
    out += ' ' + item._id._v;
    if (item._id._lo) {
      out += ' l';
    } else {
      out += ' .';
    }
    if (item._m3.hasOwnProperty('_ack') && item._m3._ack) {
      out += ' s';
    } else {
      out += ' .';
    }
    if (item._id.hasOwnProperty('_d') && item._id._d) {
      out += ' d';
    } else {
      out += ' .';
    }
    out += ' ' + item._id._pe;
    if (item._id.hasOwnProperty('_i')) {
      out += ' (' + item._id._i + ')';
    }
    out += ' ' + JSON.stringify(item._id._pa);
    if (program.patch && parents.length) {
      parents.forEach(function(p) {
        var diff = VersionedCollection.diff(item, p);
        delete diff._id;
        out += ' ' + p._id._v + ' diff: ' + JSON.stringify(diff) + ' ' + fmtPatch(diff, p);
      });
    }
  }
  return out;
}

function run(db) {
  var coll = db.db(program.database).collection('m3.'+program.collection);

  var counter = 0;

  // start at the end
  var opts = { sort: { $natural: -1 }, comment: 'mmlog' };
  if (program.number) {
    opts.limit = program.number;
  }

  // build selector in parts to maintain an index friendly order, if possible
  var selector = { '_id._co': program.collection };
  if (id) { selector['_id._id'] = id; }
  if (program.pe) {
    selector['_id._pe'] = program.pe;
  }

  if (program.sync) { selector['_m3._ack'] = true; }
  if (program.nsync) { selector['_m3._ack'] = false; }

  var stream = coll.find(selector, opts).stream();
  var error;

  stream.on('data', function(item) {
    counter++;

    if (program.patch && item._id._pa.length) {
      stream.pause();

      var parents = [];
      async.eachSeries(item._id._pa, function(pa, cb) {
        selector = { '_id._co': item._id._co, '_id._id': item._id._id, '_id._v': pa, '_id._pe': item._id._pe };
        coll.findOne(selector, function(err, p) {
          if (err) { return cb(err); }
          if (!p) {
            error = new Error('parent not found');
            console.error(error, JSON.stringify(selector));
            return cb(error);
          }

          parents.push(p);
          cb();
        });
      }, function(err) {
        if (err) {
          console.error(err, JSON.stringify(item));
          process.exit(1);
        }

        console.log(fmtItem(item, parents));

        if (counter === program.number) {
          stream.destroy();
        } else {
          stream.resume();
        }
      });
    } else {
      console.log(fmtItem(item));

      if (counter === program.number) {
        stream.destroy();
      }
    }
  });
  stream.on('error', function() {
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
  run(db);
});

