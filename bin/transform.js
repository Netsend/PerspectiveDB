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

var readline = require('readline');

var program = require('commander');
var async = require('async');

var _db = require('./_db');
var VersionedCollection = require('./../lib/versioned_collection');
var Replicator = require('./../lib/replicator');

program
  .version('0.0.3')
  .usage('[-c config.json] hook.js [hook2.js ...]')
  .description('transform all json objects read from stdin using the specified hook')
  .option('-c, --config <file>', 'configuration file with database parameters')
  .option('-v, --verbose', 'verbose logging')
  .parse(process.argv);

if (!program.args[0]) { program.help(); }

var hooks = program.args.map(function(path) {
  // if relative, prepend current working dir
  if (path[0] !== '/') {
    path = process.cwd() + '/' + path;
  }

  return require(path);
});

hooks = Replicator.loadExportHooks(hooks, {});

var config = program.config;

// if relative, prepend current working dir
if (config && config[0] !== '/') {
  config = process.cwd() + '/' + config;
}

try {
  config = require(config);
} catch(err) {
  config = {};
}

function start(db) {
  var rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false
  });

  var rlClosed = false;

  var queue = async.queue(function(item, cb) {
    if (program.verbose) { console.log('worker'); }
    try {
      VersionedCollection.runExportHooks(hooks, db, {}, item, function(err, keep) {
        if (err) { console.error('hook', err); cb(err); return; }
        if (keep) { console.log(JSON.stringify(keep)); }
        cb();
      });
    } catch(err) {
      console.error('hook', err);
      cb(err);
    }
  }, 1);

  queue.saturated = function() {
    if (program.verbose) { console.log('saturated'); }
    if (!rlClosed) { rl.pause(); }
  };

  queue.empty = function() {
    if (program.verbose) { console.log('empty'); }
    if (!rlClosed) { rl.resume(); }
  };

  queue.drain = function() {
    if (program.verbose) { console.log('drain'); }
    if (rlClosed) { if (db) { db.close(); } }
  };

  rl.on('line', function(line) {
    if (program.verbose) { console.log('line'); }
    try {
      queue.push(JSON.parse(line));
    } catch(err) {
      console.error(err, line);
    }
  });

  rl.on('close', function() {
    if (program.verbose) { console.log('readline closed'); }
    rlClosed = true;
    if (queue.idle() && db) {
      db.close();
    }
  });
}

if (config.dbName) {
  // open database
  _db(config, function(err, db) {
    if (err) { throw err; }
    start(db);
  });
} else {
  start(null);
}
