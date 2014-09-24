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

var util = require('util');

var program = require('commander');

var _db = require('./bin/_db');
var VersionedSystem = require('./lib/versioned_system');

program
  .version('0.0.2')
  .usage('[-d, -r <replicate>] config.json')
  .option('-d, --debug', 'show process information')
  .option('-r, --replicate <file>', 'replication config')
  .parse(process.argv);

if (!program.args[0]) {
  program.help();
}

var config = program.args[0];

// if relative, prepend current working dir
if (config[0] !== '/') {
  config = process.cwd() + '/' + config;
}

var config = require(config);

var replicate;
if (config.replicate) {
  replicate = config.replicate;
  delete config.replicate;
}

// cli overrides config
if (program.replicate) {
  replicate = program.replicate;
  // if relative, prepend current working dir
  if (replicate[0] !== '/') {
    replicate = process.cwd() + '/' + replicate;
  }

  replicate = require(replicate);
}

// get config path from environment
if (program.debug) { console.log('config:\n', util.inspect(config, { depth: null })); }
if (program.debug) { console.log('replicate:\n', util.inspect(replicate, { depth: null })); }

/**
 * Check if the versioned collections are on track and start tracking.
 */
function run(db) {
  (function(cb) {
    var opts = {
      debug: program.debug,
      snapshotSizes: config.snapshotSizes,
      replicate: replicate,
      haltOnMergeConflict: config.haltOnMergeConflict,
      proceedOnError: config.proceedOnError
    };
    var vs = new VersionedSystem(db, config.databases, config.collections, opts);
    vs.start(cb);

    function stopper() {
      vs.stopTrack(function(err) {
        if (err) { console.error('stopTrack error', err); }
        if (program.debug) { console.log('stopTrack done'); }
      });
    }

    var sigquit = 0, sigint = 0, sigterm = 0;

    process.on('SIGQUIT', function() {
      sigquit++;
      if (sigquit === 2) {
        console.log('received another SIGQUIT, force quit');
        process.exit(1);
      }
      if (program.debug) { console.log('received SIGQUIT shutting down... press CTRL+D again to force quit'); }
      stopper();
    });
    process.on('SIGINT', function() {
      sigint++;
      if (sigint === 2) {
        console.log('received another SIGINT, force quit');
        process.exit(1);
      }
      if (program.debug) { console.log('received SIGINT shutting down... press CTRL+C again to force quit'); }
      stopper();
    });
    process.on('SIGTERM', function() {
      sigterm++;
      if (sigterm === 2) {
        console.log('received another SIGTERM, force quit');
        process.exit(1);
      }
      if (program.debug) { console.log('received SIGTERM shutting down... send another SIGTERM to force quit'); }
      stopper();
    });
  })(function(err) {
    if (err) {
      console.error.apply(this, arguments);
      console.trace();
      var d = new Date();
      console.error(d.getTime(), d);
      process.exit(1);
    }
    if (program.debug) { console.log(new Date(), 'server: start came to an end'); }
    db.close();
  });
}

// open database
_db(config, function(err, db) {
  if (err) { throw err; }
  run(db);
});
