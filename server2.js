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

var path = require('path');
var fs = require('fs');

var program = require('commander');
var properties = require('properties');

var get = require('./lib/get_selector');
var _db = require('./bin/_db');
var VersionedSystem = require('./lib/versioned_system');

var programName = path.basename(__filename);

function sendPRs(vs, remotes) {
  Object.keys(remotes).forEach(function(remoteKey) {
    var remote = remotes[remoteKey];
    var pr = {
      username:   remote.username,
      password:   remote.password,
      path:       remote.path,
      host:       remote.host,
      port:       remote.port,
      database:   remote.database,
      collection: remote.collection
    };
    vs.sendPR(remote.vc, pr);
  });
}

program
  .version('0.0.3')
  .usage('[-d] config.ini')
  .option('-d, --debug', 'show process information')
  .parse(process.argv);

var configFile = program.args[0];

if (!configFile) {
  program.help();
}

if (process.getuid() !== 0) {
  console.error('%s: need root privileges', programName);
  process.exit(1);
}

// if relative, prepend current working dir
if (configFile[0] !== '/') {
  configFile = process.cwd() + '/' + configFile;
}

var config = properties.parse(fs.readFileSync(configFile, { encoding: 'utf8' }), { sections: true, namespaces: true });

var remoteLogin = get(config, 'remotes');

// find out if any remotes need to be initiated
if (typeof remoteLogin === 'string') {
  // if relative, prepend path to config file
  if (remoteLogin[0] !== '/') {
    remoteLogin = path.dirname(configFile) + '/' + remoteLogin;
  }

  remoteLogin = properties.parse(fs.readFileSync(remoteLogin, { encoding: 'utf8' }), { sections: true, namespaces: true }).remotes;
}

if (program.debug) { console.log('remote config', remoteLogin); }

console.time('runtime');

function start(db) {
  (function(cb) {
    var oplogColl = db.db(config.database.oplogDb || 'local').collection(config.database.oplogCollection || 'oplog.$main');

    var opts = { debug: program.debug };
    var vs = new VersionedSystem(oplogColl, opts);

    // get all vc configs
    if (program.debug) { console.log('init vc', get(config, 'vc')); }

    vs.initVCs(get(config, 'vc'), function(err) {
      if (err) { cb(err); return; }

      // call either chroot or listen (listen calls chroot)
      if (get(config, 'server')) {
        if (program.debug) { console.log('%s: preauth forking...', programName); }

        vs.listen(get(config, 'main.user') || 'nobody', get(config, 'main.chroot') || '/var/empty', { chrootConfig: get(config, 'server') }, function(err) {
          if (err) { cb(err); return; }

          // find out if any remotes need to be initiated
          if (remoteLogin) {
            console.log('pull requests', remoteLogin);
            sendPRs(vs, remoteLogin);
          }
        });
      } else {
        // chroot
        vs.chroot(get(config, 'main.user'), { path: get(config, 'main.chroot') });

        // find out if any remotes need to be initiated
        if (remoteLogin) {
          console.log('pull requests', remoteLogin);
          sendPRs(vs, remoteLogin);
        }
      }
    });

    process.once('SIGINT', function() {
      console.log('received SIGINT shutting down... press CTRL+C again to force quit');
      vs.stop(cb);
    });
    process.once('SIGTERM', function() {
      console.log('received SIGTERM shutting down... send another SIGTERM to force quit');
      vs.stop(cb);
    });
  })(function(err) {
    if (err) {
      console.error.apply(this, arguments);
      console.trace();
      var d = new Date();
      console.error(d.getTime(), d);
      process.exit(6);
    }
    if (program.debug) {
      console.log(new Date(), 'server down');
      console.timeEnd('runtime');
    }
    db.close();
  });
}

var database = config.database;
var dbCfg = {
  dbName: database.name,
  dbHost: database.path || database.host,
  dbPort: database.port,
  dbUser: database.username,
  dbPass: database.password,
  adminDb: database.adminDb
};

// open database
_db(dbCfg, function(err, db) {
  if (err) {
    console.error('%s: db error:', programName, err);
    process.exit(1);
  }
  start(db);
});
