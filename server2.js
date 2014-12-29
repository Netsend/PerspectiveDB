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
var keyFilter = require('object-key-filter');

var get = require('./lib/get_selector');
var _db = require('./bin/_db');
var VersionedSystem = require('./lib/versioned_system');
var ArrayCollection = require('./lib/array_collection');

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

// filter password out request
function debugReq(req) {
  return JSON.stringify(keyFilter(req, ['password'], true), null, 2);
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

if (program.debug) { console.log('remote config', debugReq(remoteLogin || {})); }

console.time('runtime');

// create a new mongodb like collection from an array
function arrToColl(arr) {
  return new ArrayCollection(arr, { debug: program.debug });
}

// load from config file itself
function loadSelf(key, cfg) {
  var docs = [];
  Object.keys(cfg[key]).forEach(function(name) {
    if (name === 'location' || name === 'name') {
      return;
    }
    docs.push(cfg[key][name]);
  });
  return docs;
}

// load from a separate config file
function loadFile(file) {
  var cfg;
  if (~file.indexOf('.json')) {
    cfg = require(file);
  } else {
    cfg = properties.parse(fs.readFileSync(file, { encoding: 'utf8' }), { sections: true, namespaces: true });
  }

  var docs = [];
  Object.keys(cfg).forEach(function(name) {
    if (name === 'location' || name === 'name') {
      return;
    }
    docs.push(cfg[name]);
  });
  return docs;
}

function start(db) {
  (function(cb) {
    var oplogColl = db.db(config.database.oplogDb || 'local').collection(config.database.oplogCollection || 'oplog.$main');

    var opts = { debug: program.debug };

    var parts;

    if (config.replication) {
      if (config.replication.location === 'database') {
        parts = config.replication.name.split('.');
        if (parts.length > 1) {
          opts.replicationDb = parts[0];
          opts.replicationCollName = parts.slice(1).join('.');
        } else if (parts.length === 1) {
          opts.replicationCollName = parts[0];
        }
      } else if (config.replication.location === 'self') {
        opts.replicationColl = arrToColl(loadSelf(config.replication.name, config));
      } else if (config.replication.location === 'file') {
        opts.replicationColl = arrToColl(loadFile(config.replication.name, config));
      }
    }

    if (config.users) {
      if (config.users.location === 'database') {
        parts = config.users.name.split('.');
        if (parts.length > 1) {
          opts.usersDb = parts[0];
          opts.usersCollName = parts.slice(1).join('.');
        } else if (parts.length === 1) {
          opts.usersCollName = parts[0];
        }
      } else if (config.users.location === 'self') {
        opts.usersColl = arrToColl(loadSelf(config.users.name, config));
      } else if (config.users.location === 'file') {
        opts.usersColl = arrToColl(loadFile(config.users.name, config));
      }
    }

    if (program.debug) { console.log('vs opts', opts); }

    var vs = new VersionedSystem(oplogColl, opts);

    // get all vc configs
    if (program.debug) { console.log('init vc', get(config, 'vc')); }

    vs.initVCs(get(config, 'vc'), function(err) {
      if (err) { cb(err); return; }

      // call either chroot or listen (listen calls chroot)
      if (get(config, 'server')) {
        if (program.debug) { console.log('%s: preauth forking...', programName); }

        var opts2 = {
          serverConfig: get(config, 'server'),
          chrootConfig: get(config, 'server')
        };
        vs.listen(get(config, 'main.user') || 'nobody', get(config, 'main.chroot') || '/var/empty', opts2, function(err) {
          if (err) { cb(err); return; }

          // find out if any remotes need to be initiated
          if (remoteLogin) {
            console.log('pull requests', debugReq(remoteLogin));
            sendPRs(vs, remoteLogin);
          }
          console.log('%s: ready', programName);
        });
      } else {
        // chroot
        vs.chroot(get(config, 'main.user'), { path: get(config, 'main.chroot') });

        // find out if any remotes need to be initiated
        if (remoteLogin) {
          console.log('pull requests', debugReq(remoteLogin));
          sendPRs(vs, remoteLogin);
        }
        console.log('%s: ready', programName);
      }
    });

    process.once('SIGINT', function() {
      console.log('received SIGINT shutting down... press CTRL+C again to force quit');
      vs.stop(cb);
    });
    process.once('SIGTERM', function() {
      console.log('received SIGTERM shutting down... send another SIGTERM to force quit');
      vs.stopTerm(cb);
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
  dbName: database.name || 'local',
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
