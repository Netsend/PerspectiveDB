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

var async = require('async');
var program = require('commander');
var properties = require('properties');
var keyFilter = require('object-key-filter');

var get = require('./lib/get_selector');
var _db = require('./bin/_db');
var VersionedSystem = require('./lib/versioned_system');
var ArrayCollection = require('./lib/array_collection');
var logger = require('./lib/logger');

var log;
var programName = path.basename(__filename);

program
  .version(require('./package.json').version)
  .usage('config.ini')
  .parse(process.argv);

var configFile = program.args[0];

if (!configFile) {
  program.help();
}

if (process.getuid() !== 0) {
  console.error('%s: need root privileges', programName);
  process.exit(1);
}

var startTime = new Date();

// if relative, prepend current working dir
if (configFile[0] !== '/') {
  configFile = process.cwd() + '/' + configFile;
}

var config = properties.parse(fs.readFileSync(configFile, { encoding: 'utf8' }), { sections: true, namespaces: true });

var logCfg = get(config, 'log') || {};

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

    vs.sendPR(remote.vc, pr, function(err) {
      if (err) {
        log.err('send pr error ' +  remote.vc + ' ' + err);
        return;
      }
      log.info('sent pr ' + remote.vc + ' ' + JSON.stringify(pr));
    });
  });
}

// filter password out request
function debugReq(req) {
  return JSON.stringify(keyFilter(req, ['password'], true), null, 2);
}

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

    var opts = { log: log };

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

        var replCfgs = loadSelf(config.replication.name, config);
        // convert the hide string to an array
        if (replCfgs) {
          replCfgs.forEach(function(replCfg, key) {
            if (replCfg.collections) {
              Object.keys(replCfg.collections).forEach(function(collectionName) {
                var hide = replCfg.collections[collectionName].hide;
                if (hide && typeof hide === 'string') {
                  replCfgs[key].collections[collectionName].hide = [hide];
                }
              });
            }
          });
        }

        opts.replicationColl = arrToColl(replCfgs);
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

    var remoteLogin = config.remotes;
    if (config.remotes) {
      // find out if any remotes need to be initiated
      if (typeof remoteLogin === 'string') {
        // if relative, prepend path to config file
        if (remoteLogin[0] !== '/') {
          remoteLogin = path.dirname(configFile) + '/' + remoteLogin;
        }

        remoteLogin = properties.parse(fs.readFileSync(remoteLogin, { encoding: 'utf8' }), { sections: true, namespaces: true }).remotes;
      }

      log.info('remote config', debugReq(remoteLogin || {}));
    }

    log.info('vs opts', JSON.stringify(opts));

    var vs = new VersionedSystem(oplogColl, opts);

    // get all vc configs
    var vcsCfg = get(config, 'vc');
    log.info('init vcs', JSON.stringify(vcsCfg));

    var tasks = [function(cb2) {
      // ensure async even without any other tasks
      process.nextTick(cb2);
    }];

    tasks.push(function(cb2) {
      async.eachSeries(Object.keys(vcsCfg), function(dbName, cb3) {
        async.eachSeries(Object.keys(vcsCfg[dbName]), function(collName, cb4) {
          var vcCfg = vcsCfg[dbName][collName];
          log.info(dbName, collName);

          // ensure specific log configuration overrules the global log config
          vcCfg.log = vcCfg.log || {};

          var vcLog = {};

          // set global log config
          Object.keys(logCfg).forEach(function(key) {
            vcLog[key] = logCfg[key];
          });

          var gfile = log.getFileStream();
          var gerror = log.getErrorStream();

          if (gfile) { vcLog.file = gfile; }
          if (gerror) { vcLog.error = gerror; }

          // overrule with vc specific log config
          Object.keys(vcCfg.log).forEach(function(key) {
            vcLog[key] = vcCfg.log[key];
          });

          if (vcCfg.log.level) {
            vcLog.mask = logger.levelToPrio(vcCfg.log.level);
          }

          var tasks2 = [function(cb2) {
            // ensure async even without any other tasks
            process.nextTick(cb2);
          }];

          if (vcCfg.log.file) {
            tasks2.push(function(cb5) {
              logger.openFile(vcCfg.log.file, function(err, f) {
                if (err) { cb5(err); return; }
                vcLog.file = f;
                cb5();
              });
            });
          } else {
            vcLog.file = log.getFileStream();
          }

          if (vcCfg.log.error) {
            tasks2.push(function(cb5) {
              logger.openFile(vcCfg.log.error, function(err, f) {
                if (err) { cb5(err); return; }
                vcLog.error = f;
                cb5();
              });
            });
          } else {
            vcLog.error = log.getErrorStream();
          }

          vcCfg.logCfg = vcLog;

          async.parallel(tasks2, cb4);
        }, cb3);
      }, cb2);
    });

    tasks.push(function(cb2) {
      vs.initVCs(vcsCfg, cb2);
    });

    async.series(tasks, function(err) {
      if (err) { cb(err); return; }

      // call either chroot or listen (listen calls chroot)
      var serverCfg = get(config, 'server');
      if (serverCfg && !serverCfg.disable) {
        log.info('preauth forking...');

        var preauthOpts = {
          logCfg: logCfg,
          serverConfig: serverCfg,
          chrootConfig: serverCfg
        };

        preauthOpts.logCfg = {};

        // copy global log config
        Object.keys(logCfg).forEach(function(key) {
          preauthOpts.logCfg[key] = logCfg[key];
        });

        preauthOpts.logCfg.file = log.getFileStream();
        preauthOpts.logCfg.error = log.getErrorStream();

        vs.listen(get(config, 'main.user') || 'nobody', get(config, 'main.chroot') || '/var/empty', preauthOpts, function(err) {
          if (err) { cb(err); return; }

          // find out if any remotes need to be initiated
          if (remoteLogin) {
            log.notice('sending pull request', debugReq(remoteLogin));
            sendPRs(vs, remoteLogin);
          }
          log.notice('ready');
        });
      } else {
        // chroot
        vs.chroot(get(config, 'main.user'), { path: get(config, 'main.chroot') });

        // find out if any remotes need to be initiated
        if (remoteLogin) {
          log.notice('sending pull request', debugReq(remoteLogin));
          sendPRs(vs, remoteLogin);
        }
        log.notice('ready');
      }
    });

    process.once('SIGINT', function() {
      log.notice('received SIGINT shutting down... press CTRL+C again to force quit');
      vs.stop(cb);
    });
    process.once('SIGTERM', function() {
      log.notice('received SIGTERM shutting down... send another SIGTERM to force quit');
      vs.stopTerm(cb);
    });
  })(function(err) {
    if (err) {
      // append stack trace
      Array.prototype.push.call(arguments, err.stack);
      log.crit.apply(log, arguments);
      process.exit(6);
    }

    log.info(new Date(), 'server down');
    log.info('runtime', new Date() - startTime);
    db.close();
    log.close();
  });
}

var database = config.database;
var dbCfg = {
  dbName: database.name || 'local',
  dbHost: database.path || database.host,
  dbPort: database.port,
  dbUser: database.user,
  dbPass: database.pass,
  authDb: database.authDb
};

logCfg.ident = programName;
logCfg.mask = logger.levelToPrio(logCfg.level) || logger.NOTICE;

logger(logCfg, function(err, l) {
  if (err) { throw err; }

  log = l;

  // open database
  _db(dbCfg, function(err, db) {
    if (err) {
      log.err('db', err);
      log.close(function(err) {
        if (err) { throw err; }
      });
      return;
    }
    start(db);
  });
});
