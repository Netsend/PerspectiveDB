#!/usr/bin/env node

/**
 * Copyright 2015 Netsend.
 *
 * This file is part of PersDB.
 *
 * PersDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PersDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PersDB. If not, see <https://www.gnu.org/licenses/>.
 */

/**
 * Start the PersDB server.
 *
 * - Parse options
 * - Read config
 * - Init master
 * - Start servers and clients
 */

'use strict';

var path = require('path');
var programName = path.basename(__filename, '.js');
var dirname = path.dirname;

if (process.getuid() !== 0) {
  console.error('%s: execute as root', programName);
  process.exit(1);
}

var fs = require('fs');

var async = require('async');
var hjson = require('hjson');
var program = require('commander');

var logger = require('../lib/logger');
var Master = require('../lib/master');

program
  .version(require('../package.json').version)
  .usage('config.hjson')
  .parse(process.argv);

var log;

var startTime = new Date();

var configFile = program.args[0];

if (!configFile) {
  program.help();
}

var config = hjson.parse(fs.readFileSync(configFile, 'utf8'));

// prepend the dirname of the config file to all relative path specifications in the config file
if (config.wss) {
  if (config.wss.cert && config.wss.cert[0] !== '/') {
    config.wss.cert = dirname(configFile) + '/' + config.wss.cert;
  }
  if (config.wss.key && config.wss.key[0] !== '/') {
    config.wss.key = dirname(configFile) + '/' + config.wss.key;
  }
  if (config.wss.dhparam && config.wss.dhparam[0] !== '/') {
    config.wss.dhparam = dirname(configFile) + '/' + config.wss.dhparam;
  }
}

var logCfg = config.log || {};

function start() {
  (function(cb) {
    var error;
    var tasks = [];

    // load password hashes from config or password files for each perspective
    var files = {};
    config.dbs.forEach(function(dbCfg) {
      dbCfg.perspectives.forEach(function(persCfg) {
        var username = persCfg.username || persCfg.name;
        var password = persCfg.password || persCfg.users;

        // support loading from a file
        if (password.indexOf('$2a$') !== 0) {
          var file = password;
          // prepend the dirname of the config file to all relative path specifications in the config file
          if (file && file[0] !== '/') {
            file = dirname(configFile) + '/' + file;
          }
          var stats = fs.statSync(file);
          if ((stats.mode & 6) !== 0) {
            error = new Error('should not be world readable or writable');
            console.error('%s: %s: %s', programName, file, error);
            cb(error);
            return;
          }
          if (!files[file]) {
            files[file] = hjson.parse(fs.readFileSync(file, { encoding: 'utf8' }));
          }
          password = files[file][username];
        }

        if (!username) {
          error = new Error('no name or username configured');
          console.error('%s: %s: %s', programName, dbCfg.name, error);
          cb(error);
          return;
        }
        if (password.indexOf('$2a$') !== 0) {
          error = new Error('no password configured');
          console.error('%s: %s %s: %s', programName, dbCfg.name, persCfg.name, error);
          cb(error);
          return;
        }

        // set one user account
        persCfg.users = [{ username: username, password: password, realm: dbCfg.name }];
      });
    });

    // setup logging files
    tasks.push(function(cb2) {
      // ensure specific log configurations overrule the global log config
      async.eachSeries(config.dbs, function(dbCfg, cb3) {
        log.info('loading db: %s', dbCfg.name);

        dbCfg.log = dbCfg.log || {};

        var dbLog = {};

        // copy global log config
        Object.keys(logCfg).forEach(function(key) {
          dbLog[key] = logCfg[key];
        });

        var gfile = log.getFileStream();
        var gerror = log.getErrorStream();

        if (gfile) { dbLog.file = gfile; }
        if (gerror) { dbLog.error = gerror; }

        // overrule with db specific log config
        Object.keys(dbCfg.log).forEach(function(key) {
          dbLog[key] = dbCfg.log[key];
        });

        if (dbCfg.log.level) {
          dbLog.mask = logger.levelToPrio(dbCfg.log.level);
        }

        // ensure async even without any other tasks
        var tasks2 = [function(cb4) { process.nextTick(cb4); }];

        if (dbCfg.log.file) {
          tasks2.push(function(cb4) {
            logger.openfile(dbCfg.log.file, function(err, f) {
              if (err) { cb4(err); return; }
              dbLog.file = f;
              cb4();
            });
          });
        } else {
          dbLog.file = log.getFileStream();
        }

        if (dbCfg.log.error) {
          tasks2.push(function(cb4) {
            logger.openfile(dbCfg.log.error, function(err, f) {
              if (err) { cb4(err); return; }
              dbLog.error = f;
              cb4();
            });
          });
        } else {
          dbLog.error = log.getErrorStream();
        }

        dbCfg.log = dbLog;
        async.series(tasks2, cb3);
      }, cb2);
    });

    tasks.push(function(cb2) {
      config.log = log;
      var master = new Master(config.dbs, config);

      process.once('SIGINT', function() {
        log.notice('received SIGINT shutting down... press CTRL+C again to force quit');
        master.stopTerm(cb);
      });
      process.once('SIGTERM', function() {
        log.notice('received SIGTERM shutting down... send another SIGTERM to force quit');
        master.stopTerm(cb);
      });

      master.start(cb2);
    });

    async.series(tasks, cb);
  })(function(err) {
    if (err) {
      // append stack trace
      Array.prototype.push.call(arguments, err.stack);
      log.crit.apply(log, arguments);
      process.exit(2);
    }

    log.notice('server down %s', new Date());
    log.info('runtime', new Date() - startTime);
    log.close();
  });
}

logCfg.ident = programName;
logCfg.mask = logger.levelToPrio(logCfg.level) || logger.NOTICE;

logger(logCfg, function(err, l) {
  if (err) { throw err; }

  log = l;

  log.notice('init %s', startTime);
  start();
});
