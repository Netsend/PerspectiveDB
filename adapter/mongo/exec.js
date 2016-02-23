/**
 * Copyright 2016 Netsend.
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

/* jshint -W116 */

'use strict';

var programName = require('path').basename(__filename, '.js');

if (process.getuid() !== 0) {
  console.error('%s: execute as root', programName);
  process.exit(1);
}

if (typeof process.send !== 'function') {
  console.error('%s: use child_process.spawn to invoke this module', programName);
  process.exit(2);
}

var net = require('net');

var async = require('async');
var chroot = require('chroot');
var MongoClient = require('mongodb').MongoClient;
var posix = require('posix');

var logger = require('../../lib/logger');
var OplogTransform = require('./oplog_transform');

/**
 * Instantiate a connection to a mongo database and read changes of a collection
 * via the oplog. Furthermore accept new versions from a db_exec instance and try
 * to write them to the mongo collection.
 *
 * Full FSM: init --> listen
 *
 * The first message emitted is "init" which signals that this process is ready to
 * receive the configuration data. File desciptors for the log can optionally be
 * set at fd 4 and 5. A version control channel is expected at fd 7 and a data
 * exchange channel is expected at fd 6.
 *
 * {
 *   log:            {Object}      // log configuration
 *   [url]:          {String}      // connection string
 *                                 // defaults to 127.0.0.1:27017
 *   [mongoOpts]:    {Object}      // any mongodb driver options
 *   [master]:       {Boolean}     // whether or not to enable writes
 *                                 // defaults to false
 *   [chroot]:       {String}      // defaults to /var/empty
 *   [user]:         {String}      // defaults to "nobody"
 *   [group]:        {String}      // defaults to "nobody"
 * }
 *
 * No other messages should be sent to this process.
 *
 * As soon as the connection to mongodb is setup, and version and data channels are
 * ready, a "listen" message is emitted.
 */

// globals
var log; // used after receiving the log configuration

/**
 * Start oplog transformer:
 *  * pass new versions and confirms to data channel
 *  * receive new versions with parent from data channel
 *
 * @param {mongodb.Db} oplogDb  connection to the oplog database
 * @param {String} oplogCollName  oplog collection name, defaults to oplog.$main
 * @param {String} ns  namespace of the database.collection to follow
 * @param {Object} versionControl  version control channel
 * @param {Object} dataChannel  data channel
 * @param {Object} versionControl  version control channel
 */
function start(oplogDb, oplogCollName, ns, dataChannel, versionControl) {
  log.debug('start oplog transform...');
  var ot = new OplogTransform(oplogDb, oplogCollName, ns, versionControl, versionControl, { log: log, bson: true });
  ot.pipe(dataChannel);
  ot.startStream();

  // TODO: handle new incoming objects
}

/**
 * Expect one init request (request that contains config)
 * {
 *   log:            {Object}      // log configuration
 *   db:             {String}      // database name
 *   coll:           {String}      // collection name
 *   [authorative]:  {Boolean}     // whether or not this collection should be
 *                                 // treated as an authorative source, defaults to
 *                                 // false. If true, an id/version request channel
 *                                 // is setup and sent back with the listen event
 *   [url]:          {String}      // mongodb connection string, defaults
 *                                 // to mongodb://127.0.0.1:27017/local
 *   [oplogDb]:      {String}      // oplog database, defaults to local
 *   [oplogColl]:    {String}      // oplog collection, defaults to oplog.$main
 *   [mongoOpts]:    {Object}      // any other mongodb driver options
 *   [chroot]:       {String}      // defaults to /var/empty
 *   [user]:         {String}      // defaults to "nobody"
 *   [group]:        {String}      // defaults to "nobody"
 * }
 */
process.once('message', function(msg) {
  if (msg == null || typeof msg !== 'object') { throw new TypeError('msg must be an object'); }
  if (msg.log == null || typeof msg.log !== 'object') { throw new TypeError('msg.log must be an object'); }
  if (!msg.db || typeof msg.db !== 'string') { throw new TypeError('msg.db must be a non-empty string'); }
  if (!msg.coll || typeof msg.coll !== 'string') { throw new TypeError('msg.coll must be a non-empty string'); }

  if (msg.authorative != null && typeof msg.authorative !== 'boolean') { throw new TypeError('msg.authorative must be a boolean'); }
  if (msg.url != null && typeof msg.url !== 'string') { throw new TypeError('msg.url must be a non-empty string'); }
  if (msg.oplogDb != null && typeof msg.oplogDb !== 'string') { throw new TypeError('msg.oplogDb must be a non-empty string'); }
  if (msg.oplogColl != null && typeof msg.oplogColl !== 'string') { throw new TypeError('msg.oplogColl must be a non-empty string'); }
  if (msg.mongoOpts != null && typeof msg.mongoOpts !== 'object') { throw new TypeError('msg.mongoOpts must be an object'); }
  if (msg.chroot != null && typeof msg.chroot !== 'string') { throw new TypeError('msg.chroot must be a string'); }
  if (msg.user != null && typeof msg.user !== 'string') { throw new TypeError('msg.user must be a string'); }
  if (msg.group != null && typeof msg.group !== 'string') { throw new TypeError('msg.group must be a string'); }

  var dbName = msg.db;
  var collName = msg.coll;
  var authorative = !!msg.authorative;
  var url = msg.url || 'mongodb://127.0.0.1:27017/local';
  var oplogDbName = msg.oplogDb || 'local';
  var oplogCollName = msg.oplogColl || 'oplog.$main';

  programName = 'mongo';

  process.title = 'pdb/' + programName;

  var user = msg.user || 'nobody';
  var group = msg.group || 'nobody';

  var newRoot = msg.chroot || '/var/empty';

  msg.log.ident = programName;

  var db, oplogDb; // handle to db and oplog db connection
  var dataChannel; // expect a data request/receive channel on fd 6
  var versionControl; // expect a version request/receive channel on fd 7

  // open log
  logger(msg.log, function(err, l) {
    if (err) { l.err(err); throw err; }

    log = l; // use this logger in the mt's as well

    var uid, gid;
    try {
      uid = posix.getpwnam(user).uid;
      gid = posix.getgrnam(group).gid;
    } catch(err) {
      log.err('%s %s:%s', err, user, group);
      process.exit(3);
    }

    // chroot or exit
    try {
      chroot(newRoot, user, gid);
      log.info('changed root to %s and user:group to %s:%s', newRoot, user, group);
    } catch(err) {
      log.err('changing root or user failed %j %s', msg, err);
      process.exit(8);
    }

    // open connection
    function openConnectionAndProceed() {
      var startupTasks = [];
      var shutdownTasks = [];

      // connect to the database
      startupTasks.push(function(cb) {
        log.debug('connect to database...');
        MongoClient.connect(url, function(err, dbc) {
          if (err) { log.err('connect error: %s', err); cb(err); return; }

          log.notice('connected %s', url);

          if (dbc.databaseName === dbName) {
            db = dbc;
          } else {
            db = dbc.db(dbName);
          }

          if (dbc.databaseName === oplogDbName) {
            oplogDb = dbc;
          } else {
            oplogDb = dbc.db(oplogDbName);
          }

          cb();
        });
      });

      // expect a data request/receive channel on fd 6
      startupTasks.push(function(cb) {
        log.debug('setup data channel...');
        dataChannel = new net.Socket({ fd: 6, readable: true, writable: true });
        cb();
      });

      // expect a version request/receive channel on fd 7
      startupTasks.push(function(cb) {
        log.debug('setup version control...');
        versionControl = new net.Socket({ fd: 7, readable: true, writable: true });
        cb();
      });

      shutdownTasks.push(function(cb) {
        log.info('closing data channel...');
        dataChannel.end(cb);
      });

      shutdownTasks.push(function(cb) {
        log.info('closing version control...');
        versionControl.end(cb);
      });

      shutdownTasks.push(function(cb) {
        log.info('closing database connection...');
        db.close(cb);
      });

      // handle shutdown
      function shutdown() {
        log.notice('shutting down');
        async.series(shutdownTasks, function(err) {
          if (err) { log.err('shutdown error', err); }
          if (process.connected) {
            log.info('disconnect from parent');
            process.disconnect();
          }
          log.info('shutdown complete');
        });
      }

      async.series(startupTasks, function(err) {
        if (err) {
          log.crit('not all startup tasks are completed, exiting');
          process.exit(6);
        }

        process.send('listen');
        start(oplogDb, oplogCollName, dbName + '.' + collName, dataChannel, versionControl);
      });

      // listen to kill signals
      process.once('SIGINT', shutdown);
      process.once('SIGTERM', shutdown);
    }

    openConnectionAndProceed();
  });
});

process.send('init');
