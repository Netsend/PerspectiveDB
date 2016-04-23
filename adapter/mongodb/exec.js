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
var url = require('url');

var async = require('async');
var chroot = require('chroot');
var BSONStream = require('bson-stream');
var MongoClient = require('mongodb').MongoClient;
var posix = require('posix');
var xtend = require('xtend');

var logger = require('../../lib/logger');
var OplogTransform = require('./oplog_transform');
var filterSecrets = require('../../lib/filter_secrets');

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
 *   [user]:         {String}      // defaults to "_pdbnull"
 *   [group]:        {String}      // defaults to "_pdbnull"
 * }
 *
 * No other messages should be sent to this process.
 *
 * As soon as the connection to mongodb is setup, and version and data channels are
 * ready, a "listen" message is emitted.
 */

// globals
var ot, coll, db, log; // used after receiving the log configuration

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
 * @param {Object} [options]
 *
 * Options:
 *   versionKey {String, defaults to "_v"} version key in document
 */
function start(oplogDb, oplogCollName, ns, dataChannel, versionControl, options) {
  var opts = xtend({
    versionKey: '_v'
  }, options);

  log.debug('start oplog transform...');
  ot = new OplogTransform(oplogDb, oplogCollName, ns, versionControl, versionControl, { log: log, bson: true });
  ot.pipe(dataChannel);
  ot.startStream();

  var bs = new BSONStream();
  dataChannel.pipe(bs).on('readable', function() {
    var obj = bs.read();

    if (!obj) {
      log.notice('local data channel closed, expecting shutdown');
      return;
    }

    if (obj.o == null) { // insert
      if (obj.n == null) { throw new Error('new object expected'); }
      // set version on body
      obj.n.b[opts.versionKey] = obj.n.h.v;
      coll.insertOne(obj.n.b, function(err, r) {
        if (err) { throw err; }
        if (!r.insertedCount) {
          log.notice('item not inserted %j', obj.n.h);
        } else {
          log.debug('item inserted %j', obj.n.h);
        }
      });
    } else if (obj.n == null) { // delete
      if (obj.o == null) { throw new Error('old object expected'); }
      // set version on body
      obj.o.b[opts.versionKey] = obj.o.h.v;
      coll.deleteOne(obj.o.b, function(err, r) {
        if (err) { throw err; }
        if (!r.deletedCount) {
          log.notice('item not deleted %j', obj.o.h);
        } else {
          log.debug('item deleted %j', obj.o.h);
        }
      });
    } else { // update
      // set versions on body
      obj.o.b[opts.versionKey] = obj.o.h.v;
      obj.n.b[opts.versionKey] = obj.n.h.v;

      coll.findOneAndReplace(obj.o.b, obj.n.b, function(err, r) {
        if (err) { throw err; }
        if (!r.lastErrorObject.n) {
          log.notice('item not updated %j', obj.n.h);
        } else {
          log.debug('item updated %j', obj.n.h);
        }
      });
    }
  });
}

/**
 * Expect one init request (request that contains config)
 * {
 *   log:            {Object}      // log configuration
 *   url:            {String}      // mongodb connection string
 *   coll:           {String}      // collection name
 *   [dbUser]:       {String}      // user to read the collection
 *   [oplogDbUser]:  {String}      // user to read the oplog database collection
 *   [passdb]:       {Object}      // object containing the passwords for dbUser
 *                                 // and oplogDbUser
 *   [authDb]:       {String}      // authDb database, defaults to db from url
 *   [oplogAuthDb]:  {String}      // oplog authDb database, defaults to admin
 *   [oplogDb]:      {String}      // oplog database, defaults to local
 *   [oplogColl]:    {String}      // oplog collection, defaults to oplog.$main
 *   [versionKey]:   {String}      // version key in document, defaults to "_v"
 *   [mongoOpts]:    {Object}      // any other mongodb driver options
 *   [chroot]:       {String}      // defaults to /var/empty
 *   [user]:         {String}      // system user to run this process, defaults to
 *                                 // "_pdbnull"
 *   [group]:        {String}      // system group to run this process, defaults to
 *                                 // "_pdbnull"
 * }
 */
process.once('message', function(msg) {
  if (msg == null || typeof msg !== 'object') { throw new TypeError('msg must be an object'); }
  if (msg.log == null || typeof msg.log !== 'object') { throw new TypeError('msg.log must be an object'); }
  if (!msg.url || typeof msg.url !== 'string') { throw new TypeError('msg.url must be a non-empty string'); }
  if (!msg.coll || typeof msg.coll !== 'string') { throw new TypeError('msg.coll must be a non-empty string'); }

  if (msg.dbUser != null && typeof msg.dbUser !== 'string') { throw new TypeError('msg.dbUser must be a string'); }
  if (msg.oplogDbUser != null && typeof msg.oplogDbUser !== 'string') { throw new TypeError('msg.oplogDbUser must be a string'); }
  if (msg.passdb != null && typeof msg.passdb !== 'object') { throw new TypeError('msg.passdb must be an object'); }
  if (msg.authDb != null && typeof msg.authDb !== 'string') { throw new TypeError('msg.authDb must be a non-empty string'); }
  if (msg.oplogAuthDb != null && typeof msg.oplogAuthDb !== 'string') { throw new TypeError('msg.oplogAuthDb must be a non-empty string'); }
  if (msg.oplogDb != null && typeof msg.oplogDb !== 'string') { throw new TypeError('msg.oplogDb must be a non-empty string'); }
  if (msg.oplogColl != null && typeof msg.oplogColl !== 'string') { throw new TypeError('msg.oplogColl must be a non-empty string'); }
  if (msg.versionKey != null && typeof msg.versionKey !== 'string') { throw new TypeError('msg.versionKey must be a non-empty string'); }
  if (msg.mongoOpts != null && typeof msg.mongoOpts !== 'object') { throw new TypeError('msg.mongoOpts must be an object'); }
  if (msg.chroot != null && typeof msg.chroot !== 'string') { throw new TypeError('msg.chroot must be a string'); }
  if (msg.user != null && typeof msg.user !== 'string') { throw new TypeError('msg.user must be a string'); }
  if (msg.group != null && typeof msg.group !== 'string') { throw new TypeError('msg.group must be a string'); }

  var oplogDbName = msg.oplogDb || 'local';
  var oplogCollName = msg.oplogColl || 'oplog.$main';

  var parsedUrl = url.parse(msg.url);
  var dbName = parsedUrl.pathname.slice(1); // strip prefixed "/"
  if (!dbName) { throw new Error('url must contain a database name'); }

  var collName = msg.coll;

  programName = 'mongodb';

  process.title = 'pdb/' + programName;

  var user = msg.user || '_pdbnull';
  var group = msg.group || '_pdbnull';

  var newRoot = msg.chroot || '/var/empty';

  msg.log.ident = programName;

  var oplogDb; // handle to db and oplog db connection
  var dataChannel; // expect a data request/receive channel on fd 6
  var versionControl; // expect a version request/receive channel on fd 7

  var dbPass, dbUser = msg.dbUser;
  if (dbUser && msg.passdb) {
    dbPass = msg.passdb[dbUser];
  }

  var oplogDbPass, oplogDbUser = msg.oplogDbUser;
  if (oplogDbUser && msg.passdb) {
    oplogDbPass = msg.passdb[oplogDbUser];
  }

  if (dbUser && !dbPass || !dbUser && dbPass) { throw new Error('provide both user and pass or none at all'); }
  if (oplogDbUser && !oplogDbPass || !oplogDbUser && oplogDbPass) { throw new Error('provide both oplog user and pass or none at all'); }

  // open log
  logger(msg.log, function(err, l) {
    if (err) { l.err(err); throw err; }

    log = l; // use this logger in the mt's as well

    log.debug('%j', filterSecrets(msg));

    try {
      posix.getpwnam(user).uid;
      posix.getgrnam(group).gid;
    } catch(err) {
      log.err('%s %s:%s', err, user, group);
      process.exit(3);
    }

    // chroot or exit
    try {
      chroot(newRoot, user, group);
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
        MongoClient.connect(msg.url, function(err, dbc) {
          if (err) { log.err('connect error: %s', err); cb(err); return; }
          if (dbc.databaseName !== dbName) { cb(new Error('connected to the wrong database')); return; }

          log.notice('connected %s', msg.url);
          db = dbc;
          // setup oplog db
          oplogDb = db.db(oplogDbName);
          cb();
        });
      });

      // auth to db if necessary
      if (dbUser) {
        startupTasks.push(function(cb) {
          var authDbName = msg.authDb || dbName;
          var authDb = db.db(authDbName);
          log.debug('auth to %s as %s', authDbName, dbUser);
          authDb.authenticate(dbUser, dbPass, function(err) {
            if (err) { log.err('auth to %s as %s failed: %s', authDbName, dbUser, err); cb(err); return; }
            cb();
          });
        });
      }

      // auth to oplog db if necessary
      if (oplogDbUser) {
        startupTasks.push(function(cb) {
          var authDbName = msg.oplogAuthDb || 'admin';
          log.debug('auth oplog to %s as %s', authDbName, oplogDbUser);
          var authDb = db.db(authDbName);
          authDb.authenticate(oplogDbUser, oplogDbPass, function(err) {
            if (err) { log.err('auth to %s as %s failed: %s', authDbName, oplogDbUser, err); cb(err); return; }
            cb();
          });
        });
      }

      // setup coll
      startupTasks.push(function(cb) {
        coll = db.collection(collName);
        process.nextTick(cb);
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

      /////////// SHUTDOWN TASKS

      shutdownTasks.push(function(cb) {
        if (ot) {
          log.notice('closing oplog transform...');
          ot.close(cb);
        } else {
          log.notice('no oplog transform active');
          process.nextTick(cb);
        }
      });

      shutdownTasks.push(function(cb) {
        if (dataChannel.writable) {
          log.info('closing data channel...');
          dataChannel.end(cb);
        } else {
          log.info('data channel already closed');
          dataChannel.destroy();
          process.nextTick(cb);
        }
      });

      shutdownTasks.push(function(cb) {
        if (versionControl.writable) {
          log.info('closing version control...');
          versionControl.end(cb);
        } else {
          log.info('version control already closed');
          versionControl.destroy();
          process.nextTick(cb);
        }
      });

      shutdownTasks.push(function(cb) {
        log.info('closing database connection...');
        oplogDb.close(cb);
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
          log.crit('not all startup tasks are completed %j, exiting', err);
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
