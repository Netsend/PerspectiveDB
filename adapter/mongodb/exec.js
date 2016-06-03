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
var mongodb = require('mongodb');
var posix = require('posix');
var xtend = require('xtend');

var logger = require('../../lib/logger');
var noop = require('../../lib/noop');
var OplogTransform = require('./oplog_transform');
var filterSecrets = require('../../lib/filter_secrets');

var ObjectID = mongodb.ObjectID;
var MongoClient = mongodb.MongoClient;

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
 *   [oplogTransformOpts]: {Object} // any oplog transform options
 *   [master]:       {Boolean}     // whether or not to enable writes
 *                                 // defaults to false
 *   [chroot]:       {String}      // defaults to /var/empty
 *   [user]:         {String}      // defaults to "_pdbnull"
 *   [group]:        {String}      // defaults to "_pdbnull"
 * }
 *
 * Only a kill message can be sent to signal the end of the process.
 *
 * As soon as the connection to mongodb is setup, and version and data channels are
 * ready, a "listen" message is emitted.
 */

// globals
var ot, coll, db, log; // used after receiving the log configuration

/**
 * Create an id for downstream based on the upstream id.
 *
 * @param {String} id  the id to use, converted to ObjectId if length is 24
 */
function createDownstreamId(id) {
  if (typeof id !== 'string') { throw new TypeError('id must be a string'); }
  // expect only one 0x01
  id = id.split('\x01', 2)[1];
  if (!id.length) { throw new Error('could not determine id'); }
  if (id.length === 24) {
    id = new ObjectID(id);
  }
  return id;
}

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
 * @param {Object} [opts]
 *
 * Options:
 *   any OplogTransform options
 */
function start(oplogDb, oplogCollName, ns, dataChannel, versionControl, opts) {
  // track newly written versions so that new items can be recognized correctly
  var expected = {};

  log.debug('start oplog transform...');
  ot = new OplogTransform(oplogDb, oplogCollName, ns, versionControl, versionControl, xtend(opts, {
    log: log,
    bson: true,
    expected: expected
  }));
  ot.pipe(dataChannel);
  ot.startStream();

  var bs = new BSONStream();
  dataChannel.pipe(bs).on('readable', function() {
    var obj = bs.read();

    if (!obj) {
      log.notice('local data channel closed, expecting shutdown');
      return;
    }

    // remove parent so that this item can be used later as a local confirmation
    delete obj.n.h.pa;

    expected[obj.n.h.id] = obj.n;

    if (obj.o == null || obj.o.b == null) { // insert
      if (obj.n == null) { throw new Error('new object expected'); }
      // ensure id matches the one in the header
      obj.n.b._id = createDownstreamId(obj.n.h.id);
      coll.insertOne(obj.n.b, function(err, r) {
        if (err) { throw err; }
        if (!r.insertedCount) {
          log.notice('item not inserted %j', obj.n.h);
        } else {
          log.debug('item inserted %j', obj.n.h);
        }
      });
    } else if (obj.n.h.d === true) { // delete
      if (obj.o == null) { throw new Error('old object expected'); }
      coll.deleteOne({ _id: obj.n.h.id }, function(err, r) {
        if (err) { throw err; }
        if (!r.deletedCount) {
          log.notice('item not deleted %j', obj.o.h);
        } else {
          log.debug('item deleted %j', obj.o.h);
        }
      });
    } else { // update
      // ensure id matches the one in the header
      obj.n.b._id = createDownstreamId(obj.n.h.id);
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
 *   [secrets]:      {Object}      // object containing the passwords for dbUser
 *                                 // and oplogDbUser
 *   [authDb]:       {String}      // authDb database, defaults to db from url
 *   [oplogAuthDb]:  {String}      // oplog authDb database, defaults to admin
 *   [oplogDb]:      {String}      // oplog database, defaults to local
 *   [oplogColl]:    {String}      // oplog collection, defaults to oplog.$main
 *   [oplogTransformOpts]: {Object} // any oplog transform options
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
  if (msg.secrets != null && typeof msg.secrets !== 'object') { throw new TypeError('msg.secrets must be an object'); }
  if (msg.authDb != null && typeof msg.authDb !== 'string') { throw new TypeError('msg.authDb must be a non-empty string'); }
  if (msg.oplogAuthDb != null && typeof msg.oplogAuthDb !== 'string') { throw new TypeError('msg.oplogAuthDb must be a non-empty string'); }
  if (msg.oplogDb != null && typeof msg.oplogDb !== 'string') { throw new TypeError('msg.oplogDb must be a non-empty string'); }
  if (msg.oplogColl != null && typeof msg.oplogColl !== 'string') { throw new TypeError('msg.oplogColl must be a non-empty string'); }
  if (msg.oplogTransformOpts != null && typeof msg.oplogTransformOpts !== 'object') { throw new TypeError('msg.oplogTransformOpts must be an object'); }
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
  if (dbUser && msg.secrets) {
    dbPass = msg.secrets[dbUser];
  }

  var oplogDbPass, oplogDbUser = msg.oplogDbUser;
  if (oplogDbUser && msg.secrets) {
    oplogDbPass = msg.secrets[oplogDbUser];
  }

  if (dbUser && !dbPass || !dbUser && dbPass) { throw new Error('provide both user and pass or none at all'); }
  if (oplogDbUser && !oplogDbPass || !oplogDbUser && oplogDbPass) { throw new Error('provide both oplog user and pass or none at all'); }

  // open log
  logger(msg.log, function(err, l) {
    if (err) { l.err(err); throw err; }

    log = l; // use this logger in the mt's as well

    log.debug('%j', filterSecrets(msg));

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
      chroot(newRoot, uid, gid);
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
      var shuttingDown = false;
      function shutdown() {
        if (shuttingDown) {
          log.info('shutdown already in progress');
          return;
        }
        shuttingDown = true;
        log.info('shutting down...');

        // disconnect from parent
        process.disconnect();

        async.series(shutdownTasks, function(err) {
          if (err) { log.err('shutdown error', err); }
          log.info('shutdown complete');
        });
      }

      async.series(startupTasks, function(err) {
        if (err) {
          log.crit('not all startup tasks are completed %j, exiting', err);
          process.exit(6);
        }

        process.send('listen');
        start(oplogDb, oplogCollName, dbName + '.' + collName, dataChannel, versionControl, msg.oplogTransformOpts);
      });

      // ignore kill signals
      process.once('SIGTERM', noop);
      process.once('SIGINT', noop);

      process.once('message', shutdown); // expect msg.type == kill
    }

    openConnectionAndProceed();
  });
});

process.send('init');
