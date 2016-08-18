/**
 * Copyright 2016 Netsend.
 *
 * This file is part of PerspectiveDB.
 *
 * PerspectiveDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PerspectiveDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PerspectiveDB. If not, see <https://www.gnu.org/licenses/>.
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
var stream = require('stream');
var url = require('url');

var async = require('async');
var chroot = require('chroot');
var BSONStream = require('bson-stream');
var mongodb = require('mongodb');
var posix = require('posix');
var xtend = require('xtend');

var isEqual = require('../../lib/is_equal');
var findAndDelete = require('./find_and_delete');
var logger = require('../../lib/logger');
var noop = require('../../lib/noop');
var OplogTransform = require('./oplog_transform');
var filterSecrets = require('../../lib/filter_secrets');

var MongoClient = mongodb.MongoClient;
var ObjectID = mongodb.ObjectID;

/**
 * Determine collection by splitting h.id on the first 0x01
 *
 * @param {Object} header  pdb DAG header
 * @return {String|false} name of the collection or false if none found
 */
function getMongoColl(h) {
  var res = h.id.split('\x01', 2);
  if (res.length !== 2)
    return false;
  if (!res[0]) // if falsy
    return false;
  return res[0];
}

/**
 * Determine id for mongodb. Try to get m._id, split h.id on 0x01 or just get h.id
 * without processing as a last resort.
 *
 * @param {Object} dagItem  item from the pdb DAG
 * @return {mixed} id for use in a mongodb collection
 */
function getMongoId(obj) {
  var id;
  try {
    id = obj.n.m._id;
  } catch (err) {
    // if from a non-mongo perspective, fallback
    id = obj.n.h.id;
  }

  if (typeof id === 'string') {
    // expect zero or one 0x01 byte
    var nid = id.split('\x01', 2)[1];
    if (!nid || !nid.length) {
      nid = id;
    }
    if (!nid.length) { throw new Error('could not determine id'); }

    // auto convert 24 character strings to ObjectIDs
    if (nid.length === 24) {
      try {
        nid = new ObjectID(nid);
      } catch(err) {
        log.err('could not convert id to object id %s %s', id, err);
      }
    }
  }
  return nid;
}

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
var ot, db, log; // used after receiving the log configuration
var noCollectionWhitelist; // automatically create new collections

/**
 * Start oplog transformer:
 *  * pass new versions and confirms to data channel
 *  * receive new versions with parent from data channel
 *
 * @param {mongodb.Db} oplogDb  connection to the oplog database
 * @param {String} oplogCollName  oplog collection name, defaults to oplog.$main
 * @param {String} dbName  database to follow
 * @param {mongodb.Collection[]} collections  track these collections
 * @param {Object} versionControl  version control channel
 * @param {Object} dataChannel  data channel
 * @param {mongodb.Collection} conflictCollection  conflict collection
 * @param {Object} [opts]
 *
 * Options:
 *   any OplogTransform options
 */
function start(oplogDb, oplogCollName, dbName, collections, dataChannel, versionControl, conflictCollection, opts) {
  // create a hash of collections by collection name
  var collectionMap = {};
  collections.forEach(function(collection) {
    collectionMap[collection.collectionName] = collection;
  });

  // track newly written versions so that new oplog items can be recognized correctly as a confirmation
  var expected = [];

  log.debug2('start oplog transform...');
  ot = new OplogTransform(oplogDb, oplogCollName, dbName, collections, versionControl, versionControl, expected, xtend(opts, {
    log: log,
    bson: true
  }));
  ot.pipe(dataChannel);
  ot.startStream();

  // save an object in the conflict collection
  function handleConflict(origErr, obj, cb) {
    // remove from expected
    findAndDelete(expected, item => { item === obj; });
    obj.err = origErr.message;
    if (~['delete failed', 'insert failed', 'update failed'].indexOf(obj.err)) {
      obj.errCode = 'adapterQueryFailed';
    }
    conflictCollection.insert(obj, function(err, r) {
      if (err || !r.insertedCount) {
        log.err('could not save conflicting object %s %j, original error %s', err, obj.n.h, origErr);
        cb(err);
        return;
      }

      log.debug('conflict saved %j, op: %s', obj.n.h, r.result.opTime);
      cb();
    });
  }

  dataChannel.pipe(new BSONStream()).pipe(new stream.Writable({
    objectMode: true,
    write: function(obj, enc, cb) {
      if (obj.c) {
        handleConflict(new Error('upstream merge conflict'), obj, cb);
        return;
      }

      log.debug('new item %j', obj.n.h);

      // expect this item back via the oplog if all goes well
      expected.push(obj);

      // compare collection with local head on non-existing keys (mongo query can only filter on existing keys)
      // TODO: protect against race conditions in combination with bad power loss timings.
      // Currently if there is a race condition and an item is updated by a third party right after the following check and
      // furthermore the system exits for whatever reason right after the update and before checking for a race condition, then
      // data is unconditionally and undetectably lost.
      // Because the find queries are done using all attributes, this only happens if the third party added a key, not when
      // an existing key is updated or deleted.
      // This can only be solved if the update query somehow can specify that no other keys must exist in the document.
      var collName = getMongoColl(obj.n.h);
      if (!collName) {
        log.notice('collection could not be determined');
        handleConflict(new Error('could not determine collection'), obj, cb);
        return;
      }
      var coll = collectionMap[collName];
      if (!coll) {
        if (noCollectionWhitelist) {
          // add new collection
          coll = db.collection(collName);
          collectionMap[collName] = coll;
        } else {
          log.debug('item belongs to unspecified collection: %s', collName);
          handleConflict(new Error('unspecified collection'), obj, cb);
          return;
        }
      }

      var mongoId = getMongoId(obj);
      coll.find({ _id: mongoId }).limit(1).next(function(err, doc) {
        if (err) { handleConflict(err, obj, cb); return; }
        var collItem = doc;
        var lhead;

        if (obj.n.h.d === true) { // delete
          if (obj.l == null) { handleConflict(new Error('local head expected'), obj, cb); return; }
          if (!collItem) {
            log.debug('nothing in collection by %s', mongoId);
            handleConflict(new Error('item in collection expected'), obj, cb);
            return;
          }

          // ensure a current version with id based on the given local head
          lhead = xtend(obj.l.b, { _id: mongoId });
          if (!isEqual(collItem, lhead)) { handleConflict(new Error('collection and local head are not equal'), obj, cb); return; }

          coll.findOneAndDelete(lhead, function(err, r) {
            if (err || !r.ok) {
              log.notice('NO delete %j, err: %s', obj.n.h, err);
              handleConflict(err || new Error('delete failed'), obj, cb);
              return;
            }
            if (!isEqual(r.value, lhead)) {
              obj.expected = collItem;
              obj.replaced = r.value;
              handleConflict(new Error('race condition on delete'), obj, cb);
              return;
            }
            log.debug('delete %j', obj.n.h);
            cb();
          });
        } else if (obj.l == null || obj.l.b == null) { // insert
          if (obj.n == null) { handleConflict(new Error('new object expected'), obj, cb); return; }
          if (collItem) { handleConflict(new Error('no item in collection expected'), obj, cb); return; }

          // put mongo id on the document
          coll.insertOne(xtend(obj.n.b, { _id: mongoId }), function(err, r) {
            if (err || !r.insertedCount) {
              log.notice('NO insert %j', obj.n.h);
              handleConflict(err || new Error('insert failed'), obj, cb);
              return;
            }
            log.debug('insert %j', obj.n.h);
            cb();
          });
        } else { // update
          if (obj.l == null) { handleConflict(new Error('local head expected'), obj, cb); return; }
          if (!collItem) { handleConflict(new Error('item in collection expected'), obj, cb); return; }

          // put mongo id back on the document
          lhead = xtend(obj.l.b, { _id: mongoId });
          if (!isEqual(collItem, lhead)) { handleConflict(new Error('collection and local head are not equal'), obj, cb); return; }

          coll.findOneAndReplace(lhead, xtend(obj.n.b, { _id: mongoId }), function(err, r) {
            if (err || !r.ok) {
              log.notice('NO update %j', obj.n.h);
              handleConflict(err || new Error('update failed'), obj, cb);
              return;
            }
            if (!isEqual(r.value, lhead)) {
              obj.expected = collItem;
              obj.replaced = r.value;
              handleConflict(new Error('race condition on update'), obj, cb);
              return;
            }
            log.debug('update %j', obj.n.h);
            cb();
          });
        }
      });
    }
  })).on('end', function() {
    log.notice('local data channel closed, expecting shutdown');
  });
}

/**
 * Expect one init request (request that contains config)
 * {
 *   log:            {Object}         log configuration
 *   url:            {String}         mongodb connection string
 *   [collections]:  {String|Array}   collection name or names, default to all
 *                                    collections in the database except the mongo
 *                                    system collections
 *   [dbUser]:       {String}         user to read the collection
 *   [oplogDbUser]:  {String}         user to read the oplog database collection
 *   [secrets]:      {Object}         object containing the passwords for dbUser
 *                                    and oplogDbUser
 *   [authDb]:       {String}         authDb database, defaults to db from url
 *   [oplogAuthDb]:  {String}         oplog authDb database, defaults to admin
 *   [conflictDb]:   {String}         conflict database, defaults to db from url
 *   [conflictCollection]: {String, default conflicts}     conflict collection
 *   [tmpDb]:        {String}         tmp database, defaults to db from url
 *   [tmpCollection]: {String, default _pdbtmp} tmp collection
 *   [oplogDb]:      {String}         oplog database, defaults to local
 *   [oplogColl]:    {String}         oplog collection, defaults to oplog.$main
 *   [oplogTransformOpts]: {Object}    any oplog transform options
 *   [chroot]:       {String}         defaults to /var/empty
 *   [user]:         {String}         system user to run this process, defaults to
 *                                    "_pdbnull"
 *   [group]:        {String}         system group to run this process, defaults to
 *                                    "_pdbnull"
 * }
 */
process.once('message', function(msg) {
  if (msg == null || typeof msg !== 'object') { throw new TypeError('msg must be an object'); }
  if (msg.log == null || typeof msg.log !== 'object') { throw new TypeError('msg.log must be an object'); }
  if (!msg.url || typeof msg.url !== 'string') { throw new TypeError('msg.url must be a non-empty string'); }

  if (msg.collections != null && typeof msg.collections !== 'string' && !Array.isArray(msg.collections)) { throw new TypeError('msg.collections must be an array or a non-empty string'); }
  if (msg.dbUser != null && typeof msg.dbUser !== 'string') { throw new TypeError('msg.dbUser must be a string'); }
  if (msg.oplogDbUser != null && typeof msg.oplogDbUser !== 'string') { throw new TypeError('msg.oplogDbUser must be a string'); }
  if (msg.secrets != null && typeof msg.secrets !== 'object') { throw new TypeError('msg.secrets must be an object'); }
  if (msg.authDb != null && typeof msg.authDb !== 'string') { throw new TypeError('msg.authDb must be a non-empty string'); }
  if (msg.oplogAuthDb != null && typeof msg.oplogAuthDb !== 'string') { throw new TypeError('msg.oplogAuthDb must be a non-empty string'); }
  if (msg.conflictDb != null && typeof msg.conflictDb !== 'string') { throw new TypeError('msg.conflictDb must be a non-empty string'); }
  if (msg.conflictCollection != null && typeof msg.conflictCollection !== 'string') { throw new TypeError('msg.conflictCollection must be a non-empty string'); }
  if (msg.tmpDb != null && typeof msg.tmpDb !== 'string') { throw new TypeError('msg.tmpDb must be a non-empty string'); }
  if (msg.tmpCollection != null && typeof msg.tmpCollection !== 'string') { throw new TypeError('msg.tmpCollection must be a non-empty string'); }
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

  var ns = dbName;
  var collNames = msg.collections || [];
  if (typeof collNames === 'string') {
    collNames = [collNames];
  }
  var oplogTransformOpts = xtend({}, msg.oplogTransformOpts);

  var conflictDb = msg.conflictDb || dbName;
  var conflictCollection = msg.conflictCollection || 'conflicts';

  var tmpDb = msg.tmpDb || dbName;
  var tmpCollection = msg.tmpCollection || '_pdbtmp';

  programName = 'mongodb ' + ns;

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
      log.debug2('changed root to %s and user:group to %s:%s', newRoot, user, group);
    } catch(err) {
      log.err('changing root or user failed %j %s', msg, err);
      process.exit(8);
    }

    // set core limit to maximum allowed size
    posix.setrlimit('core', { soft: posix.getrlimit('core').hard });
    log.debug2('core limit: %j, fsize limit: %j', posix.getrlimit('core'), posix.getrlimit('fsize'));

    // open connection
    function openConnectionAndProceed() {
      var startupTasks = [];
      var shutdownTasks = [];

      // connect to the database
      startupTasks.push(function(cb) {
        log.debug2('connect to database...');
        MongoClient.connect(msg.url, function(err, dbc) {
          if (err) { log.err('connect error: %s', err); cb(err); return; }
          if (dbc.databaseName !== dbName) { cb(new Error('connected to the wrong database')); return; }

          log.debug2('connected %s', msg.url);
          // setup dbs
          db = dbc;
          oplogDb = db.db(oplogDbName);
          cb();
        });
      });

      // auth to db if necessary
      if (dbUser) {
        startupTasks.push(function(cb) {
          var authDbName = msg.authDb || dbName;
          var authDb = db.db(authDbName);
          log.debug('auth as %s on %s for %s', dbUser, authDbName, dbName);
          authDb.authenticate(dbUser, dbPass, function(err) {
            if (err) {
              log.err('auth as %s on %s for %s failed: %s', dbUser, authDbName, dbName, err);
              cb(err);
              return;
            }
            cb();
          });
        });
      }

      // auth to oplog db if necessary
      if (oplogDbUser) {
        startupTasks.push(function(cb) {
          var authDbName = msg.oplogAuthDb || 'admin';
          log.debug('auth oplog as %s on %s for %s', oplogDbUser, authDbName, oplogDbName);
          var authDb = db.db(authDbName);
          authDb.authenticate(oplogDbUser, oplogDbPass, function(err) {
            if (err) {
              log.err('auth oplog as %s on %s for %s failed: %s', oplogDbUser, authDbName, oplogDbName, err);
              cb(err);
              return;
            }
            cb();
          });
        });
      }

      // default to all collections if none given
      if (!collNames.length) {
        noCollectionWhitelist = true;
        startupTasks.push(function(cb) {
          db.collections(function(err, collections) {
            if (err) { cb(err); return; }
            // blacklist mongo system collections
            var blacklist = ['system.users', 'system.profile', 'system.indexes', conflictCollection, tmpCollection];
            collections = collections.filter(coll => !~blacklist.indexOf(coll.collectionName));
            collNames = collections.map(coll => coll.collectionName);
            cb();
          });
        });
      }

      // open each collection for local use
      var collections = [];
      startupTasks.push(function(cb) {
        collNames.forEach(function(collection) {
          collections.push(db.collection(collection));
        });
        process.nextTick(cb);
      });

      // setup conflict coll
      startupTasks.push(function(cb) {
        conflictCollection = db.db(conflictDb).collection(conflictCollection);
        process.nextTick(cb);
      });

      // setup tmp coll
      startupTasks.push(function(cb) {
        tmpCollection = db.db(tmpDb).collection(tmpCollection);
        process.nextTick(cb);
      });

      // expect a data request/receive channel on fd 6
      startupTasks.push(function(cb) {
        log.debug2('setup data channel...');
        dataChannel = new net.Socket({ fd: 6, readable: true, writable: true });
        cb();
      });

      // expect a version request/receive channel on fd 7
      startupTasks.push(function(cb) {
        log.debug2('setup version control...');
        versionControl = new net.Socket({ fd: 7, readable: true, writable: true });
        cb();
      });

      /////////// SHUTDOWN TASKS

      shutdownTasks.push(function(cb) {
        if (ot) {
          log.debug2('closing oplog transform...');
          ot.close(cb);
        } else {
          log.debug('no oplog transform active');
          process.nextTick(cb);
        }
      });

      shutdownTasks.push(function(cb) {
        if (dataChannel.writable) {
          log.debug2('closing data channel...');
          dataChannel.end(cb);
        } else {
          log.debug('data channel already closed');
          dataChannel.destroy();
          process.nextTick(cb);
        }
      });

      shutdownTasks.push(function(cb) {
        if (versionControl.writable) {
          log.debug2('closing version control...');
          versionControl.end(cb);
        } else {
          log.debug('version control already closed');
          versionControl.destroy();
          process.nextTick(cb);
        }
      });

      shutdownTasks.push(function(cb) {
        log.debug2('closing database connection...');
        oplogDb.close(cb);
      });

      // handle shutdown
      var shuttingDown = false;
      function shutdown() {
        if (shuttingDown) {
          log.notice('shutdown already in progress');
          return;
        }
        shuttingDown = true;
        log.debug('shutting down...');

        async.series(shutdownTasks, function(err) {
          if (err) { log.err('shutdown error', err); }
          log.debug('shutdown complete');
          // XXX: somehow, if using a tailable cursor with awaitData in oplogReader the process wait a couple of seconds before it exits
        });
      }

      async.series(startupTasks, function(err) {
        if (err) {
          log.crit('not all startup tasks are completed %j, exiting', err);
          process.exit(6);
        }

        process.send('listen');
        start(oplogDb, oplogCollName, dbName, collections, dataChannel, versionControl, conflictCollection, xtend({
          conflicts: conflictCollection,
          tmpStorage: tmpCollection
        }, oplogTransformOpts));
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
