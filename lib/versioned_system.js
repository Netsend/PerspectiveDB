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

// sys
var util = require('util');
var EE = require('events').EventEmitter;
var fork = require('child_process').fork;

// npm
var async = require('async');
var mongodb = require('mongodb');
var Timestamp = mongodb.Timestamp;
var chroot = require('chroot');
var User = require('mongo-bcrypt-user');

// lib
var OplogReader = require('./oplog_reader');
var Replicator = require('./replicator');
var authRequest = require('./auth_request');
var pushRequest = require('./push_request');
var pullRequest = require('./pull_request');

/**
 * VersionedSystem
 *
 * Track configured versioned collections. Monitor local changes via the
 * oplog and fetch and merge new items from configured remotes.
 *
 * @param {mongodb.Db} oplogDb  authorized connection to the database that
 *                              contains the oplog.
 * @param {Object} [options]  additional options
 *
 * Options
 * - **oplogCollectionName** {String, default: "oplog.$main"}, name of the
 *     collection to read from. use "oplog.rs" if running a replica set.
 * - **snapshotSize** {Number, default: 10}  default size of snapshot collections
 *                                           in mega bytes
 * - **debug** {Boolean, default: false}  whether to do extra console logging or not
 * - **hide** {Boolean, default: false}  whether to suppress errors or not (used
 *                                       in tests)
 */
function VersionedSystem(oplogDb, opts) {
  /* jshint maxcomplexity: 18 */ /* lot's of variable setup */
  if (!(oplogDb instanceof mongodb.Db)) { throw new TypeError('oplogDb must be a mongdb.Db'); }
  if (typeof opts !== 'undefined') {
    if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  }

  EE.call(this);

  opts = opts || {};

  this._oplogDb = oplogDb;

  this._options = opts;
  this._oplogCollectionName = opts.oplogCollectionName || 'oplog.$main';
  this._oplogCollection = this._oplogDb.collection(this._oplogCollectionName);
  this._snapshotSize = opts.snapshotSize || 10;

  this.debug = !!this._options.debug;
  this._hide = !!this._options.hide;

  this._vcs = {};
}

util.inherits(VersionedSystem, EE);
module.exports = VersionedSystem;

/**
 * Return stats of all collections.
 *
 * @param {Boolean} [extended, default false]  whether to add _m3._ack counts
 * @param {Function} cb  The first parameter will contain either an Error object or
 *                       null. The second parameter is an object with collection
 *                       info.
 *
 * extended object:
 *   ack {Number}  the number of documents where _m3._ack = true
 */
VersionedSystem.prototype.info = function info(extended, cb) {
  if (typeof extended === 'function') {
    cb = extended;
    extended = false;
  }

  if (typeof extended !== 'boolean') { throw new TypeError('extended must be a boolean'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  var result = {};
  async.each(Object.keys(this._databaseCollections), function(key, cb2) {
    that._databaseCollections[key]._collection.stats(function(err, resultCollection) {
      if (err) {
        if (err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) {
          cb2(err);
          return;
        }
        resultCollection = {};
      }

      result[key] = { collection: resultCollection };

      that._databaseCollections[key]._snapshotCollection.stats(function(err, resultSnapshotCollection) {
        if (err) {
          if (err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) {
            cb2(err);
            return;
          }
          resultSnapshotCollection = {};
        }


        result[key].snapshotCollection = resultSnapshotCollection;

        if (extended) {
          that._databaseCollections[key]._snapshotCollection.count({ '_m3._ack': true }, function(err, count) {
            if (err) { cb2(err); return; }

            result[key].extended = { ack: count };
            cb2();
          });
        } else {
          cb2();
        }
      });
    });
  }, function(err) {
    cb(err, result);
  });
};

/**
 * Rebuild every versioned collection.
 *
 * Clears every versioned collection and restarts by versioning every document
 * currently in the collection.
 *
 * Tracks last processed oplog item.
 *
 * Notes:
 * * this destroys any previous versions and creates new versions of all
 *   versioned collections.
 * * No updates should occur on the collections, this is not enforced by
 *   this function.
 * * auto processing should be disabled
 *
 * @param {Function} cb  called with an Error object or null
 */
VersionedSystem.prototype.rebuild = function rebuild(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  var error;

  if (this._rebuildRunning) {
    error = new Error('rebuild already running');
    if (!this._hide) { console.error('vs rebuild', error); }
    process.nextTick(function() {
      cb(error);
    });
    return;
  }
  this._rebuildRunning = true;

  function finalize(err) {
    if (err) { cb(err); return; }
    that._stopAutoProcessing(function(err) {
      if (err) { cb(err); return; }
      that._rebuildRunning = false;
      cb(null);
    });
  }

  this._startAutoProcessing(100, function(err) {
    if (err) { finalize(err); return; }
  });

  // use the last oplog items as default offset on empty versioned collections
  that._getLatestOplogItem(function(err, latestOplogItem) {
    if (err) { return finalize(err); }

    latestOplogItem = latestOplogItem || { ts: new Timestamp(0, (new Date()).getTime() / 1000) };

    // set offsets to latest oplog item
    var offsets = {};
    Object.keys(that._databaseCollections).forEach(function(key) {
      offsets[key] = latestOplogItem.ts;
    });

    // clean up _removeLastUsedOplogItem and _removeLastUsedCollectionOplogItem
    that._removeLastUsedOplogItem(function(err) {
      if (err) { return finalize(err); }
      that._removeLastUsedCollectionOplogItem(function(err) {
        if (err) { return finalize(err); }

        // start rebuilding each versioned collection
        async.eachSeries(Object.keys(that._databaseCollections), function(databaseCollectionName, callback) {
          var vc = that._databaseCollections[databaseCollectionName];
          vc.rebuild((that._snapshotSizes[databaseCollectionName] || that._snapshotSize) * 1024 * 1024, callback);
        }, function(err) {
          if (err) { return finalize(err); }

          // tail (don't follow), starting from the oplog item we found before we started, with 100ms as tailableRetryInterval
          that._trackOplog(false, latestOplogItem.ts, offsets, 100, finalize);
        });
      });
    });
  });
};

/**
 * Make sure every versioned collection has a snapshot collection. Sizes are
 * determined by the snapshotSize and snapshotSizes options provided to the
 * constructor.
 *
 * @param {Function} cb  first parameter will be an error object or null.
 */
VersionedSystem.prototype.ensureSnapshotCollections = function ensureSnapshotCollections(cb) {
  var that = this;
  async.eachSeries(Object.keys(this._databaseCollections), function(key, cb2) {
    var vc = that._databaseCollections[key];
    vc.ensureSnapshotCollection((that._snapshotSizes[key] || that._snapshotSize) * 1024 * 1024, cb2);
  }, cb);
};

/**
 * Fork each VC and send initial request containing database parameters, VC config
 * and an optional chroot config. Then connect an oplog reader to each vc.
 *
 * @param {Object} vcs  object containing per vc configs
 * @param {Function} cb  This will be called as soon as all VCs are initialized.
 *                       First parameter will be an error object or null.
 */
VersionedSystem.prototype.initVCs = function initVCs(vcs, cb) {
  if (typeof vcs !== 'object') { throw new TypeError('vcs must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  async.eachSeries(Object.keys(vcs), function(dbName, cb2) {
    async.eachSeries(Object.keys(vcs[dbName]), function(collectionName, cb3) {
      var ns = dbName + '.' + collectionName;

      if (that.debug) { console.log('vs initVCs forking: %s...', ns); }

      var vc = fork(__dirname + '/versioned_collection_exec', { env: {}, silent: true });

      vc.on('error', function(err) {
        console.error('vs vc error', ns, err);
      });

      vc.stderr.on('data', function(data) {
        console.error('vs vc stderr', ns, data.toString());
      });

      vc.on('exit', function(code, sig) {
        console.log('vs vc exit', ns, code, sig);
      });

      that._vcs[ns] = vc;

      // send initial config after vc is ready to receive messages
      vc.once('message', function(msg) {
        if (msg !== 'init') {
          if (!that._hide) { console.error('vs initVCs expected message "init"', ns, msg); }
          cb3(new Error('expected first message to be "init"'));
          return;
        }

        // ensure database name
        var dbConfig = vc.dbConfig || {};
        dbConfig.dbName = dbConfig.dbName || dbName;

        // ensure collection name
        vc.collectionName = vc.collectionName || collectionName;

        vc.send({
          dbConfig: dbConfig,
          vcConfig: {
            collectionName: vc.collectionName || collectionName,
            debug: that.debug,
            hide: that._hide
          },
          chrootConfig: {
            newRoot: vc.chrootNewRoot,
            user: vc.chrootUser
          }
        });

        // wait for the child to send the "listen" message, and start sending oplog items
        vc.once('message', function(msg) {
          if (msg !== 'listen') {
            if (!that._hide) { console.error('vs initVCs expected message "listen"', ns, msg); }
            cb3(new Error('expected second message to be "listen"'));
            return;
          }

          // setup oplog connection to this vc
          that._trackOplogVc(ns, true, cb3);
        });
      });
    }, cb2);
  }, cb);
};

/**
 * Send a pull request to a versioned collection.
 *
 * @param {String} ns  namespace of the versioned collection
 * @param {Object} pullRequest  pull request to send
 *
 * A pull request should have the following structure:
 * {
 *   username:     {String}
 *   password:     {String}
 *   [path]:       {String}
 *   [host]:       {String}     // defaults to 127.0.0.1
 *   [port]:       {Number}     // defaults to 2344
 *   [database]:   {String}     // defaults to db.databaseName
 *   [collection]: {String}     // defaults to vcCfg.collectionName
 * }
 */
VersionedSystem.prototype.sendPR = function sendPR(ns, pullReq) {
  if (typeof ns !== 'string') { throw new TypeError('ns must be a string'); }
  if (typeof pullReq !== 'object') { throw new TypeError('pullReq must be an object'); }

  if (typeof this._vcs[ns] !== 'object') { throw new Error('no versioned collection for ns found'); }
  if (!pullRequest.valid(pullReq)) { throw new Error('invalid pull request'); }

  this._vcs[ns].send(pullReq);
};

/**
 * Chroot this process.
 *
 * @param {String} user  user to drop privileges to
 * @param {Object} [opts]  options
 *
 * Options
 *   path {String, default "/var/empty"}  new root
 */
VersionedSystem.prototype.chroot = function (user, opts) {
  if (typeof user !== 'string') { throw new TypeError('user must be a string'); }
  opts = opts || {};
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }

  var newPath = opts.path || '/var/empty';
  chroot(newPath, user);
  if (this.debug) { console.log('vs: changed root to "%s" and user to "%s"', newPath, user); }
};

/**
 * Fork a pre-auth server that handles incoming push requests. Verify password,
 * find replication config and pass a push request to the appropriate versioned
 * collection.
 *
 * Note: chroots right after pre-auth server is started
 *
 * @param {String} user  username to drop privileges to
 * @param {String} newRoot  new root path
 * @param {Object} preauthCfg  configuration object send to preauth process
 * @param {Function} cb  First parameter will be an Error object or null.
 *
 * preauthCfg:
 * {
 *   serverConfig:
 *   {
 *     [host]:         {String}     // defaults to 127.0.0.1
 *     [port]:         {Number}     // defaults to 2344
 *     [path]:         {String}     // defaults to /tmp/preauth-2344.sock
 *
 *
 *   chrootConfig:
 *   {
 *     [user]:         {String}     // defaults to "nobody"
 *     [newRoot]:      {String}     // defaults to /var/empty
 *   }
 * }
 *
 * @void
 */
VersionedSystem.prototype.createServer = function createServer(user, newRoot, preauthCfg, cb) {
  if (typeof user !== 'string') { throw new TypeError('user must be a string'); }
  if (typeof newRoot !== 'string') { throw new TypeError('newRoot must be a string'); }
  if (typeof preauthCfg !== 'object') { throw new TypeError('preauthCfg must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  function connErrorHandler(conn, err) {
    console.error('vs createServer connection error', conn.remoteAddress, err);

    try {
      conn.write('invalid auth request\n');
      conn.destroy();
    } catch(err) {
      console.error('vs createServer connection write or disconnect error', err);
    }
  }

  // handle incoming authentication requests
  function handleMessage(req, conn) {
    if (that.debug) { console.log('vs createServer', conn.remoteAddress); }

    if (!authRequest.valid(req)) {
      connErrorHandler(conn, new Error('invalid auth request'));
      return;
    }

    // verify credentials and lookup replication config
    that._verifyAuthRequest(req, function(err, replCfg) {
      if (err) { connErrorHandler(conn, err); return; }

      // create a push request from the auth request and replication config
      var pushReq = {
        filter: replCfg.offset,
        hooks: replCfg.offset,
        hooksOpts: {},
        offset: req.offset
      };

      Object.keys(replCfg, function(key) {
        if (!~['filter', 'hooks', 'hooksOpts', 'offset'].indexof(key)) {
          pushReq.hooksOpts[key] = replCfg[key];
        }
      });

      if (!pushRequest.valid(pushReq)) {
        if (!that._hide) { console.error('vs createServer unable to construct a valid push request', req, pushReq); }
        connErrorHandler(conn, new Error('unable to construct a valid push request'));
        return;
      }

      // now send this push request and connection to the appropriate versioned collection
      var ns = req.database + '.' + req.collection;
      that._vcs[ns].send(pushReq, conn);
    });
  }

  // open preauth server
  var preauth = fork(__dirname + '/lib/preauth_exec', { env: {} });
  if (this.debug) { console.log('vs preauth forked'); }

  this.chroot(user, { path: newRoot });

  // send initial config after preauth is ready to receive messages
  preauth.once('message', function(msg) {
    if (msg !== 'init') {
      if (!that._hide) { console.error('vs createServer expected message "init"', msg); }
      cb(new Error('expected first message to be "init"'));
      return;
    }

    preauth.send(preauthCfg);

    preauth.once('message', function(msg) {
      if (msg !== 'listen') {
        if (!that._hide) { console.error('vs createServer expected message "listen"', msg); }
        cb(new Error('expected second message to be "listen"'));
        return;
      }

      preauth.once('message', handleMessage);
      cb();
    });
  });

  this._preauth = preauth;
};



/////////////////////
//// PRIVATE API ////
/////////////////////



/**
 * Track oplog for the given namespace.
 *
 * @param {String} ns  follow the oplog
 * @param {Boolean} [follow, default true]  whether or not to follow the oplog
 * @param {Function} cb  Called when oplog is connected to the vc. First parameter
 *                       will be an error object or null.
 */
VersionedSystem.prototype._trackOplogVc = function _trackOplogVc(ns, follow, cb) {
  if (typeof ns !== 'string') { throw new TypeError('ns must be a string'); }
  if (typeof follow === 'function') {
    cb = follow;
    follow = true;
  }
  if (typeof follow !== 'boolean') { throw new TypeError('follow must be a boolean'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  // determine oplog offset
  that._determineOplogOffset(ns, function(err, offset) {
    if (err) { return cb(err); }

    var opts = {
      tailable: follow,
      offset: offset,
      debug: that.debug,
      hide: that._hide
    };

    var or = new OplogReader(that._oplogCollection, ns, opts);
    or.pipe(that._vcs[ns].stdin);
    cb();
  });
};

/**
 * Verify an auth request, and if valid, pass back the replication config.
 *
 * @param {Function} cb  First parameter will be an error object or null. Second
 *                       parameter will be a replication config if valid.
 */
VersionedSystem.prototype._verifyAuthRequest = function _verifyAuthRequest(req, cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  var error;

  // do a lookup in the database
  User.verifyPassword(this._usersColl, req.username, req.password, req.database, function(err, valid) {
    if (err) { cb(err); return; }

    if (!valid) {
      error = 'invalid credentials';
      if (!that._hide) { console.error('vs _verifyAuthRequest', error); }
      cb(new Error(error));
      return;
    }

    if (that.debug) { console.log('vs successfully authenticated', req.username); }

    // search for export replication config for the remote using the username
    Replicator.fetchFromDb(that._db.db(req.database).collection(that._replicationConfigCollection), 'export', req.username, function(err, replCfg) {
      if (err) { cb(err); return; }

      // check if requested collection is exported
      if (!replCfg.collections[req.collection]) {
        error = 'requested collection not exported';
        if (!that._hide) { console.error('vs createDataRequest', error, req); }
        cb(error);
        return;
      }

      cb(null, replCfg.collections[req.collection]);
    });
  });
};
