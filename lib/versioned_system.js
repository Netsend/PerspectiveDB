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
var chroot = require('chroot');
var User = require('mongo-bcrypt-user');

// lib
var VersionedCollection = require('./versioned_collection');
var Replicator = require('./replicator');
var OplogReader = require('./oplog_reader');
var authRequest = require('./auth_request');
var pushRequest = require('./push_request');
var pullRequest = require('./pull_request');

/**
 * VersionedSystem
 *
 * Track configured versioned collections. Monitor local changes via the
 * oplog and fetch and merge new items from configured remotes.
 *
 * @param {mongodb.Collection} oplogColl  oplog collection (capped collection)
 * @param {Object} [opts]  additional options
 *
 * Options
 *   debug {Boolean, default: false}  whether to do extra console logging or not
 *   hide {Boolean, default: false}  whether to suppress errors or not (used in
 *                                   tests)
 */
function VersionedSystem(oplogColl, opts) {
  if (!(oplogColl instanceof mongodb.Collection)) { throw new TypeError('oplogColl must be a mongdb.Collection'); }
  if (typeof opts !== 'undefined') {
    if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  }

  EE.call(this);

  this._oplogColl = oplogColl;
  this._oplogDb = this._oplogColl.db;

  this._options = opts || {};

  this.debug = !!this._options.debug;
  this._hide = !!this._options.hide;

  this._vcs = {};
}

util.inherits(VersionedSystem, EE);
module.exports = VersionedSystem;

/**
 * Fork each VC and send initial request containing database parameters, VC config
 * and an optional chroot config. Then connect an oplog reader to each vc.
 *
 * @param {Object} vcs  object containing vcexec configs
 * @param {Boolean, default true} follow  object containing vcexec configs
 * @param {Function} cb  This will be called as soon as all VCs are initialized.
 *                       First parameter will be an error object or null. Second
 *                       parameter will be an object with oplog readers for each
 *                       vc.
 */
VersionedSystem.prototype.initVCs = function initVCs(vcs, follow, cb) {
  if (typeof vcs !== 'object') { throw new TypeError('vcs must be an object'); }

  if (typeof follow === 'function') {
    cb = follow;
    follow = true;
  }
  if (typeof follow === 'undefined' || follow === null) {
    follow = true;
  }

  if (typeof follow !== 'boolean') { throw new TypeError('follow must be a boolean'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  var error;

  var oplogReaders = {};

  async.eachSeries(Object.keys(vcs), function(dbName, cb2) {
    async.eachSeries(Object.keys(vcs[dbName]), function(collectionName, cb3) {
      var ns = dbName + '.' + collectionName;

      // load versioned collection exec config
      var vceCfg = vcs[dbName][collectionName];
      if (!vceCfg.size) {
        error = new Error('missing size');
        if (!that._hide) { console.error('vs vc error', ns, error); }
        cb3(error);
        return;
      }

      if (that.debug) { console.log('vs initVCs forking: %s...', ns); }

      var vc = fork(__dirname + '/versioned_collection_exec', { env: {}, silent: true });

      var phase = null;

      vc.on('error', function(err) {
        if (!that._hide) { console.error('vs vc error', ns, err); }

        // callback if not in listen mode yet
        if (phase !== 'listen') {
          cb3(err);
        }
      });

      vc.stderr.on('data', function(data) {
        if (!that._hide) { console.error('vs vc stderr', ns, data.toString()); }
      });

      vc.on('exit', function(code, sig) {
        if (!that._hide) { console.log('vs vc exit', ns, code, sig); }

        // callback if not in listen mode yet
        if (phase !== 'listen') {
          cb3(new Error('abnormal termination'));
        }
      });

      // send initial config after vc is ready to receive messages
      vc.once('message', function(msg) {
        if (msg !== 'init') {
          if (!that._hide) { console.error('vs initVCs expected message "init"', ns, msg); }
          cb3(new Error('expected first message to be "init"'));
          return;
        }

        phase = 'init';

        // ensure database and collection name
        vceCfg.dbName = vceCfg.dbName || dbName;
        vceCfg.collectionName = vceCfg.collectionName || collectionName;

        vc.send(vceCfg);

        // wait for the child to send the "listen" message, and start sending oplog items
        vc.once('message', function(msg) {
          if (msg !== 'listen') {
            if (!that._hide) { console.error('vs initVCs expected message "listen"', ns, msg); }
            cb3(new Error('expected second message to be "listen"'));
            return;
          }

          phase = 'listen';

          // setup oplog connection to this vc
          that._ensureSnapshotAndOplogOffset(vceCfg, function(err, offset) {
            if (err) { cb3(err); return; }

            var opts = {
              follow: follow,
              offset: offset,
              debug: that.debug,
              hide: that._hide
            };

            var or = new OplogReader(that._oplogColl, ns, opts);
            or.pipe(vc.stdin);

            // register vc
            that._vcs[ns] = vc;

            oplogReaders[ns] = or;

            cb3();
          });
        });
      });
    }, cb2);
  }, function(err) {
    if (err) { cb(err); return; }

    cb(null, oplogReaders);
  });
};

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
  async.each(Object.keys(this._vcs), function(key, cb2) {
    var dbName = key.split('.')[0];
    var collectionName = key.split('.').slice(1).join();

    var collection = that._oplogDb.db(dbName).collection(collectionName);
    var snapshotCollection = that._oplogDb.db(dbName).collection('m3.' + collectionName);

    collection.stats(function(err, resultCollection) {
      if (err) {
        if (err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) {
          cb2(err);
          return;
        }
        resultCollection = {};
      }

      result[key] = { collection: resultCollection };

      snapshotCollection.stats(function(err, resultSnapshotCollection) {
        if (err) {
          if (err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) {
            cb2(err);
            return;
          }
          resultSnapshotCollection = {};
        }


        result[key].snapshotCollection = resultSnapshotCollection;

        if (extended) {
          snapshotCollection.count({ '_m3._ack': true }, function(err, count) {
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
 * If the snapshot is empty, get the latest oplog item as offset, otherwise use
 * maxOplogPointer or Timestamp(0, 0);
 *
 * @param {Object} cfg  versioned collection configuration object
 * @param {Function} cb  Called when oplog is connected to the vc. First parameter
 *                       will be an error object or null.
 *
 * A versioned collection configuration object should have the following structure:
 * {
 *   dbName:          {String}
 *   collectionName:  {String}
 *   size:            {Number}
 *   [any VersionedCollection options]
 * }
 */
VersionedSystem.prototype._ensureSnapshotAndOplogOffset = function _ensureSnapshotAndOplogOffset(cfg, cb) {
  if (typeof cfg !== 'object') { throw new TypeError('cfg must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (typeof cfg.dbName !== 'string') { throw new TypeError('cfg.dbName must be a string'); }
  if (typeof cfg.collectionName !== 'string') { throw new TypeError('cfg.collectionName must be a string'); }
  if (typeof cfg.size !== 'number') { throw new TypeError('cfg.size must be a number'); }

  var vc = new VersionedCollection(this._oplogDb.db(cfg.dbName), cfg.collectionName, cfg);

  var that = this;
  var error;

  var oplogOffset;

  // get oldest item from oplog
  this._oplogColl.findOne({}, { sort: { $natural: 1 } }, function(err, oldestOplogItem) {
    if (err) { cb(err); return; }

    if (!oldestOplogItem) {
      error = new Error('no oplog item found');
      if (!that._hide) { console.error(vc.ns, error); }
      cb(error);
      return;
    }

    // get newest item from oplog
    that._oplogColl.findOne({}, { sort: { $natural: -1 } }, function(err, newestOplogItem) {
      if (err) { cb(err); return; }

      if (that.debug) { console.log('vs _ensureSnapshotAndOplogOffset oplog span', oldestOplogItem.ts, newestOplogItem.ts); }

      vc._snapshotCollection.count(function(err, items) {
        if (err) { return cb(err); }

        // get max oplog pointer from snapshot
        vc.maxOplogPointer(function(err, snapshotOffset) {
          if (err) { cb(err); return; }

          if (!snapshotOffset && items) {
            error = new Error('vc contains snapshots but no oplog pointer');
            if (!that._hide) { console.error(vc.ns, error); }
            cb(error);
            return;
          }

          // if found, use it, but warn if it's outside the current range of the oplog
          if (snapshotOffset) {
            if (snapshotOffset.lessThan(oldestOplogItem.ts) || snapshotOffset.greaterThan(newestOplogItem.ts)) {
              if (!that._hide) { console.log('WARNING: oplog pointer outside current oplog range', snapshotOffset, oldestOplogItem.ts, newestOplogItem.ts); }
            }
            oplogOffset = snapshotOffset;
          } else if (!items) {
            // if snapshot is empty, use newest oplog item
            oplogOffset = newestOplogItem.ts;
          }

          vc.ensureSnapshotCollection(cfg.size, function(err) {
            if (err) { cb(err); return; }

            cb(null, oplogOffset);
          });
        });
      });
    });
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
