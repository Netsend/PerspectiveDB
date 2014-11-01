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

var net = require('net');

var async = require('async');
var chroot = require('chroot');

var _db = require('../bin/_db');
var VersionedCollectionReader = require('./versioned_collection_reader');
var VersionedCollection = require('./versioned_collection');
var dataRequest = require('./data_request');

/**
 * Instantiate new Versioned Collection that gets data from remotes and the oplog
 * and setup Versioned Collection Readers for incoming data requests.
 *
 * 1. setup data requests to remotes
 * 2. handle incoming data request
 * 3. handle oplog items on stdin
 *
 * This module should be forked and the process expects the first message to be
 * have the following data:
 *
 * {
 *   dbConfig:     {}
 *   chrootConfig: {}
 *   vcConfig:     {}
 * }
 *
 * dbConfig:
 * {
 *   dbName:       {String}
 *   [dbUser]:     {String}
 *   [dbPass]:     {String}
 *   [adminDb]:    {String}
 * }
 *
 * chrootConfig:
 * {
 *   newRoot:      {String}
 *   user:         {String}
 *   [group]:      {String}
 * }
 *
 * vcConfig:
 * {
 *   all VersionedCollection params and options
 *   [remoteAuth]:   [remote*]
 * }
 *
 * remote:
 * {
 *   username:     {String}
 *   password:     {String}
 * }
 *
 *
 * Any subsequent messages sent to this process should be data requests that have
 * the following structure:
 *
 * {
 *   [filter]:     {}
 *   [hooks]:      {String}     // array of hook file names
 *   [hooksOpts]:  {}           // options passed to each hook
 *   [offset]:     {String}
 * }
 *
 */

var programName = 'VersionedCollection';

function connErrorHandler(conn, e) {
  console.error('%s: connection error: "%s"', programName, e);
  try {
    conn.destroy();
  } catch(err) {
    console.error('%s: connection write or disconnect error: "%s"', programName, err);
  }
}

/**
 * Instantiate new Versioned Collection that gets data from remotes and the oplog
 * and setup Versioned Collection Readers for incoming data requests.
 *
 * 1. setup data requests to remotes
 * 2. handle incoming data request
 * 3. handle oplog items on stdin
 */
function startVc(db, chrootCfg, vcCfg) {
  if (typeof db !== 'object') { throw new TypeError('db must be an object'); }
  if (typeof chrootCfg !== 'object') { throw new TypeError('chrootCfg must be an object'); }
  if (typeof vcCfg !== 'object') { throw new TypeError('vcCfg must be an object'); }

  if (typeof chrootCfg.user !== 'string') { throw new TypeError('chrootCfg.user must be a string'); }
  if (typeof chrootCfg.newRoot !== 'string') { throw new TypeError('chrootCfg.newRoot must be a string'); }
  if (typeof vcCfg.collectionName !== 'string') { throw new TypeError('vcCfg.collectionName must be a string'); }

  if (typeof vcCfg.remoteAuth !== 'undefined') {
    if (!Array.isArray(vcCfg.remoteAuth)) { throw new TypeError('vcCfg.remoteAuth must be an array'); }
  }

  programName = 'vc ' + db.databaseName + '.' + vcCfg.collectionName;

  // then chroot
  try {
    chroot(chrootCfg.newRoot, chrootCfg.user);
    console.log('%s: changed root to "%s" and user to "%s"', programName, chrootCfg.newRoot, chrootCfg.user);
  } catch(err) {
    console.error('%s: changing root or user failed', programName, err);
    process.exit(1);
  }

  var vc = new VersionedCollection(db, vcCfg.collectionName, vcCfg);

  // setup data requests to remoteAuth
  async.eachSeries(vcCfg.remoteAuth || [], function(remote, cb) {
    if (!remote.host && !remote.path) { throw new Error('provide either host or path'); }

    // find last item of this remote
    vc.lastByPerspective(remote, function(err, last) {
      if (err) { cb(err); return; }

      function connHandler(conn) {
        // expect either bson or a disconnect after the auth request is sent
        conn.pipe(vc);

        // send auth request
        var authReq = {
          username: remote.username,
          password: remote.password,
          database: db.databaseName,
          collection: vcCfg.collectionName,
          offset: last
        };

        conn.write(JSON.stringify(authReq), cb);
      }

      if (remote.path) {
        net.connect(remote.path, connHandler);
      } else {
        net.connect(remote.port || 2344, remote.host, connHandler);
      }
    });
  }, function(err) {
    if (err) { console.error('vc problem connecting to remotes', err); return; }

    console.log('vc importing from remotes');
  });

  // handle incoming data requests
  process.on('message', function(msg, conn) {
    console.log('%s: incoming ipc message', programName, msg);

    if (!dataRequest.valid(msg)) {
      connErrorHandler(conn, 'invalid data request');
      return;
    }

    // might have filters, hooks, hooksOpts and offset
    var vcr = new VersionedCollectionReader(db, vcCfg.collectionName, msg);
    vcr.pipe(conn);
  });

  // handle oplog items on stdin
  process.stdin.on('data', function(obj) {
    vc.saveOplogItem2(obj, function(err) {
      if (err) {
        console.error('save oplog item error', err, obj);
        throw err;
      }
    });

    // pause stream if buffer exceeds max length
    if (vc._queueLimit <= vc._oplogBuffer.length) {
      if (!vc._hide) { console.log(vc.databaseName, vc.collectionName, 'saveOplogItem queue full, wait and retry. queue length: ', vc._oplogBuffer.length); }

      process.stdin.pause();

      setTimeout(function() {
        if (vc.debug) { console.log(vc.databaseName, vc.collectionName, 'saveOplogItem retry...', vc._oplogBuffer.length); }

        process.stdin.resume();
      }, vc._queueLimitRetryTimeout);
    }
  });
}

if (typeof process.send !== 'function') {
  throw new Error('this module should be invoked via child_process.fork');
}

// expect one init request (request that contains db auth credentials, chroot config and vc config)
process.once('message', function(msg) {
  if (typeof msg !== 'object') { throw new TypeError('msg must be an object'); }
  if (typeof msg.dbConfig !== 'object') { throw new TypeError('msg.dbConfig must be an object'); }
  if (typeof msg.chrootConfig !== 'object') { throw new TypeError('msg.chrootConfig must be an object'); }
  if (typeof msg.vcConfig !== 'object') { throw new TypeError('msg.vcConfig must be an object'); }

  // open database
  _db(msg.dbConfig, function(err, db) {
    if (err) { throw err; }
    startVc(db, msg.chrootConfig, msg.vcConfig);
  });
});
