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

var chroot = require('chroot');

var _db = require('../bin/_db');
var VersionedCollectionReader = require('./versioned_collection_reader');
var VersionedCollection = require('./versioned_collection');
var RemoteTransform = require('./remote_transform');
var pullRequest = require('./pull_request');
var pushRequest = require('./push_request');

/**
 * Instantiate new Versioned Collection and handle incoming oplog items on stdin.
 *
 * 1. handle oplog items on stdin
 * 2. handle incoming pull requests by sending out an auth request
 * 3. handle incoming push requests by setting up a VC Reader
 *
 * This module should be forked and the process expects the first message to be
 * have the following data:
 *
 * {
 *   dbName:          {String}
 *   collectionName:  {String}
 *   [dbUser]:        {String}
 *   [dbPass]:        {String}
 *   [adminDb]:       {String}
 *   [any VersionedCollection options]
 *   [chrootUser]:    {String}      // defaults to "nobody"
 *   [chrootNewRoot]: {String}      // defaults to /var/empty
 * }
 *
 * After the database connection and data handlers are setup, this process sends
 * a message that contains the string "listen", signalling that it's ready to
 * receive data on stdin.
 *
 * Any subsequent messages sent to this process should be either pull requests or
 * push requests accompanied with a socket.
 *
 * A pull request should have the following structure:
 * {
 *   username:     {String}
 *   password:     {String}
 *   [path]:       {String}
 *   [host]:       {String}     // defaults to 127.0.0.1
 *   [port]:       {Number}     // defaults to 2344
 *   [database]:   {String}     // defaults to db.databaseName
 *   [collection]: {String}     // defaults to initial collectionName
 * }
 * only path or host and port should be set, not both
 *
 * A push request should have the following structure:
 * {
 *   [filter]:     {}
 *   [hooks]:      {String}     // array of hook file names
 *   [hooksOpts]:  {}           // options passed to each hook
 *   [offset]:     {String}
 * }
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

function startVc(db, cfg) {
  if (typeof db !== 'object') { throw new TypeError('db must be an object'); }
  if (typeof cfg !== 'object') { throw new TypeError('cfg must be an object'); }

  programName = 'vc ' + db.databaseName + '.' + cfg.collectionName;

  // chroot config
  var chrootUser = cfg.chrootUser || 'nobody';
  var chrootNewRoot = cfg.chrootNewRoot || '/var/empty';

  // then chroot
  try {
    chroot(chrootNewRoot, chrootUser);
    console.log('%s: changed root to "%s" and user to "%s"', programName, chrootNewRoot, chrootUser);
  } catch(err) {
    console.error('%s: changing root or user failed', programName, cfg, err);
    process.exit(1);
  }

  var vc = new VersionedCollection(db, cfg.collectionName, cfg);

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

  // handle pull requests by sending an auth request
  function handlePullReq(pullReq) {
    var username, password, path, host, port, database, collection;

    username   = pullReq.username;
    password   = pullReq.password;
    path       = pullReq.path;
    host       = pullReq.host       || '127.0.0.1';
    port       = pullReq.port       || 2344;
    database   = pullReq.database   || db.databaseName;
    collection = pullReq.collection || cfg.collectionName;

    // find last item of this remote
    vc.lastByPerspective(database, function(err, last) {
      if (err) { console.error(err, pullReq); return; }

      // create remote transform to ensure _id._pe is set to this remote
      var rt = new RemoteTransform(database);

      function connHandler(conn) {
        // send auth request
        var authReq = {
          username:   username,
          password:   password,
          database:   database,
          collection: collection,
          offset:     last
        };

        // expect either bson or a disconnect after the auth request is sent
        conn.pipe(rt).pipe(vc);
        conn.write(JSON.stringify(authReq));
      }

      if (path) {
        net.connect(path, connHandler);
      } else {
        net.connect(port, host, connHandler);
      }
    });
  }

  // handle incoming pull and push requests
  process.on('message', function(req, conn) {
    console.log('%s: incoming ipc message', programName, req);

    if (pullRequest.valid(req)) {
      handlePullReq(req);
    } else if (pushRequest.valid(req)) {
      var vcr = new VersionedCollectionReader(db, cfg.collectionName, req);
      vcr.pipe(conn);
    } else {
      connErrorHandler(conn, 'invalid push or pull request');
    }
  });

  // send a "listen" signal
  process.send('listen');
}

if (typeof process.send !== 'function') {
  throw new Error('this module should be invoked via child_process.fork');
}

process.send('init');

// expect one init request (request that contains db name and collection name and any other opts)
process.once('message', function(msg) {
  if (typeof msg !== 'object') { throw new TypeError('msg must be an object'); }
  if (typeof msg.dbName !== 'string') { throw new TypeError('msg.dbName must be a string'); }
  if (typeof msg.collectionName !== 'string') { throw new TypeError('msg.collectionName must be a string'); }

  // open database
  _db(msg, function(err, db) {
    if (err) { throw err; }
    startVc(db, msg);
  });
});
