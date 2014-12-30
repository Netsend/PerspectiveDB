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
var BSONStream = require('bson-stream');
var keyFilter = require('object-key-filter');

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
 *   [dbPort]:        {Number}      // defaults to 27017
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

// filter password out request
function debugReq(req) {
  return JSON.stringify(keyFilter(req, ['password']));
}

function connErrorHandler(conn, e) {
  console.error('%s: connection error: "%s"', programName, e);
  try {
    conn.destroy();
  } catch(err) {
    console.error('%s: connection write or disconnect error: "%s"', programName, err);
  }
}

// return a string identifier of the connection if connected over ip
function createConnId(conn) {
  var connId;
  if (conn.remoteAddress) {
    connId = conn.remoteAddress + '-' + conn.remotePort + '-' + conn.localAddress + '-' + conn.localPort;
  } else {
    connId = 'UNIX domain socket';
  }
  return connId;
}

var conns = [];
function registerConnection(conn) {
  var connId = createConnId(conn);

  console.log('%s: conn registering %s', programName, conns.length, connId);

  conns.push(conn);

  conn.on('error', function(err) {
    console.error('%s: conn error: %s %s', programName, JSON.stringify(err), connId);
  });

  conn.once('end', function() {
    console.log('%s: conn end: %s', programName, connId);
  });

  conn.once('close', function() {
    console.log('%s: conn close: %s', programName, connId);
    var i = conns.indexOf(conn);
    if (~i) {
      console.log('%s: deleting conn %s', programName, i);
      conns.splice(i, 1);
    } else {
      console.error('%s: conn not found %s', programName, i);
    }
  });
}

function closeConnections(cb) {
  async.each(conns, function(conn, cb2) {
    var connId = createConnId(conn);

    console.log('%s: closing connection %s...', programName, connId);

    var closed = false;
    conn.once('close', function() {
      console.log('%s: conn closed %s', programName, connId);
      closed = true;
      cb2();
    });

    // give the peer some time to end
    setTimeout(function() {
      if (closed) { return; }

      console.log('%s: conn destroyed %s', programName, connId);
      conn.destroy();
      cb2();
    }, 100);

    conn.end();
  }, cb);
}

var vcrs = [];
function registerVcr(vcr, conn) {
  var connId = createConnId(conn);

  console.log('%s: vcr registering %s', programName, connId);

  vcrs.push(vcr);

  vcr.on('error', function(err) {
    console.error('%s: vcr error: %s %s', programName, JSON.stringify(err), connId);
  });

  vcr.once('end', function() {
    console.log('%s: vcr end: %s', programName, connId);
  });

  vcr.once('close', function() {
    console.log('%s: vcr close: %s', programName, connId);
    var i = vcrs.indexOf(vcr);
    if (~i) {
      console.log('deleting vcr %s', i);
      vcrs.splice(i, 1);
    } else {
      console.error('vcr not found %s', i);
    }
  });
}

function closeVcrs(cb) {
  async.each(vcrs, function(vcr, cb2) {
    if (vcr.closed) { cb2(); return; }

    vcr.once('error', cb2);
    vcr.once('close', cb2);
    vcr.close();
  }, cb);
}

function startVc(db, cfg) {
  if (typeof db !== 'object') { throw new TypeError('db must be an object'); }
  if (typeof cfg !== 'object') { throw new TypeError('cfg must be an object'); }

  programName = 'vce ' + db.databaseName + '.' + cfg.collectionName;

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
  vc.startAutoProcessing(cfg.autoProcessInterval);

  // handle oplog items on stdin
  var bs = new BSONStream();
  process.stdin.pause(); // enforce old-mode
  process.stdin.pipe(bs).on('data', function(obj) {
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
  process.stdin.resume();

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

      var conn;
      if (path) {
        console.log('connecting to %s', path);
        conn = net.createConnection(path);
      } else {
        console.log('connecting to %s:%s', host, port);
        conn = net.createConnection(port, host);
      }

      conn.once('connect', function() {
        registerConnection(conn);
      });

      // send auth request
      var authReq = {
        username:   username,
        password:   password,
        database:   database,
        collection: collection
      };

      if (last) {
        authReq.offset = last;
      }

      // expect either bson or a disconnect after the auth request is sent
      var bs2 = new BSONStream();

      // create remote transform to ensure _id._pe is set to this remote
      var rt = new RemoteTransform(database);

      rt.on('error', function(err) {
        console.error('remote transform error', err, JSON.stringify(conn.address()));
      });

      // handle incoming BSON data
      conn.pipe(bs2).pipe(rt).pipe(vc);

      // send auth request
      conn.write(JSON.stringify(authReq) + '\n');
    });
  }

  // handle incoming pull and push requests
  function handleIncomingMsg(req, conn) {
    console.log('%s: incoming ipc message', programName, debugReq(req));

    if (pullRequest.valid(req)) {
      if (conn) { connErrorHandler(conn, 'pull request can not have a connection associated'); }
      handlePullReq(req);
    } else if (pushRequest.valid(req)) {
      if (!conn) { console.error('push request must have a connection associated'); return; }

      req.raw = true;
      req.debug = cfg.debug;

      registerConnection(conn);

      var vcr = new VersionedCollectionReader(db, cfg.collectionName, req);

      registerVcr(vcr, conn);

      vcr.pipe(conn);
    } else {
      connErrorHandler(conn, 'invalid push or pull request');
    }
  }
  process.on('message', handleIncomingMsg);

  // handle shutdown
  function shutdown() {
    console.log('%s: shutting down', programName);
    closeVcrs(function(err) {
      if (err) { throw err; }

      console.log('%s: vcrs closed', programName);

      closeConnections(function(err) {
        if (err) { throw err; }

        console.log('%s: connections closed', programName);

        process.stdin.unpipe(bs);
        console.log('%s: stdin oplog reader unpiped', programName);

        process.stdin.pause();
        console.log('%s: stdin paused', programName);

        process.removeListener('message', handleIncomingMsg);

        bs.end(function(err) {
          if (err) { throw err; }
          console.log('%s: bson stream ended', programName);

          vc.stopAutoProcessing(function(err) {
            if (err) { throw err; }
            console.log('%s: vc stopped processing', programName);
            db.close(function(err) {
              if (err) { throw err; }
              console.log('%s: db closed', programName);
            });
          });
        });
      });
    });
  }

  // listen to kill signals
  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);

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
