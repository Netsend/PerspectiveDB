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
var fs = require('fs');
var path = require('path');

var async = require('async');
var chroot = require('chroot');
var BSONStream = require('bson-stream');
var keyFilter = require('object-key-filter');

var _db = require('../bin/_db');
var VersionedCollection = require('./versioned_collection');
var RemoteTransform = require('./remote_transform');
var pullRequest = require('./pull_request');
var pushRequest = require('./push_request');
var connManager = require('./conn_manager');

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
 *   [hookPaths]:     {Array}       // list of paths to load hooks from
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
 *   [filter]:     {}
 *   [hooks]:      {String}     // array of hook file names
 *   [hooksOpts]:  {}           // options passed to each hook
 *   [offset]:     {String}
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

var programName = 'vce';

/**
 * Require all js files in the given directories.
 *
 * @param {Array} hooks  list of directories
 * @param {String} [basePath]  base dir, defaults to project root and is appended
 *                             to relative dirs
 * @return {Object} list of hooks indexed by path
 */
function requireJsFromDirs(hooks, basePath) {
  if (!Array.isArray(hooks)) { throw new TypeError('hooks must be an array'); }

  basePath = basePath || __dirname + '/../';

  // ensure trailing slash
  if (basePath[basePath.length - 1] !== '/') {
    basePath += '/';
  }

  var result = {};
  hooks.forEach(function(dir) {
    // ensure trailing slash
    if (dir[dir.length - 1] !== '/') {
      dir += '/';
    }

    var npath;
    if (dir[0] === '/') {
      npath = dir;
    } else {
      // prepend base path to relative dirs
      npath = basePath + dir;
    }

    var files = fs.readdirSync(npath);
    files.forEach(function(file) {
      // only load regular files that end with .js
      if ((file.indexOf('.js') === file.length - 3) && fs.statSync(npath + file).isFile()) {
        console.log('%s: loading %s', programName, npath + file);
        result[path.basename(file, '.js')] = require(npath + file);
      }
    });
  });

  return result;
}

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

var loadedHooks = {};

var vcrs = [];
function registerVcr(vcr, conn) {
  var connId = connManager.createId(conn);

  console.log('%s: vcr registering %s', programName, connId);

  vcrs.push(vcr);

  vcr.on('error', function(err) {
    console.error('%s: vcr error: %s %s', programName, err, connId);
  });

  vcr.once('end', function() {
    console.log('%s: vcr end: %s', programName, connId);

    var i = vcrs.indexOf(vcr);
    if (~i) {
      console.log('%s: deleting vcr %s', programName, i);
      vcrs.splice(i, 1);
    } else {
      console.error('%s: vcr not found %s', programName, i);
    }
  });
}

function closeVcrs(cb) {
  async.each(vcrs, function(vcr, cb2) {
    vcr.once('error', cb2);
    vcr.once('end', cb2);
    vcr.close();
  }, cb);
}

function startVc(db, cfg) {
  if (typeof db !== 'object') { throw new TypeError('db must be an object'); }
  if (typeof cfg !== 'object') { throw new TypeError('cfg must be an object'); }

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

  var cm = connManager.create({ debug: cfg.debug });

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
      if (!vc._hide) { console.log('%s: saveOplogItem queue full, wait and retry. %s %s', programName, vc._queueLimitRetryTimeout, vc._oplogBuffer.length); }

      bs.pause();

      setTimeout(function() {
        if (vc.debug) { console.log('%s: saveOplogItem retry %s %s', programName, vc._queueLimitRetryTimeout, vc._oplogBuffer.length); }

        bs.resume();
      }, vc._queueLimitRetryTimeout);
    }
  });
  process.stdin.resume();

  // convert text names of hooks to the actuel required hook module
  // and creates a hook for all the keys to hide
  // req must be a push or pull request
  function setupHooks(req) {
    if (req.hooks && req.hooks.length) {
      var hooks = [];
      req.hooks.forEach(function(hookName) {
        if (!loadedHooks[hookName]) {
          var error = 'hook requested that is not loaded';
          console.error('%s: error %s %s', programName, error, hookName);
          throw new Error(error);
        }
        hooks.push(loadedHooks[hookName]);
      });
      req.hooks = hooks;
    }

    if (req.hooksOpts && req.hooksOpts.hide) {
      req.hooks = req.hooks || [];
      // create a hook for keys to hide
      var keysToHide = req.hooksOpts.hide;
      req.hooks.push(function(db, item, opts, cb) {
        keysToHide.forEach(function(key) {
          delete item[key];
        });
        cb(null, item);
      });
    }
  }

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
        console.log('%s: connecting to %s', programName, path);
        conn = net.createConnection(path);
      } else {
        console.log('%s: connecting to %s:%s', programName, host, port);
        conn = net.createConnection(port, host);
      }

      cm.register(conn, {
        reconnectOnError: true
      });

      // send auth request
      var authReq = {
        username:   username,
        password:   password,
        database:   database,
        collection: collection
      };

      if (last) {
        authReq.offset = last._id._v;
      }

      // expect either bson or a disconnect after the auth request is sent
      var bs2 = new BSONStream();

      // create remote transform to ensure _id._pe is set to this remote and run all hooks
      var opts = {
        db: db,
        hooks: pullReq.hooks || [],
        hooksOpts: pullReq.hooksOpts || {},
        debug: cfg.debug
      };
      var rt = new RemoteTransform(database, opts);

      rt.on('error', function(err) {
        console.error('remote transform error', err, JSON.stringify(conn.address()));
      });

      // handle incoming BSON data
      conn.pipe(bs2).pipe(rt).pipe(vc);

      conn.once('end', function() {
        conn.unpipe(bs2);
        console.log('%s: conn end unpiped conn--bs2', programName);
        bs2.end();
      });

      bs2.once('end', function() {
        bs2.unpipe(rt);
        console.log('%s: bs2 end unpiped bs2--rt ', programName);
        rt.end();
      });

      rt.once('end', function() {
        rt.unpipe(vc);
        console.log('%s: rt end unpiped rt--vc', programName);
      });

      // send auth request
      if (cfg.debug) { console.log('%s: sending auth request', programName, debugReq(authReq)); }
      conn.write(JSON.stringify(authReq) + '\n');
    });
  }

  // handle incoming pull and push requests
  function handleIncomingMsg(req, conn) {
    console.log('%s: incoming ipc message', programName, debugReq(req));

    if (pullRequest.valid(req)) {
      if (conn) { connErrorHandler(conn, 'pull request can not have a connection associated'); }
      // convert all hooks to the loaded module and transform hide keys into a hook function
      setupHooks(req);
      handlePullReq(req);
    } else if (pushRequest.valid(req)) {
      if (!conn) { console.error('push request must have a connection associated'); return; }

      req.raw = true;

      // convert all hooks to the loaded module and transform hide keys into a hook function
      try {
        setupHooks(req);
      } catch (err) {
        connErrorHandler(conn, err);
        return;
      }

      req.debug = cfg.debug;

      cm.register(conn);

      var vcr = vc.createReader(req);

      registerVcr(vcr, conn);

      vcr.pipe(conn);

      vcr.once('end', function() {
        vcr.unpipe(conn);
        console.log('%s: vcr end unpiped vcr--conn ', programName);
      });
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

      cm.close(function(err) {
        if (err) { throw err; }

        console.log('%s: connections closed', programName);

        process.stdin.unpipe(bs);
        console.log('%s: unpiped stdin--bs', programName);

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

  programName = 'vce ' + msg.dbName + '.' + msg.collectionName;

  // require any hooks before chrooting
  if (msg.hookPaths) {
    try {
      loadedHooks = requireJsFromDirs(msg.hookPaths);
    } catch(err) {
      console.error('error loading hooks', msg.hookPaths, err);
      process.exit(2);
    }
  }

  // open database
  _db(msg, function(err, db) {
    if (err) { throw err; }
    startVc(db, msg);
  });
});
