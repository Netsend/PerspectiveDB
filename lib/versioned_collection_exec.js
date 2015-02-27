/**
 * Copyright 2014, 2015 Netsend.
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

var fs = require('fs');

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
var logger = require('./logger');

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
 *   [log]:           {Object}      // log configuration object
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

var log; // used after received the first message

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
        log.info('loading', npath + file);
        result[require('path').basename(file, '.js')] = require(npath + file);
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
  log.err('connection error:', e);
  try {
    conn.destroy();
  } catch(err) {
    log.err('connection write or disconnect error:', err);
  }
}

var loadedHooks = {};

var vcrs = [];
function registerVcr(vcr, conn) {
  var dest = connManager.destination(conn);

  log.notice('vcr registering', dest);

  vcrs.push(vcr);

  vcr.on('error', function(err) {
    log.err('vcr error:', err, dest);
  });

  vcr.once('end', function() {
    log.notice('vcr end:', dest);

    var i = vcrs.indexOf(vcr);
    if (~i) {
      log.info('deleting vcr', i);
      vcrs.splice(i, 1);
    } else {
      log.err('vcr not found', i);
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
    log.info('changed root and user to:', chrootNewRoot, chrootUser);
  } catch(err) {
    log.err('changing root or user failed', cfg, err);
    process.exit(1);
  }

  var vc = new VersionedCollection(db, cfg.collectionName, cfg);
  vc.startAutoProcessing(cfg.autoProcessInterval);

  var cm = connManager.create({ debug: cfg.debug, hide: cfg.hide });

  // handle oplog items on stdin
  var bs = new BSONStream();
  process.stdin.pause(); // enforce old-mode
  process.stdin.pipe(bs).on('data', function(obj) {
    vc.saveOplogItem2(obj, function(err) {
      if (err) {
        log.err('save oplog item error', err, obj);
        throw err;
      }
    });

    // pause stream if buffer exceeds max length
    if (vc._queueLimit <= vc._oplogBuffer.length) {
      log.warning('saveOplogItem queue full, wait and retry.', vc._queueLimitRetryTimeout, vc._oplogBuffer.length);

      bs.pause();

      setTimeout(function() {
        log.info('saveOplogItem retry', vc._queueLimitRetryTimeout, vc._oplogBuffer.length);

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
          log.err(error, hookName);
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
      if (err) { log.err(err, pullReq); return; }

      function connectHandler(err, conn2) {
        if (err) { log.err(err, pullReq); return; }

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
        opts.hooksOpts.to = db;

        var rt = new RemoteTransform(database, opts);

        rt.on('error', function(err) {
          log.err('remote transform error', err, JSON.stringify(conn2.address()));
        });

        // handle incoming BSON data
        conn2.pipe(bs2).pipe(rt).pipe(vc);

        conn2.once('end', function() {
          conn2.unpipe(bs2);
          log.notice('conn2 end unpiped conn2--bs2');
          bs2.end();
        });

        bs2.once('end', function() {
          bs2.unpipe(rt);
          log.notice('bs2 end unpiped bs2--rt ');
          rt.end();
        });

        rt.once('end', function() {
          rt.unpipe(vc);
          log.notice('rt end unpiped rt--vc');
        });

        // send auth request
        log.notice('sending auth request', debugReq(authReq));
        conn2.write(JSON.stringify(authReq) + '\n');
      }

      if (path) {
        log.notice('connecting to', path);
        cm.open(path, { reconnectOnError: true }, connectHandler);
      } else {
        log.notice('connecting to', host, port);
        cm.open(host, port, { reconnectOnError: true }, connectHandler);
      }
    });
  }

  // handle incoming pull and push requests
  function handleIncomingMsg(req, conn) {
    log.notice('incoming ipc message', debugReq(req));

    if (pullRequest.valid(req)) {
      if (conn) { connErrorHandler(conn, 'pull request can not have a connection associated'); }
      // convert all hooks to the loaded module and transform hide keys into a hook function
      setupHooks(req);
      handlePullReq(req);
    } else if (pushRequest.valid(req)) {
      if (!conn) { log.err('push request must have a connection associated'); return; }

      req.raw = true;

      // convert all hooks to the loaded module and transform hide keys into a hook function
      try {
        setupHooks(req);
      } catch (err) {
        connErrorHandler(conn, err);
        return;
      }

      req.debug = cfg.debug;

      req.hooksOpts = req.hooksOpts || {};
      req.hooksOpts.from = db;

      cm.registerIncoming(conn);

      var vcr = vc.createReader(req);

      registerVcr(vcr, conn);

      vcr.pipe(conn);

      vcr.once('end', function() {
        vcr.unpipe(conn);
        log.notice('vcr end unpiped vcr--conn ');
      });
    } else {
      connErrorHandler(conn, 'invalid push or pull request');
    }
  }
  process.on('message', handleIncomingMsg);

  // handle shutdown
  function shutdown() {
    log.notice('shutting down');
    closeVcrs(function(err) {
      if (err) { throw err; }

      log.notice('vcrs closed');

      cm.close(function(err) {
        if (err) { throw err; }

        log.notice('connections closed');

        process.stdin.unpipe(bs);
        log.notice('unpiped stdin--bs');

        process.stdin.pause();
        log.notice('stdin paused');

        process.removeListener('message', handleIncomingMsg);

        bs.end(function(err) {
          if (err) { throw err; }
          log.notice('bson stream ended');

          vc.stopAutoProcessing(function(err) {
            if (err) { throw err; }
            log.notice('vc stopped processing');
            db.close(function(err) {
              if (err) { throw err; }
              log.notice('db closed');
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

  // open log
  msg.log = msg.log || {};
  msg.log.ident = msg.log.ident || programName;
  msg.log.file = process.stdout;
  msg.log.error = process.stderr;

  logger(msg.log, function(err, l) {
    if (err) { throw err; }

    log = l;

    // require any hooks before chrooting
    if (msg.hookPaths) {
      try {
        loadedHooks = requireJsFromDirs(msg.hookPaths);
      } catch(err) {
        log.err('error loading hooks', msg.hookPaths, err);
        process.exit(2);
      }
    }

    // open database
    _db(msg, function(err, db) {
      if (err) {
        log.err('connecting to database', err);
        process.exit(3);
      }
      startVc(db, msg);
    });
  });
});
