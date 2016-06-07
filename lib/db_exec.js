/**
 * Copyright 2014, 2015, 2016 Netsend.
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

var fs = require('fs');

var async = require('async');
var bson = require('bson');
var BSONStream = require('bson-stream');
var chroot = require('chroot');
var keyFilter = require('object-key-filter');
var LDJSONStream = require('ld-jsonstream');
var level = require('level-packager')(require('leveldown'));
var mkdirp = require('mkdirp');
var posix = require('posix');

var MergeTree = require('./merge_tree');
var dataRequest = require('./data_request');
var logger = require('./logger');
var noop = require('./noop');
var parsePersConfigs = require('./parse_pers_configs');

var BSON = new bson.BSONPure.BSON();

/**
 * Instantiates a merge tree and handles incoming and outgoing requests.
 *
 * 1. setup all import and export hooks, filters etc.
 * 2. send and receive data requests on incoming connections
 *
 * This module should not be included but forked. A message is sent when it enters
 * a certain state and to signal the parent it's ready to receive messages.
 *
 * Full FSM: init --> listen
 *
 * The first message emitted is "init" which signals that this process is ready to
 * receive configuration data, which consists of the db config, including
 * perspective configs and log configuration. File desciptors for the log should be
 * passed after sending the config.
 *
 * {
 *   log:            {Object}      // log configuration
 *   [name]:         {String}      // name of this database, defaults to "persdb"
 *   [path]:         {String}      // db path within the chroot directory, defaults
 *                                 // to "/data"
 *   [chroot]:       {String}      // defaults to /var/persdb
 *   [hookPaths]:    {Array}       // list of paths to load hooks from
 *   [user]:         {String}      // defaults to "pdblevel"
 *   [group]:        {String}      // defaults to "pdblevel"
 *   [debug]:        {Boolean}     // defaults to false
 *   [perspectives]: {Array}       // array of other perspectives
 *   [mergeTree]:    {Object}      // any MergeTree options
 * }
 *
 * After the database is opened and hooks are loaded, this process emits a message
 * named "listen", signalling that it's ready to receive local and remote data
 * requests, a head lookup channel request, a startMerge signal or a kill signal.
 *
 * Data channel and head lookup requests should be accompanied with a file
 * descriptor.
 *
 * {
 *   type: 'remoteDataChannel'
 *   perspective:       {String}      name of the perspective
 *   receiveBeforeSend: {Boolean}     whether to wait with sending of data request
 * }
 *
 * {
 *   type: 'localDataChannel'
 * }
 *
 * {
 *   type: 'headLookup'
 * }
 *
 * {
 *   type: 'kill'
 * }
 *
 * For remote data channels a data request is sent back to the client. After this a
 * data request is expected from the other side so that the real data exchange
 * (BSON) can be started.
 *
 * A data request has the following stucture:
 * {
 *   start:          {String|Boolean}  whether or not to receive data as well. Either a
 *                                     boolean or a base64 encoded version number.
 * }
 *
 * A local data channel is used to update the local tree. Only one local data
 * channel can be active. Received versions must be either new versions or
 * confirmations of merged versions. If a new version is created by a remote the
 * merge is sent back with the previous head on the connection.
 *
 * A head lookup can be done by sending an id. The last version of that id in the
 * local tree is sent back. If no id is given, the last saved version in the tree
 * is returned.
 *
 * A head lookup request has the following stucture:
 * {
 *   id:          {String}  id, optional
 * }
 *
 * Merging remote perspectives with the local perspective can be initiated by sending
 * a startMerge signal with an optional interval (defaults to 5000ms).
 *
 * {
 *   type: 'startMerge'
 *   interval:       {Number, default 5000}  number of ms to check for new versions
 *                                           to merge
 * }
 */

var log; // used after receiving the log configuration

var programName = 'dbe';

/**
 * Require all js files in the given directories.
 *
 * @param {Array} hooks  list of directories
 * @return {Object} list of hooks indexed by path
 */
function requireJsFromDirsSync(hooks) {
  if (!Array.isArray(hooks)) { throw new TypeError('hooks must be an array'); }

  var result = {};
  hooks.forEach(function(dir) {
    // ensure trailing slash
    if (dir[dir.length - 1] !== '/') {
      dir += '/';
    }

    var files = fs.readdirSync(dir);
    files.forEach(function(file) {
      // only load regular files that end with .js
      if ((file.indexOf('.js') === file.length - 3) && fs.statSync(dir + file).isFile()) {
        log.info('loading', dir + file);
        result[require('path').basename(file, '.js')] = require(dir + file);
      }
    });
  });

  return result;
}

// filter password out request
function debugReq(req) {
  return keyFilter(req, ['password'], true);
}

function connErrorHandler(conn, connId, err) {
  log.err('connection error: %s %s', err, connId);
  try {
    conn.destroy();
  } catch(err) {
    log.err('connection write or disconnect error: %s', err);
  }
}

// return a string identifier of the connection if connected over ip
function createConnId(conn) {
  var connId;
  if (conn.remoteAddress) {
    connId = conn.remoteAddress + '-' + conn.remotePort + '-' + conn.localAddress + '-' + conn.localPort;
  } else if (conn._handle && conn._handle.fd) {
    connId = conn._handle.fd;
  } else {
    connId = 'unknown socket';
  }
  return connId;
}

// globals
var db;
var headLookupReceived = false;
var localDataChannelReceived = false;
var merging = false;
var connections = {};

/**
 * Lookup the head of a given id, or the head of the whole tree. Operates only on
 * the local tree.
 */
function headLookupConnHandler(conn, mt) {
  log.info('head lookup channel %s %j', createConnId(conn), conn.address());

  var error;
  var connId = createConnId(conn);
  if (connections[connId]) {
    connErrorHandler(conn, connId, new Error('connection already exists'));
    return;
  }

  // expect head lookup requests
  var ls = new LDJSONStream({ maxDocLength: 512 });

  ls.on('error', function(err) {
    connErrorHandler(conn, connId, err);
  });

  connections[connId] = conn;

  conn.on('error', function(err) {
    log.err('%s: %s', connId, err);
    ls.end();
  });
  conn.on('close', function() {
    log.info('%s: close', connId);
    ls.end();
    delete connections[connId];
  });

  var ltree = mt.getLocalTree();

  conn.pipe(ls).on('readable', function() {
    var req = ls.read();
    if (req == null) { return; }
    log.info('head lookup %j', req);

    function lookupById(id, cb) {
      var head;
      ltree.getHeads({ id: req.id, skipConflicts: true, skipDeletes: true, bson: true }, function(h, next) {
        if (head) {
          error = new Error('already found another non-conflicting and non-deleted head');
          log.err('head lookup error for %s "%s" %j %j', req.id, error, head.h, h.h);
          next(error);
          return;
        }
        head = h;
        next();
      }, function(err) {
        if (err) { cb(err); return; }

        // if head not found but in buffer then wait, otherwise cb
        if (head) {
          cb(null, head);
        } else {
          log.info('id not in tree, search buffer', id);
          if (ltree.inBufferById(id)) {
            log.info('id still in buffer to be written, wait 100ms');
            setTimeout(function() {
              lookupById(id, cb);
            }, 100);
          } else {
            log.info('id not in buffer either', id);
            cb(null, null);
          }
        }
      });
    }

    if (req.id) {
      lookupById(req.id, function(err, head) {
        if (err) { connErrorHandler(conn, connId, err); return; }

        // write the head or an empty object if not found
        if (head) {
          conn.write(head);
        } else {
          conn.write(BSON.serialize({}));
        }
      });
    } else {
      ltree.lastVersion(function(err, v) {
        if (err) { connErrorHandler(conn, connId, err); return; }

        if (!v) {
          // no version yet, write empty object
          conn.write(BSON.serialize({}));
        } else {
          ltree.getByVersion(v, { bson: true }, function(err, h) {
            if (err) { connErrorHandler(conn, connId, err); return; }
            if (!h) {
              error = new Error('head not found');
              log.err('head lookup error for %s "%s"', req.id, error);
              connErrorHandler(conn, connId, error);
              return;
            }

            conn.write(h);
          });
        }
      });
    }
  });
}

/**
 * Handle incoming data for the local tree and pass back new merged versions
 * to the connection.
 *
 * Currently not possible to get older versions, only new merges from the moment
 * this handler is setup. So make sure it is setup before startMerging is called
 * and that it is started in a synced state with the other end of the connection
 * (bootstrapped with an empty local tree).
 *
 * - setup writer to the local tree
 * - hookup merge handler to the connection (but don't start merging)
 */
function localDataConnHandler(conn, mt) {
  log.info('local data channel %s %j', createConnId(conn), conn.address());

  var connId = createConnId(conn);
  if (connections[connId]) {
    connErrorHandler(conn, connId, new Error('connection already exists'));
    return;
  }

  conn.on('close', function() {
    log.info('%s: close', connId);
    delete connections[connId];
  });

  connections[connId] = conn;

  // write new data to local tree
  var bs = new BSONStream();

  bs.on('error', function(err) {
    log.err('bson %s', err);
    conn.end();
  });

  // pipe data to local write stream
  conn.pipe(bs).pipe(mt.createLocalWriteStream());

  // setup merge handler to write data back to the connection
  mt.ensureMergeHandler(function mergeHandler(newVersion, prevVersion, cb) {
    conn.write(BSON.serialize({
      n: newVersion,
      o: prevVersion
    }), cb);
  });
}

/**
 * - expect one data request
 * - send one data request
 * - maybe export data
 * - maybe import data
 */
function remoteDataConnHandler(conn, mt, pers, receiveBeforeSend) {
  log.info('client connected %s %j', createConnId(conn), conn.address());

  var connId = createConnId(conn);
  if (connections[connId]) {
    connErrorHandler(conn, connId, new Error('connection already exists'));
    return;
  }

  var mtr, mtw, dataReqReceived, dataReqSent;

  conn.on('error', function(err) {
    log.err('%s: %s', connId, err);
    if (mtr) { mtr.close(); }
  });
  conn.on('close', function() {
    log.info('%s: close', connId);
    if (mtr) { mtr.close(); }
    delete connections[connId];
  });

  connections[connId] = conn;

  // expect one data request
  // create line delimited json stream
  var ls = new LDJSONStream({ flush: false, maxDocs: 1, maxBytes: 512 });

  ls.on('error', function(err) {
    connErrorHandler(conn, connId, err);
  });

  var dataReq; // must be accessible in finalize if receiveBeforeSend

  // setup mtr and mtw if data requests have been received and sent
  function finalize() {
    // handle receiveBeforeSend
    if (!dataReqSent && receiveBeforeSend && dataReqReceived && dataReq) {
      log.notice('setup pipes and send back data request %j', dataReq);
      conn.write(JSON.stringify(dataReq) + '\n');
      dataReqSent = true;
    }
    if (dataReqReceived && dataReqSent) {
      // send data to remote if requested and allowed
      if (mtr) { mtr.pipe(conn); }

      // receive data from remote if requested and allowed
      if (mtw) {
        // expect bson
        var bs = new BSONStream();

        bs.on('error', function(err) {
          log.err('bson %s', err);
          conn.end();
        });

        // receive data from remote
        conn.pipe(bs).pipe(mtw);
      }
    }
  }

  conn.pipe(ls).once('readable', function() {
    var req = ls.read();
    if (req == null) { return; }
    log.info('req received %j', req);

    conn.unpipe(ls);
    // push back any data in ls
    if (ls.buffer.length) {
      log.info('push back %d bytes', ls.buffer.length);
      conn.unshift(ls.buffer);
    }

    if (!dataRequest.valid(req)) {
      log.err('invalid data request %j', req);
      connErrorHandler(conn, connId, 'invalid data request');
      return;
    }

    dataReqReceived = true;

    // determine start and whether there is an export key
    // if so, open merge tree reader
    if (req.start && !pers.export) {
      log.notice('data requested but no export configured for "%s"', pers.name);
    }
    if (req.start && pers.export) {
      var readerOpts = {
        log: log,
        tail: true,
        tailRetry: pers.tailRetry || 5000,
        bson: true
      };
      if (typeof req.start === 'string') {
        readerOpts.first = req.start;
        readerOpts.excludeFirst = true;
      }
      if (typeof pers.export === 'object') {
        readerOpts.filter    = pers.export.filter;
        readerOpts.hooks     = pers.export.hooks;
        readerOpts.hooksOpts = pers.export.hooksOpts;
      }

      mtr = mt.createReadStream(readerOpts);

      mtr.on('error', function(err) {
        log.err('mtr error %s %s', connId, err);
      });

      mtr.on('end', function() {
        log.notice('mtr end %s', connId);
      });
    }
    finalize();
  });

  // send data request with last offset if there are any import rules
  if (pers.import) {
    // find last received item of this remote
    mt.lastReceivedFromRemote(pers.name, 'base64', function(err, last) {
      if (err) {
        log.err('remoteDataConnHandler lastReceivedFromRemote %s %s', err, pers.name);
        connErrorHandler(conn, connId, err);
        return;
      }

      log.info('last %s %j', pers.name, last);

      // create remote transform to ensure h.pe is set to this remote and run all hooks
      var wsOpts = {
        db:         db,
        filter:     pers.import.filter || {},
        hooks:      pers.import.hooks || [],
        hooksOpts:  pers.import.hooksOpts || {},
        log:        log
      };
      // some export hooks need the name of the database
      wsOpts.hooksOpts.to = db;

      // set hooksOpts with all keys but the pre-configured ones
      Object.keys(pers.import).forEach(function(key) {
        if (!~['filter', 'hooks', 'hooksOpts'].indexOf(key)) {
          wsOpts.hooksOpts[key] = pers.import[key];
        }
      });

      mtw = mt.createRemoteWriteStream(pers.name, wsOpts);

      mtw.on('error', function(err) {
        log.err('merge tree "%s"', err);
        conn.end();
      });

      // send data request with last offset
      dataReq = { start: true };
      if (last) {
        dataReq.start = last;
      }

      if (!receiveBeforeSend || dataReqReceived) {
        log.notice('setup pipes and send back data request %j', dataReq);
        conn.write(JSON.stringify(dataReq) + '\n');
        dataReqSent = true;
      }
      finalize();
    });
  } else {
    log.notice('signal that no data is expected');
    conn.write(JSON.stringify({ start: false }) + '\n');

    dataReqSent = true;
    finalize();
  }
}

/**
 * Start listening. Expect only remoteDataChannel messages after that.
 */
function postChroot(cfg) {
  if (typeof cfg !== 'object') { throw new TypeError('cfg must be an object'); }

  // setup list of connections to initiate and create an index by perspective name
  var persCfg = parsePersConfigs(cfg.perspectives || []);
  log.info('persCfg %j', debugReq(persCfg));

  // return hooksOpts with all but the pre-configured keys
  function createHooksOpts(cfg) {
    var hooksOpts = cfg.hooksOpts || {};

    Object.keys(cfg).forEach(function(key) {
      if (!~['filter', 'hooks', 'hooksOpts', 'hide'].indexOf(key)) {
        hooksOpts[key] = cfg[key];
      }
    });

    return hooksOpts;
  }

  // if hooksOpts has a hide key, push a new hook in hooks
  function ensureHideHook(hooksOpts, hooks) {
    if (hooksOpts && hooksOpts.hide) {
      // create a hook for keys to hide
      var keysToHide = hooksOpts.hide;
      hooks.push(function(db, item, opts, cb) {
        keysToHide.forEach(function(key) {
          delete item[key];
        });
        cb(null, item);
      });
    }
  }

  // load hook function in place of name
  function replaceHookNames(hooks) {
    var error;
    if (hooks && hooks.length) {
      hooks.forEach(function(hookName, i) {
        if (!cfg.loadedHooks[hookName]) {
          error = new Error('hook requested that is not loaded');
          log.err('loadHooks %s %s', error, hookName);
          throw error;
        }
        hooks[i] = cfg.loadedHooks[hookName];
      });
    }
  }

  // replace hooks and hide keys with actual hook implementations
  Object.keys(persCfg.pers).forEach(function(name) {
    var pers = persCfg.pers[name];
    if (pers.import) {
      if (pers.import.hooks) {
        replaceHookNames(pers.import.hooks);
        if (pers.import.hooksOpts) {
          ensureHideHook(pers.import.hooksOpts, pers.import.hooks);
          pers.import.hooksOpts = createHooksOpts(pers.import.hooksOpts);
        }
      }
    }
    if (pers.export) {
      if (pers.export.hooks) {
        replaceHookNames(pers.export.hooks);
        if (pers.export.hooksOpts) {
          ensureHideHook(pers.export.hooksOpts, pers.export.hooks);
          pers.export.hooksOpts = createHooksOpts(pers.export.hooksOpts);
        }
      }
    }
  });

  var mtOpts = cfg.mergeTree || {};
  mtOpts.perspectives = Object.keys(persCfg.pers);
  mtOpts.log = log;

  // set global, used in remoteDataConnHandler
  var mt = new MergeTree(db, mtOpts);

  //var cm = connManager.create({ log: log });
  var error;

  // handle incoming messages
  // determine the type of message
  function handleIncomingMsg(msg, conn) {
    log.notice('incoming ipc message %j', msg);

    switch (msg.type) {
    case 'headLookup':
      // can only start merging if currently not yet merging
      if (headLookupReceived) {
        error = new Error('already registered a head lookup channel');
        log.err('%s %j', error, msg);
        connErrorHandler(conn, null, error);
        return;
      }

      headLookupReceived = true;
      headLookupConnHandler(conn, mt);
      break;
    case 'startMerge':
      // can only start merging if currently not yet merging
      if (merging) {
        error = new Error('already merging');
        log.err('%s %j', error, msg);
        connErrorHandler(conn, null, error);
        return;
      }

      merging = true;

      // start auto-merging
      mt.mergeAll({ interval: msg.interval || mtOpts.autoMergeInterval || 5000 });
      break;
    case 'localDataChannel':
      if (!conn) {
        log.err('handleIncomingMsg connection missing %j', msg);
        return;
      }

      // setup at most one local data channel
      if (localDataChannelReceived) {
        error = new Error('already received a local data channel request');
        log.err('%s %j', error, msg);
        connErrorHandler(conn, null, error);
        return;
      }

      localDataChannelReceived = true;
      localDataConnHandler(conn, mt);
      break;
    case 'remoteDataChannel':
      if (!conn) {
        log.err('handleIncomingMsg connection missing %j', msg);
        return;
      }

      // authenticated connection, incoming internal data request
      var perspective = msg.perspective;

      var pers = persCfg.pers[perspective];

      if (!pers) {
        error = new Error('unknown perspective');
        log.err('%s %j', error, msg);
        connErrorHandler(conn, perspective, error);
        return;
      }
      remoteDataConnHandler(conn, mt, pers, msg.receiveBeforeSend);
      break;
    case 'kill':
      // stop this process
      shutdown();
      break;
    default:
      error = new Error('unknown message type');
      log.err('%s %j', error, msg);
      connErrorHandler(conn, null, error);
      break;
    }
  }

  process.on('message', handleIncomingMsg);

  // handle shutdown
  var shuttingDown = false;
  function shutdown() {
    if (shuttingDown) {
      log.info('shutdown already in progress');
      return;
    }
    shuttingDown = true;
    log.info('shutting down...');

    // stop handling incoming messages
    process.removeListener('message', handleIncomingMsg);

    async.each(Object.keys(connections), function(connId, cb) {
      log.info('closing %s', connId);
      var conn = connections[connId];
      conn.once('close', cb);
      conn.end();
    }, function(err) {
      if (err) { log.err('error closing connection: %s', err); }

      mt.close(function(err) {
        if (err) { log.err('error closing mt: %s', err); }

        log.info('mt closed');

        db.close(function(err) {
          if (err) { log.err('error closing db: %s', err); }
          log.notice('closed');
        });
      });
    });
  }

  // ignore kill signals
  process.once('SIGTERM', noop);
  process.once('SIGINT', noop);

  process.on('SIGUSR2', function() {
    // create object with connection stats
    var connStats = Object.keys(connections).map(function(connId) {
      var conn = connections[connId];
      var res = {};
      res[connId] = {
        read: conn.bytesRead,
        written: conn.bytesWritten
      };
      return res;
    });
    mt.stats(function(err, mtStats) {
      if (err) { log.err('SIGUSR2:\n%s', err); return; }

      log.notice('SIGUSR2:\n%j\nconnections:\n%j', mtStats, connStats);
    });
  });

  // send a "listen" signal
  process.send('listen');
}

if (typeof process.send !== 'function') {
  throw new Error('this module should be invoked via child_process.fork');
}

process.send('init');

/**
 * Expect a db config, log config and any merge tree options.
 *
 * {
 *   log:            {Object}      // log configuration
 *   [name]:         {String}      // name of this database, defaults to "persdb"
 *   [path]:         {String}      // db path within the chroot directory, defaults
 *                                 // to "/data"
 *   [chroot]:       {String}      // defaults to /var/persdb
 *   [hookPaths]:    {Array}       // list of paths to load hooks from
 *   [user]:         {String}      // defaults to "pdblevel"
 *   [group]:        {String}      // defaults to "pdblevel"
 *   [debug]:        {Boolean}     // defaults to false
 *   [perspectives]: {Array}       // array of other perspectives
 *   [mergeTree]:    {Object}      // any MergeTree options
 * }
 */
process.once('message', function(msg) {
  if (typeof msg !== 'object') { throw new TypeError('msg must be an object'); }
  if (typeof msg.log !== 'object') { throw new TypeError('msg.log must be an object'); }

  if (msg.name != null && typeof msg.name !== 'string') { throw new TypeError('msg.name must be a string'); }
  if (msg.path != null && typeof msg.path !== 'string') { throw new TypeError('msg.path must be a string'); }
  if (msg.chroot != null && typeof msg.chroot !== 'string') { throw new TypeError('msg.chroot must be a string'); }
  if (msg.hookPaths != null && !Array.isArray(msg.hookPaths)) { throw new TypeError('msg.hookPaths must be an array'); }
  if (msg.user != null && typeof msg.user !== 'string') { throw new TypeError('msg.user must be a string'); }
  if (msg.group != null && typeof msg.group !== 'string') { throw new TypeError('msg.group must be a string'); }
  if (msg.debug != null && typeof msg.debug !== 'boolean') { throw new TypeError('msg.debug must be a string'); }
  if (msg.perspectives != null && !Array.isArray(msg.perspectives)) { throw new TypeError('msg.perspectives must be an array'); }
  if (msg.mergeTree != null && typeof msg.mergeTree !== 'object') { throw new TypeError('msg.mergeTree must be an object'); }

  programName = 'dbe ' + msg.name || 'persdb';

  process.title = 'pdb/' + programName;

  var path = msg.path || '/data';
  // ensure leading slash
  if (path[0] !== '/') {
    path = '/' + path;
  }

  var user = msg.user || 'pdblevel';
  var group = msg.group || 'pdblevel';

  var newRoot = msg.chroot || '/var/persdb';

  msg.log.ident = programName;

  // open log
  logger(msg.log, function(err, l) {
    if (err) { l.err(err); throw err; }

    log = l; // use this logger in the mt's as well

    // require any hooks before chrooting
    if (msg.hookPaths) {
      try {
        msg.loadedHooks = requireJsFromDirsSync(msg.hookPaths);
      } catch(err) {
        log.err('error loading hooks: "%s" %s', msg.hookPaths, err);
        process.exit(2);
      }
    } else {
      msg.loadedHooks = {};
    }

    var uid, gid;
    try {
      uid = posix.getpwnam(user).uid;
      gid = posix.getgrnam(group).gid;
    } catch(err) {
      log.err('%s %s:%s', err, user, group);
      process.exit(3);
    }

    // chroot or exit
    function doChroot() {
      try {
        chroot(newRoot, uid, gid);
        log.debug('changed root to %s and user:group to %s:%s', newRoot, user, group);
      } catch(err) {
        log.err('changing root or user failed: %s %s:%s "%s"', newRoot, user, group, err);
        process.exit(8);
      }
    }

    // set core limit to maximum allowed size
    posix.setrlimit('core', { soft: posix.getrlimit('core').hard });
    log.debug('core limit: %j, fsize limit: %j', posix.getrlimit('core'), posix.getrlimit('fsize'));

    // open db and call postChroot or exit
    function openDbAndProceed() {
      level(path, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, dbc) {
        if (err) {
          log.err('opening db %s', err);
          process.exit(9);
        }
        log.debug('opened db %s', path);

        db = dbc;

        postChroot(msg);
      });
    }

    // ensure chroot exists
    mkdirp(newRoot, '755', function(err) {
      if (err) {
        log.err('cannot make chroot', err);
        process.exit(3);
      }

      // ensure database directory exists
      fs.stat(newRoot + path, function(err, stats) {
        if (err && err.code !== 'ENOENT') {
          log.err('stats on path failed %s %j', err, debugReq(msg));
          process.exit(4);
        }

        if (err && err.code === 'ENOENT') {
          fs.mkdir(newRoot + path, 0o700, function(err) {
            if (err) {
              log.err('path creation failed %s %j', err, debugReq(msg));
              process.exit(5);
            }

            fs.chown(newRoot + path, uid, gid, function(err) {
              if (err) {
                log.err('setting path ownership failed %s %j', err, debugReq(msg));
                process.exit(6);
              }

              doChroot();
              openDbAndProceed();
            });
          });
        } else {
          if (!stats.isDirectory()) {
            log.err('path exists but is not a directory %j %j', stats, debugReq(msg));
            process.exit(7);
          }
          doChroot();
          openDbAndProceed();
        }
      });
    });
  });
});
