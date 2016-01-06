/**
 * Copyright 2014, 2015 Netsend.
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
var chroot = require('chroot');
var BSONStream = require('bson-stream');
var LDJSONStream = require('ld-jsonstream');
var keyFilter = require('object-key-filter');
var level = require('level');
var posix = require('posix');

var MergeTree = require('./merge_tree');
//var connManager = require('./conn_manager');
var dataRequest = require('./data_request');
var logger = require('./logger');
var parsePersConfigs = require('./parse_pers_configs');

//var sshClient = require('ssh-client');

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
 *   [user]:         {String}      // defaults to "nobody"
 *   [group]:        {String}      // defaults to "nobody"
 *   [debug]:        {Boolean}     // defaults to false
 *   [perspectives]: {Array}       // array of other perspectives
 *   [mergeTree]:    {Object}      // any MergeTree options
 * }
 *
 * After the database is opened and hooks are loaded, this process emits a message
 * named "listen", signalling that it's ready to receive data requests.
 *
 * Any subsequent messages sent to this process should be authenticated auth
 * requests (internal authorized connection forwards). These should be accompanied
 * with the connection and a perspective.
 * {
 *   perspective:    {String}      name of the perspective
 * }
 *
 * A data request is sent on the incoming connection and one is expected from the
 * other side.
 * {
 *   start:          {String|Boolean}  whether or not to receive data as well. Either a
 *                                     boolean or a base64 encoded version number.
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
        log.info('dbe loading', dir + file);
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
  log.err('dbe connection error: %s %s', err, connId);
  try {
    conn.destroy();
  } catch(err) {
    log.err('dbe connection write or disconnect error: %s', err);
  }
}

// return a string identifier of the connection if connected over ip
function createConnId(conn) {
  var connId;
  if (conn.remoteAddress) {
    connId = conn.remoteAddress + '-' + conn.remotePort + '-' + conn.localAddress + '-' + conn.localPort;
  } else {
    connId = 'unknown socket';
  }
  return connId;
}

// globals
var db;
var connections = {};

/**
 * - expect one data request
 * - send one data request
 * - maybe export data
 * - maybe import data
 */
function connHandler(conn, mt, pers) {
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
  var ls = new LDJSONStream({ flush: false, maxBytes: 512 });

  // setup mtr and mtw if data requests have been received and sent
  function finalize() {
    if (dataReqReceived && dataReqSent) {
      // send data to remote if requested and allowed
      if (mtr) { mtr.pipe(conn); }

      // receive data from remote if requested and allowed
      if (mtw) {
        // expect bson
        var bs = new BSONStream();

        bs.on('error', function(err) {
          log.err('dbe bson %s', err);
          conn.end();
        });

        // receive data from remote
        conn.pipe(bs).pipe(mtw);
      }
    }
  }

  conn.pipe(ls).once('data', function(req) {
    log.info('req received %j', req);

    conn.unpipe(ls);
    // push back any data in ls
    if (ls.buffer.length) {
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
    if (req.start && pers.export) {
      var readerOpts = {
        log: log,
        first: req.offset,
        tail: true,
        raw: true
      };
      if (typeof pers.export === 'object') {
        readerOpts.filter    = pers.export.filter;
        readerOpts.hooks     = pers.export.hooks;
        readerOpts.hooksOpts = pers.export.hooksOpts;
      }

      mtr = mt.createReadStream(readerOpts);

      mtr.on('error', function(err) {
        log.err('dbe mtr error %s %s', connId, err);
      });

      mtr.on('end', function() {
        log.notice('dbe mtr end %s', connId);
      });
    }
    finalize();
  });

  // send data request with last offset if there are any import rules
  if (pers.import) {
    // find last local item of this remote
    mt.lastByPerspective(pers.name, 'base64', function(err, last) {
      if (err) {
        log.err('dbe connHandler lastByPerspective %s %s', err, pers.name);
        connErrorHandler(conn, connId, err);
        return;
      }

      log.info('dbe last %s %j', pers.name, last);

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
        log.err('dbe merge tree "%s"', err);
        conn.end();
      });

      // send data request with last offset
      var dataReq = { start: true };
      if (last) {
        dataReq.start = last;
      }

      log.notice('dbe setup pipes and send back data request %j', dataReq);
      conn.write(JSON.stringify(dataReq) + '\n');

      dataReqSent = true;
      finalize();
    });
  } else {
    log.notice('dbe signal that no data is expected');
    conn.write(JSON.stringify({ start: false }) + '\n');

    dataReqSent = true;
    finalize();
  }
}

/**
 * Start listening.
 */
function postChroot(cfg) {
  if (typeof cfg !== 'object') { throw new TypeError('cfg must be an object'); }

  // setup list of connections to initiate and create an index by perspective name
  var persCfg = parsePersConfigs(cfg.perspectives || []);
  log.info('dbe persCfg %j', debugReq(persCfg));

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
          log.err('dbe loadHooks %s %s', error, hookName);
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
  mtOpts.autoMergeInterval = mtOpts.autoMergeInterval || 1000;

  // set global, used in connHandler
  var mt = new MergeTree(db, mtOpts);

  //var cm = connManager.create({ log: log });
  var error;

  // handle incoming messages
  // determine if a auth response or stripped auth request has to be sent
  function handleIncomingMsg(req, conn) {
    if (!conn) {
      log.err('dbe handleIncomingMsg connection missing %j', req);
      return;
    }

    log.notice('dbe incoming ipc message %j', req);

    // authenticated connection, incoming internal data request
    var perspective = req.perspective;

    var pers = persCfg.pers[perspective];

    if (!pers) {
      error = new Error('unknown perspective');
      log.err('dbe %s %j', error, req);
      connErrorHandler(conn, perspective, error);
      return;
    }

    connHandler(conn, mt, pers);
  }

  process.on('message', handleIncomingMsg);

  // handle shutdown
  function shutdown() {
    log.notice('dbe shutting down');

    // stop handling incoming messages
    process.removeListener('message', handleIncomingMsg);

    async.each(Object.keys(connections), function(connId, cb) {
      log.info('closing %s', connId);
      var conn = connections[connId];
      conn.once('close', cb);
      conn.end();
    }, function(err) {
      if (err) { log.err('dbe error closing connection: %s', err); }

      mt.close(function(err) {
        if (err) { log.err('dbe error closing mt: %s', err); }

        db.close(function(err) {
          if (err) { log.err('dbe error closing db: %s', err); }

          //process.disconnect();
          log.notice('dbe closed');

          // TODO: check why server hangs and does not exit when running in bi-sync mode
          // https://groups.google.com/forum/#!msg/nodejs/DzxUYzbzkns/9XYC74b0NVUJ might be related
          //process.exit();
        });
      });
    });
  }

  // listen to kill signals
  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);
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
 *   [user]:         {String}      // defaults to "nobody"
 *   [group]:        {String}      // defaults to "nobody"
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

  var user = msg.user || 'nobody';
  var group = msg.group || 'nobody';

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
        log.err('dbe error loading hooks: "%s" %s', msg.hookPaths, err);
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
      log.err('dbe %s %s:%s', err, user, group);
      process.exit(3);
    }

    // chroot or exit
    function doChroot() {
      try {
        chroot(newRoot, user, gid);
        log.notice('dbe changed root to %s and user:group to %s:%s', newRoot, user, group);
      } catch(err) {
        log.err('dbe changing root or user failed: %s %s:%s "%s"', newRoot, user, group, err);
        process.exit(8);
      }
    }

    // open db and call postChroot or exit
    function openDbAndProceed() {
      level(path, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, dbc) {
        if (err) {
          log.err('dbe opening db %s', err);
          process.exit(9);
        }
        log.info('dbe opened db %s', path);

        db = dbc;

        postChroot(msg);
      });
    }

    // ensure database directory exists
    fs.stat(newRoot + path, function(err, stats) {
      if (err && err.code !== 'ENOENT') {
        log.err('dbe stats on path failed %s %j', err, debugReq(msg));
        process.exit(4);
      }

      if (err && err.code === 'ENOENT') {
        fs.mkdir(newRoot + path, 0o755, function(err) {
          if (err) {
            log.err('dbe path creation failed %s %j', err, debugReq(msg));
            process.exit(5);
          }

          fs.chown(newRoot + path, uid, gid, function(err) {
            if (err) {
              log.err('dbe setting path ownership failed %s %j', err, debugReq(msg));
              process.exit(6);
            }

            doChroot();
            openDbAndProceed();
          });
        });
      } else {
        if (!stats.isDirectory()) {
          log.err('dbe path exists but is not a directory %j %j', stats, debugReq(msg));
          process.exit(7);
        }
        doChroot();
        openDbAndProceed();
      }
    });
  });
});
