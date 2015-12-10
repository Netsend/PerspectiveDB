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
var url = require('url');

var chroot = require('chroot');
var BSONStream = require('bson-stream');
var keyFilter = require('object-key-filter');
var level = require('level');
var posix = require('posix');

var MergeTree = require('./merge_tree');
var connManager = require('./conn_manager');
var logger = require('./logger');

//var sshClient = require('ssh-client');

/**
 * Instantiates a merge tree and handles incoming and outgoing requests.
 *
 * 1. setup all import and export hooks, filters etc.
 * 2. initiate connections to other locations for pull/push requests
 * 3. handle incoming connections with pull/push requests
 *
 * This module should be forked and sends messages when it enters a certain state
 * and to signal the parent it's ready to receive a certain message.
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
 * named "listen", signalling that it's ready to receive stripped auth requests.
 * Furthermore, this process will start initiating auth requests to all
 * perspectives with a "connect" attribute.
 *
 * Any subsequent messages sent to this process should be authenticated auth
 * requests (stripped auth requests). These should be accompanied with a connection
 * and be stripped from the username, password and db field, thus should only have
 * an "offset" attribute:
 * {
 *   [offset]:       {String}  optional version offset
 * }
 *
 * An incoming auth request is responded with an auth response object, which has the
 * following structure:
 * {
 *   start:          {String|Boolean}  whether or not to receive data as well. Either a
 *                                     boolean or a base64 encoded version number.
 * }
 *
 * New connections are initiated for each perspective with a "connect" attribute.
 * The sent auth request looks as follows:
 * {
 *   username:     {String}
 *   password:     {String}
 *   [db]:         {String}  name of the database to request, defaults to the name
 *                           of the local database
 *   [offset]:     {String}  optional version offset
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

/**
 * Quickly parse array of perspective configs. Determine which perspectives should
 * be connected to and parse connection url. Create Index of perspectives by name.
 *
 * Return:
 *   connect: [pers1, pers2, ...]  // perspectives this db should init
 *   pers: {
 *     name:
 *       import:
 *         filter
 *         hooks
 *         hooksOpts
 *         offset
 *       export:
 *         filter
 *         hooks
 *         hooksOpts
 *   }
 */
function parsePersConfigs(arr) {
  var result = {
    connect: [],
    pers: {}
  };

  arr.forEach(function(pcfg) {
    if (pcfg.connect) {
      result.connect.push(pcfg.name);
      pcfg.connect = url.parse(pcfg.connect);
    }
    result.pers[pcfg.name] = pcfg;
  });

  return result;
}

// filter password out request
function debugReq(req) {
  return keyFilter(req, ['password'], true);
}

function connErrorHandler(conn, e) {
  log.err('dbe connection error: %s', e);
  try {
    conn.destroy();
  } catch(err) {
    log.err('dbe connection write or disconnect error: %s', err);
  }
}

var mtrs = [];
function registerMtr(mtr, conn) {
  var dest = connManager.destination(conn);

  log.notice('dbe mtr registering %s', dest);

  mtrs.push(mtr);

  mtr.on('error', function(err) {
    log.err('dbe mtr error %s %s', dest, err);
  });

  mtr.once('end', function() {
    log.notice('dbe mtr end %s', dest);

    var i = mtrs.indexOf(mtr);
    if (~i) {
      log.info('dbe deleting mtr %d', i);
      mtrs.splice(i, 1);
    } else {
      log.err('dbe mtr not found %d', i);
    }
  });
}

// register a connection, or if a connection already exists, kill the old one
var conns = {};
function registerIncoming(conn, name, cb) {
  if (conns[name]) {
    // first close old connection
    conns[name].once('close', function() {
      // recurse
      registerIncoming(conn, name, cb);
    });
    conns.end();
    return;
  }

  // register new connection
  conns[name] = conn;

  // cleanup the socket on close
  conns[name].on('close', function() {
    delete conns[name];
  });
  process.nextTick(cb);
}

function closeMtrs() {
  mtrs.forEach(function(mtr) {
    mtr.pause(); // read stream
  });
}

/**
 * Start listening.
 */
function postChroot(db, cfg) {
  if (typeof db !== 'object') { throw new TypeError('db must be an object'); }
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

  var mt = new MergeTree(db, mtOpts);

  var cm = connManager.create({ log: log });

  // handle incoming messages
  function handleIncomingMsg(req, conn) {
    if (!conn) {
      log.err('dbe handleIncomingMsg connection missing %j', req);
      return;
    }

    log.notice('dbe incoming ipc message %j', debugReq(req));

    // authenticated connection, incoming auth request:
    // * determine auth response
    // * send data requested in the auth request

    var remote = req.username;

    var pers = persCfg.pers[remote];

    if (!pers) {
      log.err('dbe mtr handleIncomingMsg "unknown remote" %j', req);
      connErrorHandler(conn, new Error('unknown remote'));
      return;
    }

    // register incoming connection
    registerIncoming(conn, remote, function(err) {
      if (err) {
        log.err('dbe handleIncomingMsg registerIncoming %s %j', err, req);
        try {
          conn.destroy();
        } catch(err) {
          log.err('dbe connection write or disconnect error: %s', err);
        }
        return;
      }

      var mtr;
      if (pers.export) {
        var readerOpts = {
          log: log,
          first: req.offset,
          follow: true,
          raw: true
        };
        if (typeof pers.export === 'object') {
          readerOpts.filter    = pers.export.filter;
          readerOpts.hooks     = pers.export.hooks;
          readerOpts.hooksOpts = pers.export.hooksOpts;
        }

        mtr = mt.createReadStream(readerOpts);
        registerMtr(mtr, conn);
      }

      // send reply with last offset if there are any import rules
      if (pers.import) {
        // find last local item of this remote
        mt.lastByPerspective(remote, 'base64', function(err, last) {
          if (err) {
            log.err('dbe handleIncomingMsg lastByPerspective %s %j', err, req);
            connErrorHandler(conn, err);
            return;
          }

          log.info('dbe last %s %j', remote, last);

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
            if (!~['filter', 'hooks', 'hooksOpts', 'offset'].indexOf(key)) {
              wsOpts.hooksOpts[key] = pers.import[key];
            }
          });

          // expect bson
          var bs = new BSONStream();
          var mtw = mt.createRemoteWriteStream(remote, wsOpts);

          bs.on('error', function(err) {
            log.err('dbe bson %s', err);
            conn.end();
          });

          mtw.on('error', function(err) {
            log.err('dbe merge tree "%s"', err);
            conn.end();
          });

          // send auth response with last offset
          var authRes = { start: true };
          if (last) {
            authRes.start = last;
          }

          log.notice('dbe setup pipes and send back auth response %j', authRes);
          conn.write(JSON.stringify(authRes) + '\n');

          // receive data from remote
          conn.pipe(bs).pipe(mtw);

          // pass requested data to remote if requested and allowed
          if (mtr) { mtr.pipe(conn); }
        });
      } else {
        log.notice('dbe signal that no data is expected');
        conn.write(JSON.stringify({ start: false }) + '\n');

        // send data to remote if requested and allowed
        if (mtr) { mtr.pipe(conn); }
      }
    });
  }

  // initiate connection to external location
  // TODO: setup ssh and tcp handler
  /*
  function initConnection(cfg, cb) {
    var authReq, username, password, remote, proto, host, port, database;

    username   = cfg.connect.username;
    password   = cfg.connect.password;
    remote     = cfg.name;
    proto      = cfg.connect.protocol   || 'tcp';
    host       = cfg.connect.host       || '127.0.0.1';
    port       = cfg.connect.port       || 2344;
    database   = cfg.connect.database   || db.name;

    // find last item of this remote
    mt.lastByPerspective(remote, function(err, last) {
      if (err) {
        log.err('dbe initConnection lastByPerspective %s %j', err, cfg);
        cb(err);
        return;
      }

      switch (proto) {
        case 'tcp':
          // TODO
          log.notice('dbe connecting over tcp', host, port);
          cm.open(host, port, { reconnectOnError: true }, connectHandler);
          break;
        case 'ssh':
          // TODO
          //sshClient;
          break;
        default:
          cb(new Error('unknown proto'));
          return;
      }
    });
  }
  */

  process.on('message', handleIncomingMsg);

  // handle shutdown
  function shutdown() {
    log.notice('dbe shutting down');
    closeMtrs();
    log.notice('dbe mtrs closed');

    cm.close(function(err) {
      if (err) { throw err; }

      log.notice('dbe connections closed');

      process.removeListener('message', handleIncomingMsg);

      log.notice('dbe db closed');

      // TODO: check why server hangs and does not exit when running in bi-sync mode
      // https://groups.google.com/forum/#!msg/nodejs/DzxUYzbzkns/9XYC74b0NVUJ might be related
      //process.exit();
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
        chroot(newRoot, user, group);
        log.info('dbe changed root to %s and user:group to %s:%s', newRoot, user, group);
      } catch(err) {
        log.err('dbe changing root or user failed %j %s', msg, err);
        process.exit(8);
      }
    }

    // open db and call postChroot or exit
    function openDbAndProceed() {
      level(path, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, db) {
        if (err) {
          log.err('dbe opening db %s', err);
          process.exit(9);
        }
        log.info('dbe opened db %s', path);

        postChroot(db, msg);
      });
    }

    // ensure database directory exists
    fs.stat(newRoot + path, function(err, stats) {
      if (err && err.code !== 'ENOENT') {
        log.err('dbe stats on path failed %s %j', err, msg);
        process.exit(4);
      }

      if (err && err.code === 'ENOENT') {
        fs.mkdir(newRoot + path, 0o755, function(err) {
          if (err) {
            log.err('dbe path creation failed %s %j', err, msg);
            process.exit(5);
          }

          fs.chown(newRoot + path, uid, gid, function(err) {
            if (err) {
              log.err('dbe setting path ownership failed %s %j', err, msg);
              process.exit(6);
            }

            doChroot();
            openDbAndProceed();
          });
        });
      } else {
        if (!stats.isDirectory()) {
          log.err('dbe path exists but is not a directory %j %j', stats, msg);
          process.exit(7);
        }
        doChroot();
        openDbAndProceed();
      }
    });
  });
});
