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

'use strict';

var fs = require('fs');
var net = require('net');
var path = require('path');
var url = require('url');

var async = require('async');
var chroot = require('chroot');
var BSONStream = require('bson-stream');
var keyFilter = require('object-key-filter');
var level = require('level');

var MergeTree = require('./merge_tree');
var RemoteTransform = require('./remote_transform');
var pullRequest = require('./pull_request');
var pushRequest = require('./push_request');
var dataHandler = require('./data_handler');
var connManager = require('./conn_manager');
var logger = require('./logger');

//var sshClient = require('ssh-client');

/**
 * Instantiates a merge tree and handles incoming and outgoing requests.
 *
 * 1. setup all import and export hooks, filters etc.
 * 1. initiate connections to other locations for pull/push requests
 * 2. handle incoming connections with pull/push requests
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
 *   path:           {String}
 *   name:           {String}      // name of this database
 *   log:            {Object}      // log configuration
 *   [chroot]:       {String}      // defaults to basename of path
 *   [hookPaths]:    {Array}       // list of paths to load hooks from
 *   [user]:         {String}      // defaults to "nobody"
 *   [debug]:        {Boolean}     // defaults to false
 *   [perspectives]: {Object}
 *   [any MergeTree options]
 * }
 *
 * After the database is opened and hooks are loaded, this process emits a message
 * named "listen", signalling that it's ready to receive auth requests.
 * Furthermore, this process will start initiating auth requests to all
 * perspectives with a "connect" attribute.
 *
 * Any subsequent messages sent to this process should be authenticated auth
 * requests. These should be accompanied with a connection and be stripped from the
 * username, password and db field, thus should only have an "offset" attribute:
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
 * Determine all used client protocols in the given config to connect to.
 *
 * @param {Object} cfg  parsed perspective config
 * @return {Object} client protos, "tcp" and/ort "ssh" with a truth value
 */
function determineClientProtos(cfg) {
  if (!Array.isArray(arr)) { throw new TypeError('arr must be an array'); }

  var protos = {};
  cfg.connect.forEach(function(name) {
    protos[cfg[name].connect.protocol] = true;
  });
  return protos;
}

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

// create a data handler from a perspective import or export config
function createDH(cfg, offset) {
  var dh = {};

  // set extra hook and other options on config
  dh.hooksOpts = cfg.hooksOpts || {};

  if (cfg.filter) { dh.filter = cfg.filter; }
  if (cfg.hooks)  { dh.hooks  = cfg.hooks; }
  if (offset)     { dh.offset = offset; }

  // set hooksOpts with all keys but the pre-configured ones
  Object.keys(cfg).forEach(function(key) {
    if (!~['filter', 'hooks', 'hooksOpts', 'offset'].indexOf(key)) {
      dh.hooksOpts[key] = cfg[key];
    }
  });

  if (!dataHandler.valid(dh)) {
    throw new Error('unable to construct a valid data handler');
  }

  return dh;
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
  return keyFilter(req, ['password']);
}

function connErrorHandler(conn, e) {
  log.err('connection error: %s', e);
  try {
    conn.destroy();
  } catch(err) {
    log.err('connection write or disconnect error: %s', err);
  }
}

var mtrs = [];
function registerMtr(mtr, conn) {
  var dest = connManager.destination(conn);

  log.notice('mtr registering %s', dest);

  mtrs.push(mtr);

  mtr.on('error', function(err) {
    log.err('mtr error %s %s', dest, err);
  });

  mtr.once('end', function() {
    log.notice('mtr end %s', dest);

    var i = mtrs.indexOf(mtr);
    if (~i) {
      log.info('deleting mtr %d', i);
      mtrs.splice(i, 1);
    } else {
      log.err('mtr not found %d', i);
    }
  });
}

function closeMtrs(cb) {
  async.each(mtrs, function(mtr, cb2) {
    mtr.once('error', cb2);
    mtr.once('end', cb2);
    mtr.close();
  }, cb);
}

/**
 * Start listening.
 */
function postChroot(db, cfg) {
  if (typeof db !== 'object') { throw new TypeError('db must be an object'); }
  if (typeof cfg !== 'object') { throw new TypeError('cfg must be an object'); }

  // setup list of connections to initiate and create an index by perspective name
  var persCfg = parsePersConfigs(cfg.perspectives);

  var mtOpts = cfg.mergeTree || {};
  mtOpts.perspectives = Object.keys(persCfg.pers);

  var mt = new MergeTree(db, mtOpts);

  var cm = connManager.create({ log: log });

  async.eachSeries(persCfg.connect, function(pers, cb) {
    initConnection(persCfg[pers], function() {
      // skip errors
      cb();
    });
  });

  // initiate connection to external location
  // TODO: setup ssh and tcp handler, maybe skip websocket client
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
        log.err('initConnection lastByPerspective %s %j', err, cfg);
        cb(err);
        return;
      }

      function connectHandler(err, conn2) {
        if (err) { log.err('initConnection connectHandler %s %j', err, authReq); return; }

        // send auth request
        var authReq = {
          username:   username,
          password:   password,
          database:   database
        };

        if (last) {
          authReq.offset = last._id._v;
        }

        // expect either bson or a disconnect after the auth request is sent
        var bs2 = new BSONStream();

        // create remote transform to ensure _id._pe is set to this remote and run all hooks
        var opts = {
          db: db,
          hooks: authReq.hooks || [],
          hooksOpts: authReq.hooksOpts || {},
          log: log
        };
        opts.hooksOpts.to = db;

        var rt = new RemoteTransform(database, opts);

        rt.on('error', function(err) {
          log.err('remote transform error %s %j', err, conn2.address());
        });

        // handle incoming BSON data
        conn2.pipe(bs2).pipe(rt).pipe(mt);

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
          rt.unpipe(mt);
          log.notice('rt end unpiped rt--mt');
        });

        // send auth request
        log.notice('sending auth request %j', debugReq(authReq));
        conn2.write(JSON.stringify(authReq) + '\n');
      }

      switch (proto) {
        case 'tcp':
          // TODO
          log.notice('connecting over tcp', host, port);
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

  // replace hook names with the actual implementation
  // and create one hook for all the keys to hide
  function loadHooks(hooks, hooksOpts) {
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

  // handle incoming messages
  function handleIncomingMsg(req, conn) {
    log.notice('incoming ipc message %j', debugReq(req));

    if (conn) {
      req.raw = true;

      // convert all hooks to the loaded module and transform hide keys into a hook function
      try {
        loadHooks(req.hooks, req.hooksOpts);
      } catch (err) {
        connErrorHandler(conn, err);
        return;
      }

      req.log = log;

      req.hooksOpts = req.hooksOpts || {};
      req.hooksOpts.from = db;

      cm.registerIncoming(conn);

      var mtr = mt.createReader(req);

      registerMtr(mtr, conn);

      mtr.pipe(conn);

      mtr.once('end', function() {
        mtr.unpipe(conn);
        log.notice('mtr end unpiped mtr--conn ');
      });
      // TODO: implement handling of incoming connections
      // find last item of this remote
      mt.lastByPerspective(remote, function(err, last) {
        if (err) {
          log.err('initConnection lastByPerspective %s %j', err, cfg);
          cb(err);
          return;
        }

        // send reply with last

        loadHooks(req.hooks, req.hooksOpts);
        initConnection(req);
      });
    } else {
    }
  }
  process.on('message', handleIncomingMsg);

  // handle shutdown
  function shutdown() {
    log.notice('shutting down');
    closeMtrs(function(err) {
      if (err) { throw err; }

      log.notice('mtrs closed');

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

          mt.stopAutoProcessing(function(err) {
            if (err) { throw err; }
            log.notice('mt stopped processing');
            db.close(function(err) {
              if (err) { throw err; }
              log.notice('db closed');

              // TODO: check why server hangs and does not exit when running in bi-sync mode
              // https://groups.google.com/forum/#!msg/nodejs/DzxUYzbzkns/9XYC74b0NVUJ might be related
              log.info('force quit');
              process.exit();
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

/**
 * Expect a db config, log config and any merge tree options.
 *
 * {
 *   path:           {String}      // location on disk to store LevelDB
 *   name:           {String}      // name of this database
 *   log:            {Object}      // log configuration
 *   [perspectives]: {Object}      // perspective configuration
 *   [hookPaths]:    {Array}       // list of paths to load hooks from
 *   [chroot]:       {String}      // defaults to basename of path
 *   [user]:         {String}      // defaults to "nobody"
 *   [debug]:        {Boolean}     // defaults to false
 *   [mergeTree]     {Object}      // any MergeTree options
 * }
 */
process.once('message', function(msg) {
  if (typeof msg !== 'object') { throw new TypeError('msg must be an object'); }
  if (typeof msg.path !== 'string') { throw new TypeError('msg.path must be a string'); }
  if (typeof msg.name !== 'string') { throw new TypeError('msg.name must be a string'); }
  if (typeof msg.log !== 'object') { throw new TypeError('msg.log must be an object'); }

  programName = 'dbe ' + msg.name;

  process.title = 'pdb/' + programName;

  var opts = {};
  opts.log = msg.log;
  opts.log.ident = programName;

  // open log
  logger(opts.log, function(err, l) {
    if (err) { l.err(err); throw err; }

    opts.log = l; // use this logger in the mt's as well

    // require any hooks before chrooting
    if (msg.hookPaths) {
      try {
        opts.loadedHooks = requireJsFromDirs(msg.hookPaths);
      } catch(err) {
        opts.log.err('error loading hooks %s %s', msg.hookPaths, err);
        process.exit(2);
      }
    }

    // open db
    level(msg.path, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, db) {
      if (err) {
        opts.log.err('opening db %s', err);
        process.exit(3);
      }

      // chroot config
      var chrootUser = msg.user || 'nobody';
      var chrootNewRoot = msg.chroot || path.basename(msg.path);

      // then chroot
      try {
        chroot(chrootNewRoot, chrootUser);
        opts.log.info('changed root to %s and user to %s', chrootNewRoot, chrootUser);
      } catch(err) {
        opts.log.err('changing root or user failed %j %s', msg, err);
        process.exit(4);
      }

      postChroot(db, opts);
    });
  });
});
