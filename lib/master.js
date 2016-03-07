/**
 * Copyright 2014-2016 Netsend.
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

// node
var util = require('util');
var EE = require('events').EventEmitter;
var spawn = require('child_process').spawn;

// npm
var async = require('async');
var chroot = require('chroot');
var User = require('array-bcrypt-user');
var keyFilter = require('object-key-filter');
var posix = require('posix');

// local
//var MergeTree = require('./merge_tree');
//var Replicator = require('./replicator');
var authRequest = require('./auth_request');
//var pushRequest = require('./push_request');
//var pullRequest = require('./pull_request');

var noop = function() {};

/**
 * Master
 *
 * Track configured dbs. Fetch and merge new items from configured remotes.
 *
 * @param {Array} dbs              array containing db configurations
 * @param {Object} [opts]          additional options
 *
 * Options
 *   log:          {Object}        log object that contains debug2, debug, info,
 *                                 notice, warning, err, crit and emerg functions.
 *                                 Uses console.log and console.error by default.
 *   wss:          {Object}        secure WebSocket server configuration
 *     key:        {String}        websocket: path to PEM encoded private key
 *     cert:       {String}        websocket: path to PEM encoded certificate
 *     dhparam:    {String}        websocket: path to PEM encoded DH paramters
 *     [host]:     {String}        websocket: host, defaults to 127.0.0.1
 *     [port]:     {Number}        websocket: port, defaults to 3344
 *   localPort:    {Number}        tcp port for local tcp server, defaults to 2344
 *   chroot:       {String}        defaults to /var/empty
 *   user:         {String}        defaults to "nobody"
 *   group:        {String}        defaults to "nobody"
 */
function Master(dbs, opts) {
  if (!Array.isArray(dbs)) { throw new TypeError('dbs must be an array'); }

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }

  if (opts.log != null && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }

  if (opts.wss == null) { opts.wss = {}; }
  if (typeof opts.wss !== 'object') { throw new TypeError('opts.wss must be an object'); }

  if (opts.wss.key != null && typeof opts.wss.key !== 'string') { throw new TypeError('opts.wss.key must be a string'); }
  if (opts.wss.cert != null && typeof opts.wss.cert !== 'string') { throw new TypeError('opts.wss.cert must be a string'); }
  if (opts.wss.dhparam != null && typeof opts.wss.dhparam !== 'string') { throw new TypeError('opts.wss.dhparam must be a string'); }
  if (opts.wss.host != null && typeof opts.wss.host !== 'string') { throw new TypeError('opts.wss.host must be a string'); }
  if (opts.wss.port != null && typeof opts.wss.port !== 'number') { throw new TypeError('opts.wss.port must be a number'); }

  if (opts.localPort != null && typeof opts.localPort !== 'number') { throw new TypeError('opts.localPort must be a number'); }
  if (opts.chroot != null && typeof opts.chroot !== 'string') { throw new TypeError('opts.chroot must be a string'); }
  if (opts.user != null && typeof opts.user !== 'string') { throw new TypeError('opts.user must be a string'); }
  if (opts.group != null && typeof opts.group !== 'string') { throw new TypeError('opts.group must be a string'); }

  EE.call(this);

  this._dbs = dbs;
  this._opts = opts || {};

  this._log = this._opts.log || {
    emerg:   console.error,
    alert:   console.error,
    crit:    console.error,
    err:     console.error,
    warning: console.log,
    notice:  console.log,
    info:    console.log,
    debug:   console.log,
    debug2:  console.log,
    getFileStream: noop,
    getErrorStream: noop,
    close: noop
  };

  // db exec instances
  this._dbe = {};

  // index configs by db name
  this._dbsCfg = {};

  var that = this;
  dbs.forEach(function(cfg) {
    that._dbsCfg[cfg.name] = cfg;
  });
}

util.inherits(Master, EE);
module.exports = Master;

/**
 * Filter secrets like passwords out of an object, recursively.
 *
 * @param {Object} obj  object containing secrets
 * @return {Object} return a new filtered object
 */
Master.filterSecrets = function filterSecrets(obj) {
  return keyFilter(obj, ['passdb', 'password', 'wssKey', 'wssDhparam'], true);
};

/**
 * Fork dbs, start preauth and hookup communication channels.
 *
 * @param {Function} cb  First parameter will be an error object or null.
 */
Master.prototype.start = function start(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  // call either chroot or listen (listen calls chroot)
  this._log.info('start');

  var that = this;
  this.initDbs(function(err) {
    if (err) { cb(err); return; }
    that.listen(function(err) {
      if (err) { cb(err); return; }
      // TODO: init auth requests after listen, see sendPR
    });
  });
};

/**
 * Fork each db and send initial request containing db config.
 *
 * @param {Object} dbs  object containing dbexec configs
 * @param {Function} cb  This will be called as soon as all dbs are initialized.
 *                       First parameter will be an error object or null.
 */
Master.prototype.initDbs = function initDbs(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  var error;

  async.eachSeries(this._dbs, function(dbCfg, cb2) {
    dbCfg = Master.filterSecrets(dbCfg);

    if (typeof dbCfg.name !== 'string') {
      error = new Error('db.name must be a string');
      that._log.err('master initDbs %s error %j', dbCfg.name, error);
      cb2(error);
      return;
    }

    var name = dbCfg.name;

    if (that._dbe[name]) {
      error = new Error('db already started');
      that._log.err('master initDbs %s error %j', name, error);
      cb2(error);
      return;
    }

    that._startDb(dbCfg, function(err, db) {
      if (err) { cb2(err); return; }

      // register db
      that._dbe[name] = db;
      that._log.notice('dbe registered %s', name);

      // signal that it should start merging`
      db.send({ type: 'startMerge' });
      cb2();
    });
  }, cb);
};

/**
 * Return stats of all collections.
 *
 * @param {Boolean} [extended, default false]  whether to add _m3._ack counts
 * @param {Array} [nsList, default this._dbe]  list of namespaces
 * @param {Function} cb  The first parameter will contain either an Error object or
 *                       null. The second parameter is an object with collection
 *                       info.
 *
 * extended object:
 *   ack {Number}  the number of documents where _m3._ack = true
 */
 /*
Master.prototype.info = function info(opts, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = undefined;
  }

  if (typeof opts !== 'undefined') {
    if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
    if (typeof opts.extended !== 'undefined' && typeof opts.extended !== 'boolean') { throw new TypeError('extended must be a boolean'); }
    if (typeof opts.nsList !== 'undefined' && !Array.isArray(opts.nsList)) { throw new TypeError('nsList must be an array'); }
  }

  opts = opts || {};

  // default values
  if (typeof opts.extended === 'undefined') { opts.extended = false; }
  if (typeof opts.nsList === 'undefined') { opts.nsList = Object.keys(this._dbe); }

  var extended = opts.extended;
  var nsList = opts.nsList;

  var that = this;
  var result = {};
  async.each(nsList, function(key, cb2) {
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
 */

/**
 * Send a pull/push request for a certain db to a specific location.
 *
 * Adds the following options to the PR based on the import config:
 *   [filter]:     {Object}
 *   [hooks]:      {Array}
 *   [hooksOpts]:  {Object}
 *   [offset]:     {String}
 *
 * @param {String} ns  namespace of the db
 * @param {Object} pullRequest  pull request to send
 * @param {Function} cb  callback is called once the PR is sent
 *
 * A pull request should have the following structure:
 * {
 *   username:     {String}
 *   password:     {String}
 *   database:     {String}
 *   collection:   {String}
 *   [path]:       {String}
 *   [host]:       {String}     // defaults to 127.0.0.1
 *   [port]:       {Number}     // defaults to 2344
 * }
 */
 /*
Master.prototype.sendPR = function sendPR(ns, pullReq, cb) {
  if (typeof ns !== 'string') { throw new TypeError('ns must be a string'); }
  if (typeof pullReq !== 'object') { throw new TypeError('pullReq must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (typeof pullReq.username !== 'string') { throw new TypeError('pullReq.username must be a string'); }
  if (typeof pullReq.password !== 'string') { throw new TypeError('pullReq.password must be a string'); }
  if (typeof pullReq.database !== 'string') { throw new TypeError('pullReq.database must be a string'); }
  if (typeof pullReq.collection !== 'string') { throw new TypeError('pullReq.collection must be a string'); }

  var localDb, localColl;
  var parts = ns.split('.');
  if (parts.length <= 1) { throw new Error('ns must contain at least a database name and a collection name separated by a dot'); }

  localDb = parts[0];
  localColl = parts.slice(1).join('.');

  var that = this;
  var error;

  if (typeof this._dbe[ns] !== 'object') {
    error = 'ns not found for this database';
    that._log.err('master sendPR', ns, error);
    throw new Error(error);
  }
  if (!pullRequest.valid(pullReq)) { throw new Error('invalid pull request'); }

  // filter password out request
  function debugReq(req) {
    return keyFilter(req, ['password']);
  }

  // set replication config collection
  var replicationDb;
  var replicationColl;
  if (this._replicationColl) {
    replicationColl = this._replicationColl;
  } else {
    if (this._replicationDb) {
      replicationDb = this._oplogDb.db(this._replicationDb);
    } else {
      replicationDb = this._oplogDb.db(localDb);
    }
    replicationColl = replicationDb.collection(this._replicationCollName);
  }

  // search for import replication config for the remote using the remote name
  Replicator.fetchFromDb(replicationColl, 'import', pullReq.database, function(err, replCfg) {
    if (err) { cb(err); return; }

    // check if requested collection is imported
    if (!replCfg.collections[localColl]) {
      error = 'requested collection has no import replication config';
      that._log.err('master sendPR', ns, error);
      cb(new Error(error));
      return;
    }

    replCfg = replCfg.collections[localColl];

    // set extra hook and other options on pull request
    pullReq.hooksOpts = pullReq.hooksOpts || {};

    if (replCfg.filter) { pullReq.filter = replCfg.filter; }
    if (replCfg.hooks)  { pullReq.hooks  = replCfg.hooks; }
    if (pullReq.offset) { pullReq.offset = pullReq.offset; }

    // set hooksOpts with all keys but the pre-configured ones
    Object.keys(replCfg).forEach(function(key) {
      if (!~['filter', 'hooks', 'hooksOpts', 'offset'].indexOf(key)) {
        pullReq.hooksOpts[key] = replCfg[key];
      }
    });

    if (!pullRequest.valid(pullReq)) {
      that._log.err('master sendPR unable to construct a valid pull request %j', debugReq(pullReq));
      cb(new Error('unable to construct a valid pull request'));
      return;
    }

    that._log.info('master sendPR pull request forwarded %j', debugReq(pullReq));

    // now send this pull request to the appropriate versioned collection
    that._dbe[ns].send(pullReq);
  });
};
 */

/**
 * Fork a pre-auth server that handles incoming auth requests. Verify password
 * and pass a stripped auth request to the appropriate db.
 *
 * Note: chroots right after pre-auth server is started
 *
 * @param {Function} cb  First parameter will be an Error object or null.
 *
 * preauth accepts the following options.
 * {
 *   log:            {Object}     // log configuration object, child.stdout will be
 *                                // mapped to file and child.stderr will be mapped
 *                                // to error or file
 *     [console]     {Boolean, default: false}  whether to log to the console
 *     [file]        {String|Number|Object}  log all messages to this file, either
 *                                // a filename, file descriptor or writable stream
 *     [error]       {String|Number|Object}  extra file to log errors only, either
 *                                // a filename, file descriptor or writable stream
 *     [mask]        {Number, default NOTICE}  set a minimum priority for "file"
 *     [silence]     {Boolean, default false}  whether to suppress logging or not
 *   }
 *   [port]:         {Number}      // tcp port, defaults to 2344
 *   [wss]:          {Boolean}     // whether or not to enable secure WebSocket
 *                                 // defaults to false
 *   [wssHost]:      {String}      // websocket host, defaults to 127.0.0.1
 *                                 // only if "wss" is true
 *   [wssPort]:      {Number}      // websocket port, defaults to 3344
 *                                 // only if "wss" is true
 *   [chroot]:       {String}      // defaults to /var/empty
 *   [user]:         {String}      // defaults to "nobody"
 *   [group]:        {String}      // defaults to "nobody"
 * }
 */
Master.prototype.listen = function listen(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  if (!Object.keys(this._dbe).length) { throw new TypeError('run initDbs first'); }

  function connErrorHandler(conn, e) {
    try {
      var error = { error: 'invalid auth request' };
      that._log.err('master listen %j', conn.address(), error, e);
      conn.write(JSON.stringify(error));
      conn.destroy();
    } catch(err) {
      that._log.err('master listen connection write or disconnect error', err);
    }
  }

  // handle incoming authentication requests
  function handleMessage(authReq, conn) {
    that._log.info('master listen %d %d %j', conn.bytesRead, conn.bytesWritten, conn.address());

    if (!authRequest.valid(authReq)) {
      that._log.err('master listen invalid auth request %j', Master.filterSecrets(authReq));
      connErrorHandler(conn, new Error('invalid auth request'));
      return;
    }

    // verify credentials and lookup replication config
    that._verifyAuthRequest(authReq, function(err) {
      if (err) {
        that._log.err('master listen auth request %j, err %j', Master.filterSecrets(authReq), err);
        connErrorHandler(conn, err);
        return;
      }

      // now forward the authenticated connection to the appropriate db
      that._dbe[authReq.db].send({ type: 'remoteDataChannel', perspective: authReq.username }, conn);
    });
  }

  that._log.info('master listen forking preauth');

  var stdio = ['ignore', 'inherit', 'inherit', 'ipc'];

  var wssKey, wssCert, wssDhparam, wssHost, wssPort;

  try {
    wssKey = this._opts.wss.key;
    wssCert = this._opts.wss.cert;
    wssDhparam = this._opts.wss.dhparam;
    wssHost = this._opts.wss.host;
    wssPort = this._opts.wss.port;
  } catch(err) {
  }

  var user = this._opts.user || 'nobody';
  var group = this._opts.group || 'nobody';

  var newRoot = this._opts.chroot || '/var/empty';

  var preauthCfg = {
    log: this._log.getOpts(),
    port: this._opts.localPort,
    wss: !!wssKey || !!wssCert || !!wssDhparam,
    wssCert: wssCert,
    wssKey: wssKey,
    wssDhparam: wssDhparam,
    wssHost: wssHost,
    wssPort: wssPort,
    chroot: newRoot,
    user: user,
    group: group
  };

  preauthCfg.log.file = this._log.getFileStream();
  preauthCfg.log.error = this._log.getErrorStream();

  // use fd 4 if a file stream is opened
  if (preauthCfg.log.file) {
    stdio[4] = preauthCfg.log.file.fd;
    preauthCfg.log.file = 4;
  }

  // use fd 5 if an error stream is opened
  if (preauthCfg.log.error) {
    stdio[5] = preauthCfg.log.error.fd;
    preauthCfg.log.error = 5;
  }

  that._log.info('master listen preauth: %j', Master.filterSecrets(preauthCfg));

  // open preauth server
  var preauth = spawn(process.execPath, [__dirname + '/preauth_exec'], {
    cwd: process.cwd(),
    env: {},
    stdio: stdio
  });

  this._preauth = preauth;
  var childPhase;

  preauth.on('exit', function(code, signal) {
    that._log.info('preauth child exit', code, signal);
  });
  preauth.on('close', function(code, signal) {
    that._log.info('preauth child close', code, signal);
    // close on premature exit
    if (childPhase !== 'listen') {
      that._log.info('preauth pre-mature exit');
      process.kill(process.pid);
    }
    childPhase = 'close';
    that._preauth = null;
  });
  preauth.on('error', function(err) {
    that._log.err('preauth child error', err);
  });

  var uid, gid;
  try {
    uid = posix.getpwnam(user).uid;
    gid = posix.getgrnam(group).gid;
  } catch(err) {
    this._log.err('%s %s:%s', err, user, group);
    this.stopTerm(function(err) {
      if (err) { that._log.err(err); }
    });
    return;
  }

  // chroot this process before forking preauth
  try {
    chroot(newRoot, user, gid);
  } catch(err) {
    this._log.err('can not chroot "%s"', err);
    this.stopTerm(function(err) {
      if (err) { that._log.err(err); }
    });
    return;
  }

  // send initial config after preauth is ready to receive messages
  preauth.once('message', function(msg) {
    if (msg !== 'init') {
      that._log.err('master listen expected message "init"', msg);
      cb(new Error('expected first message to be "init"'));
      return;
    }

    childPhase = 'init';

    that._log.info('master listen "init" received');

    preauth.send(preauthCfg);

    preauth.once('message', function(msg) {
      if (msg !== 'listen') {
        that._log.err('master listen expected message "listen"', msg);
        cb(new Error('expected second message to be "listen"'));
        return;
      }

      childPhase = 'listen';

      that._log.info('master listen "listen" received');

      preauth.on('message', handleMessage);
      cb();
    });
  });
};

/**
 * Stop pre-auth server, db instances (not catching SIGTERM).
 *
 * @param {Function} cb  First parameter will be an Error object or null.
 */
Master.prototype.stopTerm = function stopTerm(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  var tasks = [];

  var dbe = Object.keys(this._dbe);
  this._log.notice('master stopTerm dbe', dbe);

  dbe.forEach(function(key) {
    var dbe = that._dbe[key];
    if (dbe) {
      tasks.push(function(cb2) {
        that._log.notice('closing dbe', key);
        dbe.on('close', function() {
          that._log.notice('master stopTerm closed dbe', key);
          cb2();
        });
        dbe.kill();
      });
    } else {
      that._log.notice('dbe already closed', key);
    }
  });

  if (this._preauth) {
    tasks.push(function(cb2) {
      if (that._preauth) {
        that._preauth.on('close', function(err) {
          if (err) { cb2(err); return; }
          that._log.notice('preauth closed');
          cb2();
        });
        that._preauth.kill();
      } else {
        that._log.notice('preauth already closed');
      }
    });
  }

  async.series(tasks, cb);
};



/////////////////////
//// PRIVATE API ////
/////////////////////



/**
 * Start and initialize a db. Return the forked child when it's ready to accept
 * pull and push requests.
 *
 * @param {Object} cfg             db exec config
 * @param {Function} cb            First parameter will be an error object or
 *                                 null, second parameter will be the forked child
 *                                 on success.
 *
 * db exec config:
 * {
 *   [log]:          {Object}      // log configuration
 *   [path]:         {String}      // db path within the chroot directory, defaults
 *                                 // to "/data"
 *   [chroot]:       {String}      // defaults to /var/persdb
 *   [hookPaths]:    {Array}       // list of paths to load hooks from
 *   [user]:         {String}      // defaults to "nobody"
 *   [group]:        {String}      // defaults to "nobody"
 *   [perspectives]: {Array}       // array of other perspectives
 *   [mergeTree]:    {Object}      // any MergeTree options
 * }
 */
Master.prototype._startDb = function _startDb(cfg, cb) {
  if (typeof cfg !== 'object') { throw new TypeError('cfg must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (cfg.log == null) { cfg.log = this._log; }
  if (typeof cfg.log !== 'object') { throw new TypeError('cfg.log must be an object'); }

  if (cfg.path != null && typeof cfg.path !== 'string') { throw new TypeError('cfg.path must be a string'); }
  if (cfg.chroot != null && typeof cfg.chroot !== 'string') { throw new TypeError('cfg.chroot must be a string'); }
  if (cfg.hookPaths != null && !Array.isArray(cfg.hookPaths)) { throw new TypeError('cfg.hookPaths must be an array'); }
  if (cfg.user != null && typeof cfg.user !== 'string') { throw new TypeError('cfg.user must be a string'); }
  if (cfg.group != null && typeof cfg.group !== 'string') { throw new TypeError('cfg.group must be a string'); }
  if (cfg.perspectives != null && !Array.isArray(cfg.perspectives)) { throw new TypeError('cfg.perspectives must be an array'); }
  if (cfg.mergeTree != null && typeof cfg.mergeTree !== 'object') { throw new TypeError('cfg.mergeTree must be an object'); }

  cfg.hookPaths = cfg.hookPaths || [__dirname + '/../hooks', __dirname + '/../local/hooks'];

  var name = cfg.name;
  var that = this;

  that._log.info('master _startDb forking: %s', name);

  var stdio = ['ignore', 'inherit', 'inherit', 'ipc'];

  // use fd 4 if a file stream is opened
  if (cfg.log.file) {
    stdio[4] = cfg.log.file.fd;
    cfg.log.file = 4;
  }

  // use fd 5 if an error stream is opened
  if (cfg.log.error) {
    stdio[5] = cfg.log.error.fd;
    cfg.log.error = 5;
  }

  var dbe = spawn(process.execPath, [__dirname + '/db_exec'], {
    cwd: process.cwd(),
    env: {},
    stdio: stdio
  });

  var phase = null;

  dbe.on('error', function(err) {
    that._log.err('master _startDb error', name, err);

    // callback if not in listen mode yet
    if (phase !== 'listen') {
      cb(err);
    }
  });

  dbe.once('close', function(code, sig) {
    that._log.notice('master _startDb close', name, code, sig);

    // callback if not in listen mode yet
    if (phase !== 'listen') {
      cb(new Error('abnormal termination'));
    }
  });

  dbe.once('exit', function(code, sig) {
    that._log.notice('master _startDb exit', name, code, sig);

    // callback if not in listen mode yet
    if (phase !== 'listen') {
      cb(new Error('abnormal termination'));
    }
  });

  // send db and log config after dbe is ready to receive messages
  dbe.once('message', function(msg) {
    if (msg !== 'init') {
      that._log.err('master _startDb expected message "init"', name, msg);
      cb(new Error('expected first message to be "init"'));
      return;
    }

    phase = 'init';
    that._log.info('master _startDb "init" received', name);

    dbe.send(cfg);

    // wait for the child to send the "listen" message
    dbe.once('message', function(msg) {
      if (msg !== 'listen') {
        that._log.err('master _startDb expected message "listen"', name, msg);
        cb(new Error('expected second message to be "listen"'));
        return;
      }

      that._log.info('master _startDb "listen" received', name);

      phase = 'listen';

      cb(null, dbe);
    });
  });
};

/**
 * If the snapshot is empty, get the latest oplog item as offset, otherwise use
 * maxOplogPointer or Timestamp(0, 0);
 *
 * @param {Object} cfg  versioned collection configuration object
 * @param {Function} cb  Called when oplog is connected to the dbe. First parameter
 *                       will be an error object or null.
 *
 * A versioned collection configuration object should have the following structure:
 * {
 *   dbName:          {String}
 *   collectionName:  {String}
 *   size:            {Number}
 *   [any VersionedCollection options]
 * }
 /
Master.prototype._ensureSnapshotAndOplogOffset = function _ensureSnapshotAndOplogOffset(cfg, cb) {
  if (typeof cfg !== 'object') { throw new TypeError('cfg must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (typeof cfg.dbName !== 'string') { throw new TypeError('cfg.dbName must be a string'); }
  if (typeof cfg.collectionName !== 'string') { throw new TypeError('cfg.collectionName must be a string'); }
  if (typeof cfg.size !== 'number') { throw new TypeError('cfg.size must be a number'); }

  var opts = {
    log: this._log,
    localPerspective: cfg.localPerspective,
    versionKey: cfg.versionKey,
    remotes: cfg.remotes
  };

  var vc = new MergeTree(this._oplogDb.db(cfg.dbName), cfg.collectionName, opts);

  var that = this;
  var error;

  var oplogOffset;

  // get oldest item from oplog
  this._oplogColl.findOne({}, { sort: { $natural: 1 } }, function(err, oldestOplogItem) {
    if (err) { cb(err); return; }

    if (!oldestOplogItem) {
      error = new Error('no oplog item found');
      that._log.err(vc.ns, error);
      cb(error);
      return;
    }

    // get newest item from oplog
    that._oplogColl.findOne({}, { sort: { $natural: -1 } }, function(err, newestOplogItem) {
      if (err) { cb(err); return; }

      that._log.info('master _ensureSnapshotAndOplogOffset oplog span', oldestOplogItem.ts, newestOplogItem.ts);

      vc._snapshotCollection.count(function(err, items) {
        if (err) { return cb(err); }

        // get max oplog pointer from snapshot
        vc.maxOplogPointer(function(err, snapshotOffset) {
          if (err) { cb(err); return; }

          if (!snapshotOffset && items) {
            error = new Error('vc contains snapshots but no oplog pointer');
            that._log.err(vc.ns, error);
            cb(error);
            return;
          }

          // if found, use it, but warn if it's outside the current range of the oplog
          if (snapshotOffset) {
            if (snapshotOffset.lessThan(oldestOplogItem.ts) || snapshotOffset.greaterThan(newestOplogItem.ts)) {
              that._log.warning('oplog pointer outside current oplog range', snapshotOffset, oldestOplogItem.ts, newestOplogItem.ts);
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
*/

/**
 * Verify an auth request, and if valid, pass back the replication config.
 *
 * @param {Object} req  auth request
 * @param {Function} cb  First parameter will be an error object or null.
 */
Master.prototype._verifyAuthRequest = function _verifyAuthRequest(req, cb) {
  if (typeof req !== 'object') { throw new TypeError('req must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;
  var error;

  var cfg = this._dbsCfg[req.db];

  if (!cfg || !cfg.perspectives) {
    error = 'invalid credentials';
    that._log.err('master _verifyAuthRequest no config found for db:', req.db, error);
    process.nextTick(function() {
      cb(new Error(error));
    });
    return;
  }

  // search for perspective bound user db
  var passDb, replCfg;
  cfg.perspectives.some(function(persCfg) {
    if (persCfg.name === req.username) {
      passDb = persCfg.passdb;
      replCfg = persCfg;
      return true;
    }
  });

  if (!passDb || (!replCfg.export && !replCfg.import)) {
    error = 'invalid credentials';
    that._log.err('master _verifyAuthRequest no users or import / export config found', req.db, error);
    process.nextTick(function() {
      cb(new Error(error));
    });
    return;
  }

  // do a lookup in the database
  User.find(passDb, req.username, req.db, function(err, user) {
    if (err) { cb(err); return; }
    if (!user) {
      error = 'invalid credentials';
      that._log.err('master _verifyAuthRequest user not found', req.db, error);
      cb(new Error(error));
      return;
    }

    user.verifyPassword(req.password, function(err, valid) {
      if (err) { cb(err); return; }

      if (!valid) {
        error = 'invalid credentials';
        that._log.err('master _verifyAuthRequest %s %s %s', req.db, req.username, error);
        cb(new Error(error));
        return;
      }

      that._log.info('master successfully authenticated "%s"', req.username);

      cb();
    });
  });
};
