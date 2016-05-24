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
var EE = require('events').EventEmitter;
var fs = require('fs');
var url = require('url');
var util = require('util');
var spawn = require('child_process').spawn;

// npm
var async = require('async');
var chroot = require('chroot');
var User = require('array-bcrypt-user');
var posix = require('posix');
var xtend = require('xtend');

// local
//var MergeTree = require('./merge_tree');
//var Replicator = require('./replicator');
var authRequest = require('./auth_request');
var loadSecrets = require('./load_secrets');
var filterSecrets = require('./filter_secrets');
//var pushRequest = require('./push_request');
//var pullRequest = require('./pull_request');
var noop = require('./noop');

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
 *   tunnels:      {Array}         list of ssh tunnels to setup before connecting
 *   wss:          {Object}        secure WebSocket server configuration
 *     key:        {String}        websocket: path to PEM encoded private key
 *     cert:       {String}        websocket: path to PEM encoded certificate
 *     dhparam:    {String}        websocket: path to PEM encoded DH paramters
 *     [host]:     {String}        websocket: host, defaults to 127.0.0.1
 *     [port]:     {Number}        websocket: port, defaults to 3344
 *   localPort:    {Number}        tcp port for local tcp server, defaults to 2344
 *   configBase:   {String}        base path of the main config file
 *   chroot:       {String}        defaults to /var/empty
 *   user:         {String}        defaults to "_pdbnull"
 *   group:        {String}        defaults to "_pdbnull"
 *   adapterPath:  {String}        defaults to "../adapter"
 */
function Master(dbs, opts) {
  if (!Array.isArray(dbs)) { throw new TypeError('dbs must be an array'); }

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }

  if (opts.log != null && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }

  if (opts.tunnels == null) { opts.tunnels = []; }
  if (!Array.isArray(opts.tunnels)) { throw new TypeError('opts.tunnels must be an array'); }

  if (opts.wss == null) { opts.wss = {}; }
  if (typeof opts.wss !== 'object') { throw new TypeError('opts.wss must be an object'); }

  if (opts.wss.key != null && typeof opts.wss.key !== 'string') { throw new TypeError('opts.wss.key must be a string'); }
  if (opts.wss.cert != null && typeof opts.wss.cert !== 'string') { throw new TypeError('opts.wss.cert must be a string'); }
  if (opts.wss.dhparam != null && typeof opts.wss.dhparam !== 'string') { throw new TypeError('opts.wss.dhparam must be a string'); }
  if (opts.wss.host != null && typeof opts.wss.host !== 'string') { throw new TypeError('opts.wss.host must be a string'); }
  if (opts.wss.port != null && typeof opts.wss.port !== 'number') { throw new TypeError('opts.wss.port must be a number'); }

  if (opts.localPort != null && typeof opts.localPort !== 'number') { throw new TypeError('opts.localPort must be a number'); }
  if (opts.configBase != null && typeof opts.configBase !== 'string') { throw new TypeError('opts.configBase must be a string'); }
  if (opts.chroot != null && typeof opts.chroot !== 'string') { throw new TypeError('opts.chroot must be a string'); }
  if (opts.user != null && typeof opts.user !== 'string') { throw new TypeError('opts.user must be a string'); }
  if (opts.group != null && typeof opts.group !== 'string') { throw new TypeError('opts.group must be a string'); }
  if (opts.adapterPath != null && typeof opts.adapterPath !== 'string') { throw new TypeError('opts.adapterPath must be a string'); }

  EE.call(this);

  this._dbs = dbs;
  this._opts = xtend({
    configBase: '',
    chroot: '/var/empty',
    user: '_pdbnull',
    group: '_pdbnull',
    adapterPath: __dirname + '/../adapter/'
  }, opts);

  // postfix adapter path with "/"
  if (this._opts.adapterPath[this._opts.adapterPath.length - 1] !== '/') {
    this._opts.adapterPath += '/';
  }

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

  // list of adapters
  this._adapters = {};

  // list of instantiated adapters
  this._runningAdapters = [];

  // list of tunnel instances
  this._tunnels = [];

  // list of client instances
  this._clients = [];

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
 * Load adapters, fork dbs, start preauth and hookup communication channels.
 *
 * @param {Function} cb  First parameter will be an error object or null.
 */
Master.prototype.start = function start(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  // call either chroot or listen (listen calls chroot)
  this._log.info('start');

  var that = this;

  var apath = this._opts.adapterPath;
  fs.readdir(apath, function(err, files) {
    if (err) { cb(err); return; }
    async.map(files.map(file => apath + file), fs.stat, function(err, result) {
      if (err) { cb(err); return; }

      result.forEach(function(stat, i) {
        if (stat.isDirectory()) {
          var name = files[i];
          var path = apath + name + '/exec.js';
          var stats = fs.statSync(path);
          if (stats.isFile()) {
            that._log.info('master adapter %s loaded', name);
            that._adapters[name] = path;
          } else {
            that._log.notice('master adapter %s misses exec.js', name);
          }
        }
      });

      that.initTunnels(function(err) {
        if (err) { cb(err); return; }
        that.initDbs(function(err) {
          if (err) { cb(err); return; }
          that.initClients(function(err) {
            if (err) { cb(err); return; }
            that.listen(function(err) {
              if (err) { cb(err); return; }
              // TODO: init auth requests after listen, see sendPR
            });
          });
        });
      });
    });
  });
};

/**
 * Fork each tunnel and send initial request containing tunnel config.
 *
 * @param {Function} cb  This will be called as soon as all tunnels are
 *                       initialized. First parameter will be an error object or
 *                       null.
 */
Master.prototype.initTunnels = function initTunnels(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  async.eachSeries(this._opts.tunnels, function(tunnelCfg, cb2) {
    that._startTunnel(tunnelCfg, cb2);
  }, cb);
};

/**
 * Fork each perspective client and send initial request containing client config.
 *
 * @param {Function} cb  This will be called as soon as all clients are
 *                       initialized. First parameter will be an error object or
 *                       null.
 */
Master.prototype.initClients = function initClients(cb) {
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  var tasksFirst = [];
  var tasksLast = [];

  // make sure dbs with connect are executed after all listening dbs are executed
  this._dbs.forEach(function(dbCfg) {
    if (dbCfg.connect) {
      tasksLast.push(dbCfg);
    } else {
      tasksFirst.push(dbCfg);
    }
  });

  async.eachSeries(this._dbs, function(dbCfg, cb2) {
    async.eachSeries(dbCfg.perspectives, function(peCfg, cb3) {
      var username = peCfg.username;
      if (!username) { process.nextTick(cb3); return; }

      if (!peCfg.secrets) {
        process.nextTick(function() {
          cb3(new Error('no secrets file found'));
        });
        return;
      }

      var dbName = peCfg.database || peCfg.name;
      if (!dbName) {
        process.nextTick(function() {
          cb3(new Error('could not determine database name'));
        });
        return;
      }

      // prepend the config base to a relative path specification
      let file = peCfg.secrets;
      that._log.debug('master initClients %s loading %s', dbName, file);
      if (file && file[0] !== '/') {
        file = that._opts.configBase + file;
      }
      try {
        let secrets = loadSecrets(file);
        if (!secrets[username]) {
          that._log.err('master initClients password not found for %s %s', dbName, username);
          throw new Error('password not found');
        }
        peCfg.password = secrets[username];
        that._log.debug('master initClients %s loaded %s', username, file);
      } catch(err) {
        that._log.err('master initClients %s loading %s %s', username, file, err);
        process.nextTick(function() {
          cb3(err);
        });
        return;
      }

      peCfg.database = dbName;
      that._startClient(peCfg, function(err, ceChild) {
        if (err) { cb3(err); return; }

        // handle incoming authentication requests
        ceChild.on('message', function handleMessage(msg, conn) {
          if (msg !== 'connection') {
            that._log.err('master initClients expected "connection" message, got: %j', msg);
            throw new Error('expected connection message');
          }
          that._log.info('master initClients new connection %d %d %j', conn.bytesRead, conn.bytesWritten, conn.address());

          // forward the connection to the appropriate db
          that._dbe[dbCfg.name].send({ type: 'remoteDataChannel', perspective: peCfg.name }, conn);
        });
        cb3();
      });
    }, cb2);
  }, cb);
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

  async.eachSeries(this._dbs, function(dbCfgSecret, cb2) {
    var dbCfg = filterSecrets(dbCfgSecret);

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

      var tasks = [];

      // check if there is a local source, if so, setup a head lookup channel, a local data channel and start the accompanying adapter
      if (dbCfg.source) {
        tasks.push(function(cb3) {
          var parsedUrl = url.parse(dbCfg.source.url);
          var proto = parsedUrl.protocol.slice(0, parsedUrl.protocol.length - 1); // strip postfixed ":"
          if (!that._adapters[proto]) {
            error = new Error('unsupported protocol');
            that._log.err('master initDbs %s error %j', dbCfg.source.url, error);
            process.nextTick(function() {
              cb3(error);
            });
            return;
          }

          if (dbCfgSecret.source.secrets) {
            // prepend the config base to a relative path specification
            let file = dbCfgSecret.source.secrets;
            if (file && file[0] !== '/') {
              file = that._opts.configBase + file;
            }
            try {
              var secrets = loadSecrets(file);
              // filter all but the needed secrets
              dbCfg.source.secrets = {};
              dbCfg.source.secrets[dbCfg.source.dbUser] = secrets[dbCfg.source.dbUser];
              dbCfg.source.secrets[dbCfg.source.oplogDbUser] = secrets[dbCfg.source.oplogDbUser];
              that._log.debug('master initDbs %s loaded %s', dbCfg.source.url, file);
            } catch(err) {
              that._log.err('master initDbs %s loading %s %s', dbCfg.source.url, file, err);
              process.nextTick(function() {
                cb3(err);
              });
              return;
            }
          }

          that._forkAdapter(proto, dbCfg.source, function afterInit(err, adapter, dataChannel, headLookup) {
            if (err) { cb3(err); return; }

            db.send({ type: 'localDataChannel' }, dataChannel);
            db.send({ type: 'headLookup' }, headLookup);
          }, cb3);
        });
      }

      tasks.push(function(cb3) {
        // signal that it should start merging
        db.send({ type: 'startMerge' });
        process.nextTick(cb3);
      });

      async.series(tasks, cb2);
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
 *   [user]:         {String}      // defaults to "_pdbnull"
 *   [group]:        {String}      // defaults to "_pdbnull"
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
      that._log.err('master listen invalid auth request %j', filterSecrets(authReq));
      connErrorHandler(conn, new Error('invalid auth request'));
      return;
    }

    // verify credentials
    that._verifyAuthRequest(authReq, function(err) {
      if (err) {
        that._log.err('master listen auth request %j, err %j', filterSecrets(authReq), err);
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
    // continue
  }

  var user = this._opts.user || '_pdbnull';
  var group = this._opts.group || '_pdbnull';

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

  that._log.info('master listen preauth: %j', filterSecrets(preauthCfg));

  // open preauth server
  var preauth = spawn(process.execPath, ['--abort-on-uncaught-exception', __dirname + '/preauth_exec'], {
    cwd: process.cwd(),
    env: {},
    stdio: stdio
  });

  this._preauth = preauth;
  var childPhase;

  preauth.on('exit', function(code, signal) {
    that._log.info('preauth child exit', code, signal);
    childPhase = 'close';
    that._preauth = null;
    that.stopTerm();
  });

  preauth.on('error', function(err) {
    that._log.err('preauth child error', err);
    that.stopTerm();
  });

  var uid, gid;
  try {
    uid = posix.getpwnam(user).uid;
    gid = posix.getgrnam(group).gid;
  } catch(err) {
    this._log.err('%s %s:%s', err, user, group);
    this.stopTerm();
    return;
  }

  // chroot this process before forking preauth
  try {
    chroot(newRoot, uid, gid);
  } catch(err) {
    this._log.err('can not chroot "%s"', err);
    this.stopTerm();
    return;
  }

  // set core limit to maximum allowed size
  posix.setrlimit('core', { soft: posix.getrlimit('core').hard });
  this._log.info('core limit: %j, fsize limit: %j', posix.getrlimit('core'), posix.getrlimit('fsize'));

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
 * Stop pre-auth server, db instances (not catching SIGTERM) and running adapters.
 *
 * @param {Function} cb  First parameter will be an Error object or null.
 */
Master.prototype.stopTerm = function stopTerm(cb) {
  if (!cb) { cb = noop; }

  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (this._shuttingDown) {
    this._log.info('master stopTerm already shutting down');
    return;
  }

  this._shuttingDown = true;

  var that = this;

  var tasks = [];

  this._log.notice('master stopTerm clients %d', this._clients.length);

  this._clients.forEach(function(client, i) {
    if (client && client.connected) {
      that._log.notice('closing client %d', i);
      tasks.push(function(cb2) {
        client.on('exit', function() {
          that._log.notice('closed client %d', i);
          cb2();
        });
        that._log.info('sending kill signal to client %d', i);
        client.send({ type: 'kill' });
      });
    } else {
      that._log.notice('client %d already closed', i);
    }
  });

  var dbe = Object.keys(this._dbe);
  this._log.notice('master stopTerm dbe', dbe);

  dbe.forEach(function(key) {
    var dbe = that._dbe[key];
    if (dbe && dbe.connected) {
      tasks.push(function(cb2) {
        that._log.notice('closing dbe', key);
        dbe.on('exit', function() {
          that._log.notice('master stopTerm closed dbe', key);
          cb2();
        });
        dbe.send({ type: 'kill' });
      });
    } else {
      that._log.notice('dbe already closed', key);
    }
  });

  if (this._preauth) {
    tasks.push(function(cb2) {
      if (that._preauth && that._preauth.connected) {
        that._preauth.on('exit', function(err) {
          if (err) { cb2(err); return; }
          that._log.notice('preauth closed');
          cb2();
        });
        that._log.notice('closing preauth');
        that._preauth.send({ type: 'kill' });
      } else {
        that._log.notice('preauth already closed');
      }
    });
  }

  this._log.notice('master stopTerm adapters %d', this._runningAdapters.length);

  this._runningAdapters.forEach(function(adapter, i) {
    if (adapter && adapter.connected) {
      that._log.notice('closing adapter %d', i);
      tasks.push(function(cb2) {
        adapter.on('exit', function() {
          that._log.notice('closed adapter %d', i);
          cb2();
        });
        that._log.info('sending kill signal to adapter %d', i);
        adapter.send({ type: 'kill' });
      });
    } else {
      that._log.notice('adapter %d already closed', i);
    }
  });

  this._log.notice('master stopTerm tunnels %d', this._tunnels.length);

  this._tunnels.forEach(function(tunnel, i) {
    if (tunnel && tunnel.connected) {
      that._log.notice('closing tunnel %d', i);
      tasks.push(function(cb2) {
        tunnel.on('exit', function() {
          that._log.notice('closed tunnel %d', i);
          cb2();
        });
        that._log.info('sending kill signal to tunnel %d', i);
        tunnel.send({ type: 'kill' });
      });
    } else {
      that._log.notice('tunnel %d already closed', i);
    }
  });

  async.series(tasks, cb);
};



/////////////////////
//// PRIVATE API ////
/////////////////////



/**
 * Fork an adapter that is connected to a db process.
 *
 * @param {String} name  Name of the adapter to start.
 * @param {Object} cfg  Adapter specific configuration
 * @param {Function} afterInit  Called after the forked process has sent an init
 *                              signal. First parameter will be the adapter, second
 *                              parameter will be the data channel, third parameter
 *                              will be the head lookup file descriptor.
 * @param {Function} cb  First parameter will be an Error object or null.
 *
 * Adapters should accept the following options.
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
 *   [chroot]:       {String}      // defaults to /var/empty
 *   [user]:         {String}      // defaults to "_pdbnull"
 *   [group]:        {String}      // defaults to "_pdbnull"
 * }
 *
 * Furthermore an adapter should expect a data channel on fd6 and a head lookup on
 * fd7.
 */
Master.prototype._forkAdapter = function _forkAdapter(name, cfg, afterInit, cb) {
  if (typeof name !== 'string') { throw new TypeError('name must be a string'); }
  if (typeof cfg !== 'object') { throw new TypeError('cfg must be an object'); }
  if (typeof afterInit !== 'function') { throw new TypeError('afterInit must be a function'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var that = this;

  this._log.info('master forkAdapter %s', name);

  var stdio = ['ignore', 'inherit', 'inherit', 'ipc', null, null, 'pipe', 'pipe'];

  var adapterCfg = xtend({
    log: this._log.getOpts(),
    chroot: '/var/empty',
    user: '_pdbnull',
    group: '_pdbnull'
  }, cfg);

  adapterCfg.log.file = this._log.getFileStream();
  adapterCfg.log.error = this._log.getErrorStream();

  // use fd 4 if a file stream is opened
  if (adapterCfg.log.file) {
    stdio[4] = adapterCfg.log.file.fd;
    adapterCfg.log.file = 4;
  }

  // use fd 5 if an error stream is opened
  if (adapterCfg.log.error) {
    stdio[5] = adapterCfg.log.error.fd;
    adapterCfg.log.error = 5;
  }

  that._log.info('master forkAdapter adapter: %j', filterSecrets(adapterCfg));

  // open adapter server
  var adapter = spawn(process.execPath, ['--abort-on-uncaught-exception', this._adapters[name]], {
    cwd: process.cwd(),
    env: {},
    stdio: stdio
  });

  var index = this._runningAdapters.length; // save index of the new adapter for later use
  this._runningAdapters.push(adapter);
  var childPhase;

  adapter.on('exit', function(code, signal) {
    that._log.info('%s: adapter child exit', name, code, signal);
    that._runningAdapters[index] = false;
    childPhase = 'close';
    that.stopTerm();
  });
  adapter.on('error', function(err) {
    that._log.err('%s: adapter child error %j', name, err);
    that.stopTerm();
  });

  // send initial config after adapter is ready to receive messages
  adapter.once('message', function(msg) {
    if (msg !== 'init') {
      that._log.err('master forkAdapter expected message "init"', msg);
      cb(new Error('expected first message to be "init"'));
      return;
    }

    childPhase = 'init';

    that._log.info('master forkAdapter "init" received');

    afterInit(null, adapter, adapter.stdio[6], adapter.stdio[7]);

    adapter.send(adapterCfg);

    adapter.once('message', function(msg) {
      if (msg !== 'listen') {
        that._log.err('master forkAdapter expected message "listen"', msg);
        cb(new Error('expected second message to be "listen"'));
        return;
      }

      childPhase = 'listen';
      that._log.info('master forkAdapter "listen" received');
      cb();
    });
  });
};

/**
 * Start and initialize a tunnel. Return the forked child when it's ready to accept
 * connections over it.
 *
 * @param {Object} tunnel          tunnel exec config
 * @param {Function} cb            First parameter will be an error object or
 *                                 null, second parameter will be the forked child
 *                                 on success.
 *
 * tunnel exec config:
 * {
 *   connect:        {String}        connection string, ssh://host[:port]
 *   fingerprint:    {String}        sha256 hash of the host key in base64 or hex
 *   key:            {Object}        path to private key to authenticate
 *   forward:        {String}        ssh -L format [bind_address:]port:host:hostport
 *   sshUser:        {String}        username on the ssh server
 *   [log]:          {Object}        log configuration
 *   [name]:         {String}        custom name for this tunnel
 *   [chroot]:       {String}        defaults to /var/empty
 *   [user]:         {String}        defaults to "_pdbnull"
 *   [group]:        {String}        defaults to "_pdbnull"
 * }
 */
Master.prototype._startTunnel = function _startTunnel(tunnel, cb) {
  if (typeof tunnel !== 'object') { throw new TypeError('tunnel must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (tunnel.log == null) { tunnel.log = this._opts.log; }
  if (typeof tunnel.log !== 'object') { throw new TypeError('tunnel.log must be an object'); }

  if (!tunnel.connect || typeof tunnel.connect !== 'string') { throw new TypeError('tunnel.connect must be a string'); }
  if (!tunnel.fingerprint || typeof tunnel.fingerprint !== 'string') { throw new TypeError('tunnel.fingerprint must be a string'); }
  if (!tunnel.key || typeof tunnel.key !== 'string') { throw new TypeError('tunnel.key must be a string'); }
  if (!tunnel.forward || typeof tunnel.forward !== 'string') { throw new TypeError('tunnel.forward must be a string'); }
  if (!tunnel.sshUser || typeof tunnel.sshUser !== 'string') { throw new TypeError('tunnel.sshUser must be a string'); }

  if (tunnel.name != null && !Array.isArray(tunnel.name)) { throw new TypeError('tunnel.name must be an array'); }
  if (tunnel.chroot != null && typeof tunnel.chroot !== 'string') { throw new TypeError('tunnel.chroot must be a string'); }
  if (tunnel.user != null && typeof tunnel.user !== 'string') { throw new TypeError('tunnel.user must be a string'); }
  if (tunnel.group != null && typeof tunnel.group !== 'string') { throw new TypeError('tunnel.group must be a string'); }

  var name = tunnel.name || tunnel.connect;
  var that = this;

  that._log.info('master _startTunnel forking: %s', name);

  var stdio = ['ignore', 'inherit', 'inherit', 'ipc'];

  // use fd 4 if a file stream is opened
  if (tunnel.log.file) {
    stdio[4] = tunnel.log.file.fd;
    tunnel.log.file = 4;
  }

  // use fd 5 if an error stream is opened
  if (tunnel.log.error) {
    stdio[5] = tunnel.log.error.fd;
    tunnel.log.error = 5;
  }

  // check protocol
  var parsedUrl = url.parse(tunnel.connect);
  this._log.debug('master _startTunnel parsed url %j', parsedUrl);
  if (parsedUrl.protocol && parsedUrl.protocol !== 'ssh:') {
    this._log.err('master _startTunnel unsupported protocol %s', parsedUrl.protocol);
    throw new Error('unsupported protocol');
  }

  tunnel.host = parsedUrl.hostname || parsedUrl.pathname;
  tunnel.port = parseInt(parsedUrl.port) || 22;

  // prepend the config base to a relative path specification
  let file = tunnel.key;
  this._log.debug('master _startTunnel %s loading %s', tunnel.connect, file);
  if (file && file[0] !== '/') {
    tunnel.key = this._opts.configBase + file;
  }

  var te = spawn(process.execPath, ['--abort-on-uncaught-exception', __dirname + '/tunnel_exec'], {
    cwd: process.cwd(),
    env: {},
    stdio: stdio
  });

  // register tunnel
  var index = this._tunnels.length; // save index of the new tunnel for later use
  this._tunnels.push(te);
  this._log.notice('tunnel registered %s', tunnel.connect);

  var phase = null;

  te.once('exit', function(code, sig) {
    that._log.notice('master _startTunnel exit', name, code, sig);
    that._tunnels[index] = false;
    phase = 'close';
    that.stopTerm();
  });

  te.on('error', function(err) {
    that._log.err('master _startTunnel error', name, err);
    that.stopTerm();
  });

  // send db and log config after dbe is ready to receive messages
  te.once('message', function(msg) {
    if (msg !== 'init') {
      that._log.err('master _startTunnel expected message "init"', name, msg);
      cb(new Error('expected first message to be "init"'));
      return;
    }

    phase = 'init';
    that._log.info('master _startTunnel "init" received', name);

    te.send(tunnel);

    // wait for the child to send the "listen" message
    te.once('message', function(msg) {
      if (msg !== 'listen') {
        that._log.err('master _startTunnel expected message "listen"', name, msg);
        cb(new Error('expected second message to be "listen"'));
        return;
      }

      that._log.info('master _startTunnel "listen" received', name);

      phase = 'listen';

      cb(null, te);
    });
  });
};

/**
 * Start and initialize connections to other dbs. Return the forked child when it's
 * ready. Always connect to a port on the localhost. Use a tunnel to connect to
 * remote hosts.
 *
 * @param {Object} cfg             client exec config
 * @param {Function} cb            First parameter will be an error object or
 *                                 null, second parameter will be the forked child
 *                                 on success.
 *
 * client exec config:
 * {
 *   username:       {String}      // username to login with
 *   password:       {String}      // password to authenticate
 *   database:       {String}      // database to authenticate with
 *   [log]:          {Object}      // log configuration
 *   [port]:         {Number}      // port, defaults to 2344 (might be a tunnel)
 *   [name]:         {String}      // custom name for this client
 *   [chroot]:       {String}      // defaults to /var/empty
 *   [user]:         {String}      // defaults to "_pdbnull"
 *   [group]:        {String}      // defaults to "_pdbnull"
 * }
 */
Master.prototype._startClient = function _startClient(cfg, cb) {
  if (typeof cfg !== 'object') { throw new TypeError('cfg must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (cfg.log == null) { cfg.log = this._opts.log; }
  if (typeof cfg.log !== 'object') { throw new TypeError('cfg.log must be an object'); }

  if (!cfg.username || typeof cfg.username !== 'string') { throw new TypeError('cfg.username must be a string'); }
  if (!cfg.password || typeof cfg.password !== 'string') { throw new TypeError('cfg.password must be a string'); }
  if (!cfg.database || typeof cfg.database !== 'string') { throw new TypeError('cfg.database must be a string'); }

  if (cfg.port != null && typeof cfg.port !== 'number') { throw new TypeError('cfg.port must be an array'); }
  if (cfg.name != null && typeof cfg.name !== 'string') { throw new TypeError('cfg.name must be a string'); }
  if (cfg.chroot != null && typeof cfg.chroot !== 'string') { throw new TypeError('cfg.chroot must be a string'); }
  if (cfg.user != null && typeof cfg.user !== 'string') { throw new TypeError('cfg.user must be a string'); }
  if (cfg.group != null && typeof cfg.group !== 'string') { throw new TypeError('cfg.group must be a string'); }

  cfg.port = cfg.port || 2344;
  var name = cfg.name || cfg.port;
  var that = this;

  that._log.info('master _startClient forking: %s', name);

  var stdio = ['ignore', 'inherit', 'inherit', 'ipc'];

  var clientCfg = xtend({
    log: this._log.getOpts(),
    chroot: '/var/empty',
    user: '_pdbnull',
    group: '_pdbnull'
  }, cfg);

  clientCfg.log.file = this._log.getFileStream();
  clientCfg.log.error = this._log.getErrorStream();

  // use fd 4 if a file stream is opened
  if (clientCfg.log.file) {
    stdio[4] = clientCfg.log.file.fd;
    clientCfg.log.file = 4;
  }

  // use fd 5 if an error stream is opened
  if (clientCfg.log.error) {
    stdio[5] = clientCfg.log.error.fd;
    clientCfg.log.error = 5;
  }

  var ce = spawn(process.execPath, ['--abort-on-uncaught-exception', __dirname + '/client_exec'], {
    cwd: process.cwd(),
    env: {},
    stdio: stdio
  });

  // register client
  var index = this._clients.length; // save index of the new client for later use
  this._clients.push(ce);
  this._log.notice('client registered %s', clientCfg.port);

  var phase = null;

  ce.once('exit', function(code, sig) {
    that._log.notice('master _startClient exit', name, code, sig);
    that._clients[index] = false;
    phase = 'close';
    that.stopTerm();
  });

  ce.on('error', function(err) {
    that._log.err('master _startClient error', name, err);
    that.stopTerm();
  });

  // send db and log config after dbe is ready to receive messages
  ce.once('message', function(msg) {
    if (msg !== 'init') {
      that._log.err('master _startClient expected message "init"', name, msg);
      cb(new Error('expected first message to be "init"'));
      return;
    }

    phase = 'init';
    that._log.info('master _startClient "init" received', name);

    ce.send(clientCfg);

    // wait for the child to send the "listen" message
    ce.once('message', function(msg) {
      if (msg !== 'listen') {
        that._log.err('master _startClient expected message "listen"', name, msg);
        cb(new Error('expected second message to be "listen"'));
        return;
      }

      that._log.info('master _startClient "listen" received', name);

      phase = 'listen';

      cb(null, ce);
    });
  });
};

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
 *   [user]:         {String}      // defaults to "pdblevel"
 *   [group]:        {String}      // defaults to "pdblevel"
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

  var dbe = spawn(process.execPath, ['--abort-on-uncaught-exception', __dirname + '/db_exec'], {
    cwd: process.cwd(),
    env: {},
    stdio: stdio
  });

  var phase = null;

  dbe.once('exit', function(code, sig) {
    that._log.notice('master _startDb exit', name, code, sig);
    phase = 'close';
    that.stopTerm();
  });

  dbe.on('error', function(err) {
    that._log.err('master _startDb error', name, err);
    that.stopTerm();
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
 * Verify auth request.
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
