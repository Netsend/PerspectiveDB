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

var async = require('async');

/**
 * Replicator
 *
 * Check which collections to tail and where to save data.
 *
 * @param {Object} [options]  additional options
 *
 * Options
 * - **debug** {Boolean, default: false}  whether to do extra console logging or not
 * - **hide** {Boolean, default: false}  whether to suppress errors or not (used
 *                                       in tests)
 */
function Replicator(options) {
  options = options || {};

  this._options = options;
  this.debug = !!this._options.debug;
}
module.exports = Replicator;

/**
 * Return an array of database collection combinations that both import each other.
 *
 * @param {Object} config  replication config
 * @return {Array} an array where each element is an object of two db collections
 * that import from each other (name as key, boolean true as value)
 */
Replicator.bidirFrom = function bidirFrom(config) {
  if (typeof config !== 'object' || Array.isArray(config)) { throw new TypeError('config must be an object'); }

  var result = [];
  var inResult = {};

  var from = {};
  // build a map of full db.collection names in both ways
  Object.keys(config).forEach(function(system) {
    if (config[system].from) {
      Object.keys(config[system].from).forEach(function(remote) {
        var collections = Object.keys(config[system].from[remote]);
        collections.forEach(function(collection) {
          from[system + '.' + collection] = from[system + '.' + collection] || {};
          from[system + '.' + collection][remote + '.' + collection] = true;
        });
      });
    }
  });

  // find cross-links
  Object.keys(from).forEach(function(toKey) {
    Object.keys(from[toKey]).forEach(function(fromKey) {
      if (from[fromKey] && from[fromKey][toKey]) {
        if (!inResult[fromKey + '.' + toKey]) {
          var item = {};
          item[fromKey] = true;
          item[toKey] = true;
          result.push(item);
          inResult[fromKey + '.' + toKey] = true;
          inResult[toKey + '.' + fromKey] = true;
        }
      }
    });
  });

  return result;
};

/**
 * Return a new config that only replicates to the given database.collection.
 *
 * @param {Object} config  replication config
 * @param {String} target  database collection name
 * @return {Object} new config that is a subset of the given input
 */
Replicator.replicateTo = function replicateTo(config, target) {
  if (typeof config !== 'object' || Array.isArray(config)) { throw new TypeError('config must be an object'); }
  if (typeof target !== 'string') { throw new TypeError('target must be a string'); }

  var targetDb, targetCollection;
  targetDb = target.split('.')[0];
  targetCollection = target.split('.')[1];

  if (!targetDb) { throw new Error('could not determine target database name'); }
  if (!targetCollection) { throw new Error('could not determine target collection name'); }

  var result = {};

  // return empty result if no from in targetDb
  if (!config[targetDb] || !config[targetDb].from) {
    return result;
  }

  // build a map of sources the target will receive.
  var receiveFrom = {};
  Object.keys(config[targetDb].from).forEach(function(targetSource) {
    Object.keys(config[targetDb].from[targetSource]).forEach(function(collection) {
      // only if collection is requested
      if (collection !== targetCollection) { return; }

      // only if collection is non-negative
      if (config[targetDb].from[targetSource][collection]) {
        receiveFrom[targetSource + '.' + collection] = true;
      }
    });
  });

  // build a map of sources that send to the target, that intersects with receiveFrom
  var sendToTarget = {};
  Object.keys(config).forEach(function(db) {
    if (db === targetDb) { return; }
    if (!config[db].to || !config[db].to[targetDb]) { return; }
    Object.keys(config[db].to[targetDb]).forEach(function(collection) {
      // only if collection is non-negative
      if (config[db].to[targetDb][collection]) {
        // only if target is receiving this collection (intersectsion)
        var name = db + '.' + collection;
        if (receiveFrom[name]) {
          sendToTarget[name] = true;
        }
      }
    });
  });

  // get subset from config based on sendToTarget
  Object.keys(sendToTarget).forEach(function(dbColl) {
    var sourceDb, sourceCollection;
    sourceDb = dbColl.split('.')[0];
    sourceCollection = dbColl.split('.')[1];

    // setup from
    result[targetDb] = result[targetDb] || {};
    result[targetDb].from = result[targetDb].from || {};
    result[targetDb].from[sourceDb] = result[targetDb].from[sourceDb] || {};
    result[targetDb].from[sourceDb][sourceCollection] = config[targetDb].from[sourceDb][sourceCollection];

    // setup to
    result[sourceDb] = result[sourceDb] || {};
    result[sourceDb].to = result[sourceDb].to || {};
    result[sourceDb].to[targetDb] = result[sourceDb].to[targetDb] || {};
    result[sourceDb].to[targetDb][sourceCollection] = config[sourceDb].to[targetDb][sourceCollection];
  });

  return result;
};

/**
 * Split a replication configuration object into export and import keys.
 *
 * @param {Object} config  replication config
 * @return {Object} an object with per system and collection filters for easy lookup.
 */
Replicator.splitImportExport = function splitImportExport(config) {
  if (typeof config !== 'object' || Array.isArray(config)) { throw new TypeError('config must be an object'); }

  var result = {
    import: {},
    export: {}
  };

  Object.keys(config).forEach(function(system) {
    if (config[system].from) {
      Object.keys(config[system].from).forEach(function(remote) {
        var collections = Object.keys(config[system].from[remote]);
        collections.forEach(function(collection) {
          result.import[system] = result.import[system] || {};
          result.import[system][remote+'.'+collection] = config[system].from[remote][collection];
        });
      });
    }

    if (config[system].to) {
      Object.keys(config[system].to).forEach(function(remote) {
        var collections = Object.keys(config[system].to[remote]);
        collections.forEach(function(collection) {
          result.export[system+'.'+collection] = result.export[system+'.'+collection] || {};
          result.export[system+'.'+collection][remote] = config[system].to[remote][collection];
        });
      });
    }
  });
  return result;
};

/**
 * Verify for each import there is a corresponding export.
 *
 * @param {Object} config  split replication config
 * @return {Object} import entries not matched with an export
 */
Replicator.verifyImportExport = function verifyImportExport(config) {
  if (typeof config !== 'object' || Array.isArray(config)) { throw new TypeError('config must be an object'); }
  if (typeof config.import !== 'object' || Array.isArray(config)) { throw new TypeError('config.import must be an object'); }
  if (typeof config.export !== 'object' || Array.isArray(config)) { throw new TypeError('config.export must be an object'); }

  var result = {};
  Object.keys(config.import).forEach(function(importer) {
    Object.keys(config.import[importer]).forEach(function(systemCollection) {
      // only check if the value for the import is truthy
      if (!config.import[importer][systemCollection]) { return; }
      if (!(config.export[systemCollection] && config.export[systemCollection][importer])) {
        result[importer] = result[importer] || [];
        result[importer].push(systemCollection);
      }
    });
  });
  return result;
};

/**
 * Get an object with all information needed to start tails for the provided config.
 * Ensures for every provided versioned collection:
 * * source and destination versioned collections
 * * filters
 * * transformations
 * * hooks
 * * last item in destination of the source
 *
 * @param {Object} config  import and export config of the systems to replicate
 * @param {Object} vcs  object containing versioned collections. Database name plus
 *                     collection name should be the key.
 * @param {Function} cb  First parameter will be an Error or null. Second parameter
 *                       will be an array of objects containing all info to tail.
 */
Replicator.getTailOptions = function getTailOptions(config, vcs, cb) {
  if (typeof config !== 'object' || Array.isArray(config)) { throw new TypeError('config must be an object'); }
  if (typeof vcs !== 'object' || Array.isArray(vcs)) { throw new TypeError('vcs must be an object'); }

  var unmatchingImports = Replicator.verifyImportExport(config);
  if (Object.keys(unmatchingImports).length) {
    throw new TypeError('config has imports that don\'t match an export: ' + JSON.stringify(unmatchingImports));
  }

  var that = this;
  var result = {};

  var imports = Object.keys(config.import);

  // initiate by imports
  async.eachSeries(imports, function(importer, cb2) {
    async.eachSeries(Object.keys(config.import[importer]), function(dbColl, cb3) {
      var options = {};

      var srcName = dbColl;
      var dstDb = dbColl.split('.')[0];
      var dstName = importer + '.' + dbColl.split('.')[1];

      if (!vcs[srcName]) {
        process.nextTick(function() {
          cb3(new Error('export not in vcs: ' + srcName));
        });
        return;
      }

      if (!vcs[dstName]) {
        process.nextTick(function() {
          cb3(new Error('import not in vcs: ' + dstName));
        });
        return;
      }

      options.from = vcs[srcName];
      options.to = vcs[dstName];

      options.to.addRemote(dstDb);

      options.filter = config.export[dbColl][importer].filter;

      var hookPath = config.hookPath || '../hooks';

      function requireHooks(filenames) {
        return (filenames || []).map(function(filename) {
          var m = require(hookPath + '/' + filename);
          m.DEBUG = that.debug;
          return m;
        });
      }

      options.importHooks = requireHooks(config.import[importer][srcName].hooks);
      options.exportHooks = requireHooks(config.export[dbColl][importer].hooks);

      // get transformation on keys to hide
      var transform;
      var keysToHide = config.export[dbColl][importer].hide;
      if (keysToHide && keysToHide.length) {
        transform = function(obj) {
          keysToHide.forEach(function(key) {
            delete obj[key];
          });
          return obj;
        };
      }

      options.transform = transform;

      // now lookup the last version of this source
      vcs[dstName].lastByPerspective(dstDb, function(err, item) {
        if (err) { return cb3(err); }
        if (!item) {
          options.last = false;
        } else {
          options.last = item._id._v;
        }
        result[srcName+'+'+dstName] = options;
        cb3();
      });
    }, cb2);
  }, function(err) {
    cb(err, result);
  });
};

/**
 * Include all given hook files, return an array of hook functions. Ignore
 * files that can not be included.
 *
 * @param {Array} files  array of file names to include
 * @param {String, default ../hooks} [path]  optional path to prefix files with
 * @return {Array} array of included hook module functions
 */
Replicator.loadHooks = function loadHooks(files, path) {
  if (!Array.isArray(files)) { throw new TypeError('files must be an array'); }

  path = path || __dirname + '/../hooks';
  if (typeof path !== 'string') { throw new TypeError('path must be a string'); }

  // require hooks
  var hooks = [];
  files.forEach(function(file) {
    var hook;
    try {
      hook = require(path + '/' + file);
    } catch(err) {
      if (err.code === 'MODULE_NOT_FOUND') {
        // retry with standard dir
        hook = require(__dirname + '/../hooks/' + file);
      } else {
        throw err;
      }
    }
    hooks.push(hook);
  });
  return hooks;
};

/**
 * Include given config and load hooks, filter and transform hidden fields into
 * a hook for every collection.
 *
 * @param {Object} config  replication configuration object
 * @param {Object} [options]  object containing configurable parameters
 *
 * options:
 *   hookPath {String, default ../hooks}  path to prefix hooks
 *
 * @return {Object}  an object with collection names as key and the following
 * structure:
 *   {
 *     [collectionName]: {
 *       filter:      {Object}      object of filters to use
 *       hooks:       {Array}       array of hook functions to run
 *       orig:        {Object}      original config
 *     }
 *   }
 */
Replicator.loadConfig = function loadConfig(config, options) {
  if (typeof config !== 'object') { throw new TypeError('config must be an object'); }

  options = options || {};
  if (typeof options !== 'object') { throw new TypeError('options must be an object'); }

  var hookPath = options.hookPath || __dirname + '/../hooks';
  if (typeof hookPath !== 'string') { throw new TypeError('options.hookPath must be a string'); }

  var loadedConfig = {};
  Object.keys(config).forEach(function(collName) {
    loadedConfig[collName] = loadedConfig[collName] || {};

    // filter
    loadedConfig[collName].filter = config[collName].filter;

    // hooks
    var hooks = [];
    if (config[collName].hooks) {
      hooks = Replicator.loadHooks(config[collName].hooks, hookPath);
    }

    // create a hook for keys to hide
    var keysToHide = config[collName].hide;
    if (keysToHide && keysToHide.length) {
      hooks.push(function(db, item, opts, cb) {
        keysToHide.forEach(function(key) {
          delete item[key];
        });
        cb(null, item);
      });
    }

    if (hooks.length) { loadedConfig[collName].hooks = hooks; }

    // orig, a reference to the original config object
    loadedConfig[collName].orig = config[collName];
  });

  return loadedConfig;
};

/**
 * Fetch replication config by type and remote in the given collection.
 *
 * @param {Object} coll  collection that should contain config
 * @param {String} type  either "import" or "export"
 * @param {String} remote  name of the remote to fetch
 * @param {Function} cb  First parameter will be an error object or null. Second
 *                       parameter will be the replication config if no error
 *                       occurred.
 */
Replicator.fetchFromDb = function fetchFromDb(coll, type, remote, cb) {
  if (typeof coll !== 'object') { throw new TypeError('coll must be an object'); }
  if (type !== 'import' && type !== 'export') { throw new TypeError('type must be either "import" or "export"'); }
  if (typeof remote !== 'string') { throw new TypeError('remote must be a string'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var error;

  // search replication config for the remote using the username
  coll.findOne({ type: type, remote: remote }, function(err, replCfg) {
    if (err) {
      console.error('Replicator', err, type, remote);
      cb(err);
      return;
    }

    if (!replCfg) {
      error = new Error('replication config not found');
      console.error('Replicator', error, type, remote);
      cb(error);
      return;
    }

    cb(null, replCfg);
  });
};
