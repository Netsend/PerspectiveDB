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

// Rebase items of one collection on versions of another collection

var mongodb = require('mongodb');

var VersionedSystem = require('./versioned_system');
var VersionedCollection = require('./versioned_collection');
var Replicator = require('./replicator');

var error;

/**
 * Rebase items of one collection on versions of all collections that replicate
 * to it. Backup the collection, clear the original, start replicating and once
 * that is all done, copy back the backup and start versioning to set in sync.
 *
 * Note: the complete history of the collection will be rebuild since new parents
 * will be set on items that previously existed.
 *
 * 0. make sure all collections are in sync
 * 1. ensure the tmp collection is empty
 * 2. copy all items from the collection to the tmp collection
 * 3. clear the collection and versioned collection
 * 4. start vs to replicate into collection1
 * 5. upsert all items from the tmp collection back into collection
 * 6. start versioning again
 * 7. drop tmp collection
 *
 * @param {VersionedSystem} vsExample  versioned system with loaded replication
 *                                     config
 * @param {VersionedCollection} vc  versioned collection to rebase
 * @param {mongodb.Collection} tmpColl  temporary collection that can be dropped
 * @param {Object} [opts]  options
 * @param {Function} cb  first parameter will be an error or null.
 *
 * Options
 *   **debug** {Boolean}  whether or not to show debugging info
 */
function rebase(vsExample, vc, tmpColl, opts, cb) {
  if (!(vsExample instanceof VersionedSystem)) { throw new TypeError('vsExample must be a VersionedSystem'); }
  if (!(vc instanceof VersionedCollection)) { throw new TypeError('vc must be a VersionedCollection'); }
  if (!(tmpColl instanceof mongodb.Collection)) { throw new TypeError('tmpColl must be a mongodb.Collection'); }

  if (typeof opts === 'function') {
    cb = opts;
    opts = {};
  }

  opts = opts || {};
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var debug = opts.debug || false;
  if (typeof debug !== 'boolean') { throw new TypeError('opts.debug must be a boolean'); }

  // create a new versioned system first without replication, later only replication of the versioned collection that is being rebased.
  var vsOpts = {
    debug: opts.debug,
    hide: vsExample.hide,
    oplogCollectionName: vsExample.oplogCollectionName,
    localDbName: vsExample.localDbName,
    localStorageName: vsExample.localStorageName,
    snapshotSize: vsExample._snapshotSize,
    snapshotSizes: vsExample._snapshotSizes,
    tailableRetryInterval: 100,
    autoProcessInterval: 100
  };
  var vs = new VersionedSystem(vsExample._oplogDb, vsExample._databaseNames, vsExample._collectionNames, vsOpts);

  if (Replicator.bidirFrom(vs._replicate).length) { throw new Error('bidirectional imports unsupported'); }

  if (debug) { console.log('rebasing', vc.databaseName + '.' + vc.collectionName, 'with', JSON.stringify(vs._replicate)); }

  if (debug) { console.log('### REBASE', '0. make sure all collections are in sync'); }
  vs.start(false, function(err) {
    if (err) {
      console.error('rebase', 0, err);
      return cb(err);
    }

    if (debug) { console.log('### REBASE', '1. ensure the tmp collection is empty'); }
    tmpColl.drop(function(err) {
      if (err && err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) {
        console.error('rebase', 1, err);
        return cb(err);
      }

      if (debug) { console.log('### REBASE', '2. copy all items from the collection to the tmp collection'); }

      var s = vc._collection.find().stream();
      var tmpItemCounter = 0;
      s.on('data', function(item) {
        tmpItemCounter++;
        tmpColl.insert(item, { w: 1 }, function(err, inserted) {
          if (err) {
            console.error('rebase', 2, err);
            return cb(err);
          }

          if (inserted.length !== 1) {
            error = new Error('item not inserted into temporary collection');
            console.error('rebase', 2, error);
            return cb(error);
          }
        });
      });

      s.on('error', function(err) {
        console.error('rebase', 2, err);
        return cb(err);
      });

      s.on('close', function() {
        if (debug) { console.log('### REBASE', '3. clear the collection and versioned collection'); }

        // do not drop but remove and rebuild to preserve collection sizes
        vc._collection.remove({}, function(err) {
          if (err) {
            console.error('rebase remove', 3, err);
            return cb(err);
          }

          vc.rebuild(vs._snapshotSizes[vc.databaseName + '.' + vc.collectionName], function(err) {
            if (err) {
              console.error('rebase rebuild', 3, err);
              return cb(err);
            }

            if (debug) { console.log('### REBASE', '4. start versioning, this will automatically rebuild and replicate'); }
            // setup replication config
            vs._replicate = Replicator.replicateTo(vsExample._replicate, vc.databaseName + '.' + vc.collectionName);
            vs.start(false, function(err) {
              if (err) {
                console.error('rebase', 4, err);
                return cb(err);
              }

              var updateCounter = 0;

              if (debug) { console.log('### REBASE', '5. upsert all items from the tmp collection back into collection'); }
              var s = tmpColl.find().stream();
              s.on('data', function(item) {
                vc._collection.update({ _id: item._id }, item, { upsert: true, w: 1 }, function(err, updated) {
                  if (err) {
                    console.error('rebase', 5, err);
                    return cb(err);
                  }

                  if (updated !== 1) {
                    error = new Error('item in collection not updated');
                    console.error('rebase', 5, error, item);
                    return cb(error);
                  }

                  if (debug) { console.log('rebase', 5, 'updated', updated); }

                  updateCounter++;
                  if (updateCounter === tmpItemCounter) {
                    proceed6();
                  }
                });
              });

              s.on('error', function(err) {
                console.error('rebase', 5, err);
                return cb(err);
              });

              s.on('close', function() {
                if (tmpItemCounter === 0) {
                  proceed6();
                }
              });

              function proceed6() {
                if (debug) { console.log('### REBASE', '6. start versioning again, this will automatically sync the updates'); }
                vs.start(false, function(err) {
                  if (err) {
                    console.error('rebase', 6, err);
                    return cb(err);
                  }

                  if (debug) { console.log('### REBASE', '7. drop tmp collection'); }
                  tmpColl.drop(function(err) {
                    if (err && err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) {
                      console.error('rebase', 7, err);
                      return cb(err);
                    }
                    cb();
                  });
                });
              }
            });
          });
        });
      });
    });
  });
}

module.exports = rebase;
