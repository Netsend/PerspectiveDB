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

/*jshint -W068, -W030, nonew: false */

var should = require('should');
var Timestamp = require('mongodb').Timestamp;

var VersionedSystem = require('../../lib/versioned_system');

var db, db2, oplogDb;
var databaseName = 'test_versioned_system';
var databaseName2 = 'test2';
var oplogDatabase = 'local';

var databaseNames = [databaseName, databaseName2, 'foo', 'bar'];
var Database = require('../_database');

// open database connection
var database = new Database(databaseNames);
before(function(done) {
  database.connect(function(err, dbs) {
    if (err) { throw err; }
    db = dbs[0];
    db2 = dbs[1];
    oplogDb = db.db(oplogDatabase);

    // drop local db
    var vs = new VersionedSystem(oplogDb, [databaseName], ['foo']);
    vs._localStorageCollection.drop(function(err) {
      if (err && err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) { throw err; }
      done();
    });
  });
});

after(function(done) {
  var vs = new VersionedSystem(oplogDb, [databaseName], ['foo']);
  vs._localStorageCollection.drop(function(err) {
    if (err && err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) { throw err; }
    database.disconnect(done);
  });
});

describe('VersionedSystem', function() {
  describe('constructor', function() {
    it('should require oplogDb', function() {
      (function() { new VersionedSystem(); }).should.throw('provide oplogDb');
    });

    it('should require oplogDb to be a mongodb.Db', function() {
      (function() { new VersionedSystem({}); }).should.throw('oplogDb must be a mongdb.Db');
    });

    it('should require databaseNames to be an array', function() {
      (function() { new VersionedSystem(oplogDb, {}); }).should.throw('databaseNames must be an array');
    });

    it('should require databaseNames to have one or more items', function() {
      (function() { new VersionedSystem(oplogDb, []); }).should.throw('databaseNames must not be empty');
    });

    it('should require collectionNames to be an array', function() {
      (function() { new VersionedSystem(oplogDb, ['foo'], {}); }).should.throw('collectionNames must be an array');
    });

    it('should require collectionNames to have one or more items', function() {
      (function() { new VersionedSystem(oplogDb, ['foo'], []); }).should.throw('collectionNames must not be empty');
    });

    it('should require options.replicate to be an object', function() {
      (function() { new VersionedSystem(oplogDb, ['foo'], ['bar'], { replicate: [] }); }).should.throw('options.replicate must be an object');
    });

    it('should construct', function() {
      (function() { new VersionedSystem(oplogDb, ['foo'], ['bar'], { replicate: {} }); }).should.not.throw();
    });
  });

  describe('allVersionedCollectionsEmpty', function() {
    var collName  = 'allVersionedCollectionsEmpty';
    var collName2 = 'allVersionedCollectionsEmptyColl2';

    it('should return true with only empty versioned collections', function(done) {
      var opts = { debug: false };
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName, collName2], opts);
      vs.allVersionedCollectionsEmpty(function(err, result) {
        if (err) { throw err; }
        should.strictEqual(result, true);
        done();
      });
    });

    it('should return false if one of the versioned collections is not empty', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName, oplogCollectionName: collName };
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName, collName2], opts);
      var vc = vs._databaseCollections[databaseName + '.' + collName];
      vc._snapshotCollection.insert({ foo: 'bar' }, function(err, inserts) {
        if (err) { throw err; }
        should.equal(inserts.length, 1);
        vs.allVersionedCollectionsEmpty(function(err, result) {
          if (err) { throw err; }
          should.strictEqual(result, false);
          done();
        });
      });
    });
  });

  describe('minTimestamp', function() {
    it('should require array as argument', function() {
      (function() { VersionedSystem.minTimestamp(); }).should.throw('times must be an array');
    });

    it('should default to null if array is empty', function() {
      var result = VersionedSystem.minTimestamp([]);
      should.strictEqual(result, null);
    });

    it('should support 2147483646s', function() {
      var ts = new Timestamp(0, 2147483646);
      var result = VersionedSystem.minTimestamp([ts]);
      should.strictEqual(result.equals(ts), true);
    });

    it('should nullify the max of 2147483647', function() {
      var ts = new Timestamp(0, 2147483647);
      var result = VersionedSystem.minTimestamp([ts]);
      should.strictEqual(result, null);
    });

    it('should find min of 3 timestamps with gaps in the array', function() {
      var ts1 = new Timestamp(0, 2147483646);
      var ts2 = new Timestamp(0, 2147483645);
      var ts3 = new Timestamp(0, 2147483644);
      var result = VersionedSystem.minTimestamp([null, undefined, ts1, null, ts3, undefined, undefined, ts2, null]);
      should.strictEqual(result.equals(ts3), true);
    });
  });

  describe('start', function() {
    var collName = 'start';

    it('should require follow to be a boolean', function() {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      (function() { vs.start(); }).should.throw('follow must be a boolean');
    });

    it('should require cb to be a function', function() {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      (function() { vs.start(false); }).should.throw('cb must be a function');
    });

    it('should not require a last used oplog item', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      vs.start(false, done);
    });

    it('needs an old last used oplog item', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      var item = { _id: 'lastUsedOplogItem', ts: new Timestamp(1, 1) };
      vs._saveLastUsedOplogItem(item, done);
    });

    it('needs an item in the versioned collection', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      var vc = vs._databaseCollections[databaseName + '.' + collName];
      var item = { _id: 'A' };
      vc._snapshotCollection.insert(item, {w: 1}, done);
    });

    it('should error if last used oplog item is older then the oldest oplog item and not all versioned collections are empty', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      vs.start(function(err) {
        should.equal(err.message, 'not on track with oplog');
        done();
      });
    });

    it('needs a newer last used oplog item', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      vs.rebuild(done);
    });

    it('should start', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { debug: false });
      vs.start(false, done);
    });

    it('should run twice and close everything after running the first time', function(done) {
      this.timeout(3000);
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { debug: false });
      vs.start(false, function(err) {
        if (err) { throw err; }
        vs.start(false, done);
      });
    });
  });

  describe('trackOplogAndVCs', function() {
    var collName = 'trackOplogAndVCs';

    it('should require follow to be a boolean', function() {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      (function() { vs.trackOplogAndVCs(); }).should.throw('follow must be a boolean');
    });

    it('should require cb to be a function', function() {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      (function() { vs.trackOplogAndVCs(false); }).should.throw('cb must be a function');
    });

    it('should start', function(done) {
      this.timeout(3000);
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { debug: false });
      vs.trackOplogAndVCs(false, done);
    });

    it('should start with simple replication config and empty versioned collections', function(done) {
      var replicate = {
        'foo': {
          'to': {
            'bar': {
              'collFoo': true,
              'collBar': true
            }
          }
        },
        'bar': {
          'from': {
            'foo': {
              'collFoo': true,
              'collBar': true
            }
          }
        }
      };

      var vs = new VersionedSystem(oplogDb, ['foo', 'bar'], ['collFoo', 'collBar'], { replicate: replicate, debug: false });
      vs.trackOplogAndVCs(false, done);
    });

    it('needs an item in the versioned collection', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      var vc = vs._databaseCollections[databaseName + '.' + collName];
      var item = { _id: 'A' };
      vc._snapshotCollection.insert(item, {w: 1}, function(err) {
        if (err) { throw err; }
        vs.rebuild(done);
      });
    });

    it('should start with simple replication config and non-empty versioned collections', function(done) {
      var replicate = {
        'foo': {
          'to': {
            'bar': {
              'collFoo': true,
              'collBar': true
            }
          }
        },
        'bar': {
          'from': {
            'foo': {
              'collFoo': true,
              'collBar': true
            }
          }
        }
      };

      var vs = new VersionedSystem(oplogDb, ['foo', 'bar'], ['collFoo', 'collBar'], { replicate: replicate, hide: true });
      vs.trackOplogAndVCs(false, done);
    });
  });

  describe('setupVcToVcConfigs', function() {
    var collName = 'setupVcToVcConfigs';

    it('should require follow to be a boolean', function() {
      var replicate = {};
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { replicate: replicate });
      (function() { vs.setupVcToVcConfigs(); }).should.throw('follow must be a boolean');
    });

    it('should require cb to be a function', function() {
      var replicate = {};
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { replicate: replicate });
      (function() { vs.setupVcToVcConfigs(false); }).should.throw('cb must be a function');
    });

    it('should return empty array', function(done) {
      var replicate = {};
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { replicate: replicate });

      vs.setupVcToVcConfigs(function(err, configs) {
        should.equal(err, null);
        should.deepEqual(configs, []);
        done();
      });
    });

    it('should return array with two configs', function(done) {
      var replicate = {
        'foo': {
          'to': {
            'bar': {
              'collFoo': {
                'hooks': [],
                'filter': {}
              },
              'collBar': {
                'hooks': [],
                'filter': {}
              }
            }
          }
        },
        'bar': {
          'from': {
            'foo': {
              'collFoo': {
                'hooks': [],
                'filter': {}
              },
              'collBar': {
                'hooks': [],
                'filter': {}
              }
            }
          }
        }
      };

      var vs = new VersionedSystem(oplogDb, ['foo', 'bar'], ['collFoo', 'collBar'], { replicate: replicate, debug: false });

      vs.setupVcToVcConfigs(function(err, configs) {
        if (err) { throw err; }
        should.strictEqual(configs.length, 2);
        done();
      });
    });
  });

  describe('trackVersionedCollections', function() {
    var collName1 = 'trackVersionedCollections1';
    var collName2 = 'trackVersionedCollections2';

    it('should require "from" to be a VersionedCollection', function() {
      var opts = { remotes: [databaseName] };
      var vs = new VersionedSystem(db, [databaseName], [collName1, collName2], opts);
      (function() { vs.trackVersionedCollections(); }).should.throw('from must be a VersionedCollection');
    });

    it('should require "to" to be a VersionedCollection', function() {
      var opts = { remotes: [databaseName] };
      var vs = new VersionedSystem(db, [databaseName], [collName1, collName2], opts);
      var vc1 = vs._databaseCollections[databaseName + '.' + collName1];
      (function() { vs.trackVersionedCollections(vc1); }).should.throw('to must be a VersionedCollection');
    });

    it('should require "opts" to be an object', function() {
      var opts = { remotes: [databaseName] };
      var vs = new VersionedSystem(db, [databaseName], [collName1, collName2], opts);
      var vc1 = vs._databaseCollections[databaseName + '.' + collName1];
      var vc2 = vs._databaseCollections[databaseName + '.' + collName2];
      (function() { vs.trackVersionedCollections(vc1, vc2); }).should.throw('opts must be an object');
    });

    it('should require "cb" to be a function', function() {
      var opts = { remotes: [databaseName] };
      var vs = new VersionedSystem(db, [databaseName], [collName1, collName2], opts);
      var vc1 = vs._databaseCollections[databaseName + '.' + collName1];
      var vc2 = vs._databaseCollections[databaseName + '.' + collName2];
      (function() { vs.trackVersionedCollections(vc1, vc2, {}); }).should.throw('cb must be a function');
    });

    // create capped collection
    it('needs a capped collection for databaseName.collName1', function(done) {
      database.createCappedColl(db, 'm3.' + collName1, done);
    });

    it('needs a capped collection for databaseName2.collName2', function(done) {
      database.createCappedColl(db2, 'm3.' + collName2, done);
    });

    it('needs an item that can be replicated in collName1', function(done) {
      var item = { _id: { _co: collName1, _id : 'baz', _v: 'A', _pe: '_local', _pa: [], _lo: true, _i: 1 }, _m3: { _ack: false } };
      db.collection('m3.' + collName1).insert(item, { w: 1 }, function(err, inserts) {
        if (err) { throw err; }
        should.equal(inserts.length, 1);
        done();
      });
    });

    it('should replicate one collection to the other', function(done) {
      var opts = { debug: false, remotes: [databaseName, databaseName2] };
      var vs = new VersionedSystem(db, [databaseName, databaseName2], [collName1, collName2], opts);
      var vc1 = vs._databaseCollections[databaseName + '.' + collName1];
      var vc2 = vs._databaseCollections[databaseName2 + '.' + collName2];
      vs.trackVersionedCollections(vc1, vc2, { follow: false }, function(err) {
        if (err) { throw err; }
        vc2._snapshotCollection.find().toArray(function(err, items) {
          if (err) { throw err; }
          should.equal(items.length, 2);

          should.deepEqual(items[0], {
            _id: { _co: collName2, _id : 'baz', _v: 'A', _pe: databaseName, _pa: [] },
            _m3: { _ack: false }
          });
          should.deepEqual(items[1], {
            _id: { _co: collName2, _id : 'baz', _v: 'A', _pe: '_local', _pa: [], _i: 1 },
            _m3: { _ack: false }
          });
          done();
        });
      });

      // flush queues after tail is closed
      vs.on('trackVersionedCollectionsCloseTail', function() { vc2.processQueues(); });
    });

    it('needs another item that can be replicated in collName1', function(done) {
      var item = { _id: { _co: collName1, _id : 'baz', _v: 'B', _pe: '_local', _pa: ['A'], _lo: true, _i: 2 }, _m3: { _ack: false }, foo: 'bar' };
      db.collection('m3.' + collName1).insert(item, { w: 1 }, function(err, inserts) {
        if (err) { throw err; }
        should.equal(inserts.length, 1);
        done();
      });
    });

    it('should replicate new items in one collection to the other', function(done) {
      var opts = { remotes: [databaseName], debug: false };
      var vs = new VersionedSystem(db, [databaseName, databaseName2], [collName1, collName2], opts);
      var vc1 = vs._databaseCollections[databaseName + '.' + collName1];
      var vc2 = vs._databaseCollections[databaseName2 + '.' + collName2];

      vs.trackVersionedCollections(vc1, vc2, { follow: false, offset: 'A' }, function(err) {
        if (err) { throw err; }
        vc2._snapshotCollection.find().toArray(function(err, items) {
          if (err) { throw err; }
          should.equal(items.length, 4);

          should.deepEqual(items[2], {
            _id: { _co: collName2, _id : 'baz', _v: 'B', _pe: databaseName, _pa: ['A'] },
            _m3: { _ack: false },
            foo: 'bar'
          });
          should.deepEqual(items[3], {
            _id: { _co: collName2, _id : 'baz', _v: 'B', _pe: '_local', _pa: ['A'], _i: 2 },
            _m3: { _ack: false },
            foo: 'bar'
          });
          done();
        });
      });

      // flush queues after tail is closed
      vs.on('trackVersionedCollectionsCloseTail', function() { vc2.processQueues(); });
    });

    it('needs some other items that can be replicated in collName2', function(done) {
      var item1 = { _id: { _co: collName1, _id : 'foo', _v: 'O', _pe: '_local', _pa: [], _lo: true, _i: 3 },    _m3: { _ack: false }, foo: 'baz' };
      var item2 = { _id: { _co: collName1, _id : 'foo', _v: 'P', _pe: '_local', _pa: ['O'], _lo: true, _i: 4 }, _m3: { _ack: false }, foo: 'bar' };
      var item3 = { _id: { _co: collName1, _id : 'foo', _v: 'Q', _pe: '_local', _pa: ['P'], _lo: true, _i: 5 }, _m3: { _ack: false }, foo: 'bar' };
      var item4 = { _id: { _co: collName1, _id : 'foo', _v: 'R', _pe: '_local', _pa: ['Q'], _lo: true, _i: 6 }, _m3: { _ack: false }, foo: 'qux' };
      var items = [item1, item2, item3, item4];
      db.collection('m3.' + collName1).insert(items, { w: 1 }, function(err, inserts) {
        if (err) { throw err; }
        should.equal(inserts.length, items.length);
        done();
      });
    });

    it('should replicate one collection to the other and use filter', function(done) {
      var opts = { remotes: [databaseName] };
      var vs = new VersionedSystem(db, [databaseName, databaseName2], [collName1, collName2], opts);
      var vc1 = vs._databaseCollections[databaseName + '.' + collName1];
      var vc2 = vs._databaseCollections[databaseName2 + '.' + collName2];

      vs.trackVersionedCollections(vc1, vc2, { follow: false, filter: { foo: 'bar' }, offset: 'B' }, function(err) {
        if (err) { throw err; }
        vc2._snapshotCollection.find().toArray(function(err, items) {
          if (err) { throw err; }
          should.equal(items.length, 8);

          should.deepEqual(items[4], {
            _id: { _co: collName2, _id : 'foo', _v: 'P', _pe: databaseName, _pa: [] },
            _m3: { _ack: false },
            foo: 'bar'
          });
          should.deepEqual(items[5], {
            _id: { _co: collName2, _id : 'foo', _v: 'Q', _pe: databaseName, _pa: ['P'] },
            _m3: { _ack: false },
            foo: 'bar'
          });
          should.deepEqual(items[6], {
            _id: { _co: collName2, _id : 'foo', _v: 'P', _pe: '_local', _pa: [], _i: 3 },
            _m3: { _ack: false },
            foo: 'bar'
          });
          should.deepEqual(items[7], {
            _id: { _co: collName2, _id : 'foo', _v: 'Q', _pe: '_local', _pa: ['P'], _i: 4 },
            _m3: { _ack: false },
            foo: 'bar'
          });
          done();
        });
      });

      // flush queues after tail is closed
      vs.on('trackVersionedCollectionsCloseTail', function() { vc2.processQueues(); });
    });

    // use offset Q to reach R item
    it('should replicate one collection to the other and use import hooks', function(done) {
      var opts = { remotes: [databaseName], debug: false };
      var vs = new VersionedSystem(db, [databaseName, databaseName2], [collName1, collName2], opts);
      var vc1 = vs._databaseCollections[databaseName + '.' + collName1];
      var vc2 = vs._databaseCollections[databaseName2 + '.' + collName2];

      var importHooks = [
        function(db, item, opts, cb) {
          item.hookPassed = true;
          cb(null, item);
        },
        function(db, item, opts, cb) {
          item.secondHook = true;
          cb(null, item);
        }
      ];

      vs.trackVersionedCollections(vc1, vc2, { follow: false, importHooks: importHooks, offset: 'Q' }, function(err) {
        if (err) { throw err; }
        vc2._snapshotCollection.find().toArray(function(err, items) {
          if (err) { throw err; }
          should.equal(items.length, 10);

          should.deepEqual(items[8], {
            _id: { _co: collName2, _id : 'foo', _v: 'R', _pe: databaseName, _pa: ['Q'] },
            _m3: { _ack: false },
            foo: 'qux',
            hookPassed: true,
            secondHook: true
          });
          should.deepEqual(items[9], {
            _id: { _co: collName2, _id : 'foo', _v: 'R', _pe: '_local', _pa: ['Q'], _i: 5 },
            _m3: { _ack: false },
            foo: 'qux',
            hookPassed: true,
            secondHook: true
          });
          done();
        });
      });

      // flush queues after tail is closed
      vs.on('trackVersionedCollectionsCloseTail', function() { vc2.processQueues(); });
    });

    it('needs some other items that can be replicated in collName2', function(done) {
      var item1 = { _id: { _co: collName1, _id : 'foo', _v: 'S', _pe: '_local', _pa: ['R'], _lo: true, _i: 7 }, _m3: { _ack: false }, foo: 'baz' };
      var items = [item1];
      db.collection('m3.' + collName1).insert(items, { w: 1 }, function(err, inserts) {
        if (err) { throw err; }
        should.equal(inserts.length, items.length);
        done();
      });
    });

    // use offset R to reach S item
    it('should replicate one collection to the other and use export hooks', function(done) {
      var opts = { remotes: [databaseName] };
      var vs = new VersionedSystem(db, [databaseName, databaseName2], [collName1, collName2], opts);
      var vc1 = vs._databaseCollections[databaseName + '.' + collName1];
      var vc2 = vs._databaseCollections[databaseName2 + '.' + collName2];

      var exportHooks = [
        function(db, item, opts, cb) {
          item.hookPassed = true;
          cb(null, item);
        },
        function(db, item, opts, cb) {
          item.secondHook = true;
          cb(null, item);
        }
      ];

      vs.trackVersionedCollections(vc1, vc2, { follow: false, exportHooks: exportHooks, offset: 'R' }, function(err) {
        if (err) { throw err; }
        vc2._snapshotCollection.find().toArray(function(err, items) {
          if (err) { throw err; }
          should.equal(items.length, 12);

          should.deepEqual(items[10], {
            _id: { _co: collName2, _id : 'foo', _v: 'S', _pe: databaseName, _pa: ['R'] },
            _m3: { _ack: false },
            foo: 'baz',
            hookPassed: true,
            secondHook: true
          });
          should.deepEqual(items[11], {
            _id: { _co: collName2, _id : 'foo', _v: 'S', _pe: '_local', _pa: ['R'], _i: 6 },
            _m3: { _ack: false },
            foo: 'baz',
            hookPassed: true,
            secondHook: true
          });
          done();
        });
      });

      // flush queues after tail is closed
      vs.on('trackVersionedCollectionsCloseTail', function() { vc2.processQueues(); });
    });
  });

  describe('rebuild', function() {
    var collName = 'rebuild';

    it('should require cb to be a function', function() {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      (function() { vs.rebuild({}); }).should.throw('cb must be a function');
    });

    it('should require to not be auto processing', function() {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      vs._startAutoProcessing();
      (function() { vs.rebuild(function() {}); }).should.throw('stop auto processing before rebuilding');
    });

    it('rebuild an empty collection', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { debug: false });
      vs.rebuild(done);
    });

    it('needs an item in the collection for further testing', function(done) {
      var item = { _id:  'foo', _v: 'A' };
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { debug: false });
      var vc = vs._databaseCollections[databaseName + '.' + collName];
      vc._collection.insert(item, {w: 1}, done);
    });

    it('should rebuild', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { debug: false });
      vs.rebuild(done);
    });

    it('should have versioned and ackd the item in the collection', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      var vc = vs._databaseCollections[databaseName + '.' + collName];
      vc._snapshotCollection.find().toArray(function(err, items) {
        if (err) { throw err; }
        should.deepEqual(items, [{ _id:{ _co: 'rebuild', _id: 'foo', _v: 'A', _pe: '_local', _pa: [], _lo: true, _i: 1 }, _m3: { _ack: true }}]);
        done();
      });
    });

    it('should not have altered the version of the item in the collection', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      var vc = vs._databaseCollections[databaseName + '.' + collName];
      vc._collection.find().toArray(function(err, items) {
        if (err) { throw err; }
        should.deepEqual(items, [{ _id: 'foo', _v: 'A' }]);
        done();
      });
    });

    it('should have saved the last oplog item as the last used oplog item', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      vs._getLastUsedOplogItem(function(err, lastUsed) {
        if (err) { throw err; }
        delete lastUsed.ts;
        should.deepEqual(lastUsed, {
          op: 'u',
          ns: databaseName + '.rebuild',
          o2: { _id: 'foo' },
          o: { _id: 'foo', _v: 'A' },
          _id: 'lastUsedOplogItem'
        });
        done();
      });
    });

    it('should not have saved any last used collection oplog item, since there is none (at least not user initiated)', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { debug: false });
      vs._getLastUsedCollectionOplogItem(function(err, lastUsed) {
        if (err) { throw err; }
        should.deepEqual(lastUsed, null);
        done();
      });
    });
  });

  describe('_versionedCollectionCounts', function() {
    var collName  = '_versionedCollectionCounts';
    var collName2 = '_versionedCollectionCountsColl2';

    it('needs an item in collName2 for further testing', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName, collName2], {});
      vs._databaseCollections[databaseName+'.'+collName2]._snapshotCollection.insert({ foo: 'bar' }, function(err, result) {
        if (err) { throw err; }
        should.equal(result.length, 1);
        done();
      });
    });

    it('should only bootstrap the snapshot collection of the collection with one object', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName, oplogCollectionName: collName };
      var vs = new VersionedSystem(db, [databaseName], [collName, collName2], opts);
      vs._versionedCollectionCounts(function(err, result) {
        should.equal(err, null);
        should.deepEqual(result, {
          'test_versioned_system._versionedCollectionCounts': 0,
          'test_versioned_system._versionedCollectionCountsColl2': 1
        });
        done();
      });
    });
  });

  describe('_getOldestOplogItem', function() {
    var collName = '_getOldestOplogItem';
    var oplogCollectionName = '_getOldestOplogItemOplog';

    it('should not find an oplog item', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName, oplogCollectionName: oplogCollectionName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._getOldestOplogItem(function(err, item) {
        if (err) { throw err; }
        should.equal(item, null);
        done();
      });
    });

    it('should insert oplog item without timestamp and with timestamp', function(done) {
      var time = new Date();
      var item1 = { _id: 'A' };
      var item2 = { _id: 'B', ts: time };

      var opts = { localDbName: databaseName, localStorageName: collName, oplogCollectionName: oplogCollectionName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._oplogCollection.insert([item1, item2], {w: 1}, function(err, inserted) {
        if (err) { throw err; }
        should.equal(inserted.length, 2);
        done();
      });
    });

    it('should find the first oplog item with timestamp', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName, oplogCollectionName: oplogCollectionName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._getOldestOplogItem(function(err, item) {
        if (err) { throw err; }
        should.deepEqual(item._id, 'B');
        done();
      });
    });
  });

  describe('_getLatestOplogItem', function() {
    var collName = '_getLatestOplogItem';
    var oplogCollectionName = '_getLatestOplogItemOplog';

    it('should not find an oplog item', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName, oplogCollectionName: oplogCollectionName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._getLatestOplogItem(function(err, item) {
        if (err) { throw err; }
        should.equal(item, null);
        done();
      });
    });

    it('needs items in the oplog, with and without timestamp', function(done) {
      var time1 = new Date(1);
      var time2 = new Date(22);
      var item1 = { _id: 'A' };
      var item2 = { _id: 'B', ts: time1 };
      var item3 = { _id: 'C', ts: time2 };
      var item4 = { _id: 'D' };

      var opts = { localDbName: databaseName, localStorageName: collName, oplogCollectionName: oplogCollectionName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._oplogCollection.insert([item1, item2, item3, item4], {w: 1}, function(err, inserted) {
        if (err) { throw err; }
        should.equal(inserted.length, 4);
        done();
      });
    });

    it('should find the latest oplog item with timestamp', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName, oplogCollectionName: oplogCollectionName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._getLatestOplogItem(function(err, item) {
        if (err) { throw err; }
        should.deepEqual(item._id, 'C');
        done();
      });
    });
  });

  describe('_getLastUsedCollectionOplogItem', function() {
    var collName = '_getLastUsedCollectionOplogItem';

    it('should return null when no last used oplog item is saved', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._getLastUsedCollectionOplogItem(function(err, item) {
        if (err) { throw err; }
        should.equal(item, null);
        done();
      });
    });

    it('should insert last used oplog item', function(done) {
      var time = new Date();
      var item = { _id: 'lastUsedCollectionOplogItem', ts: time };

      var opts = { localDbName: databaseName, localStorageName: collName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._localStorageCollection.insert(item, {w: 1}, function(err, inserted) {
        if (err) { throw err; }
        should.equal(inserted.length, 1);
        done();
      });
    });

    it('should return the last saved oplog item', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._getLastUsedCollectionOplogItem(function(err, item) {
        if (err) { throw err; }
        should.equal(item._id, 'lastUsedCollectionOplogItem');
        done();
      });
    });
  });

  describe('_getLastUsedOplogItem', function() {
    var collName = '_getLastUsedOplogItem';

    it('should return null when no last used oplog item is saved', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName };
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], opts);
      vs._getLastUsedOplogItem(function(err, item) {
        if (err) { throw err; }
        should.equal(item, null);
        done();
      });
    });

    it('should insert last used oplog item', function(done) {
      var time = new Date();
      var item = { _id: 'lastUsedOplogItem', ts: time };

      var opts = { localDbName: databaseName, localStorageName: collName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._localStorageCollection.insert(item, {w: 1}, function(err, inserted) {
        if (err) { throw err; }
        should.equal(inserted.length, 1);
        done();
      });
    });

    it('should return the last saved oplog item', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._getLastUsedOplogItem(function(err, item) {
        if (err) { throw err; }
        should.equal(item._id, 'lastUsedOplogItem');
        done();
      });
    });
  });

  describe('_saveLastUsedCollectionOplogItem', function() {
    var collName = '_saveLastUsedCollectionOplogItem';

    it('should require item to be an object', function() {
      var opts = { localDbName: databaseName, localStorageName: collName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      (function() { vs._saveLastUsedCollectionOplogItem(); }).should.throw('item must be an object');
    });

    it('should require cb to be a function', function() {
      var opts = { localDbName: databaseName, localStorageName: collName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      (function() { vs._saveLastUsedCollectionOplogItem({}); }).should.throw('cb must be a function');
    });

    it('should require item.ts to be a mongodb.Timestamp', function() {
      var opts = { localDbName: databaseName, localStorageName: collName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      (function() { vs._saveLastUsedCollectionOplogItem({}, function() {}); }).should.throw('item.ts must be a mongodb.Timestamp');
    });

    it('should save this._lastUsedOplogItem and call back', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName };
      var item = { _id: 'lastUsedCollectionOplogItem', ts: new Timestamp(1, 12323) };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._saveLastUsedCollectionOplogItem(item, done);
    });

    it('should overwrite the _id', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName };
      var time = new Timestamp(1, 123);
      var item = { _id: 'someTest', ts: time };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._saveLastUsedCollectionOplogItem(item, function(err) {
        if (err) { throw err; }
        should.deepEqual(item, { _id: 'lastUsedCollectionOplogItem', ts: time });
        done();
      });
    });
  });

  describe('_saveLastUsedOplogItem', function() {
    var collName = '_saveLastUsedOplogItem';

    it('should require item to be an object', function() {
      var opts = { localDbName: databaseName, localStorageName: collName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      (function() { vs._saveLastUsedOplogItem(); }).should.throw('item must be an object');
    });

    it('should require cb to be a function', function() {
      var opts = { localDbName: databaseName, localStorageName: collName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      (function() { vs._saveLastUsedOplogItem({}); }).should.throw('cb must be a function');
    });

    it('should require item.ts to be a mongodb.Timestamp', function() {
      var opts = { localDbName: databaseName, localStorageName: collName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      (function() { vs._saveLastUsedOplogItem({}, function() {}); }).should.throw('item.ts must be a mongodb.Timestamp');
    });

    it('should save this._lastUsedOplogItem and call back', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName };
      var item = { _id: 'lastUsedOplogItem', ts: new Timestamp(1, 12323) };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._saveLastUsedOplogItem(item, done);
    });

    it('should overwrite the _id', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName };
      var time = new Timestamp(1, 123);
      var item = { _id: 'someTest', ts: time };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._saveLastUsedOplogItem(item, function(err) {
        if (err) { throw err; }
        should.deepEqual(item, { _id: 'lastUsedOplogItem', ts: time });
        done();
      });
    });
  });

  describe('info', function() {
    var collName  = 'info';

    it('should create a collection with one object and a collection without objects', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      vs._databaseCollections[databaseName+'.'+collName]._collection.insert({ foo: 'bar' }, function(err, result) {
        if (err) { throw err; }
        should.equal(result.length, 1);
        done();
      });
    });

    it('should show info of the collection and the snapshot collection', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      vs.info(function(err, result) {
        if (err) { throw err; }
        should.strictEqual(result[databaseName+'.'+collName].collection.count, 1);
        should.strictEqual(result[databaseName+'.'+collName].collection.capped, undefined);
        should.strictEqual(result[databaseName+'.'+collName].snapshotCollection.count, undefined);
        should.strictEqual(result[databaseName+'.'+collName].snapshotCollection.capped, undefined);
        done();
      });
    });

    it('should show extended info of the collection and the snapshot collection', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      vs.info(true, function(err, result) {
        should.equal(err, null);
        should.strictEqual(result[databaseName+'.'+collName].collection.count, 1);
        should.strictEqual(result[databaseName+'.'+collName].collection.capped, undefined);
        should.strictEqual(result[databaseName+'.'+collName].snapshotCollection.count, undefined);
        should.strictEqual(result[databaseName+'.'+collName].snapshotCollection.capped, undefined);
        should.strictEqual(result[databaseName+'.'+collName].extended.ack, 0);
        done();
      });
    });
  });

  /////
  // dump() is tested in a separate file since it's very slow because of database creation and deletion

  describe('_ensureSnapshotCollections', function() {
    var collName = '_ensureSnapshotCollections';

    it('should default to using the snapshotSize option', function(done) {
      var opts = { snapshotSize: 0.01171875, localDbName: databaseName, localStorageName: collName, oplogCollectionName: collName };
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], opts);
      vs.ensureSnapshotCollections(function(err) {
        if (err) { throw err; }
        vs.info(function(err, result) {
          should.equal(err, null);
          should.strictEqual(result[databaseName+'.'+collName].snapshotCollection.count, 0);
          should.strictEqual(result[databaseName+'.'+collName].snapshotCollection.capped, true);
          should.strictEqual(result[databaseName+'.'+collName].snapshotCollection.storageSize, 12288);
          done();
        });
      });
    });

    it('should overrule the default snapshotSize with the provided snapshotSizes', function(done) {
      var collectionName = '_ensureSnapshotCollectionsSnapshotSizes';
      var snapshotSizes = {};
      snapshotSizes[databaseName+'.'+collectionName] = 0.00390625;
      var opts = { snapshotSize: 0.00193023681641, snapshotSizes: snapshotSizes };
      var vs = new VersionedSystem(oplogDb, [databaseName], [collectionName], opts);
      vs.ensureSnapshotCollections(function(err) {
        if (err) { throw err; }
        vs.info(function(err, result) {
          should.equal(err, null);
          should.strictEqual(result[databaseName+'.'+collectionName].snapshotCollection.count, 0);
          should.strictEqual(result[databaseName+'.'+collectionName].snapshotCollection.capped, true);
          should.strictEqual(result[databaseName+'.'+collectionName].snapshotCollection.storageSize, 4096);
          done();
        });
      });
    });
  });

  describe('lastUsedOplogInOplog', function() {
    var collName = 'lastUsedOplogInOplog';

    it('should require an item in the oplog', function(done) {
      var opts = { hide: true, localDbName: databaseName, localStorageName: collName, oplogCollectionName: collName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs.lastUsedOplogInOplog(function(err, item) {
        should.equal(err.message, 'first oplog item not found');
        should.equal(item, undefined);
        done();
      });
    });

    it('needs an oplog item for further tests', function(done) {
      var time = new Timestamp(1, 123);
      var item = { _id: 'A', ts: time };

      var opts = { localDbName: databaseName, localStorageName: collName, oplogCollectionName: collName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._oplogCollection.insert(item, {w: 1}, function(err, inserted) {
        if (err) { throw err; }
        should.equal(inserted.length, 1);
        done();
      });
    });

    it('should return true when no last used oplog item found', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName, oplogCollectionName: collName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs.lastUsedOplogInOplog(function(err, item) {
        if (err) { throw err; }
        should.strictEqual(item, true);
        done();
      });
    });

    it('should call back with with false when last used timestamp <= lowest in oplog', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName, oplogCollectionName: collName };
      var item = { ts: new Timestamp(1, 1) };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._saveLastUsedOplogItem(item, function(err) {
        if (err) { throw err; }
        vs.lastUsedOplogInOplog(function(err, onTrack) {
          should.equal(onTrack, false);
          done();
        });
      });
    });

    it('should call back with with true when last used timestamp === lowest in oplog', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName, oplogCollectionName: collName };
      var item = { ts: new Timestamp(1, 123) };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._saveLastUsedOplogItem(item, function(err) {
        if (err) { throw err; }
        vs.lastUsedOplogInOplog(function(err, onTrack) {
          should.equal(onTrack, true);
          done();
        });
      });
    });

    it('should call back with with true when last used timestamp > lowest in oplog', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName, oplogCollectionName: collName };
      var item = { ts: new Timestamp(1, 124) };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._saveLastUsedOplogItem(item, function(err) {
        if (err) { throw err; }
        vs.lastUsedOplogInOplog(function(err, onTrack) {
          should.equal(onTrack, true);
          done();
        });
      });
    });

    it('should call back with with true when last used timestamp > lowest in oplog (increment in same timestamp)', function(done) {
      var opts = { localDbName: databaseName, localStorageName: collName, oplogCollectionName: collName };
      var item = { ts: new Timestamp(2, 123) };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._saveLastUsedOplogItem(item, function(err) {
        if (err) { throw err; }
        vs.lastUsedOplogInOplog(function(err, onTrack) {
          should.equal(onTrack, true);
          done();
        });
      });
    });
  });

  // see https://github.com/mongodb/node-mongodb-native/issues/1046
  describe('_resolveSnapshotToOplog', function() {
    var collName = '_resolveSnapshotToOplog';
    var lastOffset;

    it('should require callback to be a function', function() {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      var vc = vs._databaseCollections[databaseName + '.' + collName];
      (function() { vs._resolveSnapshotToOplog(null, vc); }).should.throw('cb must be a function');
    });

    it('should require items in the snapshot', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { hide: true });
      var vc = vs._databaseCollections[databaseName + '.' + collName];
      var offset = new Timestamp(0, (new Date()).getTime() / 1000);
      vs._resolveSnapshotToOplog(offset, vc, function(err) {
        should.equal(err.message, 'no modifications on snapshot found');
        done();
      });
    });

    // do a rebuild
    it('needs an item to be versioned for further testing', function(done) {
      var item = { _id: 'quux', _v: 'X' };
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { debug: false });
      var vc = vs._databaseCollections[databaseName + '.' + collName];
      vc._collection.insert(item, {w: 1}, function(err) {
        if (err) { throw err; }
        vs.rebuild(done);
      });
    });

    // do a second rebuild
    it('needs a second rebuild for further testing', function(done) {
      lastOffset = new Timestamp((0, new Date()).getTime() / 1000);
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      vs.rebuild(done);
    });

    it('should return the first oplog item which is a snapshot insert since a rebuild is system initiated', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { debug: false });
      var vc = vs._databaseCollections[databaseName + '.' + collName];
      vs._resolveSnapshotToOplog(vc, function(err, oplogItem) {
        if (err) { throw err; }
        var ts = oplogItem.ts;
        should.strictEqual(lastOffset.lessThan(ts), true);
        should.deepEqual(oplogItem, {
          ts: ts,
          op: 'i',
          ns: databaseName + '.m3._resolveSnapshotToOplog',
          o: { _id: { _co: collName, _id: 'quux', _v: 'X', _pe: '_local', _pa: [], _lo: true, _i: 1 }, _m3: { _ack: false } }
        });
        done();
      });
    });
  });

  // see https://github.com/mongodb/node-mongodb-native/issues/1046
  describe('_resolveSnapshotOffsets', function() {
    var collName = '_resolveSnapshotOffsets';
    var collName2 = '_resolveSnapshotOffsets2';
    var collName3 = '_resolveSnapshotOffsets3';
    var lastOffset;

    it('should require cb to be a function', function() {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName, collName2]);
      (function() { vs._resolveSnapshotOffsets(); }).should.throw('cb must be a function');
    });

    it('should default to null if versioned collections are empty', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName, collName2]);
      vs._resolveSnapshotOffsets(function(err, offsets) {
        if (err) { throw err; }
        should.deepEqual(offsets, {
          'test_versioned_system._resolveSnapshotOffsets': null,
          'test_versioned_system._resolveSnapshotOffsets2': null
        });
        done();
      });
    });

    it('needs some ackd items in two versioned collections for further tests', function(done) {
      lastOffset = new Timestamp(0, (new Date()).getTime() / 1000);

      var doc  = { _id: 'foo', _v: 'A' };
      var doc2 = { _id: 'bar', _v: 'B' };

      var vs = new VersionedSystem(oplogDb, [databaseName], [collName, collName2], { debug: false });
      var vc1 = vs._databaseCollections[databaseName + '.' + collName];
      var vc2 = vs._databaseCollections[databaseName + '.' + collName2];
      vc1._collection.insert(doc, {w: 1}, function(err, inserted) {
        if (err) { throw err; }
        should.equal(inserted.length, 1);
        vc2._collection.insert(doc2, {w: 1}, function(err, inserted) {
          if (err) { throw err; }
          should.equal(inserted.length, 1);
          vs.rebuild(done);
        });
      });
    });

    it('should find both oplog items since both correspond with an ackd item in each versioned collection', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName, collName2], { debug: false });
      vs._resolveSnapshotOffsets(function(err, offsets) {
        if (err) { throw err; }
        should.strictEqual(offsets[databaseName + '._resolveSnapshotOffsets'].greaterThan(lastOffset), true);
        should.strictEqual(offsets[databaseName + '._resolveSnapshotOffsets2'].greaterThan(lastOffset), true);
        done();
      });
    });

    it('needs a non-ackd item in a non-empty versioned collection for further tests', function(done) {
      lastOffset = new Timestamp(0, (new Date()).getTime() / 1000);

      var doc  = { _id: { _co: collName3, _id: 'foo', _v: 'C', _pe: '_local', _pa: [], _lo: true, _i: 3 } };

      var vs = new VersionedSystem(oplogDb, [databaseName], [collName3]);
      var vc = vs._databaseCollections[databaseName + '.' + collName3];
      vc._snapshotCollection.insert(doc, {w: 1}, function(err, inserted) {
        if (err) { throw err; }
        should.equal(inserted.length, 1);
        done();
      });
    });

    // oplog resolver dependent
    it('should crash on inserts that are not inserted via saveRemoteItem or saveOplogItem', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName, collName2, collName3], { hide: true });
      vs._resolveSnapshotOffsets(function(err) {
        should.equal(err.message, 'event si inappropriate in current state SnapshotInsert');
        done();
      });
    });

    it('needs to have a predefined timestamp', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      vs._localStorageCollection.insert({ _id: databaseName + '._resolveSnapshotOffsets', ts: new Timestamp(456789, 123) }, done);
    });

    it('should use the predefined ts', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { debug: false });
      vs._resolveSnapshotOffsets(function(err, offsets) {
        if (err) { throw err; }
        should.strictEqual(offsets[databaseName + '._resolveSnapshotOffsets'].equals(new Timestamp(456789, 123)), true);
        done();
      });
    });
  });

  describe('_determineOplogOffsets', function() {
    var collName = '_determineOplogOffsets';

    it('should require cb to be a function', function() {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      (function() { vs._determineOplogOffsets({}); }).should.throw('cb must be a function');
    });

    it('should callback with no offset for the versioned collection (since it\'s empty)', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      vs._determineOplogOffsets(function(err, offsets) {
        if (err) { throw err; }
        should.strictEqual(offsets[databaseName + '._determineOplogOffsets'], null);
        done();
      });
    });

    it('should callback with the last used oplog item for the local.main.$oplog collection if all vc\'s are null', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      vs._getLastUsedOplogItem(function(err, lastUsed) {
        if (err) { throw err; }
        vs._determineOplogOffsets(function(err, offsets) {
          if (err) { throw err; }
          should.deepEqual(offsets['local.oplog.$main'].equals(lastUsed.ts), true);
          done();
        });
      });
    });

    it('needs to have no last used oplog item', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      vs._localStorageCollection.remove({ _id: 'lastUsedOplogItem' }, done);
    });

    it('should callback with Timestamp 0, 0 for local.main.$oplog, since vc\'s and last used are null', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      vs._determineOplogOffsets(function(err, offsets) {
        if (err) { throw err; }
        should.deepEqual(offsets['local.oplog.$main'].equals(new Timestamp(0, 0)), true);
        done();
      });
    });

    it('needs to have an offset for the versioned collection, insert and version an item', function(done) {
      var ns = databaseName + '.' + collName;
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { debug: false });
      var vc = vs._databaseCollections[ns];
      var item = { ts: new Timestamp(123, 456), op: 'i', ns: ns, o: { _id: 'foo', foo: 'bar', _v: 'abc' } };
      vc.saveOplogItem(item, function() {});
      vc.processQueues(done);
    });

    it('should callback with the provided minimum if it\'s higher than the last used oplog item', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { debug: false });
      vs._determineOplogOffsets(function(err, offsets) {
        if (err) { throw err; }
        should.deepEqual(offsets['local.oplog.$main'].equals(new Timestamp(0, 0)), false);
        should.deepEqual(offsets['local.oplog.$main'].equals(offsets[databaseName + '._determineOplogOffsets']), true);
        done();
      });
    });
  });

  describe('_tailOplog', function() {
    var collName = '_tailOplog';
    var oplogCollectionName = '_tailOplogOplog';

    var ns = databaseName + '.' + collName;
    var ts1 = new Timestamp(0, new Date() / 1000 + 1000);
    var ts2 = new Timestamp(1, new Date() / 1000 + 2000);
    var ts3 = new Timestamp(2, new Date() / 1000 + 3000);
    var ts4 = new Timestamp(3, new Date() / 1000 + 4000);
    var fixtures = [
      { 'ts' : ts1, 'op' : 'i', 'ns' : ns, 'o' : { '_id' : 'foo', 'foo' : 'bar', '_v' : 'abc', } },
      { 'ts' : ts2, 'op' : 'u', 'ns' : ns, 'o2' : { '_id' : 'foo' }, 'o' : { 'myVer' : 3, '_v' : 'abc' } },
      { 'ts' : ts3, 'op' : 'u', 'ns' : ns, 'o2' : { '_id' : 'foo' }, 'o' : { '_v' : 'abc', 'mySecondAttr' : 0 } },
      { 'ts' : ts4, 'op' : 'd', 'ns' : ns, 'b' : true, 'o' : { '_id' : 'foo' } }
    ];

    it('should require ts to be a mongodb.Timestamp', function() {
      var opts = { oplogCollectionName: oplogCollectionName, debug: false };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      (function() { vs._tailOplog({}); }).should.throw('ts must be a mongodb.Timestamp');
    });

    it('should require tailable to be a boolean', function() {
      var opts = { oplogCollectionName: oplogCollectionName, debug: false };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      (function() { vs._tailOplog(new Timestamp(), null, {}); }).should.throw('tailable must be a boolean');
    });

    it('should require tailableRetryInterval to be a number', function() {
      var opts = { oplogCollectionName: oplogCollectionName, debug: false };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      (function() { vs._tailOplog(new Timestamp(), true, {}); }).should.throw('tailableRetryInterval must be a number');
    });

    it('should require cb to be a function', function() {
      var opts = { oplogCollectionName: oplogCollectionName, debug: false };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      (function() { vs._tailOplog(new Timestamp(), true, 0, {}); }).should.throw('cb must be a function');
    });

    it('should err if oplog collection is not capped', function(done) {
      var opts = { debug: false, hide: true, localDbName: databaseName, localStorageName: collName, oplogCollectionName: oplogCollectionName };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      vs._tailOplog(new Timestamp(0, 0), true, function(err) {
        should.equal(err.message, 'tailable cursor requested on non capped collection');
        done();
      });
    });

    it('should close if tailable is false', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { debug: false });
      var lastOffset = new Timestamp(0, (new Date()).getTime() / 1000 + 1);
      vs._tailOplog(lastOffset, false, function(err, item, vc, next) {
        if (err) { throw err; }
        if (!item) { return done(); }
        next();
      });
    });

    it('needs oplog fixtures for future tests', function(done) {
      database.createCappedColl(db, oplogCollectionName, function(err) {
        if (err) { throw err; }
        db.collection(oplogCollectionName).insert(fixtures, { w: 1 }, function(err, inserted) {
          if (err) { throw err; }
          inserted.length.should.equal(fixtures.length);
          done();
        });
      });
    });

    it('should call once with every item and a final call without an item', function(done) {
      var opts = {
        oplogCollectionName: oplogCollectionName,
        debug: false
      };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      var i = 0;
      vs._tailOplog(new Timestamp(0, 0), false, function(err, item, vc, next) {
        if (err) { throw err; }
        if (!item) {
          should.equal(i, fixtures.length);
          done();
          return;
        }
        i++;
        next();
      });
    });

    it('should callback with all items after the given timestamp', function(done) {
      var opts = {
        oplogCollectionName: oplogCollectionName,
        debug: false
      };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      var i = 0;

      // start after the first item in the oplog
      vs._tailOplog(fixtures[0].ts, false, function(err, item, vc, next) {
        if (err) { throw err; }
        if (!item) {
          should.equal(i, fixtures.length - 1);
          done();
          return;
        }
        i++;
        next();
      });
    });

    it('should return the cursor stream', function() {
      var opts = {
        oplogCollectionName: oplogCollectionName,
        debug: false
      };
      var vs = new VersionedSystem(db, [databaseName], [collName], opts);
      var tail = vs._tailOplog(new Timestamp(10, 10), true, function() {});
      // should return cursor stream
      should.equal(tail._cursor.tailable, true);
    });
  });

  // see https://github.com/mongodb/node-mongodb-native/issues/1046
  describe('trackOplog', function() {
    var collName = 'trackOplog';

    it('should require follow to be a boolean', function() {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      (function() { vs.trackOplog({}); }).should.throw('follow must be a boolean');
    });

    it('should require cb to be a function', function() {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      (function() { vs.trackOplog(false, {}); }).should.throw('cb must be a function');
    });

    it('should run', function(done) {
      this.timeout(3000);
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { debug: false });
      vs.trackOplog(false, done);
    });

    it('should track the collection insert if it is inserted into the oplog after trackOplog is started', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { debug: false });
      var vc = vs._databaseCollections[databaseName + '.' + collName];

      var item = { _id: 'bar', _v: 'cde', foo: 'bar' };

      vs.trackOplog(done);

      // insert extra item in collection after trackOplog is started
      vs.on('trackOplogStartTail', function() {
        vc._collection.insert(item, { w: 1 }, function(err) {
          if (err) { throw err; }
        });
      });

      vs.on('trackOplog', function(item, vc) {
        vc.processQueues(function(err) {
          if (err) { throw err; }

          vs.stopTrack(function(err) {
            if (err) { throw err; }
            vc._snapshotCollection.find().toArray(function(err, items) {
              if (err) { throw err; }
              should.strictEqual(items.length, 1);

              should.deepEqual(items[0], {
                _id: { _co: 'trackOplog', _id: 'bar', _v: 'cde', _pe: '_local', _pa: [], _lo: true, _i: 1 },
                _m3: { _ack: false },
                foo: 'bar'
              });
            });
          });
        });
      });
    });

    it('needs a rebuild', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { debug: false });
      vs.rebuild(done);
    });

    it('should run', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { debug: false });
      vs.trackOplog(false, done);
    });
  });

  describe('stopTrack', function() {
    var collName = 'stopTrack';

    it('should require cb to be a function', function() {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], {});
      (function() { vs.stopTrack({}); }).should.throw('cb must be a function');
    });

    it('should callback', function(done) {
      var vs = new VersionedSystem(oplogDb, [databaseName], [collName], { debug: false });
      vs.stopTrack(done);
    });
  });
});
