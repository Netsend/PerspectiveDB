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
});
