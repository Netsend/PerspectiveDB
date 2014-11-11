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

    done();
  });
});

after(database.disconnect.bind(database));

describe('VersionedSystem', function() {
  describe('constructor', function() {
    it('should require oplogDb to be a mongodb.Db', function() {
      (function() { new VersionedSystem({}); }).should.throw('oplogDb must be a mongdb.Db');
    });

    it('should require opts', function() {
      (function() { new VersionedSystem(oplogDb, 1); }).should.throw('opts must be an object');
    });

    it('should construct', function() {
      (function() { new VersionedSystem(oplogDb); }).should.not.throw();
    });
  });

   /**
    * initVCs
    * sendPR
    * chroot
    * createServer
    * _trackOplogVc
    * _verifyAuthRequest
    */

  describe('initVCs non-root', function() {
    var collName = 'initVCs';

    it('should require vcs to be an object', function() {
      var vs = new VersionedSystem(oplogDb);
      (function() { vs.initVCs(1); }).should.throw('vcs must be an object');
    });

    it('should require cb to be a function', function() {
      var vs = new VersionedSystem(oplogDb);
      (function() { vs.initVCs({}); }).should.throw('cb must be a function');
    });

    it('should callback', function(done) {
      var vs = new VersionedSystem(oplogDb);
      vs.initVCs({}, done);
    });

    it('should fail if not running as root', function(done) {
      var vcCfg = {
        someDb: {
          someColl: { }
        }
      };
      var vs = new VersionedSystem(oplogDb, { hide: true });
      vs.initVCs(vcCfg, function(err) {
        should.strictEqual(err.message, 'abnormal termination');
        done();
      });
    });
  });

  xdescribe('info', function() {
    var collName  = 'info';

    it('should create a collection with one object and a collection without objects', function(done) {
      db.collection('m3.' + collName).insert({ foo: 'bar' }, function(err, result) {
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

  xdescribe('_ensureSnapshotCollections', function() {
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
