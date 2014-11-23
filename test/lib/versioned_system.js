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

var db, db2, oplogDb, oplogColl;
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
    oplogColl = oplogDb.collection('oplog.$main');
    done();
  });
});

after(database.disconnect.bind(database));

describe('VersionedSystem', function() {
  describe('constructor', function() {
    it('should require oplogColl to be a mongodb.Collection', function() {
      (function() { new VersionedSystem({}); }).should.throw('oplogColl must be a mongdb.Collection');
    });

    it('should require opts to be an object', function() {
      (function() { new VersionedSystem(oplogColl, 1); }).should.throw('opts must be an object');
    });

    it('should construct without options', function() {
      (function() { new VersionedSystem(oplogColl); }).should.not.throw();
    });

    it('should construct with options', function() {
      (function() { new VersionedSystem(oplogColl, { hide: true }); }).should.not.throw();
    });
  });

  /**
   * initVCs
   * sendPR
   * chroot
   * listen
   * _ensureSnapshotAndOplogOffset
   * _verifyAuthRequest
   */

  describe('initVCs non-root', function() {
    it('should require vcs to be an object', function() {
      var vs = new VersionedSystem(oplogColl);
      (function() { vs.initVCs(1); }).should.throw('vcs must be an object');
    });

    it('should require cb to be a function', function() {
      var vs = new VersionedSystem(oplogColl);
      (function() { vs.initVCs({}); }).should.throw('cb must be a function');
    });

    it('should callback', function(done) {
      var vs = new VersionedSystem(oplogColl);
      vs.initVCs({}, done);
    });

    it('should fail if no size is specified', function(done) {
      var vcCfg = {
        someDb: {
          someColl: { }
        }
      };
      var vs = new VersionedSystem(oplogColl, { hide: true });
      vs.initVCs(vcCfg, function(err) {
        should.strictEqual(err.message, 'missing size');
        done();
      });
    });

    it('should fail if not running as root', function(done) {
      var vcCfg = {
        someDb: {
          someColl: {
            size: 10
          }
        }
      };
      var vs = new VersionedSystem(oplogColl, { hide: true });
      vs.initVCs(vcCfg, function(err) {
        should.strictEqual(err.message, 'abnormal termination');
        done();
      });
    });


    /////
    // See versioned_system_root for further testing
    ///
  });

  /*
  describe('info', function() {
    /////
    // See versioned_system_root for further testing
    ///
  });
  */

  describe('_ensureSnapshotAndOplogOffset', function() {
    var oplogCollName = '_ensureSnapshotAndOplogOffsetOplog';
    var localOplogColl;

    it('should require cfg to be an object', function() {
      var vs = new VersionedSystem(oplogColl);
      var cfg = 1;
      (function() { vs._ensureSnapshotAndOplogOffset(cfg); }).should.throw('cfg must be an object');
    });

    it('should require cb to be a function', function() {
      var vs = new VersionedSystem(oplogColl);
      var cfg = {};
      (function() { vs._ensureSnapshotAndOplogOffset(cfg, {}); }).should.throw('cb must be a function');
    });

    it('should require cfg.dbName to be a string', function() {
      var vs = new VersionedSystem(oplogColl);
      var cfg = {};
      (function() { vs._ensureSnapshotAndOplogOffset(cfg, function() {}); }).should.throw('cfg.dbName must be a string');
    });

    it('should require cfg.collectionName to be a string', function() {
      var vs = new VersionedSystem(oplogColl);
      var cfg = { dbName: 'foo' };
      (function() { vs._ensureSnapshotAndOplogOffset(cfg, function() {}); }).should.throw('cfg.collectionName must be a string');
    });

    it('should require cfg.size to be a number', function() {
      var vs = new VersionedSystem(oplogColl);
      var cfg = { dbName: 'foo', collectionName: 'bar' };
      (function() { vs._ensureSnapshotAndOplogOffset(cfg, function() {}); }).should.throw('cfg.size must be a number');
    });

    it('needs a capped collection', function(done) {
      database.createCappedColl(oplogCollName, done);
    });

    it('needs an artificial oplog for testing', function(done) {
      var op1 = { ts: new Timestamp(2, 3) };
      var op2 = { ts: new Timestamp(40, 50) };
      var op3 = { ts: new Timestamp(600, 700) };

      localOplogColl = db.collection(oplogCollName);
      localOplogColl.insert([op1, op2, op3], done);
    });

    it('should create snapshot and callback with max oplog item (since snapshot was empty)', function(done) {
      var vs = new VersionedSystem(localOplogColl);
      var cfg = { dbName: 'foo', collectionName: 'bar', size: 2 };
      vs._ensureSnapshotAndOplogOffset(cfg, function(err, oplogOffset) {
        if (err) { throw err; }
        should.strictEqual(true, oplogOffset.equals(new Timestamp(600, 700)));

        db.db('foo').collectionsInfo().toArray(function(err, info) {
          if (err) { throw err; }
          should.strictEqual(info[3].name, 'foo.m3.bar');
          should.strictEqual(info[3].options.size, 2);
          done();
        });
      });
    });

    it('needs an item in the versioned collection without oplog pointer', function(done) {
      var item = { _id: { _id: 'foo' }, _m3: { _ack: true } };
      db.db('foo').collection('m3.bar').insert(item, done);
    });

    it('should require an oplog pointer in the versioned collection if it has items', function(done) {
      var vs = new VersionedSystem(localOplogColl, { hide: true });
      var cfg = { dbName: 'foo', collectionName: 'bar', size: 2 };
      vs._ensureSnapshotAndOplogOffset(cfg, function(err) {
        should.strictEqual(err.message, 'vc contains snapshots but no oplog pointer');
        done();
      });
    });

    it('needs an oplog pointer in the versioned collection thats precedes the oplog', function(done) {
      var op = { _m3: { _op: new Timestamp(1, 2) } };
      db.db('foo').collection('m3.bar').insert(op, done);
    });

    it('should callback with oplog pointer based on previous insert', function(done) {
      var vs = new VersionedSystem(localOplogColl, { hide: true });
      var cfg = { dbName: 'foo', collectionName: 'bar', size: 2 };
      vs._ensureSnapshotAndOplogOffset(cfg, function(err, oplogOffset) {
        if (err) { throw err; }
        should.strictEqual(true, oplogOffset.equals(new Timestamp(1, 2)));
        done();
      });
    });

    it('needs an oplog pointer in the versioned collection thats in the oplog range', function(done) {
      var op = { _m3: { _op: new Timestamp(12, 34) } };

      db.db('foo').collection('m3.bar').insert(op, done);
    });

    it('should callback with oplog pointer based on previous insert', function(done) {
      var vs = new VersionedSystem(localOplogColl);
      var cfg = { dbName: 'foo', collectionName: 'bar', size: 2 };
      vs._ensureSnapshotAndOplogOffset(cfg, function(err, oplogOffset) {
        if (err) { throw err; }
        should.strictEqual(true, oplogOffset.equals(new Timestamp(12, 34)));
        done();
      });
    });
  });
});
