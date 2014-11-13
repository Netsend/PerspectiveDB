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

/*jshint -W068 */

if (process.getuid() !== 0) { 
  console.error('run tests as root');
  process.exit(1);
}

var fs = require('fs');

var should = require('should');
var mongodb = require('mongodb');
var Timestamp = mongodb.Timestamp;

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
  describe('initVCs root', function() {
    it('should call back with all oplog readers', function(done) {
      var vcCfg = {
        someDb: {
          someColl: {
            size: 10
          }
        }
      };
      var vs = new VersionedSystem(oplogColl, { debug: false });
      vs.initVCs(vcCfg, function(err, oplogReaders) {
        if (err) { throw err; }
        should.strictEqual(Object.keys(oplogReaders).length, 1);
        oplogReaders['someDb.someColl'].on('end', done);
      });
    });
  });

  describe('info', function() {
    var collName  = 'info';
    var oplogCollName = '_infoOplog';
    var localOplogColl;

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

    it('needs a collection with one object and a collection without objects', function(done) {
      var item = { foo: 'bar' };
      var snapshotItem = { foo: 'bar', _m3: { _op: new Timestamp(8000, 9000) } };
      var cfg = {};
      cfg[databaseName] = {};
      cfg[databaseName][collName] = { size: 1 };

      var vs = new VersionedSystem(localOplogColl);
      vs.initVCs(cfg, function(err) {
        if (err) { throw err; }
        db.collection('m3.' + collName).insert(snapshotItem, function(err) {
          if (err) { throw err; }
          db.collection(collName).insert(item, done);
        });
      });
    });

    it('should show info of the collection and the snapshot collection', function(done) {
      var ns = databaseName + '.' + collName;
      var cfg = {};
      cfg[databaseName] = {};
      cfg[databaseName][collName] = { size: 1 };

      var vs = new VersionedSystem(localOplogColl, { hide: true });
      vs.initVCs(cfg, function(err) {
        if (err) { throw err; }
        vs.info(function(err, result) {
          if (err) { throw err; }
          should.strictEqual(result[ns].collection.count, 1);
          should.strictEqual(result[ns].collection.capped, undefined);
          should.strictEqual(result[ns].snapshotCollection.count, 1);
          should.strictEqual(result[ns].snapshotCollection.capped, true);
          done();
        });
      });
    });

    it('should show extended info of the collection and the snapshot collection', function(done) {
      var ns = databaseName + '.' + collName;
      var cfg = {};
      cfg[databaseName] = {};
      cfg[databaseName][collName] = { size: 1 };

      var vs = new VersionedSystem(localOplogColl, { hide: true });
      vs.initVCs(cfg, function(err) {
        if (err) { throw err; }
        vs.info(true, function(err, result) {
          should.equal(err, null);
          should.strictEqual(result[ns].collection.count, 1);
          should.strictEqual(result[ns].collection.capped, undefined);
          should.strictEqual(result[ns].snapshotCollection.count, 1);
          should.strictEqual(result[ns].snapshotCollection.capped, true);
          should.strictEqual(result[ns].extended.ack, 0);
          done();
        });
      });
    });
  });

  // do these tests at last
  describe('chroot', function() {
    it('should require user to be a string', function() {
      var vs = new VersionedSystem(oplogColl);
      (function() { vs.chroot(); }).should.throw('user must be a string');
    });

    it('should require opts to be an object', function() {
      var vs = new VersionedSystem(oplogColl);
      (function() { vs.chroot('foo', 1); }).should.throw('opts must be an object');
    });

    it('should chroot', function() {
      var vs = new VersionedSystem(oplogColl);

      var ls = fs.readdirSync('/');
      should.strictEqual(true, ls.length > 4);

      vs.chroot('nobody');

      ls = fs.readdirSync('/');
      should.strictEqual(0, ls.length);
      should.strictEqual(true, process.getuid() > 0);
    });
  });
});
