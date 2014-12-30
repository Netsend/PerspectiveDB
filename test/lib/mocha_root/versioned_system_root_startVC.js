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

var should = require('should');

var VersionedSystem = require('../../../lib/versioned_system');
var OplogReader = require('../../../lib/oplog_reader');

var db, db2, oplogDb, oplogColl;
var databaseName = 'test_versioned_system_startVC';
var databaseName2 = 'test2_startVC';
var oplogDatabase = 'local';

var databaseNames = [databaseName, databaseName2, 'foo', 'bar'];
var Database = require('../../_database');

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
  describe('_startVC root', function() {
    it('should require config to be an object', function() {
      var vs = new VersionedSystem(oplogColl);
      (function() { vs._startVC(); }).should.throw('config must be an object');
    });

    it('should require cb to be an object', function() {
      var vs = new VersionedSystem(oplogColl);
      (function() { vs._startVC({}); }).should.throw('cb must be a function');
    });

    it('should require config.dbName to be a string', function() {
      var vs = new VersionedSystem(oplogColl);
      (function() { vs._startVC({}, function() {}); }).should.throw('config.dbName must be a string');
    });

    it('should require config.collectionName to be a string', function() {
      var vs = new VersionedSystem(oplogColl);
      (function() { vs._startVC({ dbName: 'foo' }, function() {}); }).should.throw('config.collectionName must be a string');
    });

    it('should require config.size to be a number', function() {
      var vs = new VersionedSystem(oplogColl);
      var config = {
        dbName: 'foo',
        collectionName: 'bar'
      };
      (function() { vs._startVC(config, function() {}); }).should.throw('config.size must be a number');
    });

    it('should require config.size to be larger than 0', function() {
      var vs = new VersionedSystem(oplogColl);
      var config = {
        dbName: 'foo',
        collectionName: 'bar',
        size: 0
      };
      (function() { vs._startVC(config, function() {}); }).should.throw('config.size must be larger than 0');
    });

    it('should convert the config.size from MB to B', function(done) {
      var vs = new VersionedSystem(oplogColl);
      var config = {
        dbName: 'foo',
        collectionName: 'bar',
        size: 1
      };
      vs._startVC(config, function(err) {
        if (err) { throw err; }
        should.strictEqual(config.size, 1048576);
        done();
      });
    });

    it('should rebuild and callback with a vce and oplog reader', function(done) {
      var vs = new VersionedSystem(oplogColl, { debug: false });
      var config= {
        dbName: 'test2_startVC',
        collectionName: 'bar',
        dbPort: 27019,
        size: 1
      };
      vs._startVC(config, function(err, vc, or) {
        if (err) { throw err; }
        should.strictEqual(vc.pid !== process.pid, true);
        should.strictEqual(vc.pid > 1, true);
        should.strictEqual(or instanceof OplogReader, true);
        done();
      });
    });

    it('needs two documents in collection for further testing', function(done) {
      // insert two new documents in the collection and see if they're versioned using a
      // rebuild (one of which is already versioned). if a new snapshot collection is
      // created, rebuild should maintain original versions.
      var docs = [{ foo: 'a' }, { bar: 'b', _v: 'qux' }];
      db2.collection('someColl').insert(docs, done);
    });

    it('should rebuild a new snapshot collection', function(done) {
      var config= {
        dbName: 'test2_startVC',
        collectionName: 'someColl',
        dbPort: 27019,
        debug: false,
        tailable: true,
        autoProcessInterval: 50,
        size: 1
      };
      var vs = new VersionedSystem(oplogColl, { debug: false });
      vs._startVC(config, function(err, vc, or) {
        if (err) { throw err; }

        // oplog reader should never end
        or.on('end', function() { throw new Error('oplog reader closed'); });

        // should detect two updates in test2_startVC.someColl and set ackd
        var i = 0;
        or.on('data', function() {
          i++;
          if (i >= 2) {
            // check if items are ackd, but give vc some time to process oplog items first
            setTimeout(function() {
              db2.collection('m3.someColl').find().toArray(function(err, items) {
                if (err) { throw err; }
                should.strictEqual(items.length, 2);
                delete items[0]._id._id;
                delete items[0]._id._v;
                delete items[0]._m3._op;
                delete items[1]._id._id;
                delete items[1]._m3._op;

                should.deepEqual(items[0], {
                  foo: 'a',
                    _id: {
                      _co: 'someColl',
                      _pe: '_local',
                      _pa: [],
                      _lo: true,
                      _i: 1
                    },
                 _m3: { _ack: true }
                });
                should.deepEqual(items[1], {
                  bar: 'b',
                  _id: {
                    _co: 'someColl',
                    _v: 'qux',
                    _pe: '_local',
                    _pa: [],
                    _lo: true,
                    _i: 2
                  },
                _m3: { _ack: true }
                });
                done();
              });
            }, 60);
          }
        });
      });
    });
  });
});
