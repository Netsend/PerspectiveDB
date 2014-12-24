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

/*jshint -W068, -W030 */

// this test needs to use the real oplog, serviced by mongo. please run a separate mongod instance for this.
// the config below uses a mongo on localhost port 27019

var should = require('should');

var fetchItems = require('../../_fetch_items');

var rebase = require('../../../lib/rebase');
var VersionedSystem = require('../../../lib/versioned_system');

var db, db2, oplogDb, tmpColl;
var databaseName = 'test_rebase';
var databaseName2 = 'test_rebase2';
var oplogDatabase = 'local';

var vsConfig = {
  'databases': [databaseName, databaseName2],
  'collections': ['qux', 'quux'],
  'snapshotSizes': {
    'test_rebase.foo': 0.125,
    'test_rebase.qux': 0.125,
    'test_rebase.quux': 0.125,
    'test_rebase.basicRebase': 0.125,
    'test_rebase2.foo': 0.125,
    'test_rebase2.qux': 0.125,
    'test_rebase2.quux': 0.125,
    'test_rebase2.basicRebase': 0.125
  }
};

var Database = require('../../_database');

// open database connection
var database = new Database(vsConfig.databases);
before(function(done) {
  database.connect(function(err, dbs) {
    if (err) { throw err; }
    db = dbs[0];
    db2 = dbs[1];
    oplogDb = db.db(oplogDatabase);
    tmpColl = db.collection('fubar.tmp');
    done(err);
  });
});

after(function(done) {
  var vs = new VersionedSystem(oplogDb, [databaseName], ['foo']);
  vs._localStorageCollection.drop(function(err) {
    if (err && err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) { throw err; }
    database.disconnect(done);
  });
});

describe('rebase', function() {
  describe('constructor', function() {
    var replicate = {
      'test_rebase': {
        'to': {
          'test_rebase2': {
            'quux': true,
            'qux': true
          }
        }
      },
      'test_rebase2': {
        'from': {
          'test_rebase': {
            'quux': true,
            'qux': true
          }
        }
      }
    };

    it('should require vs to be a VersionedSystem', function() {
      (function() { rebase(); }).should.throw('vsExample must be a VersionedSystem');
    });

    it('should require vc to be a VersionedCollection', function() {
      var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes, replicate: replicate };
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);
      (function() { rebase(vs); }).should.throw('vc must be a VersionedCollection');
    });

    it('should require tmpColl to be a mongodb.Collection', function() {
      var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes, replicate: replicate };
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);
      var vc = vs._databaseCollections['test_rebase2.qux'];
      (function() { rebase(vs, vc); }).should.throw('tmpColl must be a mongodb.Collection');
    });

    it('should require cb to be a function (without opts)', function() {
      var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes, replicate: replicate };
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);
      var vc = vs._databaseCollections['test_rebase2.qux'];
      (function() { rebase(vs, vc, tmpColl); }).should.throw('cb must be a function');
    });

    it('should require cb to be a function (with opts)', function() {
      var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes, replicate: replicate };
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);
      var vc = vs._databaseCollections['test_rebase2.qux'];
      (function() { rebase(vs, vc, tmpColl, {}); }).should.throw('cb must be a function');
    });

    it('should require opts to be an object', function() {
      var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes, replicate: replicate };
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);
      var vc = vs._databaseCollections['test_rebase2.qux'];
      (function() { rebase(vs, vc, tmpColl, 'foo', function() {}); }).should.throw('opts must be an object');
    });

    it('should return without error', function(done) {
      this.timeout(5000);
      var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes, replicate: replicate };
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);
      var vc = vs._databaseCollections['test_rebase2.qux'];
      rebase(vs, vc, tmpColl, { debug: opts.debug }, done);
    });
  });

  describe('single item', function() {
    var replicate = {
      'test_rebase': {
        'to': {
          'test_rebase2': {
            'basicRebase': true
          }
        }
      },
      'test_rebase2': {
        'from': {
          'test_rebase': {
            'basicRebase': true
          }
        }
      }
    };

    it('should run with empty collection', function(done) {
      this.timeout(5000);
      var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes, replicate: {} };
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, ['basicRebase'], opts);
      vs.start(false, done);
    });

    it('needs objects in both collections', function(done) {
      // don't use replication config yet, since replicating vc1 to vc2 would lead to "no lca found"
      var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes, replicate: {}, autoProcessInterval: 10, tailableRetryInterval: 10 };
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, ['basicRebase'], opts);
      var vc1 = vs._databaseCollections['test_rebase.basicRebase'];
      var vc2 = vs._databaseCollections['test_rebase2.basicRebase'];

      vs.start(done);
      vs.on('trackOplog', function(item) {
        // done if update of B
        if (item.op === 'u' && item.o._v === 'B') {
          vs.stopTrack();
        }
      });

      var itemTest1 = { _id: 'foo', _v: 'A', bar: 'A' };
      var itemTest2 = { _id: 'foo', _v: 'B', baz: 'B' };

      vs.on('trackOplogStartTail', function() {
        vc1._collection.insert(itemTest1, { w: 1 }, function(err) {
          if (err) { throw err; }
          vc2._collection.insert(itemTest2, function(err) {
            if (err) { throw err; }
          });
        });
      });
    });

    it('inspect previous insertion, check if versioned collection contains all items', function(done) {
      var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes, replicate: {} };
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, ['basicRebase'], opts);
      var vc1 = vs._databaseCollections['test_rebase.basicRebase'];
      var vc2 = vs._databaseCollections['test_rebase2.basicRebase'];

      fetchItems(vc1, vc2, function(err, result) {
        if (err) { throw err; }

        should.equal(result['test_rebase.basicRebase'].items.length, 1);
        should.deepEqual(result['test_rebase.basicRebase'].items[0], { _id: 'foo', _v: 'A', bar: 'A' });
        should.equal(result['test_rebase.basicRebase'].m3items.length, 1);
        should.deepEqual(result['test_rebase.basicRebase'].m3items[0], {
          _id: { _co: 'basicRebase', _id: 'foo', _v: 'A', _pe: '_local', _pa: [], _lo: true, _i: 1 },
          _m3: { _ack: true },
          bar: 'A'
        });

        should.equal(result['test_rebase2.basicRebase'].items.length, 1);
        should.deepEqual(result['test_rebase2.basicRebase'].items[0], { _id: 'foo', _v: 'B', baz: 'B' });
        should.equal(result['test_rebase2.basicRebase'].m3items.length, 1);
        should.deepEqual(result['test_rebase2.basicRebase'].m3items[0], {
          _id: { _co: 'basicRebase', _id: 'foo', _v: 'B', _pe: '_local', _pa: [], _lo: true, _i: 1 },
          _m3: { _ack: true },
          baz: 'B'
        });
        done();
      });
    });

    it('should work', function(done) {
      this.timeout(5000);
      var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes, replicate: replicate };
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, ['basicRebase'], opts);
      var vc1 = vs._databaseCollections['test_rebase.basicRebase'];
      var vc2 = vs._databaseCollections['test_rebase2.basicRebase'];

      rebase(vs, vc2, tmpColl, { debug: opts.debug }, function(err) {
        if (err) { throw err; }

        fetchItems(vc1, vc2, function(err, result) {
          if (err) { throw err; }

          should.equal(result['test_rebase.basicRebase'].items.length, 1);
          should.deepEqual(result['test_rebase.basicRebase'].items[0], { _id: 'foo', _v: 'A', bar: 'A' });
          should.equal(result['test_rebase.basicRebase'].m3items.length, 1);
          should.deepEqual(result['test_rebase.basicRebase'].m3items[0], {
            _id: { _co: 'basicRebase', _id: 'foo', _v: 'A', _pe: '_local', _pa: [], _lo: true, _i: 1 },
            _m3: { _ack: true },
            bar: 'A'
          });

          should.equal(result['test_rebase2.basicRebase'].items.length, 1);

          var newVersion = result['test_rebase2.basicRebase'].items[0]._v;
          newVersion.should.match(/^[a-z0-9/+A-Z]{8}$/);
          should.deepEqual(result['test_rebase2.basicRebase'].items[0], { _id: 'foo', _v: newVersion, baz: 'B' });

          var m3items = result['test_rebase2.basicRebase'].m3items;
          should.equal(m3items.length, 3);
          should.deepEqual(m3items[0], {
            _id: { _co: 'basicRebase', _id: 'foo', _v: 'A', _pe: 'test_rebase', _pa: [] },
            _m3: { _ack: false },
            bar: 'A'
          });
          should.deepEqual(m3items[1], {
            _id: { _co: 'basicRebase', _id: 'foo', _v: 'A', _pe: '_local', _pa: [], _i: 1 },
            _m3: { _ack: true },
            bar: 'A'
          });
          should.deepEqual(m3items[2], {
            _id: { _co: 'basicRebase', _id: 'foo', _v: newVersion, _pe: '_local', _pa: ['A'], _lo: true, _i: 2 },
            _m3: { _ack: false },
            baz: 'B'
          });

          done();
        });
      });
    });
  });

  describe('multiple versions', function() {
    var replicate = {
      'test_rebase': {
        'to': {
          'test_rebase2': {
            'basicRebase': true
          }
        }
      },
      'test_rebase2': {
        'from': {
          'test_rebase': {
            'basicRebase': true
          }
        }
      }
    };

    it('needs to drop previousy created collections', function(done) {
      var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes, replicate: {} };
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, ['basicRebase'], opts);
      var vc1 = vs._databaseCollections['test_rebase.basicRebase'];
      var vc2 = vs._databaseCollections['test_rebase2.basicRebase'];

      vc1._collection.drop(function(err) {
        if (err && err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) { throw err; }
        vc2._collection.drop(function(err) {
          if (err && err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) { throw err; }
          vc1._snapshotCollection.drop(function(err) {
            if (err && err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) { throw err; }
            vc2._snapshotCollection.drop(function(err) {
              if (err && err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) { throw err; }
              done();
            });
          });
        });
      });
    });

    it('needs objects in both collections', function(done) {
      this.timeout(5000);
      var itemI1  = { _id: 'foo', _v: 'A', I: true };
      var itemI2  = { _id: 'bar', _v: 'B', I: true };
      var itemII1 = { _id: 'foo', _v: 'C', II: true };
      var itemII2 = { _id: 'baz', _v: 'D', II: true };

      // don't use replication config yet, since replicating vc1 to vc2 would lead to "no lca found"
      var opts = { hide: true, snapshotSizes: vsConfig.snapshotSizes, replicate: {} };
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, ['basicRebase'], opts);
      var vc1 = vs._databaseCollections['test_rebase.basicRebase'];
      var vc2 = vs._databaseCollections['test_rebase2.basicRebase'];

      vs.start(done);
      vs.on('trackOplog', function(item, v) {
        v.processQueues();

        // done if update of D
        if (item.op === 'u' && item.o._v === 'D') {
          vs.stopTrack();
        }
      });

      vs.on('trackOplogStartTail', function() {
        vc1._collection.insert([itemI1, itemI2], { w: 1 }, function(err) {
          if (err) { throw err; }
          vc2._collection.insert([itemII1, itemII2], function(err) {
            if (err) { throw err; }
          });
        });
      });
    });

    it('inspect previous insertion, check if versioned collection is created', function(done) {
      var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes, replicate: {} };
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, ['basicRebase'], opts);
      var vc1 = vs._databaseCollections['test_rebase.basicRebase'];
      var vc2 = vs._databaseCollections['test_rebase2.basicRebase'];

      fetchItems(vc1, vc2, function(err, result) {
        if (err) { throw err; }

        should.equal(result['test_rebase.basicRebase'].items.length, 2);
        should.deepEqual(result['test_rebase.basicRebase'].items[0], { _id: 'foo', _v: 'A', I: true });
        should.deepEqual(result['test_rebase.basicRebase'].items[1], { _id: 'bar', _v: 'B', I: true });

        should.equal(result['test_rebase.basicRebase'].m3items.length, 2);
        should.deepEqual(result['test_rebase.basicRebase'].m3items[0], {
          _id: { _co: 'basicRebase', _id: 'foo', _v: 'A', _pe: '_local', _pa: [], _lo: true, _i: 1 },
          _m3: { _ack: true },
          I: true
        });
        should.deepEqual(result['test_rebase.basicRebase'].m3items[1], {
          _id: { _co: 'basicRebase', _id: 'bar', _v: 'B', _pe: '_local', _pa: [], _lo: true, _i: 2 },
          _m3: { _ack: true },
          I: true
        });

        should.equal(result['test_rebase2.basicRebase'].items.length, 2);
        should.deepEqual(result['test_rebase2.basicRebase'].items[0], { _id: 'foo', _v: 'C', II: true });
        should.deepEqual(result['test_rebase2.basicRebase'].items[1], { _id: 'baz', _v: 'D', II: true });

        should.equal(result['test_rebase2.basicRebase'].m3items.length, 2);
        should.deepEqual(result['test_rebase2.basicRebase'].m3items[0], {
          _id: { _co: 'basicRebase', _id: 'foo', _v: 'C', _pe: '_local', _pa: [], _lo: true, _i: 1 },
          _m3: { _ack: true },
          II: true
        });
        should.deepEqual(result['test_rebase2.basicRebase'].m3items[1], {
          _id: { _co: 'basicRebase', _id: 'baz', _v: 'D', _pe: '_local', _pa: [], _lo: true, _i: 2 },
          _m3: { _ack: true },
          II: true
        });
        done();
      });
    });

    it('should work', function(done) {
      this.timeout(5000);
      var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes, replicate: replicate };
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, ['basicRebase'], opts);
      var vc1 = vs._databaseCollections['test_rebase.basicRebase'];
      var vc2 = vs._databaseCollections['test_rebase2.basicRebase'];

      rebase(vs, vc2, tmpColl, { debug: opts.debug }, function(err) {
        if (err) { throw err; }

        fetchItems(vc1, vc2, function(err, result) {
          if (err) { throw err; }

          should.equal(result['test_rebase.basicRebase'].items.length, 2);
          should.deepEqual(result['test_rebase.basicRebase'].items[0], { _id: 'foo', _v: 'A', I: true });
          should.deepEqual(result['test_rebase.basicRebase'].items[1], { _id: 'bar', _v: 'B', I: true });

          should.equal(result['test_rebase.basicRebase'].m3items.length, 2);
          should.deepEqual(result['test_rebase.basicRebase'].m3items[0], {
            _id: { _co: 'basicRebase', _id: 'foo', _v: 'A', _pe: '_local', _pa: [], _lo: true, _i: 1 },
            _m3: { _ack: true },
            I: true
          });
          should.deepEqual(result['test_rebase.basicRebase'].m3items[1], {
            _id: { _co: 'basicRebase', _id: 'bar', _v: 'B', _pe: '_local', _pa: [], _lo: true, _i: 2 },
            _m3: { _ack: true },
            I: true
          });

          should.equal(result['test_rebase2.basicRebase'].items.length, 3);
          should.deepEqual(result['test_rebase2.basicRebase'].items[0], { _id: 'bar', _v: 'B', I: true });
          should.deepEqual(result['test_rebase2.basicRebase'].items[1], { _id: 'baz', _v: 'D', II: true });

          var newVersion = result['test_rebase2.basicRebase'].items[2]._v;
          newVersion.should.match(/^[a-z0-9/+A-Z]{8}$/);
          should.deepEqual(result['test_rebase2.basicRebase'].items[2], { _id: 'foo', _v: newVersion, II: true });

          var m3items = result['test_rebase2.basicRebase'].m3items;
          should.equal(m3items.length, 6);
          should.deepEqual(m3items[0], {
            _id: { _co: 'basicRebase', _id: 'foo', _v: 'A', _pe: 'test_rebase', _pa: [] },
            _m3: { _ack: false },
            I: true
          });
          should.deepEqual(m3items[1], {
            _id: { _co: 'basicRebase', _id: 'bar', _v: 'B', _pe: 'test_rebase', _pa: [] },
            _m3: { _ack: false },
            I: true
          });
          should.deepEqual(m3items[2], {
            _id: { _co: 'basicRebase', _id: 'foo', _v: 'A', _pe: '_local', _pa: [], _i: 1 },
            _m3: { _ack: true },
            I: true
          });
          should.deepEqual(m3items[3], {
            _id: { _co: 'basicRebase', _id: 'bar', _v: 'B', _pe: '_local', _pa: [], _i: 2 },
            _m3: { _ack: true },
            I: true
          });
          should.deepEqual(m3items[4], {
            _id: { _co: 'basicRebase', _id: 'foo', _v: newVersion, _pe: '_local', _pa: ['A'], _lo: true, _i: 3 },
            _m3: { _ack: false },
            II: true
          });
          should.deepEqual(m3items[5], {
            _id: { _co: 'basicRebase', _id: 'baz', _v: 'D', _pe: '_local', _pa: [], _lo: true, _i: 4 },
            _m3: { _ack: false },
            II: true
          });

          done();
        });
      });
    });
  });

  describe('multiple items and versions', function() {
    var replicate = {
      'test_rebase': {
        'to': {
          'test_rebase2': {
            'basicRebase': true
          }
        }
      },
      'test_rebase2': {
        'from': {
          'test_rebase': {
            'basicRebase': true
          }
        }
      }
    };

    it('needs to drop previousy created collections', function(done) {
      var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes, replicate: {} };
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, ['basicRebase'], opts);
      var vc1 = vs._databaseCollections['test_rebase.basicRebase'];
      var vc2 = vs._databaseCollections['test_rebase2.basicRebase'];

      vc1._collection.drop(function(err) {
        if (err && err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) { throw err; }
        vc2._collection.drop(function(err) {
          if (err && err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) { throw err; }
          vc1._snapshotCollection.drop(function(err) {
            if (err && err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) { throw err; }
            vc2._snapshotCollection.drop(function(err) {
              if (err && err.message !== 'ns not found' && !/^Collection .* not found/.test(err.message)) { throw err; }
              done();
            });
          });
        });
      });
    });

    it('needs objects in both collections', function(done) {
      this.timeout(5000);
      var itemI1  = { _id: 'foo', _v: 'A', I: true };
      var itemI2  = { _id: 'bar', _v: 'B', I: true };
      var updateDoc  = { $set: { qux: 'quux' } };
      var itemII1 = { _id: 'foo', _v: 'C', II: true };
      var itemII2 = { _id: 'baz', _v: 'D', II: true };

      // don't use replication config yet, since replicating vc1 to vc2 would lead to "no lca found"
      var opts = { hide: true, snapshotSizes: vsConfig.snapshotSizes, replicate: {} };
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, ['basicRebase'], opts);
      var vc1 = vs._databaseCollections['test_rebase.basicRebase'];
      var vc2 = vs._databaseCollections['test_rebase2.basicRebase'];

      vs.start(done);
      vs.on('trackOplog', function(item, v) {
        v.processQueues();

        // done if update of "E" with new version
        if (item.op === 'u' && item.o._v && item.o._v.length === 8) {
          vs.stopTrack();
        }
      });

      vs.on('trackOplogStartTail', function() {
        vc1._collection.insert([itemI1, itemI2], { w: 1 }, function(err) {
          if (err) { throw err; }
          vc1._collection.update({ _id: 'bar' }, updateDoc, { w: 1 }, function(err, updates) {
            if (err) { throw err; }
            if (updates !== 1) { throw new Error('not updated'); }
            vc2._collection.insert([itemII1, itemII2], function(err) {
              if (err) { throw err; }
            });
          });
        });
      });
    });

    it('inspect previous insertion, check if versioned collection is created', function(done) {
      var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes, replicate: {} };
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, ['basicRebase'], opts);
      var vc1 = vs._databaseCollections['test_rebase.basicRebase'];
      var vc2 = vs._databaseCollections['test_rebase2.basicRebase'];

      fetchItems(vc1, vc2, function(err, result) {
        if (err) { throw err; }

        should.equal(result['test_rebase.basicRebase'].items.length, 2);
        var v = result['test_rebase.basicRebase'].items[1]._v;
        v.should.match(/.{8}/);
        should.deepEqual(result['test_rebase.basicRebase'].items[0], { _id: 'foo', _v: 'A', I: true });
        should.deepEqual(result['test_rebase.basicRebase'].items[1], { _id: 'bar', _v: v, I: true, qux: 'quux' });

        should.equal(result['test_rebase.basicRebase'].m3items.length, 3);
        should.deepEqual(result['test_rebase.basicRebase'].m3items[0], {
          _id: { _co: 'basicRebase', _id: 'foo', _v: 'A', _pe: '_local', _pa: [], _lo: true, _i: 1 },
          _m3: { _ack: true },
          I: true
        });
        should.deepEqual(result['test_rebase.basicRebase'].m3items[1], {
          _id: { _co: 'basicRebase', _id: 'bar', _v: 'B', _pe: '_local', _pa: [], _lo: true, _i: 2 },
          _m3: { _ack: true },
          I: true
        });
        should.deepEqual(result['test_rebase.basicRebase'].m3items[2], {
          _id: { _co: 'basicRebase', _id: 'bar', _v: v, _pe: '_local', _pa: ['B'], _lo: true, _i: 3 },
          _m3: { _ack: true },
          I: true,
          qux: 'quux'
        });

        should.equal(result['test_rebase2.basicRebase'].items.length, 2);
        should.deepEqual(result['test_rebase2.basicRebase'].items[0], { _id: 'foo', _v: 'C', II: true });
        should.deepEqual(result['test_rebase2.basicRebase'].items[1], { _id: 'baz', _v: 'D', II: true });

        should.equal(result['test_rebase2.basicRebase'].m3items.length, 2);
        should.deepEqual(result['test_rebase2.basicRebase'].m3items[0], {
          _id: { _co: 'basicRebase', _id: 'foo', _v: 'C', _pe: '_local', _pa: [], _lo: true, _i: 1 },
          _m3: { _ack: true },
          II: true
        });
        should.deepEqual(result['test_rebase2.basicRebase'].m3items[1], {
          _id: { _co: 'basicRebase', _id: 'baz', _v: 'D', _pe: '_local', _pa: [], _lo: true, _i: 2 },
          _m3: { _ack: true },
          II: true
        });
        done();
      });
    });

    it('should work', function(done) {
      this.timeout(5000);
      var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes, replicate: replicate };
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, ['basicRebase'], opts);
      var vc1 = vs._databaseCollections['test_rebase.basicRebase'];
      var vc2 = vs._databaseCollections['test_rebase2.basicRebase'];

      rebase(vs, vc2, tmpColl, { debug: opts.debug }, function(err) {
        if (err) { throw err; }

        fetchItems(vc1, vc2, function(err, result) {
          if (err) { throw err; }

          var v = result['test_rebase.basicRebase'].items[1]._v;
          v.should.match(/.{8}/);
          should.deepEqual(result['test_rebase.basicRebase'].items[0], { _id: 'foo', _v: 'A', I: true });
          should.deepEqual(result['test_rebase.basicRebase'].items[1], { _id: 'bar', _v: v, I: true, qux: 'quux' });

          should.equal(result['test_rebase.basicRebase'].m3items.length, 3);
          should.deepEqual(result['test_rebase.basicRebase'].m3items[0], {
            _id: { _co: 'basicRebase', _id: 'foo', _v: 'A', _pe: '_local', _pa: [], _lo: true, _i: 1 },
            _m3: { _ack: true },
            I: true
          });
          should.deepEqual(result['test_rebase.basicRebase'].m3items[1], {
            _id: { _co: 'basicRebase', _id: 'bar', _v: 'B', _pe: '_local', _pa: [], _lo: true, _i: 2 },
            _m3: { _ack: true },
            I: true
          });
          should.deepEqual(result['test_rebase.basicRebase'].m3items[2], {
            _id: { _co: 'basicRebase', _id: 'bar', _v: v, _pe: '_local', _pa: ['B'], _lo: true, _i: 3 },
            _m3: { _ack: true },
            I: true,
            qux: 'quux'
          });

          should.equal(result['test_rebase2.basicRebase'].items.length, 3);
          should.deepEqual(result['test_rebase2.basicRebase'].items[0], { _id: 'bar', _v: v, I: true, qux: 'quux' });
          should.deepEqual(result['test_rebase2.basicRebase'].items[1], { _id: 'baz', _v: 'D', II: true });

          var newVersion = result['test_rebase2.basicRebase'].items[2]._v;
          newVersion.should.match(/^[a-z0-9/+A-Z]{8}$/);
          should.deepEqual(result['test_rebase2.basicRebase'].items[2], { _id: 'foo', _v: newVersion, II: true });

          var m3items = result['test_rebase2.basicRebase'].m3items;
          should.equal(m3items.length, 8);
          should.deepEqual(m3items[0], {
            _id: { _co: 'basicRebase', _id: 'foo', _v: 'A', _pe: 'test_rebase', _pa: [] },
            _m3: { _ack: false },
            I: true
          });
          should.deepEqual(m3items[1], {
            _id: { _co: 'basicRebase', _id: 'bar', _v: 'B', _pe: 'test_rebase', _pa: [] },
            _m3: { _ack: false },
            I: true
          });
          should.deepEqual(m3items[2], {
            _id: { _co: 'basicRebase', _id: 'bar', _v: v, _pe: 'test_rebase', _pa: ['B'] },
            _m3: { _ack: false },
            I: true,
            qux: 'quux'
          });

          should.deepEqual(m3items[3], {
            _id: { _co: 'basicRebase', _id: 'foo', _v: 'A', _pe: '_local', _pa: [], _i: 1 },
            _m3: { _ack: true },
            I: true
          });
          should.deepEqual(m3items[4], {
            _id: { _co: 'basicRebase', _id: 'bar', _v: 'B', _pe: '_local', _pa: [], _i: 2 },
            _m3: { _ack: false },
            I: true
          });
          should.deepEqual(m3items[5], {
            _id: { _co: 'basicRebase', _id: 'bar', _v: v, _pe: '_local', _pa: ['B'], _i: 3 },
            _m3: { _ack: true },
            I: true,
            qux: 'quux'
          });

          should.deepEqual(m3items[6], {
            _id: { _co: 'basicRebase', _id: 'foo', _v: newVersion, _pe: '_local', _pa: ['A'], _lo: true, _i: 4 },
            _m3: { _ack: false },
            II: true
          });
          should.deepEqual(m3items[7], {
            _id: { _co: 'basicRebase', _id: 'baz', _v: 'D', _pe: '_local', _pa: [], _lo: true, _i: 5 },
            _m3: { _ack: false },
            II: true
          });

          done();
        });
      });
    });
  });
});
