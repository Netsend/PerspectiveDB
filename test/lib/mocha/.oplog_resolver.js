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

// this test needs to use the real oplog, serviced by mongo. please run a separate mongod instance for this.
// the config below uses a mongo on localhost port 27019

var should = require('should');
var mongodb = require('mongodb');
var Timestamp = mongodb.Timestamp;

var OplogResolver = require('../../../lib/oplog_resolver');
var VersionedSystem = require('../../../lib/versioned_system');
var VersionedCollection = require('../../../lib/versioned_collection');

var vsConfig = {
  oplogDatabase: 'local',
  databases: ['test_oplog_resolver'],
  oplogCollectionName: 'oplog.$main',
  collections: ['foo', 'qux' ],
  snapshotSizes: {
    'test_oplog_resolver.foo': 0.125,
    'test_oplog_resolver.qux': 0.125,
  }
};

var vc, oplogDb, oplogCollection;

var db;
var databaseName = 'test_oplog_resolver';
var Database = require('../../_database');

// open database connection
var database = new Database(databaseName);
before(function(done) {
  database.connect(function(err, dbc) {
    if (err) { throw err; }
    db = dbc;
    vc = new VersionedCollection(db, 'foo', { debug: false, remotes: ['foo'] });
    oplogDb = db.db(vsConfig.oplogDatabase);
    oplogCollection = oplogDb.collection(vsConfig.oplogCollectionName);
    done(err);
  });
});

after(database.disconnect.bind(database));

// find last timestamp in oplog
function determineOffset(cb) {
  // find last item in oplog and increment low bits by one
  oplogCollection.findOne({}, { sort: { '$natural': -1 } }, function(err, item) {
    if (err) { return cb(err); }
    cb(err, new Timestamp(item.ts.getLowBits() + 1, item.ts.getHighBits()));
  });
}

describe('OplogResolver', function() {
  describe('constructor', function() {
    it('should require oplogCollection to be a mongdb.Collection', function() {
      (function() { new OplogResolver(); }).should.throw('oplogCollection must be a mongdb.Collection');
    });

    it('should require vc to be a VersionedCollection', function() {
      (function() { new OplogResolver(oplogCollection); }).should.throw('vc must be a VersionedCollection');
    });

    it('should require options to be an object', function() {
      (function() { new OplogResolver(oplogCollection, vc); }).should.throw('options must be an object');
    });

    it('should require cb to be a function', function() {
      (function() { new OplogResolver(oplogCollection, vc, {}); }).should.throw('cb must be a function');
    });

    it('should construct', function() {
      new OplogResolver(oplogCollection, vc, function() {});
    });

    it('should proceed from start to 43', function(done) {
      var fsm = [
        { type: 'insert', name: 'i', from: 'none', to: '43' }
      ];

      var fsmCbs = {
        oni: function(ev, from, to, msg) {
          should.strictEqual(msg, 'test_oplog_resolver');
          should.strictEqual(or._fsm.current, '43');
          done();
        }
      };

      var or = new OplogResolver(oplogCollection, vc, { fsm: fsm, fsmCbs: fsmCbs }, function(err) {
        if (err) { throw err; }
      });
      or._fsm.i('test_oplog_resolver');
    });

    it('should init fsm on S', function() {
      var or = new OplogResolver(oplogCollection, vc, { debug: false }, function() {});
      process.nextTick(function() {
        should.strictEqual(or._fsm.current, 'S');
      });
    });

    it('should have a fsm.si method (and crash on not having found an oplog item)', function(done) {
      var or = new OplogResolver(oplogCollection, vc, { hide: true }, function(err) {
        should.strictEqual(err.message, 'not found');
        done();
      });
      or._fsm.si();
    });
  });

  describe('_lastSnapshotCreation', function() {
    it('should require cb to be a function', function() {
      var or = new OplogResolver(oplogCollection, vc, function() {});
      (function() { or._lastSnapshotCreation(new Timestamp(0, 0)); }).should.throw('cb must be a function');
    });

    it('should run', function(done) {
      var or = new OplogResolver(oplogCollection, vc, {}, function() {});
      or._lastSnapshotCreation(function(err) {
        if (err) { throw err; }
        done();
      });
    });

    it('needs a rebuild for a clean start', function(done) {
      var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, {});
      vs.rebuild(done);
    });

    it('should return the snapshot creation item', function(done) {
      var or = new OplogResolver(oplogCollection, vc, {}, function() {});
      or._lastSnapshotCreation(function(err, item) {
        if (err) { throw err; }
        should.strictEqual(item && item.ts.greaterThan(new Timestamp(0, 0)), true);
        done();
      });
    });
  });

  describe('_lastSnapshotMod', function() {
    it('should require offset to be a mongdb.Timestamp', function() {
      var or = new OplogResolver(oplogCollection, vc, function() {});
      (function() { or._lastSnapshotMod({}); }).should.throw('offset must be a mongdb.Timestamp');
    });

    it('should require cb to be a function', function() {
      var or = new OplogResolver(oplogCollection, vc, function() {});
      (function() { or._lastSnapshotMod(new Timestamp(0, 0)); }).should.throw('cb must be a function');
    });

    it('should run', function(done) {
      var or = new OplogResolver(oplogCollection, vc, {}, function() {});
      or._lastSnapshotMod(function(err) {
        if (err) { throw err; }
        done();
      });
    });

    it('should not find any item if offset is now', function(done) {
      var or = new OplogResolver(oplogCollection, vc, {}, function() {});
      // trick with high lower bound
      var lowerBound = new Timestamp(0, (new Date()).getTime() / 1000 + 1);
      or._lastSnapshotMod(lowerBound, function(err, item) {
        if (err) { throw err; }
        should.strictEqual(item, null);
        done();
      });
    });
  });

  describe('_setSnapshotCreationItemAndLowerBound', function() {
    it('should set snapshot creation and update lower bound', function(done) {
      var or = new OplogResolver(oplogCollection, vc, { debug: false }, function() {});
      should.strictEqual(or._lowerBound.equals(new Timestamp(0, 0)), true);
      or._setSnapshotCreationItemAndLowerBound(function(err) {
        if (err) { throw err; }
        should.strictEqual(or._lowerBound.greaterThan(new Timestamp(1, 1)), true);
        should.strictEqual(or._lowerBound.equals(or._snapshotCreationItem.ts), true);
        done();
      });
    });
  });

  describe('start', function() {
    it('should update lower bound to last snapshot creation', function(done) {
      var or = new OplogResolver(oplogCollection, vc, { hide: true }, function(err) {
        should.strictEqual(err.message, 'no modifications on snapshot found');
        should.strictEqual(or._lowerBound.greaterThan(new Timestamp(0, 0)), true);
        done();
      });
      or.start();
    });

    describe('insert', function() {
      describe('user initiated', function() {
        var lastOffset;

        it('should error if no oplog item can be found (because of empty an snapshot)', function(done) {
          var lowerBound = new Timestamp(0, (new Date()).getTime() / 1000);
          var or = new OplogResolver(oplogCollection, vc, { hide: true, lowerBound: lowerBound }, function(err, oplogItem) {
            should.strictEqual(err.message, 'no modifications on snapshot found');
            should.strictEqual(oplogItem, undefined);
            done();
          });
          or.start();
        });

        it('needs a new offset', function(done) {
          determineOffset(function(err, ts) { lastOffset = ts; done(err); });
        });

        it('needs an item in the collection initiated by the "user"', function(done) {
          var item = { _id: 'foo', a: 'b' };
          vc._collection.insert(item, done);
        });

        it('should error because oplog item can not be matched with (empty) snapshot', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { hide: true }, function(err, oplogItem) {
            should.strictEqual(err.message, 'no modifications on snapshot found');
            should.strictEqual(oplogItem, undefined);
            done();
          });
          or.start();
        });

        it('should support using a lowerbound and not find any oplog items', function(done) {
          var lowerBound = new Timestamp(0, (new Date()).getTime() / 1000);
          var or = new OplogResolver(oplogCollection, vc, { hide: true, lowerBound: lowerBound }, function(err, oplogItem) {
            should.strictEqual(err.message, 'no modifications on snapshot found');
            should.strictEqual(oplogItem, undefined);
            done();
          });
          or.start();
        });

        it('needs a newly inserted item to be inserted in the snapshot', function(done) {
          var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes };
          var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);
          vs.trackOplog(false, done);

          vs.on('trackOplog', function(item, v) {
            v.processQueues();
          });

          var item = { _id: 'bar', a: 'b' };
          vs.on('trackOplogStartTail', function() {
            vc._collection.insert(item, { w: 0 });
          });
        });

        it('should find the system initiated DAG insert as being the first oplog item (by s i)', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { debug: false, lowerBound: lastOffset }, function(err, oplogItem) {
            if (err) { throw err; }
            var ts = oplogItem.ts;
            should.strictEqual(lastOffset.lessThan(ts), true);
            should.deepEqual(oplogItem, {
              ts: ts,
              op: 'i',
              ns: 'test_oplog_resolver.foo',
              o: { _id: 'bar', a: 'b' }
            });
            done();
          });
          or.start();
        });

        it('needs the newly inserted item to be ackd', function(done) {
          var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes };
          var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);
          vs.trackOplog(false, done);

          vs.on('trackOplog', function(item, v) {
            v.processQueues();
          });
        });

        it('should return the collection oplog insert item (by s ack)', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { debug: false, lowerBound: lastOffset }, function(err, oplogItem) {
            if (err) { throw err; }
            var ts = oplogItem.ts;
            should.strictEqual(lastOffset.lessThan(ts), true);
            should.deepEqual(oplogItem, {
              ts: ts,
              op: 'i',
              ns: 'test_oplog_resolver.foo',
              o: { _id: 'bar', a: 'b' }
            });
            done();
          });
          or.start();
        });

        it('should error if no oplog items can be found while the snapshot is not empty', function(done) {
          // trick with high lower bound
          var lowerBound = new Timestamp(0, (new Date()).getTime() / 1000 + 1);
          var or = new OplogResolver(oplogCollection, vc, { hide: true, lowerBound: lowerBound }, function(err, oplogItem) {
            should.strictEqual(err.message, 'no modifications on snapshot found');
            should.strictEqual(oplogItem, undefined);
            done();
          });
          or.start();
        });

        it('should return snapshot creation item if no other item is found', function(done) {
          var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes };
          var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);
          vs._getLatestOplogItem(function(err, oi) {
            if (err) { throw err; }
            var lowerBound = new Timestamp(Math.max(oi.ts.getLowBits() - 1, 0), oi.ts.getHighBits());
            var or = new OplogResolver(oplogCollection, vc, { debug: opts.debug, lowerBound: lowerBound }, function(err, oplogItem) {
              if (err) { throw err; }
              var ts = oplogItem.ts;
              should.strictEqual(lowerBound.greaterThan(ts), true);
              should.deepEqual(oplogItem, {
                ts: ts,
                op: 'c',
                ns: 'test_oplog_resolver.$cmd',
                o: {
                  create: 'm3.foo',
                  strict: true,
                  autoIndexId: true,
                  capped: true,
                  size: 10485760,
                  w: 1,
                  writeConcern: { w: 1 }
                }
              });
              done();
            });
            or.start();
          });
        });
      });

      describe('system initiated', function() {
        var lastOffset;

        it('needs a new offset', function(done) {
          determineOffset(function(err, ts) { lastOffset = ts; done(err); });
        });

        it('should error if no oplog item can be found (because of new high lower bound)', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { debug: false, hide: true, lowerBound: lastOffset }, function(err, oplogItem) {
            should.strictEqual(err.message, 'no modifications on snapshot found');
            should.strictEqual(oplogItem, undefined);
            done();
          });
          or.start();
        });

        it('needs an item in the snapshot and collection initiated by the "system", via a remote', function(done) {
          var item =  { _id: { _co: 'foo', _id: 'baz', _v: 'X', _pe: 'foo', _pa: [], _lo: true, _i: 10 }, a: 'b' };
          vc.saveRemoteItem(item, function() {});
          vc.processQueues(done);
        });

        it('should find collection creation item since system initiated collection insert oplog item (by s i) does not resolve', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { debug: false, lowerBound: lastOffset }, function(err, oplogItem) {
            if (err) { throw err; }
            var ts = oplogItem.ts;
            should.strictEqual(lastOffset.greaterThan(ts), true);
            should.deepEqual(oplogItem, {
              ts: ts,
              op: 'c',
              ns: 'test_oplog_resolver.$cmd',
              o: {
                create: 'm3.foo',
                strict: true,
                autoIndexId: true,
                capped: true,
                size: 10485760,
                w: 1,
                writeConcern: { w: 1 }
              }
            });
            done();
          });
          or.start();
        });
        // TODO: handle inconsistent (non-ack) states)
        xit('should find the system initiated DAG insert as being the first oplog item (by s i)', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { debug: true, lowerBound: lastOffset }, function(err, oplogItem) {
            if (err) { throw err; }
            var ts = oplogItem.ts;
            should.strictEqual(lastOffset.lessThan(ts), true);
            should.deepEqual(oplogItem, {
              ts: ts,
              op: 'i',
              ns: 'test_oplog_resolver.m3.foo',
              o: { _id: { _co: 'foo', _id: 'baz', _v: 'X', _pe: '_local', _pa: [], _i: 2 }, a: 'b', _m3: { _ack: false } }
            });
            done();
          });
          or.start();
        });

        it('needs the newly inserted item to be ackd', function(done) {
          var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes };
          var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);
          vs.trackOplog(false, done);

          vs.on('trackOplog', function(item, v) {
            v.processQueues();
          });
        });

        it('should return the collection oplog insert item (by s ack)', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { debug: false, lowerBound: lastOffset }, function(err, oplogItem) {
            if (err) { throw err; }
            var ts = oplogItem.ts;
            should.strictEqual(lastOffset.lessThan(ts), true);
            should.deepEqual(oplogItem, {
              ts: ts,
              op: 'i',
              ns: 'test_oplog_resolver.m3.foo',
              o: { _id: { _co: 'foo', _id: 'baz', _v: 'X', _pe: '_local', _pa: [], _i: 2 }, a: 'b', _m3: { _ack: false } }
            });
            done();
          });
          or.start();
        });

        it('needs a new offset', function(done) {
          determineOffset(function(err, ts) { lastOffset = ts; done(err); });
        });

        it('should error if no oplog items can be found while the snapshot is not empty', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { hide: true, lowerBound: lastOffset }, function(err, oplogItem) {
            should.strictEqual(err.message, 'no modifications on snapshot found');
            should.strictEqual(oplogItem, undefined);
            done();
          });
          or.start();
        });

        it('needs a system initiated update via rebuild', function(done) {
          var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes };
          var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);
          vs.rebuild(done);
        });

        it('should return the collection oplog insert item (by s ack) after rebuild', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { debug: false, lowerBound: lastOffset }, function(err, oplogItem) {
            if (err) { throw err; }
            var ts = oplogItem.ts;
            should.strictEqual(lastOffset.lessThan(ts), true);
            should.deepEqual(oplogItem, {
              ts: ts,
              op: 'i',
              ns: 'test_oplog_resolver.m3.foo',
              o: { _id: { _co: 'foo', _id: 'baz', _v: 'X', _pe: '_local', _pa: [], _lo: true, _i: 3 }, a: 'b', _m3: { _ack: false } }
            });
            done();
          });
          or.start();
        });
      });
    });

    describe('update', function() {
      describe('user initiated', function() {
        var lastOffset;

        it('needs a new item in the collection initiated by the "user" and versioned', function(done) {
          var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes };
          var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);

          var item = { _id: 'qux', a: 'b', _v: 'X' };
          vc._collection.insert(item, { w: 0 }, function(err) {
            if (err) { throw err; }
            vs.rebuild(done);
          });
        });

        it('needs a new offset', function(done) {
          determineOffset(function(err, ts) { lastOffset = ts; done(err); });
        });

        it('needs the new item to be updated by the "user"', function(done) {
          vc._collection.update({ _id: 'qux' }, { $set: { a: 'c' } }, done);
        });

        it('should error if no snapshot oplog items can be found while the snapshot is not empty', function(done) {
          // use a high lower bound
          var lowerBound = new Timestamp(0, (new Date()).getTime() / 1000 + 1);
          var or = new OplogResolver(oplogCollection, vc, { hide: true, lowerBound: lowerBound }, function(err, oplogItem) {
            should.strictEqual(err.message, 'no modifications on snapshot found');
            should.strictEqual(oplogItem, undefined);
            done();
          });
          or.start();
        });

        it('needs the snapshot to catch up with the updated collection item', function(done) {
          var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes };
          var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);
          vs.trackOplog(false, done);

          vs.on('trackOplog', function(item, v) {
            v.processQueues();
          });
        });

        it('should return the collection oplog update item (by s i)', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { debug: false, lowerBound: lastOffset }, function(err, oplogItem) {
            if (err) { throw err; }
            var ts = oplogItem.ts;
            should.strictEqual(lastOffset.lessThanOrEqual(ts), true);
            should.deepEqual(oplogItem, {
              ts: ts,
              op: 'u',
              ns: 'test_oplog_resolver.foo',
              o2: { _id: 'qux' },
              o: { $set: { a: 'c' } }
            });
            done();
          });
          or.start();
        });

        it('needs the newly inserted item to be ackd', function(done) {
          var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes };
          var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);
          vs.trackOplog(false, done);

          vs.on('trackOplog', function(item, v) {
            v.processQueues();
          });
        });

        it('should return the collection oplog update item (by s ack)', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { debug: false, lowerBound: lastOffset }, function(err, oplogItem) {
            if (err) { throw err; }
            var ts = oplogItem.ts;
            should.strictEqual(lastOffset.lessThanOrEqual(ts), true);
            should.deepEqual(oplogItem, {
              ts: ts,
              op: 'u',
              ns: 'test_oplog_resolver.foo',
              o2: { _id: 'qux' },
              o: { $set: { a: 'c' } }
            });
            done();
          });
          or.start();
        });

        it('should error if no oplog items can be found while the snapshot is not empty', function(done) {
          var lowerBound = new Timestamp(0, (new Date()).getTime() / 1000 + 1);
          var or = new OplogResolver(oplogCollection, vc, { hide: true, lowerBound: lowerBound }, function(err, oplogItem) {
            should.strictEqual(err.message, 'no modifications on snapshot found');
            should.strictEqual(oplogItem, undefined);
            done();
          });
          or.start();
        });
      });

      // depends on initiated test, since it uses { _id: qux }
      describe('system initiated', function() {
        var lastOffset, lastVersion;

        it('needs a new offset', function(done) {
          determineOffset(function(err, ts) { lastOffset = ts; done(err); });
        });

        it('should error if no oplog item can be found (because of new high lower bound)', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { debug: false, hide: true, lowerBound: lastOffset }, function(err, oplogItem) {
            should.strictEqual(err.message, 'no modifications on snapshot found');
            should.strictEqual(oplogItem, undefined);
            done();
          });
          or.start();
        });

        // reuse item from user initiated test
        it('need to find the last version of previously used item { _id: qux }', function(done) {
          vc._snapshotCollection.findOne({ '_id._id': 'qux' }, { sort: { $natural: -1 } }, function(err, item) {
            if (err) { throw err; }
            lastVersion = item._id._v;
            done();
          });
        });

        it('should start and not crash', function(done) {
          var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes };
          var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);
          vs.trackOplog(false, done);
        });

        it('needs an item in the snapshot and collection updated by the "system", via a remote (ack)', function(done) {
          // one way to trigger system initiated updates, is to insert via a remote
          // unfortunately, this means we have to insert previous items first.
          var item1 = { _id: { _co: 'foo', _id: 'qux', _v: 'X', _pe: 'foo', _pa: [], _lo: true, _i: 3 }, a: 'b' };
          var item2 = { _id: { _co: 'foo', _id: 'qux', _v: lastVersion, _pe: 'foo', _pa: ['X'], _lo: true, _i: 4 }, a: 'c' };
          var item3 = { _id: { _co: 'foo', _id: 'qux', _v: 'Y', _pe: 'foo', _pa: [lastVersion], _lo: true, _i: 5 }, a: 'd' };

          // FIXME: start tracking since booting non-ackd state is currently not supported
          var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes, remotes: vsConfig.collections };
          var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);

          vs.trackOplog(false, done);
          var ve = vs._databaseCollections[databaseName + '.foo'];

          vs.on('trackOplog', function() {
            ve.processQueues(function(err) {
              if (err) { throw err; }
            });
          });

          ve.saveRemoteItem(item1, function() {});
          ve.saveRemoteItem(item2, function() {});
          ve.saveRemoteItem(item3, function() {});

          ve.processQueues(function(err) {
            if (err) { throw err; }
          });
        });

        // TODO: handle inconsistent (non-ack) states)
        xit('should find the system initiated DAG update as being the first oplog item (by s i)', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { debug: true, lowerBound: lastOffset }, function(err, oplogItem) {
            if (err) { throw err; }
            var ts = oplogItem.ts;
            console.log(oplogItem);
            should.strictEqual(lastOffset.lessThan(ts), true);
            should.deepEqual(oplogItem, {
              ts: ts,
              op: 'i',
              ns: 'test_oplog_resolver.m3.foo',
              o: { _id: { _co: 'foo', _id: 'qux', _v: 'Y', _pe: '_local', _pa: [lastVersion], _i: 3 }, a: 'd', _m3: { _ack: false } }
            });
            done();
          });
          or.start();
        });

        it('should return the collection oplog update item (by s ack via remote)', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { debug: false, lowerBound: lastOffset }, function(err, oplogItem) {
            if (err) { throw err; }
            var ts = oplogItem.ts;
            should.strictEqual(lastOffset.lessThan(ts), true);
            should.deepEqual(oplogItem, {
              ts: ts,
              op: 'i',
              ns: 'test_oplog_resolver.m3.foo',
              o: { _id: { _co: 'foo', _id: 'qux', _v: 'Y', _pe: '_local', _pa: [lastVersion], _i: 6 }, a: 'd', _m3: { _ack: false } }
            });
            done();
          });
          or.start();
        });

        // note: rebuilding kills any parent references and yields the same test as the "insert system initiated rebuild" test
        it('needs a new offset', function(done) {
          determineOffset(function(err, ts) { lastOffset = ts; done(err); });
        });

        it('should error if no oplog items can be found while the snapshot is not empty', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { hide: true, lowerBound: lastOffset }, function(err, oplogItem) {
            should.strictEqual(err.message, 'no modifications on snapshot found');
            should.strictEqual(oplogItem, undefined);
            done();
          });
          or.start();
        });

        it('needs a system initiated update via rebuild', function(done) {
          var opts = { debug: false, hide: true, snapshotSizes: vsConfig.snapshotSizes };
          var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);
          vs.rebuild(done);
        });

        it('should return the collection oplog update item (by s ack) after rebuild', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { debug: false, lowerBound: lastOffset }, function(err, oplogItem) {
            if (err) { throw err; }
            var ts = oplogItem.ts;
            should.strictEqual(lastOffset.lessThan(ts), true);
            should.deepEqual(oplogItem, {
              ts: ts,
              op: 'i',
              ns: 'test_oplog_resolver.m3.foo',
              o: { _id: { _co: 'foo', _id: 'qux', _v: 'Y', _pe: '_local', _pa: [], _lo: true, _i: 4 }, a: 'd', _m3: { _ack: false } }
            });
            done();
          });
          or.start();
        });
      });
    });

    describe('delete', function() {
      describe('user initiated', function() {
        var lastOffset;

        it('needs a new item in the collection initiated by the "user" and versioned', function(done) {
          var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes };
          var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);

          var item = { _id: 'quux', a: 'e', _v: 'H' };
          vc._collection.insert(item, { w: 0 }, function(err) {
            if (err) { throw err; }
            vs.rebuild(done);
          });
        });

        it('needs a new offset', function(done) {
          determineOffset(function(err, ts) { lastOffset = ts; done(err); });
        });

        it('needs the new item to be deleted by the "user"', function(done) {
          // FIXME: start tracking since booting non-ackd state is currently not supported
          var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes, remotes: vsConfig.collections };
          var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);
          var ve = vs._databaseCollections[databaseName + '.foo'];

          vs.trackOplog(false, done);

          vs.on('trackOplog', function() {
            ve.processQueues(function(err) {
              if (err) { throw err; }
            });
          });

          ve._collection.remove({ _id: 'quux' }, function(err) {
            if (err) { throw err; }
          });
        });

        xit('should error if no oplog items can be found while the snapshot is not empty', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { hide: true, lowerBound: lastOffset }, function(err, oplogItem) {
            should.strictEqual(err.message, 'no modifications on snapshot found');
            should.strictEqual(oplogItem, undefined);
            done();
          });
          or.start();
        });

        it('needs the deleted item to be propagated (inserted) to the snapshot', function(done) {
          var opts = { debug: false, hide: true, snapshotSizes: vsConfig.snapshotSizes };
          var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);
          vs.trackOplog(false, done);

          vs.on('trackOplog', function(item, v) {
            v.processQueues();
          });
        });

        it('should return the collection oplog delete item (by s i, item is already ackd)', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { debug: false, lowerBound: lastOffset }, function(err, oplogItem) {
            if (err) { throw err; }
            var ts = oplogItem.ts;
            should.strictEqual(lastOffset.lessThanOrEqual(ts), true);
            should.deepEqual(oplogItem, {
              ts: ts,
              op: 'd',
              ns: 'test_oplog_resolver.foo',
              b: true,
              o: { _id: 'quux' }
            });
            done();
          });
          or.start();
        });

        it('should error if no oplog items can be found while the snapshot is not empty', function(done) {
          var lowerBound = new Timestamp(0, (new Date()).getTime() / 1000 + 1);
          var or = new OplogResolver(oplogCollection, vc, { hide: true, lowerBound: lowerBound }, function(err, oplogItem) {
            should.strictEqual(err.message, 'no modifications on snapshot found');
            should.strictEqual(oplogItem, undefined);
            done();
          });
          or.start();
        });
      });

      // TODO: versioned collection should set deleted items ack
      describe('system initiated', function() {
        var lastOffset, lastVersion;

        it('needs a new item in the collection initiated by the "system" and versioned', function(done) {
          var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes };
          var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);

          var item = { _id: 'fubar', a: 'j', _v: 'K' };
          vc._collection.insert(item, { w: 0 }, function(err) {
            if (err) { throw err; }
            vs.rebuild(done);
          });
        });

        it('needs a new offset', function(done) {
          determineOffset(function(err, ts) { lastOffset = ts; done(err); });
        });

        it('should error if no oplog item can be found (because of new high lower bound)', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { debug: false, hide: true, lowerBound: lastOffset }, function(err, oplogItem) {
            should.strictEqual(err.message, 'no modifications on snapshot found');
            should.strictEqual(oplogItem, undefined);
            done();
          });
          or.start();
        });

        it('need to find the version of the last update', function(done) {
          vc._collection.findOne({ _id: 'fubar' }, function(err, item) {
            if (err) { throw err; }
            lastVersion = item._v;
            done();
          });
        });

        it('needs an item in the snapshot and collection deleted by the "system", via a remote', function(done) {
          // one way to trigger system initiated updates, is to insert via a remote
          // unfortunately, this means we have to insert previous items first.
          var item1 = { _id: { _co: 'foo', _id: 'fubar', _v: 'K', _pe: 'foo', _pa: [], _lo: true, _i: 3 }, a: 'j' };
          var item2 =  { _id: { _co: 'foo', _id: 'fubar', _v: 'L', _pe: 'foo', _pa: [lastVersion], _lo: true, _i: 5, _d: true }, a: 'k' };
          vc.saveRemoteItem(item1, function() {});
          vc.saveRemoteItem(item2, function() {});
          vc.processQueues(done);
        });

        it('should find snapshot creation since system initiated collection delete oplog item (by s i) does not resolve', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { debug: false, lowerBound: lastOffset }, function(err, oplogItem) {
            if (err) { throw err; }
            var ts = oplogItem.ts;
            should.strictEqual(lastOffset.greaterThan(ts), true);
            should.deepEqual(oplogItem, {
              ts: ts,
              op: 'c',
              ns: 'test_oplog_resolver.$cmd',
              o: {
                create: 'm3.foo',
                strict: true,
                autoIndexId: true,
                capped: true,
                size: 131072,
                w: 1,
                writeConcern: { w: 1 }
              }
            });
            done();
          });
          or.start();
        });
        // TODO: handle inconsistent (non-ack) states)
        xit('should find the system initiated deletion as being the first oplog item (by s i)', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { debug: true, lowerBound: lastOffset }, function(err, oplogItem) {
            if (err) { throw err; }
            var ts = oplogItem.ts;
            should.strictEqual(lastOffset.lessThan(ts), true);
            should.deepEqual(oplogItem, {
              ts: ts,
              op: 'i',
              ns: 'test_oplog_resolver.m3.foo',
              o: { _id: { _co: 'foo', _id: 'fubar', _v: 'L', _pe: '_local', _pa: ['K'], _i: 4, _d: true }, a: 'k', _m3: { _ack: false } }
            });
            done();
          });
          or.start();
        });

        // TODO: versioned collection should set deleted items ack
        xit('needs the newly inserted item to be ackd', function(done) {
          var opts = { debug: false, snapshotSizes: vsConfig.snapshotSizes };
          var vs = new VersionedSystem(oplogDb, vsConfig.databases, vsConfig.collections, opts);
          vs.trackOplog(false, done);

          vs.on('trackOplog', function(item, v) {
            v.processQueues();
          });
        });

        xit('should return the collection oplog update item (by s ack)', function(done) {
          var or = new OplogResolver(oplogCollection, vc, { debug: true, hide: false, lowerBound: lastOffset }, function(err, oplogItem) {
            if (err) { throw err; }
            var ts = oplogItem.ts;
            should.strictEqual(lastOffset.lessThan(ts), true);
            should.deepEqual(oplogItem, {
              ts: ts,
              op: 'i',
              ns: 'test_oplog_resolver.m3.foo',
              o: { _id: { _co: 'foo', _id: 'quux', _v: 'Y', _pe: '_local', _pa: [lastVersion], _i: 2, _d: true }, a: 'k', _m3: { _ack: true } }
            });
            done();
          });
          or.start();
        });

        xit('should error if no oplog items can be found while the snapshot is not empty', function(done) {
          var lowerBound = new Timestamp(0, (new Date()).getTime() / 1000 + 1);
          var or = new OplogResolver(oplogCollection, vc, { hide: true, lowerBound: lowerBound }, function(err, oplogItem) {
            should.strictEqual(err.message, 'no modifications on snapshot found');
            should.strictEqual(oplogItem, undefined);
            done();
          });
          or.start();
        });
      });
    });
  });
});
