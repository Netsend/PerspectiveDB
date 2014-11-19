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

/*jshint -W068,-W116, nonew: false */

var should = require('should');
var ObjectID = require('mongodb').ObjectID;
var Timestamp = require('mongodb').Timestamp;
var Writable = require('stream').Writable;

var fetchItems = require('../../_fetch_items');

var VersionedCollection = require('../../../lib/versioned_collection');

var db;
var databaseName = 'test_versioned_collection';
var Database = require('../../_database');

// open database connection
var database = new Database(databaseName);
before(function(done) {
  database.connect(function(err, dbc) {
    db = dbc;
    done(err);
  });
});

after(database.disconnect.bind(database));

describe('versioned_collection', function() {
  var vColl;

  describe('constructor', function() {
    it('should require db to be a mongodb.Db', function() {
      (function () { new VersionedCollection(); }).should.throwError('db must be an instance of mongodb.Db');
    });

    it('should require collectionName to be a string', function() {
      (function() { new VersionedCollection(db, {}); }).should.throw('collectionName must be a string');
    });

    it('should require options.localPerspective to be a string', function() {
      (function() {
        new VersionedCollection(db, 'foo', { localPerspective: {} });
      }).should.throwError('options.localPerspective must be a string');
    });

    it('should require options.versionKey to be a string', function() {
      (function() {
        new VersionedCollection(db, 'foo', { versionKey: {} });
      }).should.throwError('options.versionKey must be a string');
    });

    it('should require options.remotes to be an array', function() {
      (function() {
        new VersionedCollection(db, 'foo', { remotes: {} });
      }).should.throwError('options.remotes must be an array');
    });

    it('should default options.localPerspective to _local', function() {
      var vc = new VersionedCollection(db, 'foo');
      should.equal(vc.localPerspective, '_local');
    });

    it('should default options.versionKey to _v', function() {
      var vc = new VersionedCollection(db, 'foo');
      should.equal(vc.versionKey, '_v');
    });

    it('should set collectionName', function() {
      var vc = new VersionedCollection(db, 'foo');
      should.equal(vc.collectionName, 'foo');
    });

    it('should set snapshotCollectionName', function() {
      var vc = new VersionedCollection(db, 'foo');
      should.equal(vc.snapshotCollectionName, 'm3.foo');
    });

    it('should set tmpCollectionName', function() {
      var vc = new VersionedCollection(db, 'foo');
      should.equal(vc.tmpCollectionName, 'm3._m3tmp');
    });

    it('should open collection', function() {
      var vc = new VersionedCollection(db, 'foo');
      vc._collection.should.have.property('collectionName');
    });

    it('should open snapshotCollection', function() {
      var vc = new VersionedCollection(db, 'foo');
      vc._snapshotCollection.should.have.property('collectionName');
    });

    it('should open tmpCollection', function() {
      var vc = new VersionedCollection(db, 'foo');
      vc._tmpCollection.should.have.property('collectionName');
    });

    it('should construct', function() {
      (function() { vColl = new VersionedCollection(db, 'foo'); }).should.not.throwError();
    });
  });

  describe('startAutoProcessing', function() {
    it('should require interval to be a number', function() {
      var vc = new VersionedCollection(db, 'foo');
      (function () { vc.startAutoProcessing({}); }).should.throwError('interval must be a number');
    });

    it('should run', function() {
      var vc = new VersionedCollection(db, 'foo');
      vc.startAutoProcessing();
    });

    it('should not crash when locked', function(done) {
      var vc = new VersionedCollection(db, 'foo', { debug: false });
      vc._locked = true;
      vc.startAutoProcessing(1);
      setTimeout(function() {
        vc._locked = false;
        vc.stopAutoProcessing(done);
      }, 10);
    });
  });

  describe('writable stream', function() {
    var collectionName = 'writableStream';
    var item = { _id: { _id: 'foo', _v: 'A', _pe: 'II', _pa: [] } };
    var opts = { localPerspective: 'I', remotes: ['II'], debug: false };

    it('should be a writable stream', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      should.strictEqual(vc instanceof Writable, true);
      done();
    });

    it('should write the item', function(done){
      var vc = new VersionedCollection(db, collectionName, opts);
      var result = vc.write(item, done);
      should.strictEqual(result, true);
    });

    it('should write the item through end', function(done){
      var vc = new VersionedCollection(db, collectionName, opts);
      vc.end(item, done);
    });

    it('should write the item through end and emit finish', function(done){
      var vc = new VersionedCollection(db, collectionName, opts);
      vc.on('finish', done);
      vc.end(item);
    });
  });

  describe('saveCollectionItem', function() {
    var collectionName = 'saveCollectionItem';

    var fooA = { _id: 'foo', _v: 'A', a: 1 };
    var fooB = { _id: 'foo', _v: 'B', b: 2 };
    var barA = { _id: 'bar', _v: 'A', b: 3 };

    it('should add a new root (in empty snapshot)', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      var ts = new Timestamp(0, 0);
      vc.saveCollectionItem(fooA, [], function(err, newObj) {
        if (err) { throw err; }
        should.deepEqual(newObj, {
          _id: { _co: 'saveCollectionItem', _id: 'foo', _v: 'A', _pe: '_local', _pa: [], _lo: true, _i: 1 },
          a: 1,
          _m3: { _ack: false, _op: ts  }
        });
        done();
      });
    });

    it('should add a child', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc.saveCollectionItem(fooB, ['A'], function(err, newObj) {
        if (err) { throw err; }
        should.deepEqual(newObj, {
          _id: { _co: 'saveCollectionItem', _id: 'foo', _v: 'B', _pe: '_local', _pa: ['A'], _lo: true, _i: 2 },
          b: 2,
          _m3: { _ack: false, _op: new Timestamp(0, 0) }
        });
        done();
      });
    });

    it('should add a new root (with existing items)', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc.saveCollectionItem(barA, [], function(err, newObj) {
        if (err) { throw err; }
        should.deepEqual(newObj, {
          _id: { _co: 'saveCollectionItem', _id: 'bar', _v: 'A', _pe: '_local', _pa: [], _lo: true, _i: 3 },
          b: 3,
          _m3: { _ack: false, _op: new Timestamp(0, 0) }
        });
        done();
      });
    });
  });

  describe('allHeads', function() {
    var collectionName = 'allHeads';

    var fooA = { _id : { _id: 'foo', _v: 'A', _pe: 'I', _pa: [] } };
    var fooB = { _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'] } };
    var fooC = { _id : { _id: 'foo', _v: 'C', _pe: 'I', _pa: ['B'] } };
    var fooD = { _id : { _id: 'foo', _v: 'D', _pe: 'I', _pa: ['C'] } };
    var fooE = { _id : { _id: 'foo', _v: 'E', _pe: 'I', _pa: ['B'] } };
    var fooF = { _id : { _id: 'foo', _v: 'F', _pe: 'I', _pa: ['E', 'C'] } };
    var fooG = { _id : { _id: 'foo', _v: 'G', _pe: 'I', _pa: ['F'] } };

    var fooDAG = [fooA, fooB, fooC, fooD, fooE, fooF, fooG];

    // create the following structure:
    // A <-- B <-- C <-- D
    //        \     \
    //         E <-- F <-- G

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert(fooDAG, {w: 1}, done);
    });

    it('should find D and G', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: 'I' }, { debug: false });
      var count = 0;
      vc.allHeads(function(id, heads) {
        count++;
        should.strictEqual(id, 'foo');
        should.deepEqual(heads, [ 'D', 'G' ]);
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(count, 1);
        done();
      });
    });

    var barA = { _id : { _id: 'bar', _v: 'A', _pe: 'I', _pa: [] } };
    var barB = { _id : { _id: 'bar', _v: 'B', _pe: 'I', _pa: ['A'] } };
    var barC = { _id : { _id: 'bar', _v: 'C', _pe: 'I', _pa: ['B', 'E'] } };
    var barD = { _id : { _id: 'bar', _v: 'D', _pe: 'I', _pa: ['C'] } };
    var barE = { _id : { _id: 'bar', _v: 'E', _pe: 'I', _pa: ['B'] } };
    var barF = { _id : { _id: 'bar', _v: 'F', _pe: 'I', _pa: ['E', 'C'] } };
    var barG = { _id : { _id: 'bar', _v: 'G', _pe: 'I', _pa: ['F'] } };

    var barDAG = [barA, barB, barE, barC, barD, barF, barG];

    // add the following structure:
    // A <-- B <-- C <-- D
    //        \  /  \
    //         E <-- F <-- G
    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert(barDAG, {w: 1}, done);
    });

    it('should find foo D and G, and bar D and G', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: 'I' }, { debug: false });
      var count = 0;
      vc.allHeads(function(id, heads) {
        count++;
        if (count === 1) {
          should.strictEqual(id, 'foo');
          should.deepEqual(heads, [ 'D', 'G' ]);
        }
        if (count === 2) {
          should.strictEqual(id, 'bar');
          should.deepEqual(heads, [ 'D', 'G' ]);
        }
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(count, 2);
        done();
      });
    });
  });

  describe('ackAncestorsAckd', function() {
    var collectionName = 'ackAncestorsAckd';

    var fooA = { _id: { _id: 'foo', _v: 'A', _pe: '_local', _i:  1, _pa: []    }, a:  1, _m3: { _ack: false } };
    var fooB = { _id: { _id: 'foo', _v: 'B', _pe: '_local', _i:  2, _pa: ['A'] }, b:  2, _m3: { _ack: false } };
    var fooC = { _id: { _id: 'foo', _v: 'C', _pe: '_local', _i:  3, _pa: ['B'] }, c:  3, _m3: { _ack: true  } };
    var fooD = { _id: { _id: 'foo', _v: 'D', _pe: '_local', _i:  4, _pa: ['C'] }, d:  4, _m3: { _ack: false } };
    var fooE = { _id: { _id: 'foo', _v: 'E', _pe: '_local', _i:  5, _pa: ['D'] }, e:  4, _m3: { _ack: false } };

    var barA = { _id: { _id: 'bar', _v: 'A', _pe: '_local', _i:  6, _pa: []    }, a: 11, _m3: { _ack: false } };
    var barB = { _id: { _id: 'bar', _v: 'B', _pe: '_local', _i:  7, _pa: ['A'] }, b: 21, _m3: { _ack: true  } };
    var barC = { _id: { _id: 'bar', _v: 'C', _pe: '_local', _i:  8, _pa: ['B'] }, c: 31, _m3: { _ack: false } };
    var barD = { _id: { _id: 'bar', _v: 'D', _pe: '_local', _i:  9, _pa: ['C'] }, d: 41, _m3: { _ack: true  } };
    var barE = { _id: { _id: 'bar', _v: 'E', _pe: '_local', _i: 10, _pa: ['D'] }, e: 41, _m3: { _ack: false } };

    var snapshotItems = [ fooA, fooB, fooC, fooD, fooE, barA, barB, barC, barD, barE ];

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert(snapshotItems, {w: 1}, done);
    });

    // should update existing values, create new values and set ackd
    it('should run', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false });
      vc.ackAncestorsAckd(function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.find().toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items.length, 10);

          should.deepEqual(items[0], { _id: { _id: 'foo', _v: 'A', _pe: '_local', _i:  1, _pa: []    }, a:  1, _m3: { _ack: true  } });
          should.deepEqual(items[1], { _id: { _id: 'foo', _v: 'B', _pe: '_local', _i:  2, _pa: ['A'] }, b:  2, _m3: { _ack: true  } });
          should.deepEqual(items[2], { _id: { _id: 'foo', _v: 'C', _pe: '_local', _i:  3, _pa: ['B'] }, c:  3, _m3: { _ack: true  } });
          should.deepEqual(items[3], { _id: { _id: 'foo', _v: 'D', _pe: '_local', _i:  4, _pa: ['C'] }, d:  4, _m3: { _ack: false } });
          should.deepEqual(items[4], { _id: { _id: 'foo', _v: 'E', _pe: '_local', _i:  5, _pa: ['D'] }, e:  4, _m3: { _ack: false } });

          should.deepEqual(items[5], { _id: { _id: 'bar', _v: 'A', _pe: '_local', _i:  6, _pa: []    }, a: 11, _m3: { _ack: true  } });
          should.deepEqual(items[6], { _id: { _id: 'bar', _v: 'B', _pe: '_local', _i:  7, _pa: ['A'] }, b: 21, _m3: { _ack: true  } });
          should.deepEqual(items[7], { _id: { _id: 'bar', _v: 'C', _pe: '_local', _i:  8, _pa: ['B'] }, c: 31, _m3: { _ack: true  } });
          should.deepEqual(items[8], { _id: { _id: 'bar', _v: 'D', _pe: '_local', _i:  9, _pa: ['C'] }, d: 41, _m3: { _ack: true  } });
          should.deepEqual(items[9], { _id: { _id: 'bar', _v: 'E', _pe: '_local', _i: 10, _pa: ['D'] }, e: 41, _m3: { _ack: false } });
          done();
        });
      });
    });
  });

  describe('copyCollectionOverSnapshot', function() {
    var collectionName = 'copyCollectionOverSnapshot';

    var fooA = { _id: 'foo', _v: 'A', a: 1 };
    var barA = { _id: 'bar', _v: 'A', b: 3 };
    var bazB = { _id: 'baz', _v: 'B', d: 4 };
    var quxA = { _id: 'qux', _v: 'A', e: 5 };
    var items = [ fooA, barA, bazB, quxA ];

    var sFooA = { _id: { _id: 'foo', _v: 'A', _pe: '_local', _i: 1, _pa: [] }, a: 1, _m3: { _ack: true, _op: new Timestamp(1414516132, 1) } };
    var sBarA = { _id: { _id: 'bar', _v: 'A', _pe: '_local', _i: 2, _pa: [] }, b: 2, _m3: { _ack: true, _op: new Timestamp(1414516133, 1) } };
    var sBazA = { _id: { _id: 'baz', _v: 'A', _pe: '_local', _i: 3, _pa: [] }, c: 3, _m3: { _ack: true, _op: new Timestamp(1414516134, 1) } };
    var sBazB = { _id: { _id: 'baz', _v: 'B', _pe: '_local', _i: 4, _pa: ['A'] }, d: 4, _m3: { _ack: false, _op: new Timestamp(0, 0) } };
    var snapshotItems = [ sFooA, sBarA, sBazA, sBazB ];

    it('should save collection items', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._collection.insert(items, {w: 1}, done);
    });

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert(snapshotItems, {w: 1}, done);
    });

    // should update existing values, create new values and set ackd
    it('should run', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false });
      vc.copyCollectionOverSnapshot(function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.find().toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items.length, 6);

          var v = items[4]._id._v;
          should.deepEqual(items[0], { _id: { _id: 'foo', _v: 'A', _pe: '_local', _i: 1, _pa: [] }, a: 1, _m3: { _ack: true, _op: new Timestamp(1414516132, 1) } });
          should.deepEqual(items[1], { _id: { _id: 'bar', _v: 'A', _pe: '_local', _i: 2, _pa: [] }, b: 2, _m3: { _ack: true, _op: new Timestamp(1414516133, 1) } });
          should.deepEqual(items[2], { _id: { _id: 'baz', _v: 'A', _pe: '_local', _i: 3, _pa: [] }, c: 3, _m3: { _ack: true, _op: new Timestamp(1414516134, 1) } });
          should.deepEqual(items[3], { _id: { _id: 'baz', _v: 'B', _pe: '_local', _i: 4, _pa: ['A'] }, d: 4, _m3: { _ack: true, _op: new Timestamp(0, 0) } });
          should.deepEqual(items[4], { _id: { _co: 'copyCollectionOverSnapshot', _id: 'bar', _v: v,   _pe: '_local', _i: 5, _pa: ['A'], _lo: true }, b: 3, _m3: { _ack: true, _op: new Timestamp(0, 0) } });
          should.deepEqual(items[5], { _id: { _co: 'copyCollectionOverSnapshot', _id: 'qux', _v: 'A', _pe: '_local', _i: 6, _pa: [], _lo: true }, e: 5, _m3: { _ack: true, _op: new Timestamp(0, 0) } });

          vc._collection.find().toArray(function(err, items2) {
            if (err) { throw err; }
            should.deepEqual(items2.length, 4);

            should.deepEqual(items2[0], { _id: 'foo', _v: 'A', a: 1 });
            should.deepEqual(items2[1], { _id: 'baz', _v: 'B', d: 4 });
            should.deepEqual(items2[2], { _id: 'qux', _v: 'A', e: 5 });
            should.deepEqual(items2[3], { _id: 'bar', _v: v,   b: 3 });

            done();
          });
        });
      });
    });
  });

  describe('lastByPerspective', function() {
    var collectionName = 'lastByPerspective';

    var A = { _id: { _id: 'foo', _v: 'A', _pe: 'bar', _i: 1 }, _m3: { _ack: true } };
    var Ap = { _id: { _id: 'foo', _v: 'A', _pe: 'foo', _i: 2 }, _m3: { _ack: true } };
    var B = { _id: { _id: 'foo', _v: 'B', _pe: 'bar', _pa: ['A'], _i: 3 }, _m3: { _ack: true } };
    var Bp = { _id: { _id: 'foo', _v: 'B', _pe: 'foo', _pa: ['A'], _i: 4 }, _m3: { _ack: true } };
    var C = { _id: { _id: 'foo', _v: 'C', _pe: 'bar', _pa: ['B'], _i: 5 }, _m3: { _ack: true } };
    var D = { _id: { _id: 'foo', _v: 'D', _pe: 'bar', _pa: ['B'], _i: 6 }, _m3: { _ack: false } };

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([A, Ap, B, Bp, C, D], {w: 1}, done);
    });

    it('should find the latest version of foo', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc.lastByPerspective('foo', function(err, item) {
        should.equal(err, null);
        should.deepEqual(item, Bp);
        done();
      });
    });

    it('should find the latest unackd version of foo', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc.lastByPerspective('foo', false, function(err, item) {
        should.equal(err, null);
        should.equal(item, null);
        done();
      });
    });

    it('should find the latest ackd version of bar', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc.lastByPerspective('bar', true, function(err, item) {
        should.equal(err, null);
        should.deepEqual(item, C);
        done();
      });
    });

    it('should find the latest version of bar, whether merged or not', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc.lastByPerspective('bar', function(err, item) {
        should.equal(err, null);
        should.deepEqual(item, D);
        done();
      });
    });

    it('should find the latest un ackd version of bar', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc.lastByPerspective('bar', false, function(err, item) {
        should.equal(err, null);
        should.deepEqual(item, D);
        done();
      });
    });

    it('should find the latest version of baz', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc.lastByPerspective('baz', function(err, item) {
        should.equal(err, null);
        should.deepEqual(item, null);
        done();
      });
    });
  });

  describe('saveRemoteItem', function() {
    var collectionName = 'saveRemoteItem';
    var perspective = 'I';

    it('should not be merging', function() {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      should.equal(vc._mergingRemoteQueue, null);
    });

    it('should require item\'s perspective to be different from the local perspective', function(done) {
      var opts = { localPerspective: perspective, remotes: [perspective], hide: true };
      var vc = new VersionedCollection(db, collectionName, opts);
      var item = { _id: { _id: 'foo', _v: 'A', _pe: perspective, _pa: [] } };

      vc.saveRemoteItem(item, function(err) {
        should.equal(err.message, 'remote must not equal local perspective');
        done();
      });
      vc.processQueues();
    });

    it('should save to the right collection and add new remotes', function(done) {
      this.timeout(10000);
      var opts = { localPerspective: perspective, debug: false };
      var vc = new VersionedCollection(db, collectionName, opts);
      var item = { _id: { _id: 'foo', _v: 'A', _pe: 'II', _pa: [] } };

      should.deepEqual(vc._remotes, []);

      vc.saveRemoteItem(item, function(err) {
        if (err) { throw err; }
        should.deepEqual(vc._remotes, ['II']);
        fetchItems(vc, function(err, result) {
          if (err) { throw err; }

          var items = result[databaseName + '.saveRemoteItem'].items;
          var m3items = result[databaseName + '.saveRemoteItem'].m3items;

          should.strictEqual(items.length, 1);
          should.strictEqual(m3items.length, 2);

          should.deepEqual(items[0], {
            _id: 'foo',
            _v: 'A'
          });
          should.deepEqual(m3items[0], {
            _id: { _co: 'saveRemoteItem', _id: 'foo', _v: 'A', _pe: 'II', _pa: [] },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });
          should.deepEqual(m3items[1], {
            _id: { _co: 'saveRemoteItem', _id: 'foo', _v: 'A', _pe: 'I', _pa: [], _i: 1 },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });
          done();
        });
      }, function() {});
      vc.processQueues();
    });

    it('should clear the _lo flag', function(done) {
      var opts = { localPerspective: perspective, remotes: ['II'] };
      var vc = new VersionedCollection(db, collectionName + 'ClearLo', opts);
      var item = { _id: { _id: 'foo', _v: 'A', _pe: 'II', _pa: [], _lo: true } };

      vc.saveRemoteItem(item, function(err) {
        if (err) { throw err; }
        fetchItems(vc, function(err, result) {
          if (err) { throw err; }

          var items = result[databaseName + '.saveRemoteItemClearLo'].items;
          var m3items = result[databaseName + '.saveRemoteItemClearLo'].m3items;

          should.strictEqual(items.length, 1);
          should.strictEqual(m3items.length, 2);

          should.deepEqual(items[0], {
            _id: 'foo',
            _v: 'A'
          });
          should.deepEqual(m3items[0], {
            _id: { _co: 'saveRemoteItemClearLo', _id: 'foo', _v: 'A', _pe: 'II', _pa: [] },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });
          should.deepEqual(m3items[1], {
            _id: { _co: 'saveRemoteItemClearLo', _id: 'foo', _v: 'A', _pe: 'I', _pa: [], _i: 1 },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });
          done();
        });
      }, function() {});
      vc.processQueues();
    });

    it('should set _m3._ack to false', function(done) {
      var opts = { localPerspective: perspective, remotes: ['II'] };
      var vc = new VersionedCollection(db, collectionName + 'M3False', opts);
      var item = { _id: { _id: 'foo', _v: 'A', _pe: 'II', _pa: [] }, _m3: { _ack: true } };

      vc.saveRemoteItem(item, function(err) {
        if (err) { throw err; }
        fetchItems(vc, function(err, result) {
          if (err) { throw err; }

          var items = result[databaseName + '.saveRemoteItemM3False'].items;
          var m3items = result[databaseName + '.saveRemoteItemM3False'].m3items;

          should.strictEqual(items.length, 1);
          should.strictEqual(m3items.length, 2);

          should.deepEqual(items[0], {
            _id: 'foo',
            _v: 'A'
          });
          should.deepEqual(m3items[0], {
            _id: { _co: 'saveRemoteItemM3False', _id: 'foo', _v: 'A', _pe: 'II', _pa: [] },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });
          should.deepEqual(m3items[1], {
            _id: { _co: 'saveRemoteItemM3False', _id: 'foo', _v: 'A', _pe: 'I', _pa: [], _i: 1 },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });
          done();
        });
      }, function() {});
      vc.processQueues();
    });

    /* test manually by removing the comments
    it('should retry when limit reached', function(done) {
      var opts = { localPerspective: perspective, remotes: ['II'], debug: false };
      var vc = new VersionedCollection(db, collectionName + 'Limit', opts);
      var item = { _id: { _id: 'foo', _v: 'A', _pe: 'II', _pa: [] } };

      vc._queueLimit = 0;
      vc._queueLimitRetryTimeout = 1000;
      vc.saveRemoteItem(item, done, function() {});
      vc.processQueues();
    });
    */

    it('should add items in insertion order', function(done) {
      var opts = { localPerspective: perspective, remotes: ['II'] };
      var vc = new VersionedCollection(db, collectionName + 'InsertionOrder', opts);
      var A = { _id: { _id: 'foo', _v: 'A', _pe: 'II', _pa: [] } };
      var B = { _id: { _id: 'bar', _v: 'B', _pe: 'II', _pa: [] } };
      var C = { _id: { _id: 'baz', _v: 'C', _pe: 'II', _pa: [] } };
      var D = { _id: { _id: 'qux', _v: 'D', _pe: 'II', _pa: [] } };
      var E = { _id: { _id: 'quux', _v: 'E', _pe: 'II', _pa: [] } };


      [A, B, C, D, E].forEach(function(item) {
        vc.saveRemoteItem(item, function(err) { if (err) { throw err; } }, function() {});
      });
      vc.processQueues(function(err) {
        if (err) { throw err; }
        fetchItems(vc, function(err, result) {
          if (err) { throw err; }

          var items = result[databaseName + '.saveRemoteItemInsertionOrder'].items;
          var m3items = result[databaseName + '.saveRemoteItemInsertionOrder'].m3items;

          should.strictEqual(items.length, 5);
          should.strictEqual(m3items.length, 10);

          should.deepEqual(m3items[0], {
            _id: { _co: 'saveRemoteItemInsertionOrder', _id: 'foo', _v: 'A', _pe: 'II', _pa: [] },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });
          should.deepEqual(m3items[1], {
            _id: { _co: 'saveRemoteItemInsertionOrder', _id: 'bar', _v: 'B', _pe: 'II', _pa: [] },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });
          should.deepEqual(m3items[2], {
            _id: { _co: 'saveRemoteItemInsertionOrder', _id: 'baz', _v: 'C', _pe: 'II', _pa: [] },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });
          should.deepEqual(m3items[3], {
            _id: { _co: 'saveRemoteItemInsertionOrder', _id: 'qux', _v: 'D', _pe: 'II', _pa: [] },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });
          should.deepEqual(m3items[4], {
            _id: { _co: 'saveRemoteItemInsertionOrder', _id: 'quux', _v: 'E', _pe: 'II', _pa: [] },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });
          should.deepEqual(m3items[5], {
            _id: { _co: 'saveRemoteItemInsertionOrder', _id: 'foo', _v: 'A', _pe: 'I', _pa: [], _i: 1 },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });
          should.deepEqual(m3items[6], {
            _id: { _co: 'saveRemoteItemInsertionOrder', _id: 'bar', _v: 'B', _pe: 'I', _pa: [], _i: 2 },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });
          should.deepEqual(m3items[7], {
            _id: { _co: 'saveRemoteItemInsertionOrder', _id: 'baz', _v: 'C', _pe: 'I', _pa: [], _i: 3 },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });
          should.deepEqual(m3items[8], {
            _id: { _co: 'saveRemoteItemInsertionOrder', _id: 'qux', _v: 'D', _pe: 'I', _pa: [], _i: 4 },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });
          should.deepEqual(m3items[9], {
            _id: { _co: 'saveRemoteItemInsertionOrder', _id: 'quux', _v: 'E', _pe: 'I', _pa: [], _i: 5 },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });
          done();
        });
      });
    });
  });

  describe('_processRemoteQueues', function() {
    var collectionName = '_processRemoteQueues';
    var perspective = 'I';

    it('should add the item to the versioned collection and merge', function(done) {
      var opts = { localPerspective: perspective, remotes: ['II'], debug: false, hide: true };
      var vc = new VersionedCollection(db, collectionName, opts);
      var item1 = { _id: { _id: 'foo', _v : 'A', _pe: 'I', _pa: [] }, foo : 'bar', _m3: { _ack: true , _op: new Timestamp(123456789, 1) } };
      var item2 = { _id: { _id: 'foo', _v : 'A', _pe: 'II', _pa: [] }};

      vc._snapshotCollection.insert(item1, {w: 1}, function(err) {
        if (err) { throw err; }
        vc.saveRemoteItem(item2, function(err) { if (err) { throw err; } }, function() {});
        vc._processRemoteQueues(function(err) {
          if (err) { throw err; }
          vc._snapshotCollection.find().toArray(function(err, items) {
            should.equal(err, null);
            should.equal(items.length, 2);
            should.deepEqual(items[0], {
              _id: { _id: 'foo', _v : 'A', _pe: 'I', _pa: [] },
              _m3: { _ack: true, _op: new Timestamp(123456789, 1)},
              foo : 'bar'
            });
            should.deepEqual(items[1], {
              _id: { _co: '_processRemoteQueues', _id: 'foo', _v : 'A', _pe: 'II', _pa: [] },
              _m3: { _ack: false, _op: new Timestamp(0, 0)}
            });
            done();
          });
        });
      });
    });

    it('should insert multiple items and callback when merged', function(done) {
      var opts = { localPerspective: perspective, remotes: ['II'], debug: false, hide: true };
      var vc = new VersionedCollection(db, collectionName, opts);
      var item = { _id: { _id: 'foo', _v : 'B', _pe: 'II', _pa: ['A'] }, baz : 'fubar' };

      vc.saveRemoteItem(item, function(err) { if (err) { throw err; } }, function() {});
      vc._processRemoteQueues(function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.find().toArray(function(err, items) {
          if (err) { throw err; }
          should.equal(items.length, 4);
          should.deepEqual(items[0], {
            _id: { _id: 'foo', _v : 'A', _pe: 'I', _pa: [] },
            _m3: { _ack: true, _op: new Timestamp(123456789, 1) },
            foo : 'bar'
          });
          should.deepEqual(items[1], {
            _id: { _co: '_processRemoteQueues', _id: 'foo', _v : 'A', _pe: 'II', _pa: [] },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });
          should.deepEqual(items[2], {
            _id: { _co: '_processRemoteQueues', _id: 'foo', _v : 'B', _pe: 'II', _pa: ['A'] },
            _m3: { _ack: false, _op: new Timestamp(0, 0) },
            baz : 'fubar'
          });
          should.deepEqual(items[3], {
            _id: { _co: '_processRemoteQueues', _id: 'foo', _v : 'B', _pe: 'I', _pa: ['A'], _i: 1 },
            _m3: { _ack: false, _op: new Timestamp(0, 0) },
            foo : 'bar',
            baz : 'fubar'
          });
          done();
        });
      });
    });
  });

  describe('saveOplogItem', function() {
    var collectionName = 'saveOplogItem';

    it('should callback with error on invalid oplog items', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc.saveOplogItem({ foo: 'bar' }, function(err) {
        'invalid oplogItem'.should.equal(err.message);
        done();
      });
    });

    it('should callback with the new document and no error', function(done) {
      var doc = { _id: 'raboof', foo: 'bar' };
      var item = { ts : 123, op : 'i', ns : 'qux.raboof', o : { _id: 'raboof', foo: 'bar' } };

      var vc = new VersionedCollection(db, collectionName, { debug: false });
      vc._collection.insert(doc, {w: 1}, function(err) {
        if (err) { throw err; }
        vc.saveOplogItem(item, function(err, newItem) {
          should.equal(err, null);
          // only check the new id
          newItem._id.should.have.property('_id', 'raboof');
          newItem._id.should.not.have.property('_ts');
          newItem._id.should.have.property('_v');
          done();
        }, function() {});
        vc.processQueues();
      });
    });

    /* test manually by removing the comments
    it('should proceed when soft limit is reached', function(done) {
      var doc = { _id: 'foo', foo: 'bar' };
      var item = { ts : 123, op : 'i', ns : 'qux.raboof', o : { _id: 'foo', foo: 'bar' } };

      var vc = new VersionedCollection(db, 'a', { debug: false });
      vc._queueLimit = 0;
      vc._collection.insert(doc, {w: 1}, function(err) {
        if (err) { throw err; }
        vc.saveOplogItem(item, function(err, newItem) {
          should.equal(err, null);
          // only check the new id
          newItem._id.should.have.property('_id', 'foo');
          newItem._id.should.not.have.property('_ts');
          newItem._id.should.have.property('_v');
          done();
        }, function() {});
        vc.processQueues();
      });
    });
    */

    it('should add an item and call back, eventually', function(done) {
      var item = {
        'ts' : new Timestamp(0, new Date() / 1000),
        'op' : 'i',
        'ns' : databaseName + '.mycoll',
        'o' : { '_id' : 'foo' }
      };
      // first clear the oplog collection
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc._clearSnapshot(function() {
        vc.saveOplogItem(item, function(err, newItem) {
          should.equal(newItem._id._id, 'foo');
          done();
        }, function() {});
        vc.processQueues();
      });
    });

    it('should add items in insertion order, with parent pointers, delete item should be last', function(done) {
      var oplogExamples = [
        { // 0 insert
          'ts' : new Timestamp(0, new Date() / 1000),
          'op' : 'i',
          'ns' : databaseName + '.mycoll',
          'o' : {
            '_id' : 'raboof',
            'foo' : 'bar'
          }
        },
        { // 1 update by full doc
          'ts' : new Timestamp(1, new Date() / 1000),
          'op' : 'u',
          'ns' : databaseName + '.mycoll',
          'o2' : { '_id' : 'raboof' },
          'o' : { foo: 'baz', '_v' : 3 }
        },
        { // 2 update by full doc
          'ts' : new Timestamp(2, new Date() / 1000),
          'op' : 'u',
          'ns' : databaseName + '.mycoll',
          'o2' : { '_id' : 'raboof' },
          'o' : { foo: 'qux', 'myVer' : 4, 'mySecondAttr' : 0 }
        },
        { // 3 update by modifier
          'ts' : new Timestamp(3, new Date() / 1000),
          'op' : 'u',
          'ns' : databaseName + '.mycoll',
          'o2' : { '_id' : 'raboof' },
          'o' : { '$set' : { 'mySecondAttr' : 2, foo: 'quux' } }
        },
        { // 4 delete
          'ts' : new Timestamp(4, new Date() / 1000),
          'op' : 'd',
          'ns' : databaseName + '.mycoll',
          'b' : true,
          'o' : { '_id' : 'raboof', foo: 'fubar' }
        }
      ];

      var vc = new VersionedCollection(db, 'saveOplogItemSequential', { debug: false });
      var lastItem = 0;
      var version1, version2, version3, version4;
      vc.saveOplogItem(oplogExamples[0], function(err, newItem) {
        if (err) { throw err; }
        should.not.exist(newItem._id._d);
        should.equal(lastItem, 0);
        should.deepEqual(newItem._id._pa, []);
        lastItem = 1;
        version1 = newItem._id._v;
        oplogExamples[1].o._v = version1;
      }, function() {});
      vc.processQueues(function(err) {
        if (err) { throw err; }
        vc.saveOplogItem(oplogExamples[1], function(err, newItem) {
          if (err) { throw err; }
          should.not.exist(newItem._id._d);
          should.equal(lastItem, 1);
          should.deepEqual(newItem._id._pa, [version1]);
          lastItem = 2;
          version2 = newItem._id._v;
          oplogExamples[2].o._v = version2;
        }, function() {});
        vc.processQueues(function(err) {
          if (err) { throw err; }
          var item;
          vc.saveOplogItem(oplogExamples[2], function(err, newItem) {
            if (err) { throw err; }
            should.not.exist(newItem._id._d);
            should.equal(lastItem, 2);
            should.deepEqual(newItem._id._pa, [version2]);
            lastItem = 3;
            version3 = newItem._id._v;

            // make sure this item has ackd set to true by applying a full doc update with correct new version
            item = {
              'ts' : new Timestamp(2, new Date() / 1000),
              'op' : 'u',
              'ns' : databaseName + '.mycoll',
              'o2' : { '_id' : 'raboof' },
              'o' : newItem
            };
            item.o._v = newItem._id._v;
            item.o._id = newItem._id._id;
            delete newItem._m3;
          }, function() {});
          vc.processQueues(function(err) {
            if (err) { throw err; }
            vc.saveOplogItem(item, function(err, newItem) {
              if (err) { throw err; }
              newItem._v = newItem._id._v;
              newItem._id = newItem._id._id;
              delete newItem._m3;
              should.deepEqual(newItem, item.o);
            }, function() {});
            vc.processQueues(function(err) {
              if (err) { throw err; }
              vc.saveOplogItem(oplogExamples[3], function(err, newItem) {
                if (err) { throw err; }
                should.not.exist(newItem._id._d);
                should.equal(lastItem, 3);
                should.deepEqual(newItem._id._pa, [version3]);
                lastItem = 4;
                version4 = newItem._id._v;
                oplogExamples[4].o._v = version4;
              }, function() {});
              vc.processQueues(function(err) {
                if (err) { throw err; }
                vc.saveOplogItem(oplogExamples[4], function(err, newItem) {
                  if (err) { throw err; }
                  should.exist(newItem._id._d);
                  should.equal(lastItem, 4);
                  should.deepEqual(newItem._id._pa, [version4]);
                  vc.stopAutoProcessing();
                }, function() {});
                vc.processQueues(done);
              });
            });
          });
        });
      });
    });
  });

  describe('_save', function() {
    var collectionName = '_save';

    it('should require cb to be a function', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function() { vc._save(); }).should.throw('cb must be a function');
    });

    it('should error on missing doc', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._save(null, function(err) {
        should.equal(err.message, 'Cannot read property \'_id\' of null');
        done();
      });
    });

    it('should error on invalid doc', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._save([], function(err) {
        should.equal(err.message, 'Cannot read property \'_id\' of undefined');
        done();
      });
    });

    it('should require doc to have an _id._id', function(done) {
      vColl._save({ _id: {} }, function(err) {
        should.equal(err.message, 'missing doc._id._id');
        done();
      });
    });

    it('should require doc to have an _id._v', function(done) {
      vColl._save({ _id: { _id: 'foo' }}, function(err) {
        should.equal(err.message, 'missing doc._id._v');
        done();
      });
    });

    it('should require doc to have an _id._pe', function(done) {
      vColl._save({ _id: { _id: 'foo', _v: 'A' }}, function(err) {
        should.equal(err.message, 'missing doc._id._pe');
        done();
      });
    });
  });

  describe('_syncDAGItemWithCollection', function() {
    var collectionName = '_syncDAGItemWithCollection';

    it('should require cb to be a function', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function() { vc._syncDAGItemWithCollection(); }).should.throw('cb must be a function');
    });

    it('should error on missing doc', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._syncDAGItemWithCollection(null, function(err) {
        should.equal(err.message, 'Cannot read property \'_id\' of null');
        done();
      });
    });

    it('should error on invalid doc', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._syncDAGItemWithCollection([], function(err) {
        should.equal(err.message, 'Cannot read property \'_id\' of undefined');
        done();
      });
    });

    it('should insert in collection', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      var vDoc = { _id: { _id: 'foo', _v: 'A' }, bar: 'baz' };
      vc._syncDAGItemWithCollection(vDoc, function(err) {
        if (err) { throw err; }
        vc._collection.find().toArray(function(err, items) {
          if (err) { throw err; }
          should.strictEqual(items.length, 1);
          should.deepEqual(items, [{ _id: 'foo', bar: 'baz', _v: 'A' }]);
          done();
        });
      });
    });

    it('should delete from collection', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      var vDoc = { _id: { _id: 'foo', _v: 'A', _d: true }, bar: 'baz' };
      vc._syncDAGItemWithCollection(vDoc, function(err) {
        if (err) { throw err; }
        vc._collection.find().toArray(function(err, items) {
          if (err) { throw err; }
          should.strictEqual(items.length, 0);
          done();
        });
      });
    });
  });

  describe('tail', function() {
    var collectionName = 'tail';
    var perspective = 'I';

    var A = {
      _id : { _id: 'foo', _v: 'A', _pe: 'I', _pa: [], _i: 1 },
      _m3: { _ack: true },
      baz : 'qux'
    };

    var B = {
      _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'], _i: 2 },
      _m3: { _ack: true },
      foo: 'bar'
    };

    var C = {
      _id : { _id: 'foo', _v: 'C', _pe: 'I', _pa: ['B'], _i: 3 },
      _m3: { _ack: true },
      baz : 'mux',
      foo: 'bar'
    };

    var D = {
      _id : { _id: 'foo', _v: 'D', _pe: 'I', _pa: ['C'], _i: 4 },
      _m3: { _ack: true },
      baz : 'qux'
    };

    var E = {
      _id : { _id: 'foo', _v: 'E', _pe: 'I', _pa: ['B'], _i: 5 },
      _m3: { _ack: true },
    };

    var F = {
      _id : { _id: 'foo', _v: 'F', _pe: 'I', _pa: ['E', 'C'], _i: 6 },
      _m3: { _ack: true },
      foo: 'bar'
    };

    var G = {
      _id : { _id: 'foo', _v: 'G', _pe: 'I', _pa: ['F'], _i: 7 },
      _m3: { _ack: true },
      baz : 'qux'
    };

    // same but without _id._pe and stripped m3 _ack
    var rA = { _id : { _id: 'foo', _v: 'A', _pa: [] }, _m3: {},
      baz : 'qux' };
    var rB = { _id : { _id: 'foo', _v: 'B', _pa: ['A'] }, _m3: {},
      foo: 'bar' };
    var rC = { _id : { _id: 'foo', _v: 'C', _pa: ['B'] }, _m3: {},
      baz : 'mux',
      foo: 'bar'  };
    var rD = { _id : { _id: 'foo', _v: 'D', _pa: ['C'] }, _m3: {},
      baz : 'qux' };
    var rE = { _id : { _id: 'foo', _v: 'E', _pa: ['B'] }, _m3: {} };
    var rF = { _id : { _id: 'foo', _v: 'F', _pa: ['E', 'C'] }, _m3: {},
      foo: 'bar' };
    var rG = { _id : { _id: 'foo', _v: 'G', _pa: ['F'] }, _m3: {},
      baz : 'qux' };

    // create the following structure:
    // A <-- B <-- C <-- D
    //        \     \             
    //         E <-- F <-- G

    it('should work with empty DAG and collection', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false });
      database.createCappedColl(vc.snapshotCollectionName, function(err) {
        if (err) { throw err; }
        vc.tail({}, '', function(err, item) {
          if (err) { throw err; }
          should.equal(item, null);
          done();
        });
      });
    });

    it('should tail without offset', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc.tail({}, '', function(err) {
        should.equal(err, null);
        done();
      });
    });

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      vc._snapshotCollection.insert([A, B, C, D, E, F, G], {w: 1}, done);
    });

    it('should require filter to be an object', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      vc.tail([], null, function(err) {
        should.equal(err.message, 'filter must be an object');
        done();
      });
    });

    it('should require offset to be a string', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      vc.tail({}, [], function(err) {
        should.equal(err.message, 'offset must be a string');
        done();
      });
    });

    it('should require options to be an object', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      vc.tail({}, '', [], function(err) {
        should.equal(err.message, 'options must be an object');
        done();
      });
    });

    it('should require options.transform to be a function', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      vc.tail({}, '', { transform: [] }, function(err) {
        should.equal(err.message, 'options.transform must be a function');
        done();
      });
    });

    it('should require options.follow to be a boolean', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      vc.tail({}, '', { follow: [] }, function(err) {
        should.equal(err.message, 'options.follow must be a boolean');
        done();
      });
    });

    it('should error if offset not found', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective, hide: true });
      vc.tail({}, 'X', { follow: false }, function(err, doc) {
        should.equal(err.message, 'offset not found');
        should.equal(doc, null);
        done();
      });
    });

    it('should return all elements when offset is empty', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective, debug: false });
      var docs = [];
      vc.tail({}, '', { follow: false }, function(err, doc, next) {
        should.equal(err, null);
        if (doc) {
          docs.push(doc);
          next();
        } else {
          should.equal(docs.length, 7);
          should.deepEqual(docs, [rA, rB, rC, rD, rE, rF, rG]);
          done();
        }
      });
    });

    it('should return only the last element if that is the offset', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective, debug: false });
      var docs = [];
      vc.tail({}, G._id._v, { follow: false }, function(err, doc, next) {
        if (err) { throw err; }
        if (doc) {
          docs.push(doc);
          next();
        } else {
          should.equal(docs.length, 1);
          should.deepEqual(docs, [rG]);
          done();
        }
      });
    });

    it('should return from offset E', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      var docs = [];
      vc.tail({}, E._id._v, { follow: false }, function(err, doc, next) {
        should.equal(err, null);
        if (doc) {
          docs.push(doc);
          next();
        } else {
          should.equal(docs.length, 3);
          should.deepEqual(docs[0], rE);
          should.deepEqual(docs[1], rF);
          should.deepEqual(docs[2], rG);
          done();
        }
      });
    });

    it('should return everything since offset C (including E)', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      var docs = [];
      vc.tail({}, C._id._v, { follow: false }, function(err, doc, next) {
        should.equal(err, null);
        if (doc) {
          docs.push(doc);
          next();
        } else {
          should.equal(docs.length, 5);
          should.deepEqual(docs[0], rC);
          should.deepEqual(docs[1], rD);
          should.deepEqual(docs[2], rE);
          should.deepEqual(docs[3], rF);
          should.deepEqual(docs[4], rG);
          done();
        }
      });
    });

    it('should return the complete DAG if filter is empty', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective, debug: false });
      var docs = [];
      vc.tail({}, A._id._v, { follow: false }, function(err, doc, next) {
        should.equal(err, null);
        if (doc) {
          docs.push(doc);
          next();
        } else {
          should.equal(docs.length, 7);
          should.deepEqual(docs[0], rA);
          should.deepEqual(docs[1], rB);
          should.deepEqual(docs[2], rC);
          should.deepEqual(docs[3], rD);
          should.deepEqual(docs[4], rE);
          should.deepEqual(docs[5], rF);
          should.deepEqual(docs[6], rG);
          done();
        }
      });
    });

    it('should not endup with two same parents A for G since F is a merge but not selected', function(done) {
      // should not find A twice for merge F
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      var docs = [];
      vc.tail({ baz: 'qux' }, A._id._v, { follow: false }, function(err, doc, next) {
        should.equal(err, null);
        if (doc) {
          docs.push(doc);
          next();
        } else {
          should.equal(docs.length, 3);
          should.deepEqual(docs[0], { _id : { _id: 'foo', _v: 'A', _pa: [] }, _m3: {}, baz : 'qux' });
          should.deepEqual(docs[1], { _id : { _id: 'foo', _v: 'D', _pa: ['A'] }, _m3: {}, baz : 'qux' });
          should.deepEqual(docs[2], { _id : { _id: 'foo', _v: 'G', _pa: ['A'] }, _m3: {}, baz : 'qux' });
          done();
        }
      });
    });

    it('should return only attrs with baz = mug and change root to C', function(done) {
      // should not find A twice for merge F
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      var docs = [];
      vc.tail({ baz: 'mux' }, A._id._v, { follow: false }, function(err, doc, next) {
        should.equal(err, null);
        if (doc) {
          docs.push(doc);
          next();
        } else {
          should.equal(docs.length, 1);
          should.deepEqual(docs[0], { _id : { _id: 'foo', _v: 'C', _pa: [] }, _m3: {}, baz : 'mux', foo: 'bar' });
          done();
        }
      });
    });

    it('should return only attrs with foo = bar and change root to B and alter subsequent parents', function(done) {
      // should not find A twice for merge F
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      var docs = [];
      vc.tail({ foo: 'bar' }, A._id._v, { follow: false }, function(err, doc, next) {
        should.equal(err, null);
        if (doc) {
          docs.push(doc);
          next();
        } else {
          should.equal(docs.length, 3);
          should.deepEqual(docs[0], { _id : { _id: 'foo', _v: 'B', _pa: [] }, _m3: {}, foo: 'bar' });
          should.deepEqual(docs[1], { _id : { _id: 'foo', _v: 'C', _pa: ['B'] }, _m3: {}, baz: 'mux', foo: 'bar' });
          should.deepEqual(docs[2], { _id : { _id: 'foo', _v: 'F', _pa: ['B', 'C'] }, _m3: {}, foo: 'bar' });
          done();
        }
      });
    });

    it('should return nothing if filters don\'t match any item', function(done) {
      // should not find A twice for merge F
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      var docs = [];
      vc.tail({ some: 'none' }, A._id._v, { follow: false }, function(err, doc, next) {
        should.equal(err, null);
        if (doc) {
          docs.push(doc);
          next();
        } else {
          should.equal(docs.length, 0);
          done();
        }
      });
    });

    it('should transform results', function(done) {
      // should not find A twice for merge F
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      var i = 0;
      var transform = function(obj) {
        delete obj.baz;
        obj.checked = true;
        if (i === 1) {
          obj.checked = false;
        }
        i++;
        return obj;
      };
      var docs = [];
      vc.tail({ baz: 'qux' }, A._id._v, { follow: false, transform: transform }, function(err, doc, next) {
        should.equal(err, null);
        if (doc) {
          docs.push(doc);
          next();
        } else {
          should.equal(docs.length, 3);
          should.deepEqual(docs[0], { _id : { _id: 'foo', _v: 'A', _pa: [] }, _m3: {}, checked : true });
          should.deepEqual(docs[1], { _id : { _id: 'foo', _v: 'D', _pa: ['A'] }, _m3: {}, checked : false });
          should.deepEqual(docs[2], { _id : { _id: 'foo', _v: 'G', _pa: ['A'] }, _m3: {}, checked : true });
          done();
        }
      });
    });

    it('should execute each hook and transform', function(done) {
      // should not find A twice for merge F
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      var i = 0;
      function transform(obj) {
        delete obj.baz;
        obj.checked = true;
        if (i === 1) {
          obj.checked = false;
        }
        i++;
        return obj;
      }
      function hook1(db, object, opts, callback) {
        object.hook1 = true;
        if (object._id._v === 'G') { object.hook1g = 'foo'; }
        callback(null, object);
      }
      function hook2(db, object, opts, callback) {
        if (object.hook1) {
          object.hook2 = true;
        }
        callback(null, object);
      }

      var docs = [];
      vc.tail({ baz: 'qux' }, A._id._v, { follow: false, transform: transform, hooks: [hook1, hook2] }, function(err, doc, next) {
        should.equal(err, null);
        if (doc) {
          docs.push(doc);
          next();
        } else {
          should.equal(docs.length, 3);
          should.deepEqual(docs[0], { _id : { _id: 'foo', _v: 'A', _pa: [] }, _m3: {}, checked : true, hook1: true, hook2: true });
          should.deepEqual(docs[1], { _id : { _id: 'foo', _v: 'D', _pa: ['A'] }, _m3: {}, checked : false, hook1: true, hook2: true});
          should.deepEqual(docs[2],
            { _id : { _id: 'foo', _v: 'G', _pa: ['A'] }, _m3: {}, checked : true, hook1g: 'foo', hook1: true, hook2: true});
          done();
        }
      });
    });

    it('should cancel hook execution and skip item if one hook filters', function(done) {
      // should not find A twice for merge F
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      function transform(obj) {
        delete obj.baz;
        obj.transformed = true;
        return obj;
      }
      // filter F. G should get parents of F which are E and C
      function hook1(db, object, opts, callback) {
        if (object._id._v === 'F') {
          return callback(null, null);
        }
        callback(null, object);
      }
      function hook2(db, object, opts, callback) {
        object.hook2 = true;
        callback(null, object);
      }

      var docs = [];
      vc.tail({}, E._id._v, { follow: false, transform: transform, hooks: [hook1, hook2] }, function(err, doc, next) {
        should.equal(err, null);
        if (doc) {
          docs.push(doc);
          next();
        } else {
          should.equal(docs.length, 2);
          should.deepEqual(docs[0], { _id : { _id: 'foo', _v: 'E', _pa: ['B'] }, _m3: {}, transformed : true, hook2: true });
          should.deepEqual(docs[1], { _id : { _id: 'foo', _v: 'G', _pa: ['E', 'C'] }, _m3: {}, transformed : true, hook2: true});
          done();
        }
      });
    });

    it('should return only attrs with foo = bar and change root to B and alter subsequent parents, filtered by hook', function(done) {
      function hook(db, object, opts, callback) {
        if (object.foo === 'bar') {
          return callback(null, object);
        }
        callback(null, null);
      }

      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      var docs = [];
      vc.tail({}, A._id._v, { follow: false, hooks: [hook] }, function(err, doc, next) {
        should.equal(err, null);
        if (doc) {
          docs.push(doc);
          next();
        } else {
          should.equal(docs.length, 3);
          should.deepEqual(docs[0], { _id : { _id: 'foo', _v: 'B', _pa: [] }, _m3: {}, foo: 'bar' });
          should.deepEqual(docs[1], { _id : { _id: 'foo', _v: 'C', _pa: ['B'] }, _m3: {}, baz: 'mux', foo: 'bar' });
          should.deepEqual(docs[2], { _id : { _id: 'foo', _v: 'F', _pa: ['B', 'C'] }, _m3: {}, foo: 'bar' });
          done();
        }
      });
    });

    it('should return only attrs with foo = bar and change root to B and alter subsequent parents, filtered by hook and offset = B', function(done) {
      function hook(db, object, opts, callback) {
        if (object.foo === 'bar') {
          return callback(null, object);
        }
        callback(null, null);
      }

      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective, debug: false });
      var docs = [];
      vc.tail({}, B._id._v, { follow: false, hooks: [hook] }, function(err, doc, next) {
        should.equal(err, null);
        if (doc) {
          docs.push(doc);
          next();
        } else {
          should.equal(docs.length, 3);
          should.deepEqual(docs[0], { _id : { _id: 'foo', _v: 'B', _pa: [] }, _m3: {}, foo: 'bar' });
          should.deepEqual(docs[1], { _id : { _id: 'foo', _v: 'C', _pa: ['B'] }, _m3: {}, baz: 'mux', foo: 'bar' });
          should.deepEqual(docs[2], { _id : { _id: 'foo', _v: 'F', _pa: ['B', 'C'] }, _m3: {}, foo: 'bar' });
          done();
        }
      });
    });

    it('should return the stream', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false });
      // needs an item in the snapshot in order for to keep tailing
      vc._snapshotCollection.insert({ a: 'b' }, function(err) {
        if (err) { throw err; }
        var r = vc.tail({}, '', function(err, item) {
          if (err) { throw err; }
          if (!item) { done(); }
        });
        should.strictEqual(typeof r === 'object', true);
        r.destroy();
      });
    });
  });

  describe('resolveVersionToIncrement', function() {
    var collectionName = 'resolveVersionToIncrement';

    var A = {
      _id : { _id: 'foo', _v: 'A', _pe: '_local', _pa: [], _i: 2 },
      _m3: { _ack: true },
      baz : 'qux'
    };

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([A], {w: 1}, done);
    });

    it('should require version to be string', function() {
      var vc = new VersionedCollection(db, collectionName, { debug: false });
      (function() { vc.resolveVersionToIncrement(); }).should.throw('version must be a string');
    });

    it('should require cb to be a function', function() {
      var vc = new VersionedCollection(db, collectionName, { debug: false });
      (function() { vc.resolveVersionToIncrement(''); }).should.throw('cb must be a function');
    });

    it('should error if version could not be resolved', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc.resolveVersionToIncrement('X', function(err) {
        should.equal(err.message, 'version could not be resolved to an increment');
        done();
      });
    });

    it('should return with i = 2', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false });
      vc.resolveVersionToIncrement('A', function(err, i) {
        if (err) { throw err; }
        should.strictEqual(i, 2);
        done();
      });
    });
  });

  describe('rebuild', function() {
    var collectionName = 'rebuild';

    it('should require cb to be a function', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function() { vc.rebuild({}); }).should.throw('cb must be a function');
    });

    it('should copy over all documents and copy back versions', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      var items = [
        { _id: 'bar', bar: 'qux' },
        { _id: 'baz', raboof: 'foobar', _v: 'A' },
        { _id: 'foo', foo: 'bar' }
      ];
      vc._collection.insert(items, {w: 1}, function(err, inserts) {
        if (err) { throw err; }
        should.equal(inserts.length, items.length);

        vc.rebuild(function(err) {
          if (err) { throw err; }
          vc._snapshotCollection.find({}, { sort: { '_id._id': 1 } }).toArray(function(err, snapshotItems) {
            should.equal(err, null);
            should.equal(snapshotItems.length, 3);

            var s1 = snapshotItems[0]._id._v;
            s1.should.match(/^[a-z0-9/+A-Z]{8}$/);
            delete snapshotItems[0]._id._v;
            should.deepEqual(snapshotItems[0], {
              _id: { _co: 'rebuild', _id: 'bar', _pe: '_local', _pa: [], _lo: true, _i: 1 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) },
              bar: 'qux'
            });

            // the version should not be altered
            should.deepEqual(snapshotItems[1], {
              _id: { _co: 'rebuild', _id: 'baz', _v: 'A', _pe: '_local', _pa: [], _lo: true, _i: 2 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) },
              raboof: 'foobar',
            });

            var s2 = snapshotItems[2]._id._v;
            s2.should.match(/^[a-z0-9/+A-Z]{8}$/);
            delete snapshotItems[2]._id._v;
            should.deepEqual(snapshotItems[2], {
              _id: { _co: 'rebuild', _id: 'foo', _pe: '_local', _pa: [], _lo: true, _i: 3 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) },
              foo: 'bar'
            });

            // should have updated the versions in the collection
            vc._collection.find({}, { sort: { _id: 1 } }).toArray(function(err, collItems) {
              should.equal(err, null);
              should.equal(collItems.length, 3);

              should.equal(collItems[0]._v, s1);
              should.equal(collItems[1]._v, 'A');
              should.equal(collItems[2]._v, s2);
              done();
            });
          });
        });
      });
    });
  });

  describe('invalidItem', function() {
    it('should require item to be an object', function() {
      VersionedCollection.invalidItem([]).should.equal('item must be an object');
    });

    it('should require item._id to be an object', function() {
      VersionedCollection.invalidItem({ _id: [] }).should.equal('item._id must be an object');
    });

    it('should require item._id._id', function() {
      VersionedCollection.invalidItem({ _id: {} }).should.equal('missing item._id._id');
    });

    it('should require item._id._v to be a string', function() {
      VersionedCollection.invalidItem({ _id: { _id: 'foo', _v: [] } }).should.equal('item._id._v must be a string');
    });

    it('should require item._id._pe to be a string', function() {
      VersionedCollection.invalidItem({ _id: { _id: 'foo', _v: 'A', _pe: [] } }).should.equal('item._id._pe must be a string');
    });

    it('should require item._id._pa to be an array', function() {
      VersionedCollection.invalidItem({
        _id: { _id: 'foo', _v: 'A', _pe: 'I', _pa: {} }
      }).should.equal('item._id._pa must be an array');
    });

    it('should be a valid item', function() {
      VersionedCollection.invalidItem({ _id: { _id: 'foo', _v: 'A', _pe: 'I', _pa: [] } }).should.equal('');
    });
  });

  describe('ensureSnapshotCollection', function() {
    var collectionName = 'ensureSnapshotCollection';

    it('should require size to be a number', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function() { vc.ensureSnapshotCollection(function() {}); }).should.throw('size must be a number');
    });

    it('should require free to be a number', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function() { vc.ensureSnapshotCollection(0, [], {}); }).should.throw('free must be a number');
    });

    it('should require cb to be a function', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function() { vc.ensureSnapshotCollection(0, 1); }).should.throw('cb must be a function');
    });

    it('should recreate if empty collection is not capped', function(done) {
      db.createCollection('m3.ensureSnapshotCollectionExists', function(err) {
        if (err) { throw err; }
        var vc = new VersionedCollection(db, 'ensureSnapshotCollectionExists', { hide: true });
        vc.ensureSnapshotCollection(1024, function(err) {
          if (err) { throw err; }
          vc._snapshotCollection.isCapped(function(err, result) {
            if (err) { throw err; }
            should.strictEqual(result, true);
            done();
          });
        });
      });
    });

    it('0.1: needs a document in de collection for further testing', function(done) {
      var vc = new VersionedCollection(db, 'ensureSnapshotCollectionFree', { debug: false });
      var docs = [];
      for (var i = 0; i < 120; i++) {
        docs.push({
          _id: i
        });
      }
      vc.ensureSnapshotCollection(4096, function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.insert(docs, { w: 1 }, done);
      });
    });

    it('0.2 should error if less than 5% is free', function(done) {
      var vc = new VersionedCollection(db, 'ensureSnapshotCollectionFree', { debug: false, hide: true });
      vc.ensureSnapshotCollection(4096, 60, function(err) {
        should.equal(err.message, 'not enough free space');
        done();
      });
    });

    it('needs a recreated non-capped collection with a document', function(done) {
      var coll = db.collection('m3.ensureSnapshotCollectionExists');
      coll.drop(function(err) {
        if (err) { throw err; }
        coll.insert({ foo: 'bar' }, function(err, inserted) {
          if (err) { throw err; }
          should.equal(inserted.length, 1);
          done();
        });
      });
    });

    it('should error if non-empty collection exists that is not capped', function(done) {
      var vc = new VersionedCollection(db, 'ensureSnapshotCollectionExists', { hide: true });
      vc.ensureSnapshotCollection(1024, function(err) {
        should.equal(err.message, 'snapshot collection not capped');
        vc._snapshotCollection.isCapped(function(err, result) {
          if (err) { throw err; }
          should.equal(result, undefined);
          done();
        });
      });
    });

    it('1.1: needs a document in de collection for further testing', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._collection.insert({ _id: 'foo', foo: 'bar', _v: 'X' }, function(err, inserted) {
        if (err) { throw err; }
        should.equal(inserted.length, 1);
        done();
      });
    });

    it('1.2: should rebuild and create a capped collection with one document', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.isCapped(function(err, result) {
        if (err) { throw err; }
        should.equal(result, null);

        vc.ensureSnapshotCollection(8192, function(err) {
          if (err) { throw err; }
          vc._snapshotCollection.stats(function(err, stats) {
            if (err) { throw err; }

            should.strictEqual(stats.capped, true);
            should.strictEqual(stats.count, 1);
            should.strictEqual(stats.storageSize, 8192);
            done();
          });
        });
      });
    });

    it('1.3: should have versioned the document in the normal collection', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._collection.find().toArray(function(err, citems) {
        if (err) { throw err; }
        vc._snapshotCollection.find().toArray(function(err, vcitems) {
          if (err) { throw err; }

          should.equal(citems.length, 1);
          should.equal(vcitems.length, 1);

          should.deepEqual(citems[0], { _id: 'foo', _v: 'X', foo: 'bar' });
          should.deepEqual(vcitems[0], {
            _id: { _co: 'ensureSnapshotCollection', _id: 'foo', _v: 'X', _pa: [],  _pe: '_local', _lo: true, _i: 1 },
            _m3: { _ack: false },
            foo: 'bar'
          });
          done();
        });
      });
    });

    it('2: should error if collection exists but is too small', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc.ensureSnapshotCollection(16000, function(err) {
        should.equal(err.message, 'snapshot collection too small');
        done();
      });
    });

    it('3: should not error or recreate if capped collection already exists and is big enough', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc.ensureSnapshotCollection(4096, function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.stats(function(err, stats) {
          if (err) { throw err; }

          should.strictEqual(stats.capped, true);
          should.strictEqual(stats.count, 1);
          should.strictEqual(stats.storageSize, 8192);
          done();
        });
      });
    });

    it('1.1: should drop the collection and the versioned collection', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._collection.drop(function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.drop(function(err) {
          if (err) { throw err; }
          done();
        });
      });
    });

    it('1.2: should create an empty capped collection', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc.ensureSnapshotCollection(4096, function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.stats(function(err, stats) {
          if (err) { throw err; }

          should.strictEqual(stats.capped, true);
          should.strictEqual(stats.count, 0);
          should.strictEqual(stats.storageSize, 4096);
          done();
        });
      });
    });

    it('2: should recreate if versioned collection exists but is too small and empty', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc.ensureSnapshotCollection(8192, function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.stats(function(err, stats) {
          if (err) { throw err; }

          should.strictEqual(stats.capped, true);
          should.strictEqual(stats.count, 0);
          should.strictEqual(stats.storageSize, 8192);
          done();
        });
      });
    });

    it('3: should not error or recreate if capped collection already exists and is big enough', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc.ensureSnapshotCollection(4096, function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.stats(function(err, stats) {
          if (err) { throw err; }

          should.strictEqual(stats.capped, true);
          should.strictEqual(stats.count, 0);
          should.strictEqual(stats.storageSize, 8192);
          done();
        });
      });
    });

    it('should use the provided size on new collections', function(done) {
      var vc = new VersionedCollection(db, collectionName+'Size');
      vc.ensureSnapshotCollection(123, function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.options(function(err, options) {
          if (err) { throw err; }
          should.equal(options.size, 123);
          done();
        });
      });
    });
  });

  describe('processQueues', function() {
    var collectionName = 'processQueues';

    it('should require cb to be a function', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function() { vc.processQueues({}); }).should.throw('cb must be a function');
    });

    it('should run', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc.processQueues(done);
    });

    it('should error when locked', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc.processQueues(function(err) {
        if (err) { throw err; }
      });
      vc.processQueues(function(err) {
        should.equal(err.message, 'already processing queues');
        done();
      });
    });
  });

  describe('cloneDAGItem', function() {
    var collectionName = 'cloneDAGItem';

    it('should create a new empty object', function() {
      var dagItem = {};

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.cloneDAGItem(dagItem);
      should.strictEqual(result == dagItem, false);
      should.deepEqual(dagItem, {});
    });

    it('should create a shallow copy of _id._id with a date object', function() {
      var dagItem = { _id: { _id: new Date(123456789) } };

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.cloneDAGItem(dagItem);
      should.strictEqual(result == dagItem, false);
      should.strictEqual(result._id._id == dagItem._id._id, true);
      should.deepEqual(dagItem, { _id: { _id: new Date(123456789) } });
    });

    it('should create a shallow copy of _id._id with an object id', function() {
      var oid = '0000007b1a5fe5c91e000002';
      var dagItem = { _id: { _id: new ObjectID(oid) } };

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.cloneDAGItem(dagItem);
      should.strictEqual(result == dagItem, false);
      should.strictEqual(result._id._id == dagItem._id._id, true);
      should.deepEqual(dagItem, { _id: { _id: new ObjectID(oid) } });
    });

    it('should create a new _id', function() {
      var dagItem = { _id: { _id: 'A', _v: 'A', _pa: [], _lo: true, _i: 10, _d: true }, a: 'b', _m3: { _ack: true } };

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.cloneDAGItem(dagItem);
      should.strictEqual(result == dagItem, false);
      should.strictEqual(result._id == dagItem._id, false);
      should.deepEqual(dagItem, { _id: { _id: 'A', _v: 'A', _pa: [], _lo: true, _i: 10, _d: true }, a: 'b', _m3: { _ack: true } });
    });

    it('should create a new _m3', function() {
      var dagItem = { _id: { _id: 'A', _v: 'A', _pa: [], _lo: true, _i: 10, _d: true }, a: 'b', _m3: { _ack: true } };

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.cloneDAGItem(dagItem);
      should.strictEqual(result == dagItem, false);
      should.strictEqual(result._m3 == dagItem._m3, false);
      should.deepEqual(dagItem, { _id: { _id: 'A', _v: 'A', _pa: [], _lo: true, _i: 10, _d: true }, a: 'b', _m3: { _ack: true } });
    });
  });

  describe('compareDAGItems', function() {
    var collectionName = 'compareDAGItems';

    it('should return true if items are empty', function() {
      var dagItem1 = {};
      var dagItem2 = {};

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.compareDAGItems(dagItem1, dagItem2);
      should.strictEqual(result, true);
      should.deepEqual(dagItem1, {});
      should.deepEqual(dagItem2, {});
    });

    it('should return true if dates are equal', function() {
      var dagItem1 = { foo: new Date(123456789) };
      var dagItem2 = { foo: new Date(123456789) };

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.compareDAGItems(dagItem1, dagItem2);
      should.strictEqual(result, true);
      should.deepEqual(dagItem1, { foo: new Date(123456789) });
      should.deepEqual(dagItem2, { foo: new Date(123456789) });
    });

    it('should return false if dates are inequal', function() {
      var dagItem1 = { foo: new Date(123456780) };
      var dagItem2 = { foo: new Date(123456789) };

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.compareDAGItems(dagItem1, dagItem2);
      should.strictEqual(result, false);
      should.deepEqual(dagItem1, { foo: new Date(123456780) });
      should.deepEqual(dagItem2, { foo: new Date(123456789) });
    });

    it('should return true if object ids for _id are equal', function() {
      var oid = '0000007b1a5fe5c91e000002';
      var dagItem1 = { _id: { _id: new ObjectID(oid) } };
      var dagItem2 = { _id: { _id: new ObjectID(oid) } };

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.compareDAGItems(dagItem1, dagItem2);
      should.strictEqual(result, true);
      should.deepEqual(dagItem1, { _id: { _id: new ObjectID(oid) } });
      should.deepEqual(dagItem2, { _id: { _id: new ObjectID(oid) } });
    });

    it('should return false if object ids for _id are not equal', function() {
      var oid = '0000007b1a5fe5c91e000002';
      var dagItem1 = { foo: new ObjectID(oid) };
      var dagItem2 = { foo: new ObjectID('0000007b1a5fe5c91e000003') };

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.compareDAGItems(dagItem1, dagItem2);
      should.strictEqual(result, false);
      should.deepEqual(dagItem1, { foo: new ObjectID(oid) });
      should.deepEqual(dagItem2, { foo: new ObjectID('0000007b1a5fe5c91e000003') });
    });

    it('should return false if object ids for other attributes are not equal', function() {
      var oid = '0000007b1a5fe5c91e000002';
      var dagItem1 = { foo: new ObjectID(oid) };
      var dagItem2 = { foo: new ObjectID('0000007b1a5fe5c91e000003') };

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.compareDAGItems(dagItem1, dagItem2);
      should.strictEqual(result, false);
      should.deepEqual(dagItem1, { foo: new ObjectID(oid) });
      should.deepEqual(dagItem2, { foo: new ObjectID('0000007b1a5fe5c91e000003') });
    });

    it('should return true if _m3 is equal', function() {
      var dagItem1 = { foo: new Date(123456789), _m3: { _ack: false } };
      var dagItem2 = { foo: new Date(123456789), _m3: { _ack: false } };

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.compareDAGItems(dagItem1, dagItem2);
      should.strictEqual(result, true);
      should.deepEqual(dagItem1, { foo: new Date(123456789), _m3: { _ack: false } });
      should.deepEqual(dagItem2, { foo: new Date(123456789), _m3: { _ack: false } });
    });

    it('should return false if _m3 is inequal', function() {
      var dagItem1 = { foo: new Date(123456780), _m3: { _ack: true } };
      var dagItem2 = { foo: new Date(123456789), _m3: { _ack: false } };

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.compareDAGItems(dagItem1, dagItem2);
      should.strictEqual(result, false);
      should.deepEqual(dagItem1, { foo: new Date(123456780), _m3: { _ack: true } });
      should.deepEqual(dagItem2, { foo: new Date(123456789), _m3: { _ack: false } });
    });

    it('should delete _id._v or another _id attribute', function() {
      var dagItem1 = { _id: { _id: 'A', _v: 'A', _pa: [], _lo: true, _i: 10, _d: true } };
      var dagItem2 = { _id: { _id: 'A', _v: 'A', _pa: [], _lo: true, _i: 11, _d: true } };

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.compareDAGItems(dagItem1, dagItem2);
      should.strictEqual(result, false);
      should.deepEqual(dagItem1, { _id: { _id: 'A', _v: 'A', _pa: [], _lo: true, _i: 10, _d: true } });
      should.deepEqual(dagItem2, { _id: { _id: 'A', _v: 'A', _pa: [], _lo: true, _i: 11, _d: true } });
    });
  });

  describe('equalValues', function() {
    it('should return true if object has equal keys and values', function() {
      var oid = '0000007b1a5fe5c91e000002';
      var val1 = { foo: new ObjectID(oid) };
      var val2 = { foo: new ObjectID(oid) };

      var result = VersionedCollection.equalValues(val1, val2);
      should.strictEqual(result, true);
    });

    it('should return false if objects differ', function() {
      var val1 = { foo: new ObjectID('0000007b1a5fe5c91e000002') };
      var val2 = { foo: new ObjectID('0000007b1a5fe5c91e000003') };

      var result = VersionedCollection.equalValues(val1, val2);
      should.strictEqual(result, false);
    });

    it('should return false if keys are not equal', function() {
      var oid = '0000007b1a5fe5c91e000002';
      var val1 = { foo: new ObjectID(oid) };
      var val2 = { bar: new ObjectID(oid) };

      var result = VersionedCollection.equalValues(val1, val2);
      should.strictEqual(result, false);
    });

    it('should return false if one object has different keys', function() {
      var oid = '0000007b1a5fe5c91e000002';
      var val1 = { foo: new ObjectID(oid) };
      var val2 = { foo: new ObjectID(oid), bar: true };

      var result = VersionedCollection.equalValues(val1, val2);
      should.strictEqual(result, false);
    });
  });

  describe('compareDAGItemWithCollectionItem', function() {
    var collectionName = 'compareDAGItemWithCollectionItem';

    it('should return true if items are empty', function() {
      var dagItem = { };
      var collectionItem = {};

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.compareDAGItemWithCollectionItem(dagItem, collectionItem);
      should.strictEqual(result, true);
    });

    it('should return true if dates are equal', function() {
      var dagItem = { foo: new Date(123456789) };
      var collectionItem = { foo: new Date(123456789) };

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.compareDAGItemWithCollectionItem(dagItem, collectionItem);
      should.strictEqual(result, true);
    });

    it('should return false if dates are inequal', function() {
      var dagItem = { foo: new Date(123456780) };
      var collectionItem = { foo: new Date(123456789) };

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.compareDAGItemWithCollectionItem(dagItem, collectionItem);
      should.strictEqual(result, false);
    });

    it('should return true if object ids for _id are equal', function() {
      var oid = '0000007b1a5fe5c91e000002';
      var dagItem = { _id: { _id: new ObjectID(oid) } };
      var collectionItem = { _id: new ObjectID(oid) };

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.compareDAGItemWithCollectionItem(dagItem, collectionItem);
      should.strictEqual(result, true);
    });

    it('should return false if object ids for _id are not equal', function() {
      var oid = '0000007b1a5fe5c91e000002';
      var dagItem = { foo: new ObjectID(oid) };
      var collectionItem = { foo: new ObjectID('0000007b1a5fe5c91e000003') };

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.compareDAGItemWithCollectionItem(dagItem, collectionItem);
      should.strictEqual(result, false);
    });

    it('should return false if object ids for other attributes are not equal', function() {
      var oid = '0000007b1a5fe5c91e000002';
      var dagItem = { foo: new ObjectID(oid) };
      var collectionItem = { foo: new ObjectID('0000007b1a5fe5c91e000003') };

      var vc = new VersionedCollection(db, collectionName);
      var result = vc.compareDAGItemWithCollectionItem(dagItem, collectionItem);
      should.strictEqual(result, false);
    });
  });

  describe('versionDoc', function() {
    var collectionName = 'versionDoc';

    it('should create a new _id object with the original _id nested', function() {
      var vc = new VersionedCollection(db, collectionName);
      var myDoc = vc.versionDoc({ _id: 1 });
      myDoc._id._id.should.equal(1);
    });

    it('should not alter the original object', function() {
      var vc = new VersionedCollection(db, collectionName);
      var doc = { _id: 'findById' };
      vc.versionDoc(doc);
      doc.should.have.property('_id', 'findById');
    });

    it('should have generated a _v property', function() {
      var vc = new VersionedCollection(db, collectionName);
      var myDoc = vc.versionDoc({ _id: 1 });
      myDoc._id._v.should.match(/^[a-z0-9/+A-Z]{8}$/);
    });

    it('should move _v property', function() {
      var vc = new VersionedCollection(db, collectionName);
      var myDoc = vc.versionDoc({ _id: 1, _v: 'A' }, true);
      should.equal(myDoc._id._v, 'A');
      should.equal(myDoc._v, null);
    });

    it('should overwrite version by default', function() {
      var vc = new VersionedCollection(db, collectionName, { versionKey: 'rev' });
      var myDoc = vc.versionDoc({ _id: 1, rev: 'A' });
      myDoc._id._v.should.match(/^[a-z0-9/+A-Z]{8}$/);
      should.equal(myDoc.rev, null);
    });

    it('should not overwrite version', function() {
      var vc = new VersionedCollection(db, collectionName, { versionKey: 'rev' });
      var myDoc = vc.versionDoc({ _id: 1, rev: 'A' }, true);
      should.equal(myDoc._id._v, 'A');
      should.equal(myDoc.rev, null);
    });
  });

  describe('diff', function() {
    it('should not show any differences', function() {
      var x = { foo: 'bar' };
      var y = { foo: 'bar' };
      var result = VersionedCollection.diff(x, y);
      should.deepEqual(result, { });
    });

    it('should show changed on one attribute', function() {
      var x = { foo: 'bar' };
      var y = { foo: 'baz' };
      var result = VersionedCollection.diff(x, y);
      should.deepEqual(result, { foo: '~' });
    });

    it('should not conflict on dates', function() {
      var time = new Date();
      var time2 = new Date(time);
      var x = { foo: time };
      var y = { foo: time2 };
      var result = VersionedCollection.diff(x, y);
      should.deepEqual(result, { });
    });

    it('should process equal object values', function() {
      var x = { foo: { a: 1 } };
      var y = { foo: { a: 1 } };
      var result = VersionedCollection.diff(x, y);
      should.deepEqual(result, { });
    });

    it('should process different object values', function() {
      var x = { foo: { a: 1 } };
      var y = { foo: { a: 2 } };
      var result = VersionedCollection.diff(x, y);
      should.deepEqual(result, { foo: '~' });
    });

    it('should process empty items', function() {
      var x = { };
      var y = { };
      var result = VersionedCollection.diff(x, y);
      should.deepEqual(result, { });
    });

    it('should show deleted keys in item', function() {
      var x = { };
      var y = { foo: 'bar' };
      var result = VersionedCollection.diff(x, y);
      should.deepEqual(result, { foo: '-' });
    });

    it('should show added keys in item', function() {
      var x = { foo: 'bar' };
      var y = { };
      var result = VersionedCollection.diff(x, y);
      should.deepEqual(result, { foo: '+' });
    });

    it('should do a more complex diff', function() {
      var x = { foo: 'bar', bar: 'baz', baz: 'qux',                 fubar: { a: 'b', c: 'd' }, foobar: { a: 'b', c: 'e' } };
      var y = {             bar: 'baz', baz: 'quux', qux: 'raboof', fubar: { a: 'b', c: 'e' }, foobar: { a: 'b', c: 'e' } };
      var result = VersionedCollection.diff(x, y);
      should.deepEqual(result, {
        foo: '+',
        baz: '~',
        qux: '-',
        fubar: '~'
      });
    });

    it('should not alter the original objects', function() {
      var x = { foo: 'bar', bar: 'baz', baz: 'qux'                 };
      var y = {             bar: 'baz', baz: 'quux', qux: 'raboof' };
      var result = VersionedCollection.diff(x, y);
      should.deepEqual(result, {
        foo: '+',
        baz: '~',
        qux: '-',
      });
      should.deepEqual(x, { foo: 'bar', bar: 'baz', baz: 'qux' });
      should.deepEqual(y, { bar: 'baz', baz: 'quux', qux: 'raboof' });
    });
  });

  describe('oplogUpdateContainsModifier', function() {
    it('should return false on non-objects', function() {
      should.equal(VersionedCollection.oplogUpdateContainsModifier(), false);
    });

    it('should return false on objects without o', function() {
      should.equal(VersionedCollection.oplogUpdateContainsModifier({ }), false);
    });

    it('should return false on objects where o is an array', function() {
      should.equal(VersionedCollection.oplogUpdateContainsModifier({ o: ['$set'] }), false);
    });

    it('should return true if first key is a string starting with "$"', function() {
      should.equal(VersionedCollection.oplogUpdateContainsModifier({ o: { '$set': 'foo' } }), true);
    });

    it('should return false if first key does not start with "$"', function() {
      should.equal(VersionedCollection.oplogUpdateContainsModifier({ o: { 'set': 'foo', '$set': 'bar' } }), false);
    });

    it('should return true if only first key does starts with "$"', function() {
      should.equal(VersionedCollection.oplogUpdateContainsModifier({ o: { '$set': 'foo', 'set': 'bar' } }), true);
    });

    it('should return true if all keys start with "$"', function() {
      should.equal(VersionedCollection.oplogUpdateContainsModifier({ o: { '$set': 'foo', '$in': 'bar' } }), true);
    });
  });

  describe('createNewVersionByUpdateDoc', function() {
    var collectionName = 'createNewVersionByUpdateDoc';

    var dagItem = {
      _id: { _id: 'foo', _v: 'A', _pe: '_local', _pa: [] },
      _m3: { _ack: true },
      bar: 'qux'
    };
    var mod = { $set: { bar: 'baz' } };
    var oplogItem = { ts: new Timestamp(1414516132, 1), o: mod, op: 'u', o2: { _id: 'foo', _v: 'A' } };

    it('should require dagItem to be an object', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function() { vc.createNewVersionByUpdateDoc(); }).should.throw('dagItem must be an object');
    });

    it('should require oplogItem to be an object', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function() { vc.createNewVersionByUpdateDoc({}); }).should.throw('oplogItem must be an object');
    });

    it('should require cb to be a function', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function() { vc.createNewVersionByUpdateDoc({}, {}); }).should.throw('cb must be a function');
    });

    it('should require op to be "u"', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      var updateItem  = {
        ts: 123,
        op: 'i',
        ns : 'qux.raboof',
        o2: { _id: 'baz' },
        o: { $set: { qux: 'quux' } }
      };
      vc.createNewVersionByUpdateDoc(dagItem, updateItem, function(err) {
        should.equal(err.message, 'oplogItem op must be "u"');
        done();
      });
    });

    it('should require an o2 object', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc.createNewVersionByUpdateDoc(dagItem, { o: mod, op: 'u' }, function(err) {
        should.equal(err.message, 'Cannot read property \'_id\' of undefined');
        done();
      });
    });

    it('should require oplogItem.o2._id', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc.createNewVersionByUpdateDoc(dagItem, { o: mod, op: 'u', o2: { } }, function(err) {
        should.equal(err.message, 'missing oplogItem.o2._id');
        done();
      });
    });

    it('should require o._id to not exist', function(done) {
      var item = {
        o: { $foo: 'bar', _id: 'applyOplogItemTest' },
        op: 'u',
        o2: { _id: 'applyOplogItemTest' }
      };
      var vc = new VersionedCollection(db, collectionName);
      vc.createNewVersionByUpdateDoc(dagItem, item, function(err) {
        should.equal(err.message, 'oplogItem contains o._id');
        done();
      });
    });

    it('should create a new version', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true });
      vc.createNewVersionByUpdateDoc(dagItem, oplogItem, function(err, item) {
        if (err) { throw err; }

        // check if version is generated
        var v = item._id._v;
        v.should.match(/^[a-z0-9/+A-Z]{8}$/);
        delete item._id._v;
        should.deepEqual(item, {
          _id: { _co: 'createNewVersionByUpdateDoc', _id: 'foo', _pe: '_local', _pa: ['A'], _lo: true },
          _m3: { _ack: false, _op: new Timestamp(1414516132, 1) },
          bar: 'baz'
        });
        done();
      });
    });

    it('should not have altered the original doc and have side-effects', function() {
      should.deepEqual(oplogItem, {
        ts: new Timestamp(5709483428, 1),
        o: { $set: { bar: 'baz' } },
        op: 'u',
        o2: { _id: 'foo', _v: 'A' }
      });
    });
  });

  describe('mergeAndSave', function() {
    var collectionName = 'mergeAndSave';

    var A = { _id : { _id: 'foo', _v: 'A', _pe: '_local', _i:  1, _pa: [] } };
    var B = { _id : { _id: 'foo', _v: 'B', _pe: '_local', _i:  2, _pa: ['A'] } };
    var C = { _id : { _id: 'foo', _v: 'C', _pe: '_local', _i:  3, _pa: ['B'] } };
    var D = { _id : { _id: 'foo', _v: 'D', _pe: '_local', _i:  4, _pa: ['C'] } };
    var E = { _id : { _id: 'foo', _v: 'E', _pe: '_local', _i:  5, _pa: ['B'] } };
    var F = { _id : { _id: 'foo', _v: 'F', _pe: '_local', _i:  6, _pa: ['E', 'C'] } };
    var G = { _id : { _id: 'foo', _v: 'G', _pe: '_local', _i:  7, _pa: ['F'] } };
    var H = { _id : { _id: 'foo', _v: 'H', _pe: '_local', _i:  8, _pa: ['F'] } };
    var J = { _id : { _id: 'foo', _v: 'J', _pe: '_local', _i:  9, _pa: ['H'] } };
    var K = { _id : { _id: 'foo', _v: 'K', _pe: '_local', _i: 10, _pa: ['J'] } };
    //var I = { _id : { _id: 'foo', _v: 'I', _pe: '_local', _i: 11, _pa: ['H', 'G', 'D'] } };

    // create the following structure:
    // A <-- B <-- C <----- D
    //        \     \
    //         E <-- F <-- G
    //                \
    //                 H
    //                  \
    //                   J <-- K
    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([A, B, C, D, E, F, G, H, J, K], {w: 1}, done);
    });

    it('should require dagItems to be an array', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function () { vc.mergeAndSave({}); }).should.throwError('dagItems must be an array');
    });

    it('should require cb to be a function', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function () { vc.mergeAndSave([], []); }).should.throwError('cb must be a function');
    });

    it('should require all ids to be equal', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      var items = [{ _id: { _id: 'foo', _pe: 'I' } }, { _id: { _id: 'bar', _pe: 'I' } }];
      vc.mergeAndSave(items, function(err) {
        should.deepEqual(err.message, 'ids must be equal');
        done();
      });
    });

    it('should require all perspectives to be equal', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      var items = [{ _id: { _id: 'foo', _pe: 'I' } }, { _id: { _id: 'foo', _pe: 'II' } }];
      vc.mergeAndSave(items, function(err) {
        should.deepEqual(err.message, 'perspectives must be equal');
        done();
      });
    });

    it('should merge D, G, K by creating two merges', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc.mergeAndSave([D, G, K], function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.find().toArray(function(err, items) {
          if (err) { throw err; }

          should.strictEqual(items.length, 12);

          var v1 = items[10]._id._v;
          v1.should.match(/^[a-z0-9/+A-Z]{8}$/);
          should.deepEqual(items[10], {
            _id : { _co: 'mergeAndSave', _id: 'foo', _v: v1, _pa: ['D', 'G'], _pe: '_local', _lo: true, _i: 11 },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });

          var v2 = items[11]._id._v;
          v2.should.match(/^[a-z0-9/+A-Z]{8}$/);
          should.deepEqual(items[11], {
            _id : { _co: 'mergeAndSave', _id: 'foo', _v: v2, _pa: [v1, 'K'], _pe: '_local', _lo: true, _i: 12 },
            _m3: { _ack: false, _op: new Timestamp(0, 0) }
          });
          done();
        });
      });
    });
  });

  describe('runHooks', function() {
    it('should accept empty array', function(done) {
      var item = { foo: 'bar' };
      VersionedCollection.runHooks([], null, item, null, function(err, newItem) {
        if (err) { throw err; }
        should.deepEqual(newItem, { foo: 'bar' });
        done();
      });
    });

    it('should run both hooks', function(done) {
      var item = { foo: 'bar' };
      var hooks = [
        function(db, item, opts, cb) {
          item.hookPassed = true;
          cb(null, item);
        },
        function(db, item, opts, cb) {
          item.secondHook = true;
          cb(null, item);
        }
      ];

      VersionedCollection.runHooks(hooks, null, item, null, function(err, newItem) {
        if (err) { throw err; }
        should.deepEqual(newItem, {
          foo: 'bar',
          hookPassed: true,
          secondHook: true
        });
        done();
      });
    });

    it('should cancel executing hooks as soon as one filters the item', function(done) {
      var item = { foo: 'bar' };
      var hooks = [
        function(db, item, opts, cb) {
          cb(null, null);
        },
        function(db, item, opts, cb) {
          item.secondHook = true;
          cb(null, item);
        }
      ];

      VersionedCollection.runHooks(hooks, null, item, null, function(err, newItem) {
        if (err) { throw err; }
        should.deepEqual(newItem, null);
        done();
      });
    });

    it('should pass the options to each hook', function(done) {
      var hooks = [
        function(db, item, opts, cb) {
          should.strictEqual(opts.baz, true);
          cb();
        }
      ];

      var item = { foo: 'bar' };

      VersionedCollection.runHooks(hooks, null, item, { baz: true }, done);
    });
  });



  /////////////////////
  //// PRIVATE API ////
  /////////////////////



  describe('_setAckd', function() {
    var collectionName = '_setAckd';

    var A = { _id : { _id: 'foo', _v: 'A', _pa: [], _pe: '_local' }, _m3: { _ack: false, _op: new Timestamp(0, 0) } };
    var B = { _id : { _id: 'foo', _v: 'B', _pa: ['A'], _pe: '_local', _i: 2 }, _m3: { _ack: false, _op: new Timestamp(0, 0) } };
    var C = { _id : { _id: 'foo', _v: 'C', _pa: ['B'], _pe: '_local', _i: 4 }, _m3: { _ack: false, _op: new Timestamp(0, 0) } };

    it('needs a capped collection and A', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([A, B, C], {w: 1}, done);
    });

    it('should ack', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._setAckd('foo', 'A', '_local', new Timestamp(0, 0), function(err) {
        if (err) { throw err; }
        done();
      });
    });

    it('should have set the item ackd', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.find({}, { sort: { '_id._v': 1 } }).toArray(function(err, items) {
        if (err) { throw err; }
        should.deepEqual(items.length, 3);
        should.deepEqual(items[0], {
          _id : { _id: 'foo', _v: 'A', _pa: [], _pe: '_local' },
          _m3: { _ack: true, _op: new Timestamp(0, 0) }
        });
        should.deepEqual(items[1], {
          _id : { _id: 'foo', _v: 'B', _pa: ['A'], _pe: '_local', _i: 2 },
          _m3: { _ack: false, _op: new Timestamp(0, 0) }
        });
        should.deepEqual(items[2], {
          _id : { _id: 'foo', _v: 'C', _pa: ['B'], _pe: '_local', _i: 4 },
          _m3: { _ack: false, _op: new Timestamp(0, 0) }
        });
        done();
      });
    });
  });

  describe('_getNextIncrement', function() {
    var collectionName = '_getNextIncrement';

    var A = { _id : { _id: 'foo', _v: 'A', _pa: [] } };
    var B = { _id : { _id: 'foo', _v: 'B', _pa: ['A'], _i: 2 } };
    var C = { _id : { _id: 'foo', _v: 'C', _pa: ['B'], _i: 4 } };
    var D = { _id : { _id: 'foo', _v: 'D', _pa: ['C'], _i: 3 } };

    it('needs a capped collection and A', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert(A, {w: 1}, done);
    });

    it('should return 1 by default', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._getNextIncrement(function(err, i) {
        if (err) { throw err; }
        should.equal(i, 1);
        done();
      });
    });

    it('should return 1 if no item with increment exists', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._getNextIncrement(function(err, i) {
        if (err) { throw err; }
        should.equal(i, 1);
        done();
      });
    });

    it('needs B, C and D', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([B, C, D], {w: 1}, function(err, inserts) {
        if (err) { throw err; }
        should.equal(inserts.length, 3);
        done();
      });
    });

    it('should return the highest increment in the snapshot collection', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._getNextIncrement(function(err, i) {
        if (err) { throw err; }
        should.equal(i, 5);
        done();
      });
    });
  });

  describe('_clearSnapshot', function() {
    var collectionName = '_clearSnapshot';

    it('should fail to determine size if not provided and collection and snapshotCollection don\'t exist', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._clearSnapshot(function(err) {
        should.equal(err.message, 'could not determine size');
        done();
      });
    });

    it('should create a new capped collection based on collection size', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._collection.insert({ foo: 'bar' }, {w: 1}, function(err, inserts) {
        if (err) { throw err; }
        should.equal(inserts.length, 1);

        vc._clearSnapshot(function(err) {
          if (err) { throw err; }
          vc._snapshotCollection.stats(function(err, stats) {
            if (err) { throw err; }
            should.equal(stats.capped, true);
            should.equal(stats.storageSize, 4096);
            should.equal(stats.count, 0);
            done();
          });
        });
      });
    });

    it('should create an empty capped collection of 3M', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._clearSnapshot(1024*1024*3, function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.stats(function(err, stats) {
          if (err) { throw err; }
          should.equal(stats.capped, true);
          should.equal(stats.storageSize, 1024*1024*3);
          done();
        });
      });
    });

    it('should create a new capped collection based on snapshot size', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert({ foo: 'bar' }, {w: 1}, function(err, inserts) {
        if (err) { throw err; }
        should.equal(inserts.length, 1);

        vc._snapshotCollection.stats(function(err, stats) {
          if (err) { throw err; }
          should.equal(stats.count, 1);

          vc._clearSnapshot(function(err) {
            if (err) { throw err; }
            vc._snapshotCollection.stats(function(err, stats) {
              if (err) { throw err; }
              should.equal(stats.capped, true);
              should.equal(stats.storageSize, 1024*1024*3);
              should.equal(stats.count, 0);
              done();
            });
          });
        });
      });
    });
  });

  describe('_createSnapshotCollection', function() {
    var collectionName = '_createSnapshotCollection';

    it('should require size to be a number', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function() { vc._createSnapshotCollection(function() {}); }).should.throw('size must be a number');
    });

    it('should require cb to be a function', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function() { vc._createSnapshotCollection(0, []); }).should.throw('cb must be a function');
    });

    it('should create a collection which is capped, has the right size and an index', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._createSnapshotCollection(4096, function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.stats(function(err, stats) {
          if (err) { throw err; }

          should.strictEqual(stats.capped, true);
          should.strictEqual(stats.storageSize, 4096);

          vc._snapshotCollection.indexInformation(function(err, ixNfo) {
            if (err) { throw err; }

            should.deepEqual(ixNfo, {
              '_id_': [['_id',1]],
              '_id_pe_i': [['_id._id',1], ['_id._pe',1], ['_id._i',-1]]
            });

            done();
          });
        });
      });
    });

    it('should throw an error if the snapshot collection already exists', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc._createSnapshotCollection(4096, function(err) {
        should.equal(err.message, 'Collection m3._createSnapshotCollection already exists. Currently in strict mode.');
        done();
      });
    });
  });

  describe('_ensureIndex', function() {
    var collectionName = '_ensureIndex';

    it('should create an index if the collection does not exist yet', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._ensureIndex(function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.indexInformation(function(err, ixNfo) {
          if (err) { throw err; }

          should.deepEqual(ixNfo, {
            '_id_': [['_id',1]],
            '_id_pe_i': [['_id._id',1], ['_id._pe',1], ['_id._i',-1]]
          });
          done();
        });
      });
    });

    it('2: should not create more than one index', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._ensureIndex(function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.indexInformation(function(err, ixNfo) {
          if (err) { throw err; }

          should.deepEqual(ixNfo, {
            '_id_': [['_id',1]],
            '_id_pe_i': [['_id._id',1], ['_id._pe',1], ['_id._i',-1]]
          });
          done();
        });
      });
    });

    it('should create an index if the collection already exists', function(done) {
      var collName = '_ensureIndex2';
      var vc = new VersionedCollection(db, collName);
      db.createCollection(collName, function(err) {
        if (err) { throw err; }
        vc._ensureIndex(function(err) {
          if (err) { throw err; }
          vc._snapshotCollection.indexInformation(function(err, ixNfo) {
            if (err) { throw err; }

            should.deepEqual(ixNfo, {
              '_id_': [['_id',1]],
              '_id_pe_i': [['_id._id',1], ['_id._pe',1], ['_id._i',-1]]
            });
            done();
          });
        });
      });
    });
  });

  describe('_ensureVersion', function() {
    var collectionName = '_ensureVersion';

    it('should require item', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function() { vc._ensureVersion(); }).should.throw('Cannot read property \'_id\' of undefined');
    });

    it('should require item._id to be an object', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function() { vc._ensureVersion({ _id: 'foo' }); }).should.throw('item._id must be an object');
    });

    it('should create and return version if it does not exist yet', function() {
      var vc = new VersionedCollection(db, collectionName);
      var item = { _id: { } };
      var v = vc._ensureVersion(item);
      v.should.match(/^[a-z0-9/+A-Z]{8}$/);
      should.equal(item._id._v, v);
    });

    it('should not change existing versions', function() {
      var vc = new VersionedCollection(db, collectionName);
      var item = { _id: { _v: 'foo' } };
      var v = vc._ensureVersion(item);
      should.equal(v, null);
      should.deepEqual(item, { _id: { _v: 'foo' } });
    });
  });

  describe('_threeWayMerge', function() {
    it('should keep when key in all three', function() {
      var lca = { foo: 'bar' };
      var x = { foo: 'bar' };
      var y = { foo: 'bar' };
      var mergedItem = VersionedCollection._threeWayMerge(x, y, lca);
      should.deepEqual(mergedItem, { foo: 'bar' });
    });

    it('should auto-merge when key in both x and y with same value', function() {
      var lca = { };
      var x = { foo: 'bar' };
      var y = { foo: 'bar' };
      var mergedItem = VersionedCollection._threeWayMerge(x, y, lca);
      should.deepEqual(mergedItem, { foo: 'bar' });
    });

    it('should conflict when key in all three but all with different values', function() {
      var lca = { foo: 'qux' };
      var x = { foo: 'bar' };
      var y = { foo: 'baz' };
      var mergedItem = VersionedCollection._threeWayMerge(x, y, lca);
      should.deepEqual(mergedItem, [ 'foo' ]);
    });

    it('should conflict when key in x and y but with different values', function() {
      var lca = { };
      var x = { foo: 'bar' };
      var y = { foo: 'baz' };
      var mergedItem = VersionedCollection._threeWayMerge(x, y, lca);
      should.deepEqual(mergedItem, [ 'foo' ]);
    });

    it('should not conflict on dates', function() {
      var time = new Date();
      var time2 = new Date(time);
      var lca = { };
      var x = { foo: time };
      var y = { foo: time2 };
      var mergedItem = VersionedCollection._threeWayMerge(x, y, lca);
      should.deepEqual(mergedItem, { foo: time });
    });

    it('should not conflict on objects', function() {
      var lca = { };
      var x = { foo: { a: 1 } };
      var y = { foo: { a: 1 } };
      var mergedItem = VersionedCollection._threeWayMerge(x, y, lca);
      should.deepEqual(mergedItem, { foo: { a: 1 } });
    });

    it('should merge when key in x has different value', function() {
      var lca = { foo: 'bar' };
      var x = { foo: 'baz' };
      var y = { foo: 'bar' };
      var mergedItem = VersionedCollection._threeWayMerge(x, y, lca);
      should.deepEqual(mergedItem, { foo: 'baz' });
    });

    it('should merge when key in y has different value', function() {
      var lca = { foo: 'bar' };
      var x = { foo: 'bar' };
      var y = { foo: 'baz' };
      var mergedItem = VersionedCollection._threeWayMerge(x, y, lca);
      should.deepEqual(mergedItem, { foo: 'baz' });
    });

    it('should delete when key only in lca', function() {
      var lca = { foo: 'bar' };
      var x = { };
      var y = { };
      var mergedItem = VersionedCollection._threeWayMerge(x, y, lca);
      should.deepEqual(mergedItem, { });
    });

    it('should merge when key in x is deleted', function() {
      var lca = { foo: 'bar' };
      var x = { };
      var y = { foo: 'bar' };
      var mergedItem = VersionedCollection._threeWayMerge(x, y, lca);
      should.deepEqual(mergedItem, { });
    });

    it('should merge when key in y is deleted', function() {
      var lca = { foo: 'bar' };
      var x = { foo: 'bar' };
      var y = { };
      var mergedItem = VersionedCollection._threeWayMerge(x, y, lca);
      should.deepEqual(mergedItem, { });
    });

    it('should merge when key is only in x', function() {
      var lca = { };
      var x = { foo: 'bar' };
      var y = { };
      var mergedItem = VersionedCollection._threeWayMerge(x, y, lca);
      should.deepEqual(mergedItem, { foo: 'bar' });
    });

    it('should merge when key is only in y', function() {
      var lca = { };
      var x = { };
      var y = { foo: 'bar' };
      var mergedItem = VersionedCollection._threeWayMerge(x, y, lca);
      should.deepEqual(mergedItem, { foo: 'bar' });
    });

    it('should do a more complex merge', function() {
      var lca = { foo: 'bar', baz: 'quz' };
      var x = { foo: 'bar', baz: 'quz', qux: 'raboof' };
      var y = { baz: 'qux' };
      var mergedItem = VersionedCollection._threeWayMerge(x, y, lca);
      should.deepEqual(mergedItem, {
        baz: 'qux',
        qux: 'raboof'
      });
    });

    it('should use a separate perspective lca', function() {
      var lcaX = { foo: 'bar', baz: 'quz' };
      var lcaY = { foo: 'bar' };
      var x = { foo: 'bar', baz: 'qux', qux: 'raboof' };
      var y = { quux: 'quz' };
      var mergedItem = VersionedCollection._threeWayMerge(x, y, lcaX, lcaY);
      should.deepEqual(mergedItem, {
        baz: 'qux',
        qux: 'raboof',
        quux: 'quz'
      });
    });

    it('should return all conflicting keys on any conflict', function() {
      var lca = { foo: 'baz' };
      var x = { foo: 'bar', baz: 'quz', qux: 'raboof' };
      var y = { baz: 'qux' };
      var mergedItem = VersionedCollection._threeWayMerge(x, y, lca);
      should.deepEqual(mergedItem, [ 'baz', 'foo' ]);
    });

    it('first lca should be leading, so foo key should exist', function() {
      var itemX = { _id: 'foo', foo: 'bar', bar: 'baz', qux: 'qux' };
      var itemY = { _id: 'foo', bar: 'raboof', qux: 'quux' };
      var lca0 = { _id: 'foo', foo: 'bar', bar: 'baz', qux: 'qux' };
      var perspectiveBoundLCA = { _id: 'foo', bar: 'baz', qux: 'qux' };

      var mergedItem = VersionedCollection._threeWayMerge(itemX, itemY, lca0, perspectiveBoundLCA);
      should.deepEqual(mergedItem, { _id: 'foo', foo: 'bar', bar: 'raboof', qux: 'quux' });
    });

    it('first lca should be leading, so yonly key should not exist if not changed', function() {
      var itemX = { _id: 'foo', foo: 'bar', bar: 'baz', qux: 'qux' };
      var itemY = { _id: 'foo', bar: 'raboof', qux: 'quux', yonly: true };
      var lca0 = { _id: 'foo', foo: 'bar', bar: 'baz', qux: 'qux' };
      var perspectiveBoundLCA = { _id: 'foo', bar: 'baz', qux: 'qux', yonly: true };

      var mergedItem = VersionedCollection._threeWayMerge(itemX, itemY, lca0, perspectiveBoundLCA);
      should.deepEqual(mergedItem, { _id: 'foo', foo: 'bar', bar: 'raboof', qux: 'quux' });
    });

    it('first lca should be leading, but yonly key should be created if changed since lcaB', function() {
      var itemX = { _id: 'foo', foo: 'bar', bar: 'baz', qux: 'qux' };
      var itemY = { _id: 'foo', bar: 'raboof', qux: 'quux', yonly: false };
      var lca0 = { _id: 'foo', foo: 'bar', bar: 'baz', qux: 'qux' };
      var perspectiveBoundLCA = { _id: 'foo', bar: 'baz', qux: 'qux', yonly: true };

      var mergedItem = VersionedCollection._threeWayMerge(itemX, itemY, lca0, perspectiveBoundLCA);
      should.deepEqual(mergedItem, { _id: 'foo', foo: 'bar', bar: 'raboof', qux: 'quux', yonly: false });
    });

    it('first lca should be leading, but yonly key should be created if created since lcaB', function() {
      var itemX = { _id: 'foo', foo: 'bar', bar: 'baz', qux: 'qux' };
      var itemY = { _id: 'foo', bar: 'raboof', qux: 'quux', yonly: false };
      var lca0 = { _id: 'foo', foo: 'bar', bar: 'baz', qux: 'qux' };
      var perspectiveBoundLCA = { _id: 'foo', bar: 'baz', qux: 'qux' };

      var mergedItem = VersionedCollection._threeWayMerge(itemX, itemY, lca0, perspectiveBoundLCA);
      should.deepEqual(mergedItem, { _id: 'foo', foo: 'bar', bar: 'raboof', qux: 'quux', yonly: false });
    });

    it('should not conflict on added keys in one perspective with the same value', function() {
      var itemX = { _id: 'foo', foo: 'bar', bar: 'baz' };
      var itemY = { _id: 'foo', foo: 'bar', bar: 'baz' };
      var lcaA = { _id: 'foo', foo: 'bar', bar: 'baz' };
      var lcaB = { _id: 'foo', foo: 'bar' };

      var mergedItem = VersionedCollection._threeWayMerge(itemX, itemY, lcaA, lcaB);
      should.deepEqual(mergedItem, { _id: 'foo', foo: 'bar', bar: 'baz' });
    });

    it('should conflict on a key added in itemY that was already in lcaA but not in lcaB', function() {
      var itemX = { _id: 'foo', foo: 'bar', bar: 'baz' };
      var itemY = { _id: 'foo', foo: 'bar', bar: 'raboof' };
      var lcaA = { _id: 'foo', foo: 'bar', bar: 'baz' };
      var lcaB = { _id: 'foo', foo: 'bar' };

      var mergedItem = VersionedCollection._threeWayMerge(itemX, itemY, lcaA, lcaB);
      should.deepEqual(mergedItem, ['bar']);
    });

    it('should conflict on a key added in itemX that was already in lcaB but not in lcaA', function() {
      // since the returned value will be based on lcaA
      var itemX = { _id: 'foo', foo: 'bar', bar: 'raboof' };
      var itemY = { _id: 'foo', foo: 'bar', bar: 'baz' };
      var lcaA = { _id: 'foo', foo: 'bar' };
      var lcaB = { _id: 'foo', foo: 'bar', bar: 'baz' };

      var mergedItem = VersionedCollection._threeWayMerge(itemX, itemY, lcaA, lcaB);
      should.deepEqual(mergedItem, ['bar']);
    });

    it('should not conflict on a key deleted in itemX that was never in itemY anyway', function() {
      var itemX = { _id: 'foo', foo: 'bar' };
      var itemY = { _id: 'foo', foo: 'bar' };
      var lcaA = { _id: 'foo', foo: 'bar', bar: 'baz' };
      var lcaB = { _id: 'foo', foo: 'bar' };

      var mergedItem = VersionedCollection._threeWayMerge(itemX, itemY, lcaA, lcaB);
      should.deepEqual(mergedItem, { _id: 'foo', foo: 'bar' });
    });

    it('should conflict on a key deleted in itemX that was add in itemY', function() {
      var itemX = { _id: 'foo', foo: 'bar' };
      var itemY = { _id: 'foo', foo: 'bar', bar: 'baz' };
      var lcaA = { _id: 'foo', foo: 'bar', bar: 'baz' };
      var lcaB = { _id: 'foo', foo: 'bar' };

      var mergedItem = VersionedCollection._threeWayMerge(itemX, itemY, lcaA, lcaB);
      should.deepEqual(mergedItem, ['bar']);
    });
  });

  describe('_intersect', function() {
    it('should require arr1', function(done) {
      VersionedCollection._intersect(null, null, function(err) {
        should.equal(err.message, 'provide arr1');
        done();
      });
    });

    it('should require arr2', function(done) {
      VersionedCollection._intersect({}, null, function(err) {
        should.equal(err.message, 'provide arr2');
        done();
      });
    });

    it('should require arr1 to be an array', function(done) {
      VersionedCollection._intersect({}, {}, function(err) {
        should.equal(err.message, 'arr1 must be an array');
        done();
      });
    });

    it('should require arr2 to be an array', function(done) {
      VersionedCollection._intersect([], {}, function(err) {
        should.equal(err.message, 'arr2 must be an array');
        done();
      });
    });

    it('should return empty set which are both a subset of each other', function(done) {
      var arr1 = [];
      var arr2 = [];
      VersionedCollection._intersect(arr1, arr2, function(err, intersection, subset) {
        should.equal(err, null);
        should.deepEqual(intersection, []);
        should.strictEqual(subset, 0);
        done();
      });
    });

    it('should return empty set', function(done) {
      var arr1 = [ 'bar' ];
      var arr2 = [ 'baz' ];
      VersionedCollection._intersect(arr1, arr2, function(err, intersection, subset) {
        should.equal(err, null);
        should.deepEqual(intersection, []);
        should.strictEqual(subset, false);
        done();
      });
    });

    it('should return element foo', function(done) {
      var arr1 = [ 'bar', 'foo' ];
      var arr2 = [ 'baz', 'foo' ];
      VersionedCollection._intersect(arr1, arr2, function(err, intersection, subset) {
        should.equal(err, null);
        should.deepEqual(intersection, [ 'foo' ]);
        should.strictEqual(subset, false);
        done();
      });
    });

    it('should find one key and one subset in arr1', function(done) {
      var arr1 = [ 'foo' ];
      var arr2 = [ 'baz', 'foo' ];
      VersionedCollection._intersect(arr1, arr2, function(err, intersection, subset) {
        should.equal(err, null);
        should.deepEqual(intersection, [ 'foo' ]);
        should.strictEqual(subset, -1);
        done();
      });
    });

    it('should find one key and one subset in arr2', function(done) {
      var arr1 = [ 'bar', 'foo' ];
      var arr2 = [ 'foo' ];
      VersionedCollection._intersect(arr1, arr2, function(err, intersection, subset) {
        should.equal(err, null);
        should.deepEqual(intersection, [ 'foo' ]);
        should.strictEqual(subset, 1);
        done();
      });
    });

    it('should find one key and both subsets of each other', function(done) {
      var arr1 = [ 'foo' ];
      var arr2 = [ 'foo' ];
      VersionedCollection._intersect(arr1, arr2, function(err, intersection, subset) {
        should.equal(err, null);
        should.deepEqual(intersection, [ 'foo' ]);
        should.strictEqual(subset, 0);
        done();
      });
    });

    it('should find two keys', function(done) {
      var arr1 = [ 'foo', 'bar', 'quux' ];
      var arr2 = [ 'foo', 'bar', 'qux' ];
      VersionedCollection._intersect(arr1, arr2, function(err, intersection, subset) {
        should.equal(err, null);
        should.deepEqual(intersection, [ 'foo', 'bar' ]);
        should.strictEqual(subset, false);
        done();
      });
    });
  });

  describe('_branchHeads', function() {
    var A  = { _id : { _id: 'foo', _v: 'A', _pa: [] } };
    var B  = { _id : { _id: 'foo', _v: 'B', _pa: ['A'] } };
    var C  = { _id : { _id: 'foo', _v: 'C', _pa: ['B'] } };
    var D  = { _id : { _id: 'foo', _v: 'D', _pa: ['C'] } };
    var E  = { _id : { _id: 'foo', _v: 'E', _pa: ['B'] } };
    var F  = { _id : { _id: 'foo', _v: 'F', _pa: ['E', 'C'] } };
    var G  = { _id : { _id: 'foo', _v: 'G', _pa: ['F'] } };
    var H  = { _id : { _id: 'foo', _v: 'H', _pa: ['F'] } };
    var I  = { _id : { _id: 'foo', _v: 'I', _pa: ['H', 'G', 'D'] } };
    var Jd = { _id : { _id: 'foo', _v: 'J', _pa: ['I'], _d: true } };
    var K  = { _id : { _id: 'foo', _v: 'K', _pa: [] } };
    var Ld = { _id : { _id: 'foo', _v: 'L', _pa: ['D'], _d: true } };
    var M  = { _id : { _id: 'foo', _v: 'M', _pa: ['L'] } };

    it('should require all items to have the same id', function() {
      var DAG = [A, { _id : { _id: 'bar', _v: 'B', _pa: ['A'] } }];
      (function () { VersionedCollection._branchHeads(DAG); }).should.throwError('id mismatch');
    });

    it('should find D and G', function() {
      // structure:
      // A <-- B <-- C <-- D
      //        \     \             
      //         E <-- F <-- G
      var DAG = [A, B, C, D, E, F, G];
      var branchHeads = VersionedCollection._branchHeads(DAG);
      should.deepEqual(branchHeads, [D, G]);
    });

    it('should find D and G', function() {
      // structure:
      //       B  <- C <-- D
      //              \             
      //   B  <- E <-- F <-- G
      var DAG = [C, D, E, F, G];
      var branchHeads = VersionedCollection._branchHeads(DAG);
      should.deepEqual(branchHeads, [D, G]);
    });

    it('should find G', function() {
      // structure:
      // A <-- B <-- C
      //        \     \             
      //         E <-- F <-- G
      var DAG = [A, B, C, E, F, G];
      var branchHeads = VersionedCollection._branchHeads(DAG);
      should.deepEqual(branchHeads, [G]);
    });

    it('should find A, E, C', function() {
      // structure:
      // A <-- C
      //  \
      //   E
      var DAG = [A, C, E];
      var branchHeads = VersionedCollection._branchHeads(DAG);
      should.deepEqual(branchHeads, [A, C, E]);
    });

    it('should find A, F', function() {
      // structure:
      // A <--   <-- C
      //        \     \
      //         E <-- F
      var DAG = [A, C, E, F];
      var branchHeads = VersionedCollection._branchHeads(DAG);
      should.deepEqual(branchHeads, [A, F]);
    });

    it('should find H, G and D', function() {
      // structure:
      // A <-- B <-- C <-- D
      //        \     \             
      //         E <-- F <-- G
      //                \             
      //                 H
      var DAG = [A, B, C, D, E, F, G, H];
      var branchHeads = VersionedCollection._branchHeads(DAG);
      should.deepEqual(branchHeads, [D, G, H]);
    });

    it('should find I', function() {
      // structure:
      // A <-- B <-- C <----- D
      //        \     \        \
      //         E <-- F <-- G  \
      //                \     \  \      
      //                 H <------- I
      var DAG = [A, B, C, D, E, F, G, H, I];
      var branchHeads = VersionedCollection._branchHeads(DAG);
      should.deepEqual(branchHeads, [I]);
    });

    it('should not find deleted items', function() {
      // structure:
      // A <-- B <-- C <----- D
      //        \     \        \
      //         E <-- F <-- G  \
      //                \     \  \
      //                 H <------- I <---- Jd
      var DAG = [A, B, C, D, E, F, G, H, I, Jd];
      var branchHeads = VersionedCollection._branchHeads(DAG);
      should.deepEqual(branchHeads, []);
    });

    it('should find deleted items when includeDeleted is true', function() {
      // structure:
      // A <-- B <-- C <----- D
      //        \     \        \
      //         E <-- F <-- G  \
      //                \     \  \
      //                 H <------- I <---- Jd
      var DAG = [A, B, C, D, E, F, G, H, I, Jd];
      var branchHeads = VersionedCollection._branchHeads(DAG, true);
      should.deepEqual(branchHeads, [Jd]);
    });

    it('should find K', function() {
      // structure:
      // A <-- B <-- C <----- D
      //        \     \        \
      //         E <-- F <-- G  \
      //                \     \  \
      //                 H <------- I <---- Jd
      // K
      var DAG = [A, B, C, D, E, F, G, H, I, Jd, K];
      var branchHeads = VersionedCollection._branchHeads(DAG);
      should.deepEqual(branchHeads, [K]);
    });

    it('should only find K even if deleted is included', function() {
      // structure:
      // A <-- B <-- C <----- D
      //        \     \        \
      //         E <-- F <-- G  \
      //                \     \  \
      //                 H <------- I <---- Jd
      // K
      var DAG = [A, B, C, D, E, F, G, H, I, Jd, K];
      var branchHeads = VersionedCollection._branchHeads(DAG, true);
      should.deepEqual(branchHeads, [K]);
    });

    it('should only find K and L', function() {
      // structure:
      // A <-- B <-- C <----- D <---- Ld
      //        \     \        \
      //         E <-- F <-- G  \
      //                \     \  \
      //                 H <------- I <---- Jd
      // K
      var DAG = [A, B, C, D, E, F, G, H, I, Jd, K, Ld];
      var branchHeads = VersionedCollection._branchHeads(DAG, true);
      should.deepEqual(branchHeads, [K, Ld]);
    });

    it('should only find M', function() {
      // structure:
      //    <--- Ld <--- M
      var DAG = [Ld, M];
      var branchHeads = VersionedCollection._branchHeads(DAG, true);
      should.deepEqual(branchHeads, [M]);
    });
  });

  describe('_walkBranch', function() {
    var collectionName = 'walkBranch';

    var A = { _id : { _id: 'foo', _v: 'A', _pa: [] } };
    var B = { _id : { _id: 'foo', _v: 'B', _pa: ['A'] } };
    var C = { _id : { _id: 'foo', _v: 'C', _pa: ['B'] } };
    var D = { _id : { _id: 'foo', _v: 'D', _pa: ['C'] } };
    var E = { _id : { _id: 'foo', _v: 'E', _pa: ['B'] } };
    var F = { _id : { _id: 'foo', _v: 'F', _pa: ['E', 'C'] } };
    var G = { _id : { _id: 'foo', _v: 'G', _pa: ['F'] } };
    var H = { _id : { _id: 'foo', _v: 'H', _pa: ['F'] } };
    var J = { _id : { _id: 'foo', _v: 'J', _pa: ['H'] } };
    var K = { _id : { _id: 'foo', _v: 'K', _pa: ['J'] } };
    var I = { _id : { _id: 'foo', _v: 'I', _pa: ['H', 'G', 'D'] } };

    // create the following structure:
    // A <-- B <-- C <----- D
    //        \     \        \
    //         E <-- F <-- G  \
    //                \     \  \      
    //                 H <------- I
    //                  \
    //                   J <-- K
    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([A, B, C, D, E, F, G, H, J, K, I], {w: 1}, done);
    });

    it('should find A after B', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      var i = 0;
      vc._walkBranch({ '_id._id': 'foo' }, B._id._v, function(err, item, stream) {
        if (!item) { return done(); }
        should.equal(err, null);
        if (i === 0) {
          should.deepEqual(item, B);
        } else {
          should.deepEqual(item, A);
          stream.destroy();
        }
        i++;
      });
    });

    it('should find F after G', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      var i = 0;
      vc._walkBranch({ '_id._id': 'foo' }, G._id._v, function(err, item, stream) {
        if (!item) { return done(); }
        should.equal(err, null);
        if (i === 0) {
          should.deepEqual(item, G);
        } else {
          should.deepEqual(item, F);
          stream.destroy();
        }
        i++;
      });
    });

    it('should find H, G, F, E, D after I', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      var i = 0;
      vc._walkBranch({ '_id._id': 'foo' }, 'I', function(err, item, stream) {
        if (!item) {
          // stream closed 
          should.equal(i, 6);
          return done();
        }
        should.equal(err, null);
        switch (i) {
        case 0:
          should.deepEqual(item, I);
          break;
        case 1:
          should.deepEqual(item, H);
          break;
        case 2:
          should.deepEqual(item, G);
          break;
        case 3:
          should.deepEqual(item, F);
          break;
        case 4:
          should.deepEqual(item, E);
          break;
        case 5:
          should.deepEqual(item, D);
          break;
        case 6:
          stream.destroy();
        }
        i++;
      });
    });
  });

  describe('_isAncestorOf', function() {
    var collectionName = 'isAncestorOf';

    var A = { _id : { _id: 'foo', _v: 'A', _pa: [] } };
    var B = { _id : { _id: 'foo', _v: 'B', _pa: ['A'] } };
    var C = { _id : { _id: 'foo', _v: 'C', _pa: ['B'] } };
    var D = { _id : { _id: 'foo', _v: 'D', _pa: ['C'] } };
    var E = { _id : { _id: 'foo', _v: 'E', _pa: ['B'] } };
    var F = { _id : { _id: 'foo', _v: 'F', _pa: ['E', 'C'] } };
    var G = { _id : { _id: 'foo', _v: 'G', _pa: ['F'] } };
    var H = { _id : { _id: 'foo', _v: 'H', _pa: ['F'] } };
    var J = { _id : { _id: 'foo', _v: 'J', _pa: ['H'] } };
    var K = { _id : { _id: 'foo', _v: 'K', _pa: ['J'] } };
    var I = { _id : { _id: 'foo', _v: 'I', _pa: ['H', 'G', 'D'] } };

    // create the following structure:
    // A <-- B <-- C <----- D
    //        \     \        \
    //         E <-- F <-- G  \
    //                \     \  \      
    //                 H <------- I
    //                  \
    //                   J <-- K
    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([A, B, C, D, E, F, G, H, J, K, I], {w: 1}, done);
    });

    it('should find A to be an ancestor of B', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._isAncestorOf(A._id._v, B, function(err, isAncestor) {
        should.equal(err, null);
        should.strictEqual(isAncestor, true);
        done();
      });
    });

    it('should find A to be an ancestor of A', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false });
      vc._isAncestorOf(A._id._v, A, function(err, isAncestor) {
        should.equal(err, null);
        should.strictEqual(isAncestor, true);
        done();
      });
    });

    it('should find C to be an ancestor of C', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._isAncestorOf(C._id._v, C, function(err, isAncestor) {
        should.equal(err, null);
        should.strictEqual(isAncestor, true);
        done();
      });
    });

    it('should find B to be an ancestor of C', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._isAncestorOf(B._id._v, C, function(err, isAncestor) {
        should.equal(err, null);
        should.strictEqual(isAncestor, true);
        done();
      });
    });

    it('should find A to be an ancestor of C', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._isAncestorOf(A._id._v, C, function(err, isAncestor) {
        should.equal(err, null);
        should.strictEqual(isAncestor, true);
        done();
      });
    });

    it('should find C to not be an ancestor of E', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._isAncestorOf(C._id._v, E, function(err, isAncestor) {
        should.equal(err, null);
        should.strictEqual(isAncestor, false);
        done();
      });
    });

    it('should find B to be an ancestor of E', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._isAncestorOf(B._id._v, E, function(err, isAncestor) {
        should.equal(err, null);
        should.strictEqual(isAncestor, true);
        done();
      });
    });

    it('should find A to be an ancestor of E', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._isAncestorOf(A._id._v, E, function(err, isAncestor) {
        should.equal(err, null);
        should.strictEqual(isAncestor, true);
        done();
      });
    });

    it('should find F to not be an ancestor of E', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._isAncestorOf(F._id._v, E, function(err, isAncestor) {
        should.equal(err, null);
        should.strictEqual(isAncestor, false);
        done();
      });
    });

    it('should find E to be an ancestor of F', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._isAncestorOf(E._id._v, F, function(err, isAncestor) {
        should.equal(err, null);
        should.strictEqual(isAncestor, true);
        done();
      });
    });

    it('should find B to be an ancestor of F', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._isAncestorOf(B._id._v, F, function(err, isAncestor) {
        should.equal(err, null);
        should.strictEqual(isAncestor, true);
        done();
      });
    });

    it('should find C to be an ancestor of F', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._isAncestorOf(C._id._v, F, function(err, isAncestor) {
        should.equal(err, null);
        should.strictEqual(isAncestor, true);
        done();
      });
    });

    it('should find D to not be an ancestor of C', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._isAncestorOf(D._id._v, C, function(err, isAncestor) {
        should.equal(err, null);
        should.strictEqual(isAncestor, false);
        done();
      });
    });

    it('should find D to not be an ancestor of C', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._isAncestorOf(D._id._v, C, function(err, isAncestor) {
        should.equal(err, null);
        should.strictEqual(isAncestor, false);
        done();
      });
    });

    it('should find D to not be an ancestor of G', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._isAncestorOf(D._id._v, G, function(err, isAncestor) {
        should.equal(err, null);
        should.strictEqual(isAncestor, false);
        done();
      });
    });

    describe('three parents', function() {
      // J, H, G, F, D
      it('should find J to not be an ancestor of I', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._isAncestorOf(J._id._v, I, function(err, isAncestor) {
          should.equal(err, null);
          should.strictEqual(isAncestor, false);
          done();
        });
      });

      it('should find H to not be an ancestor of I', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._isAncestorOf(H._id._v, I, function(err, isAncestor) {
          should.equal(err, null);
          should.strictEqual(isAncestor, true);
          done();
        });
      });

      it('should find G to not be an ancestor of I', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._isAncestorOf(G._id._v, I, function(err, isAncestor) {
          should.equal(err, null);
          should.strictEqual(isAncestor, true);
          done();
        });
      });

      it('should find F to not be an ancestor of I', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._isAncestorOf(F._id._v, I, function(err, isAncestor) {
          should.equal(err, null);
          should.strictEqual(isAncestor, true);
          done();
        });
      });

      it('should find D to not be an ancestor of I', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._isAncestorOf(D._id._v, I, function(err, isAncestor) {
          should.equal(err, null);
          should.strictEqual(isAncestor, true);
          done();
        });
      });
    });
  });

  describe('_addAllToDAG', function() {
    describe('one perspective', function() {
      var collectionName = '_addAllToDAGOnePe';

      it('should require all items to have the same perspective', function(done) {
        var vc = new VersionedCollection(db, collectionName, { hide: true });
        var item1 = { _id: { _id: 'foo', _v: 'A', _pe: '_local', _pa: [] } };
        var item2 = { _id: { _id: 'foo', _v: 'B', _pe: 'bar', _pa: [] } };
        vc._addAllToDAG([{ item: item1 }, { item: item2 }], function(err) {
          should.equal(err.message, 'perspective mismatch');
          done();
        });
      });

      it('should require to have one root', function(done) {
        var vc = new VersionedCollection(db, collectionName, { hide: true });
        var item1 = { _id: { _id: 'foo', _v: 'A', _pe: '_local', _pa: [] } };
        var item2 = { _id: { _id: 'foo', _v: 'B', _pe: '_local', _pa: [] } };
        vc._addAllToDAG([{ item: item1 }, { item: item2 }], function(err) {
          should.equal(err.message, 'root preceded');
          done();
        });
      });

      it('should require to have one head', function(done) {
        var vc = new VersionedCollection(db, collectionName, { hide: true });
        var item1 = { _id: { _id: 'foo', _v: 'A', _pe: '_local', _pa: [] } };
        var item2 = { _id: { _id: 'foo', _v: 'B', _pe: '_local', _pa: ['A'] } };
        var item3 = { _id: { _id: 'foo', _v: 'C', _pe: '_local', _pa: ['A'] } };
        vc._addAllToDAG([{ item: item1 }, { item: item2 }, { item: item3 }], function(err) {
          should.equal(err.message, 'not exactly one head');
          done();
        });
      });

      it('should insert both items with root', function(done) {
        var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true });
        var item1 = { _id: { _id: 'foo', _v: 'A', _pe: '_local', _pa: [] } };
        var item2 = { _id: { _id: 'foo', _v: 'B', _pe: '_local', _pa: ['A'] } };
        vc._addAllToDAG([{ item: item1 }, { item: item2 }], function(err) {
          if (err) { throw err; }
          // inspect DAG
          vc._snapshotCollection.find().toArray(function(err, items) {
            if (err) { throw err; }
            should.equal(items.length, 2);
            should.deepEqual(items[0], {
              _id: { _id: 'foo', _v: 'A', _pe: '_local', _pa: [], _i: 1 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });
            should.deepEqual(items[1], {
              _id: { _id: 'foo', _v: 'B', _pe: '_local', _pa: ['A'], _i: 2 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });

            // inspect collection
            vc._collection.find().toArray(function(err, items) {
              should.deepEqual(items[0], {
                _id: 'foo',
                _v: 'B'
              });
              done();
            });
          });
        });
      });

      it('should append item to previous items, fast-forward', function(done) {
        var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true });
        var item1 = { _id: { _id: 'foo', _v: 'C', _pe: '_local', _pa: ['B'] } };
        vc._addAllToDAG([{ item: item1 }], function(err) {
          if (err) { throw err; }
          // inspect DAG
          vc._snapshotCollection.find().toArray(function(err, items) {
            if (err) { throw err; }
            should.equal(items.length, 3);
            should.deepEqual(items[2], {
              _id: { _id: 'foo', _v: 'C', _pe: '_local', _pa: ['B'], _i: 3 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });

            // inspect collection
            vc._collection.find().toArray(function(err, items) {
              should.deepEqual(items[0], {
                _id: 'foo',
                _v: 'C'
              });
              done();
            });
          });
        });
      });

      it('should append item and create merge', function(done) {
        // state before: A---B---C
        // expected state after: A---B---C---X
        //                            \     /
        //                             D----
        var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true });
        var item1 = { _id: { _id: 'foo', _v: 'D', _pe: '_local', _pa: ['B'], _lo: true } };
        vc._addAllToDAG([{ item: item1 }], function(err) {
          if (err) { throw err; }
          // inspect DAG
          vc._snapshotCollection.find().toArray(function(err, items) {
            if (err) { throw err; }
            should.equal(items.length, 5);
            should.deepEqual(items[3], {
              _id: { _id: 'foo', _v: 'D', _pe: '_local', _pa: ['B'], _lo: true, _i: 4 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });

            var v = items[4]._id._v;
            should.deepEqual(items[4], {
              _id: { _co: '_addAllToDAGOnePe', _id: 'foo', _v: v, _pe: '_local', _pa: ['C', 'D'], _lo: true, _i: 5 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });

            // inspect collection
            vc._collection.find().toArray(function(err, items) {
              should.equal(items.length, 1);
              should.deepEqual(items[0], {
                _id: 'foo',
                _v: v
              });
              done();
            });
          });
        });
      });
    });

    describe('one perspective delete', function() {
      describe('empty DAG all in memory', function() {
        var collectionName = '_addAllToDAGOnePeDeleteMemory';

        it('should not accept multiple heads', function(done) {
          var vc = new VersionedCollection(db, collectionName, { hide: true });
          var A = { _id: { _id: 'foo', _v: 'A', _pe: '_local', _pa: [] } };
          var B = { _id: { _id: 'foo', _v: 'B', _pe: '_local', _pa: ['C']} };
          vc._addAllToDAG([{ item: A }, { item: B }], function(err) {
            should.equal(err.message, 'not exactly one head');
            done();
          });
        });

        it('should not accept any non-connected items', function(done) {
          var vc = new VersionedCollection(db, collectionName, { hide: true });
          var A = { _id: { _id: 'foo', _v: 'A', _pe: '_local', _pa: ['C'] } };
          vc._addAllToDAG([{ item: A }], function(err) {
            should.equal(err.message, 'parent not found');
            done();
          });
        });

        it('should not accept any non-deleted items preceding a root', function(done) {
          var vc = new VersionedCollection(db, collectionName, { hide: true });
          var A = { _id: { _id: 'foo', _v: 'A', _pe: '_local', _pa: [] } };
          var B = { _id: { _id: 'foo', _v: 'B', _pe: '_local', _pa: ['A']} };
          var C = { _id: { _id: 'foo', _v: 'C', _pe: '_local', _pa: [] } };
          vc._addAllToDAG([{ item: A }, { item: B }, { item: C }], function(err) {
            should.equal(err.message, 'root preceded');
            done();
          });
        });

        it('should accept new root if preceded by a deleted item and set as parent', function(done) {
          var vc = new VersionedCollection(db, collectionName, { debug: false });
          var A  = { _id: { _id: 'foo', _v: 'A', _pe: '_local', _pa: [] } };
          var Bd = { _id: { _id: 'foo', _v: 'B', _pe: '_local', _pa: ['A'], _d: true } };
          var C  = { _id: { _id: 'foo', _v: 'C', _pe: '_local', _pa: [] } };
          vc._addAllToDAG([{ item: A }, { item: Bd }, { item: C }], function(err) {
            if (err) { throw err; }
            vc._snapshotCollection.find().toArray(function(err, items) {
              if (err) { throw err; }
              should.equal(items.length, 3);
              should.deepEqual(items[0], {
                _id: { _id: 'foo', _v: 'A', _pe: '_local', _pa: [], _i: 1 },
                _m3: { _ack: false, _op: new Timestamp(0, 0) }
              });
              should.deepEqual(items[1], {
                _id: { _id: 'foo', _v: 'B', _pe: '_local', _pa: ['A'], _i: 2, _d: true },
                _m3: { _ack: false, _op: new Timestamp(0, 0) }
              });
              should.deepEqual(items[2], {
                _id: { _id: 'foo', _v: 'C', _pe: '_local', _pa: ['B'], _i: 3 },
                _m3: { _ack: false, _op: new Timestamp(0, 0) }
              });

              // inspect collection
              vc._collection.find().toArray(function(err, items) {
                if (err) { throw err; }
                should.deepEqual(items.length, 1);
                should.deepEqual(items[0], {
                  _id: 'foo',
                  _v: 'C'
                });
                done();
              });
            });
          });
        });
      });

      describe('non-empty DAG', function() {
        var collectionName = '_addAllToDAGOnePeDeleteSaved';

        it('needs a new root', function(done) {
          var vc = new VersionedCollection(db, collectionName, { debug: false });
          var D = { _id: { _id: 'foo', _v: 'D', _pe: '_local', _pa: [] } };
          vc._addAllToDAG([{ item: D }], function(err) {
            if (err) { throw err; }
            vc._snapshotCollection.find().toArray(function(err, items) {
              if (err) { throw err; }
              should.equal(items.length, 1);
              should.deepEqual(items[0], {
                _id: { _id: 'foo', _v: 'D', _pe: '_local', _pa: [], _i: 1 },
                _m3: { _ack: false, _op: new Timestamp(0, 0) }
              });

              vc._collection.find().toArray(function(err, items) {
                if (err) { throw err; }
                should.deepEqual(items.length, 1);
                should.deepEqual(items[0], {
                  _id: 'foo',
                  _v: 'D'
                });
                done();
              });
            });
          });
        });

        it('should not accept any (non-deleted) items preceding a root', function(done) {
          var vc = new VersionedCollection(db, collectionName, { hide: true });
          var E  = { _id: { _id: 'foo', _v: 'B', _pe: '_local', _pa: [] } };
          vc._addAllToDAG([{ item: E }], function(err) {
            should.equal(err.message, 'different root already in snapshot');
            done();
          });
        });

        it('should delete item from collection', function(done) {
          var vc = new VersionedCollection(db, collectionName, { debug: false });
          var item = { _id: { _id: 'foo', _v: 'E', _pe: '_local', _pa: ['D'], _d: true } };
          vc._addAllToDAG([{ item: item }], function(err) {
            if (err) { throw err; }
            vc._snapshotCollection.find().toArray(function(err, items) {
              if (err) { throw err; }
              should.equal(items.length, 2);
              should.deepEqual(items[1], {
                _id: { _id: 'foo', _v: 'E', _pe: '_local', _pa: ['D'], _i: 2, _d: true },
                _m3: { _ack: false, _op: new Timestamp(0, 0) }
              });

              vc._collection.find().toArray(function(err, items) {
                if (err) { throw err; }
                should.deepEqual(items.length, 0);
                done();
              });
            });
          });
        });

        it('should accept a new root if preceding item is a deletion', function(done) {
          var vc = new VersionedCollection(db, collectionName, { debug: false });
          var F  = { _id: { _id: 'foo', _v: 'F', _pe: '_local', _pa: [] } };
          vc._addAllToDAG([{ item: F }], function(err) {
            if (err) { throw err; }
            vc._snapshotCollection.find().toArray(function(err, items) {
              if (err) { throw err; }
              should.equal(items.length, 3);
              should.deepEqual(items[2], {
                _id: { _id: 'foo', _v: 'F', _pe: '_local', _pa: ['E'], _i: 3 },
                _m3: { _ack: false, _op: new Timestamp(0, 0) }
              });

              // inspect collection
              vc._collection.find().toArray(function(err, items) {
                if (err) { throw err; }
                should.deepEqual(items.length, 1);
                should.deepEqual(items[0], {
                  _id: 'foo',
                  _v: 'F'
                });
                done();
              });
            });
          });
        });
      });
    });

    describe('one perspective multiple ids', function() {
      var collectionName = '_addAllToDAGOnePeMultiId';

      it('should insert both items with root', function(done) {
        var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true });
        var item1 = { _id: { _id: 'foo', _v: 'X', _pe: '_local', _pa: [] } };
        var item2 = { _id: { _id: 'bar', _v: 'Y', _pe: '_local', _pa: [] } };
        vc._addAllToDAG([{ item: item1 }, { item: item2 }], function(err) {
          if (err) { throw err; }
          // inspect DAG
          vc._snapshotCollection.find().toArray(function(err, items) {
            if (err) { throw err; }
            should.equal(items.length, 2);
            should.deepEqual(items[0], {
              _id: { _id: 'foo', _v: 'X', _pe: '_local', _pa: [], _i: 1 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });
            should.deepEqual(items[1], {
              _id: { _id: 'bar', _v: 'Y', _pe: '_local', _pa: [], _i: 2 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });

            // inspect collection
            vc._collection.find().toArray(function(err, items) {
              should.equal(items.length, 2);
              should.deepEqual(items[0], {
                _id: 'foo',
                _v: 'X'
              });
              should.deepEqual(items[1], {
                _id: 'bar',
                _v: 'Y'
              });
              done();
            });
          });
        });
      });

      it('should append item to previous items, fast-forward', function(done) {
        var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true });
        var item1 = { _id: { _id: 'foo', _v: 'B', _pe: '_local', _pa: ['X'] } };
        var item2 = { _id: { _id: 'bar', _v: 'C', _pe: '_local', _pa: ['Y'] } };
        vc._addAllToDAG([{ item: item1 }, { item: item2 }], function(err) {
          if (err) { throw err; }
          // inspect DAG
          vc._snapshotCollection.find().toArray(function(err, items) {
            if (err) { throw err; }
            should.equal(items.length, 4);
            should.deepEqual(items[2], {
              _id: { _id: 'foo', _v: 'B', _pe: '_local', _pa: ['X'], _i: 3 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });
            should.deepEqual(items[3], {
              _id: { _id: 'bar', _v: 'C', _pe: '_local', _pa: ['Y'], _i: 4 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });

            // inspect collection
            vc._collection.find().toArray(function(err, items) {
              should.equal(items.length, 2);
              should.deepEqual(items[0], {
                _id: 'foo',
                _v: 'B'
              });
              should.deepEqual(items[1], {
                _id: 'bar',
                _v: 'C'
              });
              done();
            });
          });
        });
      });

      it('should append item and create merge', function(done) {
        var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true });
        var item1 = { _id: { _id: 'foo', _v: 'D', _pe: '_local', _pa: ['X'], _lo: true }, d: true };
        var item2 = { _id: { _id: 'bar', _v: 'D', _pe: '_local', _pa: ['Y'], _lo: true }, d: true };
        vc._addAllToDAG([{ item: item1 }, { item: item2 }], function(err) {
          if (err) { throw err; }
          // inspect DAG
          vc._snapshotCollection.find().toArray(function(err, items) {
            if (err) { throw err; }
            should.equal(items.length, 8);
            should.deepEqual(items[4], {
              _id: { _id: 'foo', _v: 'D', _pe: '_local', _pa: ['X'], _lo: true, _i: 5 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) },
              d: true
            });

            should.deepEqual(items[5], {
              _id: { _id: 'bar', _v: 'D', _pe: '_local', _pa: ['Y'], _lo: true, _i: 6 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) },
              d: true
            });

            var v1 = items[6]._id._v;
            should.deepEqual(items[6], {
              _id: { _co: '_addAllToDAGOnePeMultiId', _id: 'foo', _v: v1, _pe: '_local', _pa: ['B', 'D'], _lo: true, _i: 7 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) },
              d: true
            });

            var v2 = items[7]._id._v;
            should.deepEqual(items[7], {
              _id: { _co: '_addAllToDAGOnePeMultiId', _id: 'bar', _v: v2, _pe: '_local', _pa: ['C', 'D'], _lo: true, _i: 8 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) },
              d: true
            });

            // inspect collection
            vc._collection.find().toArray(function(err, items) {
              should.equal(items.length, 2);
              should.deepEqual(items[0], {
                _id: 'foo',
                _v: v1,
                d: true
              });
              should.deepEqual(items[1], {
                _id: 'bar',
                _v: v2,
                d: true
              });
              done();
            });
          });
        });
      });
    });

    describe('multiple perspective', function() {
      var collectionName = '_addAllToDAGMultiplePe';

      it('should insert both items with root', function(done) {
        var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true });
        var item1 = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'A', _pe: 'bar', _pa: [] } };
        var item2 = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'B', _pe: 'bar', _pa: ['A'] } };
        database.createCappedColl(vc.snapshotCollectionName, function(err) {
          if (err) { throw err; }
          vc._addAllToDAG([{ item: item1 }, { item: item2 }], function(err) {
            if (err) { throw err; }
            // inspect DAG
            vc._snapshotCollection.find().toArray(function(err, items) {
              if (err) { throw err; }
              should.equal(items.length, 4);
              should.deepEqual(items[0], {
                _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'A', _pe: 'bar', _pa: [] },
                _m3: { _ack: false, _op: new Timestamp(0, 0) }
              });
              should.deepEqual(items[1], {
                _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'B', _pe: 'bar', _pa: ['A'] },
                _m3: { _ack: false, _op: new Timestamp(0, 0) }
              });
              should.deepEqual(items[2], {
                _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'A', _pe: '_local', _pa: [], _i: 1 },
                _m3: { _ack: false, _op: new Timestamp(0, 0) }
              });
              should.deepEqual(items[3], {
                _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'B', _pe: '_local', _pa: ['A'], _i: 2 },
                _m3: { _ack: false, _op: new Timestamp(0, 0) }
              });

              // inspect collection
              vc._collection.find().toArray(function(err, items) {
                should.deepEqual(items[0], {
                  _id: new ObjectID('f00000000000000000000000'),
                  _v: 'B'
                });
                done();
              });
            });
          });
        });
      });

      it('should append item to previous items, fast-forward', function(done) {
        var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true });
        var item1 = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'C', _pe: 'bar', _pa: ['B'] } };
        vc._addAllToDAG([{ item: item1 }], function(err) {
          if (err) { throw err; }
          // inspect DAG
          vc._snapshotCollection.find().toArray(function(err, items) {
            if (err) { throw err; }
            should.equal(items.length, 6);
            should.deepEqual(items[4], {
              _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'C', _pe: 'bar', _pa: ['B'] },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });
            should.deepEqual(items[5], {
              _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'C', _pe: '_local', _pa: ['B'], _i: 3 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });

            // inspect collection
            vc._collection.find().toArray(function(err, items) {
              should.deepEqual(items[0], {
                _id: new ObjectID('f00000000000000000000000'),
                _v: 'C'
              });
              done();
            });
          });
        });
      });

      it('should append item and create merge', function(done) {
        // state before: A---B---C
        //
        //               A'--B'--C'
        //
        // expected state after: A---B---C---X
        //                            \     /
        //                             D----
        //
        //                       A'--B'--C'--X'
        //                            \     /
        //                             D'---
        var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true });
        var item1 = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'D', _pe: 'bar', _pa: ['B'] } };
        vc._addAllToDAG([{ item: item1 }], function(err) {
          if (err) { throw err; }
          // inspect DAG
          vc._snapshotCollection.find().toArray(function(err, items) {
            if (err) { throw err; }
            should.equal(items.length, 9);
            should.deepEqual(items[6], {
              _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'D', _pe: 'bar', _pa: ['B'] },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });
            should.deepEqual(items[7], {
              _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'D', _pe: '_local', _pa: ['B'], _i: 4 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });

            var v = items[8]._id._v;
            should.deepEqual(items[8], {
              _id: { _co: '_addAllToDAGMultiplePe', _id: new ObjectID('f00000000000000000000000'), _v: v, _pe: '_local', _pa: ['C', 'D'], _lo: true, _i: 5 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });

            // inspect collection
            vc._collection.find().toArray(function(err, items) {
              should.deepEqual(items[0], {
                _id: new ObjectID('f00000000000000000000000'),
                _v: v
              });
              done();
            });
          });
        });
      });

      it('should insert all heads in the collection', function(done) {
        var collName = collectionName + 'multiRemote';
        var vc = new VersionedCollection(db, collName, { debug: false, hide: true });

        var item1 = { _id: { _id: 'multi1', _v: 'A', _pe: 'remote', _pa: [] } };
        var item2 = { _id: { _id: 'multi2', _v: 'A', _pe: 'remote', _pa: [] } };

        vc._addAllToDAG([{ item: item1 }, { item: item2 }], function(err) {
          if (err) { throw err; }
          // inspect DAG
          vc._snapshotCollection.find().toArray(function(err, items) {
            if (err) { throw err; }
            should.equal(items.length, 4);
            should.deepEqual(items[0], {
              _id: { _id: 'multi1', _v: 'A', _pe: 'remote', _pa: [] },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });
            should.deepEqual(items[1], {
              _id: { _id: 'multi2', _v: 'A', _pe: 'remote', _pa: [] },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });
            should.deepEqual(items[2], {
              _id: { _id: 'multi1', _v: 'A', _pe: '_local', _pa: [], _i: 1 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });
            should.deepEqual(items[3], {
              _id: { _id: 'multi2', _v: 'A', _pe: '_local', _pa: [], _i: 2 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });

            // inspect collection
            vc._collection.find().toArray(function(err, items) {
              should.equal(items.length, 2);
              should.deepEqual(items[0], {
                _id: 'multi1',
                _v: 'A'
              });
              should.deepEqual(items[1], {
                _id: 'multi2',
                _v: 'A'
              });
              done();
            });
          });
        });
      });

      it('needs a new local version', function(done) {
        var collName = collectionName + 'multiRemote';
        var vc = new VersionedCollection(db, collName, { debug: false });

        var item = { _id: { _id: 'multi1', _v: 'B', _pe: '_local', _pa: ['A'] } };

        vc._addAllToDAG([{ item: item }], function(err) {
          if (err) { throw err; }
          // inspect DAG
          vc._snapshotCollection.find().toArray(function(err, items) {
            if (err) { throw err; }
            should.equal(items.length, 5);
            should.deepEqual(items[4], {
              _id: { _id: 'multi1', _v: 'B', _pe: '_local', _pa: ['A'], _i: 3 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) }
            });

            // inspect collection
            vc._collection.find().toArray(function(err, items) {
              should.equal(items.length, 2);
              should.deepEqual(items[0], {
                _id: 'multi1',
                _v: 'B'
              });
              should.deepEqual(items[1], {
                _id: 'multi2',
                _v: 'A'
              });
              done();
            });
          });
        });
      });

      it('should accept same root from different perspective', function(done) {
        var collName = collectionName + 'multiRemote';
        var vc = new VersionedCollection(db, collName, { debug: false });

        var item = { _id: { _id: 'multi1', _v: 'A', _pe: 'remote2', _pa: [] }, foo: 'bar' };

        vc._addAllToDAG([{ item: item }], function(err) {
          if (err) { throw err; }
          // inspect DAG
          vc._snapshotCollection.find().toArray(function(err, items) {
            if (err) { throw err; }
            should.equal(items.length, 6);
            should.deepEqual(items[5], {
              _id: { _id: 'multi1', _v: 'A', _pe: 'remote2', _pa: [] },
              _m3: { _ack: false, _op: new Timestamp(0, 0) },
              foo: 'bar'
            });

            // inspect collection
            vc._collection.find().toArray(function(err, items) {
              should.equal(items.length, 2);
              should.deepEqual(items[0], {
                _id: 'multi1',
                _v: 'B'
              });
              should.deepEqual(items[1], {
                _id: 'multi2',
                _v: 'A'
              });
              done();
            });
          });
        });
      });
    });
  });

  describe('fixConsistency', function() {
    var collectionName = 'fixConsistency';

    var A = { _id: { _id: 'foo', _v: 'A', _pe: 'I', _pa: [] } };
    var B = { _id: { _id: 'bar', _v: 'B', _pe: 'I', _pa: []} };
    var C = { _id: { _id: 'qux', _v: 'C', _pe: 'I', _pa: [] } };

    var snap1;

    it('should process the items the same way as addAllToDAG', function(done){
      var itemsA;
      var vcA = new VersionedCollection(db, 'fixConsistencyA', { hide: true });
      var vcB = new VersionedCollection(db, 'fixConsistencyB', { hide: true });
      vcA._addAllToDAG([{ item: A }, { item: B }, { item:C }], function(err) {
        vcA._snapshotCollection.find().toArray(function(err, itemsA) {
          vcB._ensureAllInDAG([{ item: A }, { item: B }, { item:C }], function(err) {
            vcB._snapshotCollection.find().toArray(function(err, itemsB) {
              should.deepEqual(itemsA, itemsB);
              done();
            });
          });
        });
      });
    });

    it('should not add the same version twice and raise an error', function(done) {
      var collectionName = 'fixConsistency1';
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc._addAllToDAG([{ item: A }, { item: B }, { item:C }], function(err) {
        if (err) { throw err; return; }
        vc._addAllToDAG([{ item: A }, { item: B }, { item:C }], function(err) {
          should.equal(err.message, 'version already exists');
          done();
        });
      });
    });
  });

  describe('_ensureLocalPerspective', function() {
    var collectionName = '_ensureLocalPerspective';

    it('should require the first item to have a perspective', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true, localPerspective: 'I' });
      vc._ensureLocalPerspective([ {} ], function(err) {
        should.equal(err.message, 'could not determine perspective');
        done();
      });
    });

    it('should require all items to have the same perspective', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true, localPerspective: 'I' });
      var item1 = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'A', _pe: 'I', _pa: [] } };
      var item2 = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'B', _pe: 'II', _pa: [] } };
      vc._ensureLocalPerspective([ item1, item2 ], function(err) {
        should.equal(err.message, 'perspective mismatch');
        done();
      });
    });

    it('should return no new items if already local', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true, localPerspective: 'I' });
      var item1 = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'A', _pe: 'I', _pa: [] } };
      var item2 = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'B', _pe: 'I', _pa: [] } };
      vc._ensureLocalPerspective([ item1, item2 ], function(err, newLocalItems) {
        if (err) { throw err; }
        should.deepEqual(newLocalItems, []);
        done();
      });
    });

    it('should create a local clone if the snapshot collection is empty', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, localPerspective: 'I' });
      var item1 = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'A', _pe: 'II', _pa: [] }, _m3: { _ack: true, _op: new Timestamp(0, 0) } };
      var item2 = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'B', _pe: 'II', _pa: ['A'] }, _m3: { _ack: true, _op: new Timestamp(0, 0) } };
      vc._ensureLocalPerspective([ item1, item2 ], function(err, newLocalItems) {
        if (err) { throw err; }
        should.strictEqual(newLocalItems.length, 2);
        should.deepEqual(newLocalItems[0], {
          _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'A', _pe: 'I', _pa: [] }, _m3: { _ack: false, _op: new Timestamp(0, 0) }
        });
        should.deepEqual(newLocalItems[1], {
          _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'B', _pe: 'I', _pa: ['A'] }, _m3: { _ack: false, _op: new Timestamp(0, 0) }
        });
        done();
      });
    });

    it('needs an item in the snapshot collection for further testing', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true, localPerspective: 'I' });
      var itemI = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'A', _pe: 'I', _pa: [], _i: 1  }, _m3: { _ack: true, _op: new Timestamp(1, 1) } };
      var itemII = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'A', _pe: 'II', _pa: [] }, _m3: { _ack: false, _op: new Timestamp(0, 0) } };
      vc._snapshotCollection.insert([itemI, itemII], done);
    });

    it('should not create new items if item already exists (input _pe local)', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true, localPerspective: 'I' });
      var item = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'A', _pe: 'I', _pa: [] } };
      vc._ensureLocalPerspective([ item ], function(err, newLocalItems) {
        if (err) { throw err; }
        should.deepEqual(newLocalItems, []);
        done();
      });
    });

    it('should not create new items if item already exists (input _pe non-local)', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true, localPerspective: 'I' });
      var item = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'A', _pe: 'II', _pa: [] } };
      vc._ensureLocalPerspective([ item ], function(err, newLocalItems) {
        if (err) { throw err; }
        should.deepEqual(newLocalItems, []);
        done();
      });
    });

    it('should not create new items if item already exists (input _pe new perspective', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: false, localPerspective: 'I' });
      var item = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'A', _pe: 'III', _pa: [] } };
      vc._ensureLocalPerspective([ item ], function(err, newLocalItems) {
        if (err) { throw err; }
        should.deepEqual(newLocalItems, []);
        done();
      });
    });

    it('should fail if the snapshot collection has an item but no lca is found', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true, localPerspective: 'I' });
      var item = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'B', _pe: 'II', _pa: ['X'] } };
      vc._ensureLocalPerspective([ item ], function(err) {
        should.equal(err.message, 'no lca found');
        done();
      });
    });

    it('should not create new items if the new item is a child of what is in the snapshot collection but is already local', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true, localPerspective: 'I' });
      var item = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'B', _pe: 'I', _pa: ['A'] } };
      vc._ensureLocalPerspective([ item ], function(err, newLocalItems) {
        if (err) { throw err; }
        should.deepEqual(newLocalItems, []);
        done();
      });
    });

    it('should create a new item if it connects to the DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true, localPerspective: 'I' });
      var item = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'B', _pe: 'II', _pa: ['A'] }, _m3: {} };
      vc._ensureLocalPerspective([ item ], function(err, newLocalItems) {
        if (err) { throw err; }
        should.deepEqual(newLocalItems, [ { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'B', _pe: 'I', _pa: ['A'] }, _m3: { _ack: false, _op: new Timestamp(0, 0) } } ]);
        done();
      });
    });

    it('should create multiple new items if they connect to each other and the first connects to the DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true, localPerspective: 'I' });
      var item1 = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'B', _pe: 'II', _pa: ['A'] }, _m3: {} };
      var item2 = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'C', _pe: 'II', _pa: ['B'] }, _m3: {} };
      vc._ensureLocalPerspective([ item1, item2 ], function(err, newLocalItems) {
        if (err) { throw err; }
        should.deepEqual(newLocalItems, [
          { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'B', _pe: 'I', _pa: ['A'] } , _m3: { _ack: false, _op: new Timestamp(0, 0) } },
          { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'C', _pe: 'I', _pa: ['B'] } , _m3: { _ack: false, _op: new Timestamp(0, 0) } }
        ]);
        done();
      });
    });

    it('needs a deleted item for further testing', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, localPerspective: 'I' });
      var item = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'B', _pe: 'I', _pa: ['A'], _d: true, _i: 2 }, _m3: { _ack: true, _op: new Timestamp(24, 1) } };
      vc._snapshotCollection.insert([item], done);
    });


    it('should work with (root) items of previously deleted items', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, localPerspective: 'I' });

      var item = { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'C', _pe: 'II', _pa: [] } };
      vc._ensureLocalPerspective([ item ], function(err, newLocalItems) {
        if (err) { throw err; }
        should.deepEqual(newLocalItems, [
          { _id: { _id: new ObjectID('f00000000000000000000000'), _v: 'C', _pe: 'I', _pa: [] }, _m3: { _ack: false, _op: new Timestamp(0, 0) } },
        ]);
        done();
      });
    });
  });

  // tests of _merge() are in test/versioned_collection_merge.js

  describe('_sortByVersionAndPerspective', function() {
    var AI    = { _id : { _id: new ObjectID('f00000000000000000000000'), _v: 'A', _pe: 'I' } };
    var AIIII = { _id : { _id: new ObjectID('f00000000000000000000000'), _v: 'A', _pe: 'IIII' } };
    var BI    = { _id : { _id: new ObjectID('f00000000000000000000000'), _v: 'B', _pe: 'I' } };
    var BII   = { _id : { _id: new ObjectID('f00000000000000000000000'), _v: 'B', _pe: 'II' } };
    var CI    = { _id : { _id: new ObjectID('f00000000000000000000000'), _v: 'C', _pe: 'I' } };
    var CII   = { _id : { _id: new ObjectID('f00000000000000000000000'), _v: 'C', _pe: 'II' } };
    var XI    = { _id : { _id: new ObjectID('f00000000000000000000000'), _pe: 'I' } };
    var XII   = { _id : { _id: new ObjectID('f00000000000000000000000'), _pe: 'II' } };
    var XVI   = { _id : { _id: new ObjectID('f00000000000000000000000'), _pe: 'VI' } };

    it('should accept an empty array', function() {
      var items = [];
      VersionedCollection._sortByVersionAndPerspective(items);
      should.deepEqual(items, []);
    });

    it('should accept single items', function() {
      var items = [AI];
      VersionedCollection._sortByVersionAndPerspective(items);
      should.deepEqual(items, [AI]);
    });

    it('should sort two items', function() {
      var items = [BI, AI];
      VersionedCollection._sortByVersionAndPerspective(items);
      should.deepEqual(items, [AI, BI]);
    });

    it('should work with duplicates', function() {
      var items = [CI, AI, BI, CI];
      VersionedCollection._sortByVersionAndPerspective(items);
      should.deepEqual(items, [AI, BI, CI, CI]);
    });

    it('should work without version', function() {
      var items = [XVI, XI, XII];
      VersionedCollection._sortByVersionAndPerspective(items);
      should.deepEqual(items, [XI, XII, XVI]);
    });

    it('should return all versions', function() {
      var items = [AI, XI, BII, BI, CI, CII, XII, AIIII];
      VersionedCollection._sortByVersionAndPerspective(items);
      should.deepEqual(items, [AI, AIIII, BI, BII, CI, CII, XI, XII]);
    });
  });

  describe('_applyOplogItem', function() {
    var collectionName = '_applyOplogItem';

    it('should require an object on the item', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._applyOplogItem({ }, function(err) {
        should.equal('missing oplogItem.o', err.message);
        done();
      });
    });

    it('should require a document with an _id', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._applyOplogItem({ o: { } }, function(err) {
        should.equal('unsupported operator: undefined', err.message);
        done();
      });
    });

    describe('insert', function() {
      var oplogItem = {
        op: 'i',
        o: {
          _id : 'bar',
          baz: 'foobar'
        }
      };

      it('should insert a new item into the DAG', function(done) {
        var vc = new VersionedCollection(db, collectionName + 'InsertWithColl', { debug: false, hide: true });
        // first make sure doc exists in the collection
        vc._applyOplogItem(oplogItem, function(err) {
          if (err) { throw err; }
          vc._snapshotCollection.find().toArray(function(err, items) {
            should.equal(err, null);
            should.equal(items.length, 1);

            // check and delete version
            var v = items[0]._id._v;
            v.should.match(/^[a-z0-9/+A-Z]{8}$/);
            delete items[0]._id._v;

            should.deepEqual(items[0], {
              _id: { _co: '_applyOplogItemInsertWithColl', _id: 'bar', _pe: '_local', _pa: [], _lo: true, _i: 1 },
              _m3: { _ack: false, _op: new Timestamp(0, 0)},
              baz: 'foobar'
            });
            done();
          });
        });
      });

      it('should not have altered the original doc and have side-effects', function() {
        should.deepEqual(oplogItem, {
          op: 'i',
          o: {
            _id : 'bar',
            baz: 'foobar'
          }
        });
      });
    });

    describe('updateFullDoc', function() {
      var dagRoot = { _id: { _id: 'foo', _v: 'A', _pe: '_local', _pa: [] }, _m3: { _ack: true, _op: new Timestamp(1414516131, 1) } };
      var oplogItem  = {
        ts: new Timestamp(1414516132, 1),
        op: 'u',
        o: { _id: 'foo', _v: 'A', qux: 'quux' },
        o2: { _id: 'foo' }
      };

      it('should insert a new item into the DAG', function(done) {
        var vc = new VersionedCollection(db, collectionName + 'UpdateFullDocWithColl', { hide: true });
        // first make sure doc exists in the collection
        vc._snapshotCollection.insert(dagRoot, {w: 1}, function(err, inserts) {
          if (err) { throw err; }
          should.equal(inserts.length, 1);
          vc._applyOplogItem(oplogItem, function(err) {
            if (err) { throw err; }
            vc._snapshotCollection.find().toArray(function(err, items) {
              should.equal(err, null);
              should.equal(items.length, 2);

              should.deepEqual(items[0], dagRoot);

              // check and delete version
              var v = items[1]._id._v;
              v.should.match(/^[a-z0-9/+A-Z]{8}$/);
              delete items[1]._id._v;

              should.deepEqual(items[1], {
                _id: { _co: '_applyOplogItemUpdateFullDocWithColl', _id: 'foo', _pe: '_local', _pa: ['A'], _lo: true, _i: 1 },
                _m3: { _ack: false, _op: new Timestamp(1414516132, 1)  },
                qux: 'quux'
              });
              done();
            });
          });
        });
      });

      it('should not have altered the original doc and have side-effects', function() {
        should.deepEqual(oplogItem, {
          ts: new Timestamp(1414516132, 1),
          op: 'u',
          o: { _id: 'foo', _v: 'A', qux: 'quux' },
          o2: { _id: 'foo' }
        });
      });
    });

    describe('updateModifier', function() {
      var mod = { $set: { bar: 'baz' } };
      var oplogItem = { ts: new Timestamp(1414516132, 1), o: mod, op: 'u', o2: { _id: 'foo', _v: 'A' } };
      var dagRoot = { _id: { _id: 'foo', _v: 'A', _pe: '_local', _pa: [] }, qux: 'quux', _m3: { _ack: true, _op: new Timestamp(1414516132, 1) } };

      it('should insert a new item into the DAG', function(done) {
        var vc = new VersionedCollection(db, collectionName + 'UpdateModifierWithColl', { hide: true });
        // first make sure doc exists in the collection
        vc._snapshotCollection.insert(dagRoot, {w: 1}, function(err, inserts) {
          if (err) { throw err; }
          should.equal(inserts.length, 1);
          vc._applyOplogItem(oplogItem, function(err) {
            if (err) { throw err; }
            vc._snapshotCollection.find().toArray(function(err, items) {
              should.equal(err, null);
              should.equal(items.length, 2);

              should.deepEqual(items[0], dagRoot);

              // check and delete version
              var v = items[1]._id._v;
              v.should.match(/^[a-z0-9/+A-Z]{8}$/);
              delete items[1]._id._v;

              should.deepEqual(items[1], {
                _id: { _co: '_applyOplogItemUpdateModifierWithColl', _id: 'foo', _pe: '_local', _pa: ['A'], _lo: true, _i: 1 },
                _m3: { _ack: false, _op: new Timestamp(1414516132, 1) },
                bar: 'baz',
                qux: 'quux'
              });
              done();
            });
          });
        });
      });

      it('should not have altered the original doc and have side-effects', function() {
        should.deepEqual(oplogItem, { ts: new Timestamp(1414516132, 1), o: mod, op: 'u', o2: { _id: 'foo', _v: 'A' } });
      });
    });
  });

  describe('_applyOplogInsertItem', function() {
    var collectionName = '_applyOplogInsertItem';

    var oplogItem = {
      ts: new Timestamp(1414516124, 1),
      op: 'i',
      o: {
        _id : 'foo',
        baz: 'raboof'
      }
    };

    var doc = { _id: 'bar', bar: 'qux', _v: 'A' };
    var oplogItem2 = {
      ts: new Timestamp(1414516125, 1),
      op: 'i',
      o: {
        _id : 'bar',
        baz: 'foobar'
      }
    };

    it('should require cb to be a function', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function() { vc._applyOplogInsertItem(); }).should.throw('cb must be a function');
    });

    it('should require op to be "i" (or "u")', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      var updateItem  = {
        op: 'd'
      };
      vc._applyOplogInsertItem(updateItem, function(err) {
        should.equal(err.message, 'oplogItem.op must be "u" or "i"');
        done();
      });
    });

    it('should require oplogItem.o._id', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._applyOplogInsertItem({ o: { }, op: 'i' }, function(err) {
        should.equal(err.message, 'missing oplogItem.o._id');
        done();
      });
    });

    it('should add to DAG and copy to collection', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc._applyOplogInsertItem(oplogItem, function(err) {
        should.equal(err, null);

        // check if added to the DAG
        vc._snapshotCollection.find().toArray(function(err, items) {
          should.equal(err, null);
          should.equal(items.length, 1);

          // check if version is generated
          var v = items[0]._id._v;
          v.should.match(/^[a-z0-9/+A-Z]{8}$/);
          should.deepEqual(items[0], {
            _id: { _co: '_applyOplogInsertItem', _id: 'foo', _v: v, _pe: '_local', _pa: [], _lo: true, _i: 1 },
            _m3: { _ack: false, _op: oplogItem.ts },
            baz: 'raboof'
          });

          // check if version in collection is inserted
          vc._collection.find().toArray(function(err, items) {
            should.equal(err, null);
            should.equal(items.length, 1);

            should.deepEqual(items[0], {
              _id: 'foo',
              baz: 'raboof',
              _v: v
            });

            done();
          });
        });
      });
    });

    it('should complain about existing parent and don\'t add to the DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc._applyOplogInsertItem(oplogItem, function(err) {
        should.equal(err.message, 'previous version of item not a deletion');
        vc._snapshotCollection.find().toArray(function(err, items) {
          should.equal(err, null);
          should.equal(items.length, 1);
          done();
        });
      });
    });

    it('should set _ack to true if version and attributes match', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });

      var oplogItem = {
        ts: new Timestamp(1414516126, 1),
        op: 'i',
        o: {
          _id : 'foo',
          baz: 'raboof'
        }
      };

      // check if added to the DAG
      vc._snapshotCollection.find().toArray(function(err, items) {
        should.equal(err, null);
        should.equal(items.length, 1);

        // get version
        var v = items[0]._id._v;
        oplogItem.o._v = v;

        vc._applyOplogInsertItem(oplogItem, function(err) {
          if (err) { throw err; }

          vc._snapshotCollection.find().toArray(function(err, items) {
            if (err) { throw err; }
            should.equal(items.length, 1);

            should.deepEqual(items[0], {
              _id: { _co: '_applyOplogInsertItem', _id: 'foo', _v: v, _pe: '_local', _pa: [], _lo: true, _i: 1 },
              _m3: { _ack: true, _op: oplogItem.ts },
              baz: 'raboof'
            });

            // check if version in collection is not altered
            vc._collection.find().toArray(function(err, items) {
              if (err) { throw err; }
              should.equal(items.length, 1);

              should.deepEqual(items[0], {
                _id: 'foo',
                baz: 'raboof',
                _v: v
              });

              done();
            });
          });
        });
      });
    });

    it('needs new doc in the normal collection for further testing', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._collection.insert(doc, {w: 1}, function(err, inserts) {
        if (err) { throw err; }
        should.equal(inserts.length, 1);
        done();
      });
    });

    it('should insert a new item into the DAG and copy to collection', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true });
      vc._applyOplogInsertItem(oplogItem2, function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.find().toArray(function(err, items) {
          should.equal(err, null);
          should.equal(items.length, 2);

          // check and delete version
          // but keep track to inspect if _v on collection item is updated
          var v = items[1]._id._v;
          v.should.match(/^[a-z0-9/+A-Z]{8}$/);

          should.deepEqual(items[1], {
            _id: { _co: '_applyOplogInsertItem', _id: 'bar', _v: v, _pe: '_local', _pa: [], _lo: true, _i: 2 },
            _m3: { _ack: false, _op: oplogItem2.ts },
            baz: 'foobar'
          });

          // check if version in collection is inserted
          vc._collection.find().toArray(function(err, items) {
            should.equal(err, null);
            should.equal(items.length, 2);

            should.deepEqual(items[1], {
              _id: 'bar',
              baz: 'foobar',
              _v: v
            });

            done();
          });
        });
      });
    });

    it('should not have altered the original doc and have side-effects', function() {
      should.deepEqual(oplogItem2, {
        ts: new Timestamp(1414516125, 1),
        op: 'i',
        o: {
          _id : 'bar',
          baz: 'foobar'
        }
      });
    });

    it('needs deletion item in DAG and delete item from collection', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false });
      var item = {
        _id: { _co: '_applyOplogInsertItem', _id: 'foo', _v: 'E', _pe: '_local', _pa: ['D'], _i: 3, _d: true },
        _m3: { _ack: true, _op: new Timestamp(1414516144, 1) }
      };
      vc._snapshotCollection.insert(item, function(err) {
        if (err) { throw err; }
        vc._collection.remove({ _id: 'foo' }, done);
      });
    });

    it('should not complain about existing parent (deletion) and add new root to DAG with prev item as parent', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc._applyOplogInsertItem(oplogItem, function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.find().toArray(function(err, items) {
          should.equal(err, null);
          should.equal(items.length, 4);

          should.deepEqual(items[2], {
            _id: { _co: '_applyOplogInsertItem', _id: 'foo', _v: 'E', _pe: '_local', _pa: ['D'], _d: true, _i: 3 },
            _m3: { _ack: true, _op: new Timestamp(1414516144, 1) }
          });

          var v = items[3]._id._v;
          v.should.match(/^[a-z0-9/+A-Z]{8}$/);
          should.deepEqual(items[3], {
            _id: { _co: '_applyOplogInsertItem', _id: 'foo', _v: v, _pe: '_local', _pa: ['E'], _lo: true, _i: 4 },
            _m3: { _ack: false, _op: oplogItem.ts  },
            baz: 'raboof'
          });

          // check if version in collection is inserted
          vc._collection.find({ _id: 'foo' }).toArray(function(err, items) {
            should.equal(err, null);
            should.equal(items.length, 1);
            should.deepEqual(items[0], {
              _id: 'foo',
              baz: 'raboof',
              _v: v
            });

            done();
          });
        });
      });
    });
  });

  describe('_applyOplogUpdateFullDoc', function() {
    var collectionName = '_applyOplogUpdateFullDoc';

    var time = new Date();
    var oplogItem = {
      ts: new Timestamp(1414516188, 1),
      op: 'u',
      o: { _id: 'foo', _v: 'A', qux: 'quux', foo: time }
    };

    it('should require cb to be a function', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function() { vc._applyOplogUpdateFullDoc(); }).should.throw('cb must be a function');
    });

    it('should require op to be "u" or ("i")', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      var updateItem  = {
        op: 'd'
      };
      vc._applyOplogUpdateFullDoc(updateItem, function(err) {
        should.equal(err.message, 'oplogItem.op must be "u" or "i"');
        done();
      });
    });

    it('should require oplogItem.o._id', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._applyOplogUpdateFullDoc({ o: {}, op: 'u' }, function(err) {
        should.equal(err.message, 'missing oplogItem.o._id');
        done();
      });
    });

    it('should complain about missing parent and don\'t add to the DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc._applyOplogUpdateFullDoc(oplogItem, function(err) {
        should.equal(err.message, 'previous version of item not found');
        vc._snapshotCollection.find().toArray(function(err, items) {
          should.equal(err, null);
          should.equal(items.length, 0);
          done();
        });
      });
    });

    it('should insert root into DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      var item = {
        _id: { _id: 'foo', _v: 'A', _pe: '_local', _pa: [], _lo: true },
        _m3: { _ack: true, _op: new Timestamp(1414516187, 1) },
        bar: 'qux'
      };
      vc._snapshotCollection.insert(item, {w: 1}, function(err, inserts) {
        if (err) { throw err; }
        should.equal(inserts.length, 1);
        done();
      });
    });

    it('should find last ackd item as parent and add to DAG', function(done) {
      var item = {
        ts: new Timestamp(1414516190, 1),
        op: 'u',
        o: { _id: 'foo', qux: 'quux', foo: time }
      };

      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true });
      vc._applyOplogUpdateFullDoc(item, function(err) {
        should.equal(err, null);

        // check if added to the DAG
        vc._snapshotCollection.find().toArray(function(err, items) {
          should.equal(err, null);
          should.equal(items.length, 2);

          // check if version is generated
          items[1]._id._v.should.match(/^[a-z0-9/+A-Z]{8}$/);
          delete items[1]._id._v;
          should.deepEqual(items[1], {
            _id: { _co: '_applyOplogUpdateFullDoc', _id: 'foo', _pe: '_local', _pa: ['A'], _lo: true, _i: 1 },
            _m3: { _ack: false, _op: item.ts },
            qux: 'quux',
            foo: time
          });

          // check if version in collection is inserted
          vc._collection.find().toArray(function(err, items) {
            should.equal(err, null);
            should.equal(items.length, 1);
            done();
          });
        });
      });
    });

    it('should insert a new item into the DAG that was already in the collection', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true });
      vc._applyOplogUpdateFullDoc(oplogItem, function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.find().toArray(function(err, items) {
          should.equal(err, null);
          should.equal(items.length, 3);

          // check and delete parents
          should.equal(items[2]._id._pa.length, 1);
          should.equal(items[2]._id._pa[0], items[1]._id._v);
          delete items[2]._id._pa;

          // check and delete version
          // but keep track to inspect if _v on collection item is updated
          items[2]._id._v.should.match(/^[a-z0-9/+A-Z]{8}$/);
          delete items[2]._id._v;

          should.deepEqual(items[2], {
            _id: { _co: '_applyOplogUpdateFullDoc', _id: 'foo', _pe: '_local', _lo: true, _i: 2 },
            _m3: { _ack: false, _op: oplogItem.ts },
            qux: 'quux',
            foo: time
          });
          done();
        });
      });
    });

    it('needs an unackd item from a remote for the next test', function(done) {
      var item = {
        _id: { _co: '_applyOplogUpdateFullDoc', _id: 'foo', _v: 'X', _pe: '_local', _pa: ['A'], _i: 3 },
        _m3: { _ack: false, _op: new Timestamp(1414511111, 1) },
        qux: 'quux'
      };

      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true });
      vc._snapshotCollection.insert(item, function(err, inserted) {
        if (err) { throw err; }
        should.equal(inserted.length, 1);
        done();
      });
    });

    it('should set remote item ackd', function(done) {
      var item = {
        ts: new Timestamp(1414511112, 1),
        op: 'u',
        o: { _id: 'foo', qux: 'quux', _v: 'X' }
      };

      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true });
      vc._applyOplogUpdateFullDoc(item, function(err) {
        should.equal(err, null);

        // check if ackd
        vc._snapshotCollection.find().toArray(function(err, items) {
          should.equal(err, null);
          should.equal(items.length, 4);

          // check if version is generated
          should.deepEqual(items[3], {
            _id: { _co: '_applyOplogUpdateFullDoc', _id: 'foo', _v: 'X', _pe: '_local', _pa: ['A'], _i: 3 },
            _m3: { _ack: true, _op: item.ts },
            qux: 'quux'
          });

          // check if version in collection is inserted
          vc._collection.find().toArray(function(err, items) {
            should.equal(err, null);
            should.equal(items.length, 1);
            done();
          });
        });
      });
    });

    describe('multiple heads', function() {
      var collectionName = '_applyOplogUpdateFullDocMultipleHeads';
      var perspective = 'I';

      var AI = {
        _id : { _id: 'foo', _v: 'A', _pe: 'I', _pa: [], _i: 1 },
        _m3: { _ack: true, _op: new Timestamp(1414511122, 1) }
      };

      var BI = {
        _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'], _i: 2 },
        _m3: { _ack: false, _op: new Timestamp(1414511133, 1) }
      };

      it('should insert root and an non-locally created item', function(done) {
        var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective, debug: false, hide: true });
        var DAG = [AI, BI];
        vc._snapshotCollection.insert(DAG, {w: 1}, function(err, inserts) {
          if (err) { throw err; }
          should.equal(DAG.length, inserts.length);
          done();
        });
      });

      it('should merge if multiple heads are created and copy merge to collection', function(done) {
        var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective, debug: true, hide: false });
        var oplogItem = {
          ts: new Timestamp(1414511144, 1),
          op: 'u',
          o: { _id: 'foo', qux: 'quux' }
        };

        vc._applyOplogUpdateFullDoc(oplogItem, function(err) {
          if (err) { throw err; }
          vc._snapshotCollection.find().toArray(function(err, items) {
            should.equal(err, null);
            should.equal(items.length, 4);

            // check and delete parents
            should.deepEqual(items[0], {
              _id : { _id: 'foo', _v: 'A', _pe: 'I', _pa: [], _i: 1 },
              _m3: { _ack: true, _op: new Timestamp(1414511122, 1) }
            });
            should.deepEqual(items[1], {
              _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'], _i: 2 },
              _m3: { _ack: false, _op: new Timestamp(1414511133, 1) }
            });

            // check and delete version
            // but keep track to inspect if merge item has the right parent
            var v2 = items[2]._id._v;
            v2.should.match(/^[a-z0-9/+A-Z]{8}$/);
            delete items[2]._id._v;

            should.deepEqual(items[2], {
              _id : { _co: '_applyOplogUpdateFullDocMultipleHeads', _id: 'foo', _pe: 'I', _pa: ['A'], _lo: true, _i: 3 },
              _m3: { _ack: false, _op: oplogItem.ts },
              qux: 'quux'
            });

            // 4th item should be the merged item
            var v3 = items[3]._id._v;
            v3.should.match(/^[a-z0-9/+A-Z]{8}$/);
            delete items[3]._id._v;

            // check for both parents
            should.deepEqual(items[3]._id._pa, ['B', v2]);
            delete items[3]._id._pa;

            should.deepEqual(items[3], {
              _id: { _co: '_applyOplogUpdateFullDocMultipleHeads', _id: 'foo', _pe: 'I', _lo: true, _i: 4 },
              _m3: { _ack: false, _op: new Timestamp(0, 0) },
              qux: 'quux'
            });

            // finally check if this merged item has been copied to the collection 
            vc._collection.find().toArray(function(err, items) {
              if (err) { throw err; }
              should.deepEqual(items, [{
                _id: 'foo',
                qux: 'quux',
                _v: v3
              }]);
              done();
            });
          });
        });
      });

      it('should not have altered the original doc and have side-effects', function() {
        should.deepEqual(oplogItem, {
          ts: new Timestamp(1414516188, 1),
          op: 'u',
          o: { _id: 'foo', _v: 'A', qux: 'quux', foo: time }
        });
      });
    });
  });

  describe('_applyOplogUpdateModifier', function() {
    var collectionName = '_applyOplogUpdateModifier';

    var mod = { $set: { bar: 'baz' } };
    var oplogItem = { ts: new Timestamp(999, 1), o: mod, op: 'u', o2: { _id: 'foo', _v: 'A' } };

    it('should require cb to be a function', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function() { vc._applyOplogUpdateModifier(); }).should.throw('cb must be a function');
    });

    it('should complain about missing parent and don\'t add to the DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._applyOplogUpdateModifier(oplogItem, function(err) {
        should.equal(err.message, 'previous version of doc not found');
        vc._snapshotCollection.find().toArray(function(err, items) {
          should.equal(err, null);
          should.equal(items.length, 0);
          done();
        });
      });
    });

    it('should insert root into DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      var item = {
        _id: { _id: 'foo', _v: 'A', _pe: '_local', _pa: [] },
        _m3: { _ack: true },
        bar: 'qux'
      };
      vc._snapshotCollection.insert(item, {w: 1}, function(err, inserts) {
        if (err) { throw err; }
        should.equal(inserts.length, 1);
        done();
      });
    });

    it('should add new item to DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true });
      vc._applyOplogUpdateModifier(oplogItem, function(err) {
        if (err) { throw err; }
        // check if added to the DAG
        vc._snapshotCollection.find().toArray(function(err, items) {
          if (err) { throw err; }
          should.equal(items.length, 2);

          // check if version is generated
          var v = items[1]._id._v;
          v.should.match(/^[a-z0-9/+A-Z]{8}$/);
          delete items[1]._id._v;
          should.deepEqual(items[1], {
            _id: { _co: '_applyOplogUpdateModifier', _id: 'foo', _pe: '_local', _pa: ['A'], _lo: true, _i: 1 },
            _m3: { _ack: false, _op: oplogItem.ts },
            bar: 'baz'
          });
          done();
        });
      });
    });

    it('should ack and then insert a new parent into the DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false, hide: true });
      vc._applyOplogUpdateModifier(oplogItem, function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.find().toArray(function(err, items) {
          should.equal(err, null);
          should.equal(items.length, 3);

          // check and delete parents
          should.equal(items[2]._id._pa.length, 1);
          should.equal(items[2]._id._pa[0], items[1]._id._v);
          delete items[2]._id._pa;

          // check and delete version
          var v = items[2]._id._v;
          v.should.match(/^[a-z0-9/+A-Z]{8}$/);
          delete items[2]._id._v;

          should.deepEqual(items[2], {
            _id: { _co: '_applyOplogUpdateModifier', _id: 'foo', _pe: '_local', _lo: true, _i: 2 },
            _m3: { _ack: false, _op: oplogItem.ts },
            bar: 'baz'
          });
          done();
        });
      });
    });

    it('should not have altered the original doc and have side-effects', function() {
      should.deepEqual(oplogItem, {
        ts: new Timestamp(999, 1),
        o: { $set: { bar: 'baz' } },
        op: 'u',
        o2: { _id: 'foo', _v: 'A' }
      });
    });

    it('regression new doc not created', function(done) {
      var collName = collectionName + 'regression1';

      var item = {
        _id: {
          _co: 'foo',
          _id: 'A',
          _v:  'bsb8uChl',
          _pe: '_local',
          _pa: ['XZiJCEpw']
        },
        _m3: { _ack: true, _op: new Timestamp(234, 1) },
        foo: 'bar',
        qux: 'qux'
      };

      var oplogItem2 = {
        ts: '5934975428123951105',
        op: 'u',
        ns: 'bar.foo',
        o2: { _id: 'A' },
        o:  { $set: { baz: 'quux' } }
      };

      var vc = new VersionedCollection(db, collName, { debug: false, hide: true });
      vc._snapshotCollection.insert(item, function(err) {
        if (err) { throw err; }
        vc._applyOplogUpdateModifier(oplogItem2, function(err) {
          if (err) { throw err; }
          vc._snapshotCollection.find().toArray(function(err, items) {
            should.equal(err, null);
            should.equal(items.length, 2);

            should.deepEqual(items[0], {
              _id: {
                _co: 'foo',
                _id: 'A',
                _v:  'bsb8uChl',
                _pe: '_local',
                _pa: ['XZiJCEpw']
              },
              _m3: { _ack: true, _op: new Timestamp(234, 1)  },
              foo: 'bar',
              qux: 'qux'
            });

            items[1]._id._v.should.not.equal('bsb8uChl');
            delete items[1]._id._v;

            should.deepEqual(items[1], {
              _id: {
                _co: '_applyOplogUpdateModifierregression1',
                _id: 'A',
                _pe: '_local',
                _pa: ['bsb8uChl'],
                _lo: true,
                _i: 1
              },
              _m3: { _ack: false, _op: '5934975428123951105' },
              baz: 'quux',
              foo: 'bar',
              qux: 'qux'
            });

            done();
          });
        });
      });
    });
  });

  describe('_applyOplogDeleteItem', function() {
    var collectionName = '_applyOplogDeleteItem';

    var oplogItem  = {
      ts: new Timestamp(1234, 1),
      op: 'd',
      o: { _id: 'foo' }
    };

    it('should require cb to be a function', function() {
      var vc = new VersionedCollection(db, collectionName);
      (function() { vc._applyOplogDeleteItem(); }).should.throw('cb must be a function');
    });

    it('should require op to be "d"', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      var updateItem  = {
        op: 'i',
        o: { qux: 'quux' }
      };
      vc._applyOplogDeleteItem(updateItem, function(err) {
        should.equal(err.message, 'oplogItem.op must be "d"');
        done();
      });
    });

    it('should require oplogItem.o._id', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._applyOplogDeleteItem({ o: {}, op: 'd' }, function(err) {
        should.equal(err.message, 'missing oplogItem.o._id');
        done();
      });
    });

    it('should complain about missing parent and don\'t add to the DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._applyOplogDeleteItem(oplogItem, function(err) {
        should.equal(err.message, 'previous version of doc not found');
        vc._snapshotCollection.find().toArray(function(err, items) {
          should.equal(err, null);
          should.equal(items.length, 0);
          done();
        });
      });
    });

    it('should insert parent into DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      var item = {
        _id: { _id: 'foo', _v: 'A', _pe: '_local', _pa: [] },
        bar: 'qux',
        _m3: { _ack: true }
      };
      vc._snapshotCollection.insert(item, {w: 1}, done);
    });

    it('should add to DAG and don\'t complain about item missing in collection', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false });
      vc._applyOplogDeleteItem(oplogItem, function(err) {
        if (err) { throw err; }

        // check if added to the DAG
        vc._snapshotCollection.find().toArray(function(err, items) {
          if (err) { throw err; }
          should.equal(items.length, 2);

          // check if version is generated
          var v = items[1]._id._v;
          v.should.match(/^[a-z0-9/+A-Z]{8}$/);
          should.deepEqual(items[1], {
            _id: { _co: '_applyOplogDeleteItem', _id: 'foo', _v: v, _pe: '_local', _pa: ['A'], _lo: true, _i: 1, _d: true },
            _m3: { _ack: true, _op: new Timestamp(1234, 1) },
            bar: 'qux'
          });

          // ensure oplog delete items are not copied back to collection
          vc._collection.find().toArray(function(err, items) {
            if (err) { throw err; }
            should.equal(items.length, 0);
            done();
          });
        });
      });
    });

    it('should work with deleted items that have no version', function(done) {
      var vc = new VersionedCollection(db, collectionName, { debug: false });
      var item = { ts: new Timestamp(123, 1), op: 'd', o: { _id: 'foo' } };
      vc._applyOplogDeleteItem(item, function(err) {
        if (err) { throw err; }

        // check if added to the DAG
        vc._snapshotCollection.find().toArray(function(err, items) {
          should.equal(err, null);
          should.equal(items.length, 3);

          // check if version is generated
          items[2]._id._v.should.match(/^[a-z0-9/+A-Z]{8}$/);
          delete items[2]._id._v;
          items[2]._id._pa.shift().should.match(/^[a-z0-9/+A-Z]{8}$/);
          should.deepEqual(items[2], {
            _id: { _co: '_applyOplogDeleteItem', _id: 'foo', _pe: '_local', _pa: [], _lo: true, _i: 2, _d: true },
            _m3: { _ack: true, _op: new Timestamp(123, 1) },
            bar: 'qux'
          });

          // ensure oplog delete items are not copied back to collection
          vc._collection.find().toArray(function(err, items) {
            should.equal(err, null);
            should.equal(items.length, 0);
            done();
          });
        });
      });
    });
  });

  describe('_findLastAckdOrLocallyCreated', function() {
    var collectionName = '_findLastAckdOrLocallyCreated';
    var perspective = 'I';

    var fooAI = {
      _id : { _id: 'foo', _v: 'A', _pe: 'I', _pa: [], _i: 1 },
      _m3: { _ack: true }
    };

    var fooBI = {
      _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'], _lo: true, _i: 2 }
    };

    var barAI = {
      _id : { _id: 'bar', _v: 'A', _pe: 'I', _pa: [], _lo: true, _i: 3 }
    };

    var barBI = {
      _id : { _id: 'bar', _v: 'B', _pe: 'I', _pa: ['A'], _i: 4 },
      _m3: { _ack: true }
    };

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      vc._snapshotCollection.insert([fooAI, barAI, barBI, fooBI], {w: 1}, done);
    });

    it('should require cb to be a function', function() {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      (function() { vc._findLastAckdOrLocallyCreated(); }).should.throw('cb must be a function');
    });

    it('should find last foo item is a locally created item', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      vc._findLastAckdOrLocallyCreated('foo', function(err, obj) {
        if (err) { throw err; }
        should.deepEqual(obj, {
          _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'], _lo: true, _i: 2 }
        });
        done();
      });
    });

    it('should find last bar item is a ackd item', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective, debug: false });
      vc._findLastAckdOrLocallyCreated('bar', function(err, obj) {
        if (err) { throw err; }
        should.deepEqual(obj, {
          _id : { _id: 'bar', _v: 'B', _pe: 'I', _pa: ['A'], _i: 4 },
          _m3: { _ack: true }
        });
        done();
      });
    });

    it('should not find a parent for baz', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      vc._findLastAckdOrLocallyCreated('baz', function(err, item) {
        if (err) { throw err; }
        should.equal(item, null);
        done();
      });
    });
  });

  describe('maxOplogPointer', function() {
    var collectionName = 'maxOplogPointer';
    var perspective = 'I';

    var fooAI = {
      _id : { _id: 'foo', _v: 'A', _pe: 'I', _pa: [], _i: 1 },
      _m3: { _ack: true, _op: new Timestamp(1414516168, 15) }
    };

    var fooBI = {
      _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'], _lo: true, _i: 2 },
      _m3: { _ack: true, _op: new Timestamp(1414516170, 158) }
    };

    var barAI = {
      _id : { _id: 'bar', _v: 'A', _pe: 'I', _pa: [], _lo: true, _i: 3 },
      _m3: { _ack: true, _op: new Timestamp(0, 0) }
    };

    var barBI = {
      _id : { _id: 'bar', _v: 'B', _pe: 'I', _pa: ['A'], _i: 4 },
      _m3: { _ack: true, _op: new Timestamp(1414516170, 165) }
    };

    it('should callback with null when no timestamps', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective, debug: false });
      vc.maxOplogPointer(function(err, result) {
        if (err) { throw err; }
        should.deepEqual(result, null);
        done();
      });
    });

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      vc._snapshotCollection.insert([barAI], {w: 1}, done);
    });

    it('should callback with null when no timestamps greater than 0, 0', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective, debug: false });
      vc.maxOplogPointer(function(err, result) {
        if (err) { throw err; }
        should.deepEqual(result, null);
        done();
      });
    });

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      vc._snapshotCollection.insert([fooAI, barBI, fooBI], {w: 1}, done);
    });
    
    it('should callback with the max oplog pointer of the versioned collection', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective, debug: false });
      vc.maxOplogPointer(function(err, result) {
        if (err) { throw err; }
        should.deepEqual(result, barBI._m3._op);
        done();
      });
    });  
  });

  describe('determineRemoteOffset', function() {
    var collectionName = 'determineRemoteOffset';
    var perspective = '_local';

    var snapshots = [
      { '_id' : { '_co' : 'tyres', '_id' : '001309478', '_v' : 'sZCqUpY6', '_pa' : [  'X2Sx6rXK' ], '_pe' : '_local', '_i' : 42 },
        '_m3' : { '_ack' : true, '_op' : new Timestamp(1415807848, 7) } },
      { '_id' : { '_co' : 'tyres', '_id' : '001309478', '_v' : 'X2Sx6rXK', '_pa' : [  '/TPEqPum' ], '_pe' : '_local', '_i' : 41 },
        '_m3' : { '_ack' : false, '_op' : new Timestamp(0, 0) } },
      { '_id' : { '_co' : 'tyres', '_id' : '001309478', '_v' : '/TPEqPum', '_pa' : [  'daiye3wo' ], '_pe' : '_local', '_i' : 40 },
        '_m3' : { '_ack' : true, '_op' : new Timestamp(1415807844, 6) } },
      { '_id' : { '_co' : 'tyres', '_id' : '001309478', '_v' : 'daiye3wo', '_pe' : '_local', '_pa' : [ ], '_lo' : true, '_i' : 1 },
        '_m3' : { '_ack' : false, '_op' : new Timestamp(0, 0) } },
      { '_id' : { '_co' : 'tyres', '_id' : '001309478', '_v' : 'daiye3wo', '_pa' : [ ], '_pe' : 'euromastercontracts' },
        '_m3' : { '_ack' : false, '_op' : new Timestamp(0, 0) } },
      { '_id' : { '_co' : 'tyres', '_id' : '001309478', '_v' : '/TPEqPum', '_pa' : [  'daiye3wo' ], '_pe' : 'euromastercontracts' },
        '_m3' : { '_ack' : false, '_op' : new Timestamp(0, 0) } },
      { '_id' : { '_co' : 'tyres', '_id' : '001309478', '_v' : 'X2Sx6rXK', '_pa' : [  '/TPEqPum' ], '_pe' : 'euromastercontracts' },
        '_m3' : {  } }
    ];

    var extraSnapshot = { '_id' : { '_co' : 'tyres', '_id' : '001309478', '_v' : 'sZCqUpY6', '_pa' : [  'X2Sx6rXK' ], '_pe' : 'euromastercontracts' },
        '_m3' : { '_ack' : false, '_op' : new Timestamp(0, 0) } };

    it('should callback with null when no snapshots', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      vc.determineRemoteOffset('euromastercontracts', function(err, result) {
        if (err) { throw err; }
        should.deepEqual(result, null);
        done();
      });
    });

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      vc._snapshotCollection.insert(snapshots, {w: 1}, done);
    });
    
    it('should callback with the last received snapshot version for the perspective', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      vc.determineRemoteOffset('euromastercontracts', function(err, result) {
        if (err) { throw err; }
        should.deepEqual(result, 'X2Sx6rXK');
        done();
      });
    });

    it('should save extra DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      vc._snapshotCollection.insert(extraSnapshot, {w: 1}, done);
    });

    it('should callback with the new last received snapshot version for the perspective', function(done) {
      var vc = new VersionedCollection(db, collectionName, { localPerspective: perspective });
      vc.determineRemoteOffset('euromastercontracts', function(err, result) {
        if (err) { throw err; }
        should.deepEqual(result, 'sZCqUpY6');
        done();
      });
    }); 
  });

  describe('_generateRandomVersion', function() {
    it('should generate a new id of 5 bytes', function() {
      VersionedCollection._generateRandomVersion(5).length.should.equal(8);
    });
    it('should encode in base64', function() {
      VersionedCollection._generateRandomVersion(3).should.match(/^[a-z0-9/+A-Z]{4}$/);
    });
    it('should generate a new id of 6 bytes default', function() {
      VersionedCollection._generateRandomVersion().should.match(/^[a-z0-9/+A-Z]{8}$/);
    });
    it('should error with unsupported sizes', function() {
      (function() { VersionedCollection._generateRandomVersion('a'); }).should.throwError('Argument #1 must be number > 0');
    });
  });

  describe('invalidOplogItem', function() {
    it('should not be valid if no parameter given', function() {
      should.strictEqual(VersionedCollection.invalidOplogItem(), 'missing item');
    });
    it('should not be valid if parameter is empty object', function() {
      should.strictEqual(VersionedCollection.invalidOplogItem({}), 'missing item.o');
    });
    it('should not be valid if object has no o attribute', function() {
      should.strictEqual(VersionedCollection.invalidOplogItem({ ts: 'a', op: 'b', ns: 'c' }), 'missing item.o');
    });
    it('should not be valid if object has no ts attribute', function() {
      should.strictEqual(VersionedCollection.invalidOplogItem({ o: 'a', op: 'b', ns: 'c' }), 'missing item.ts');
    });
    it('should not be valid if object has no op attribute', function() {
      should.strictEqual(VersionedCollection.invalidOplogItem({ ts: 'a', o: 'b', ns: 'c' }), 'missing item.op');
    });
    it('should not be valid if object has no ns attribute', function() {
      should.strictEqual(VersionedCollection.invalidOplogItem({ ts: 'a', o: 'b', op: 'c' }), 'missing item.ns');
    });
    it('should be valid if op is u', function() {
      should.strictEqual(VersionedCollection.invalidOplogItem({ op: 'u', ts: 'a', o: 'b', ns: 'c' }), '');
    });
    it('should be valid if op is i', function() {
      should.strictEqual(VersionedCollection.invalidOplogItem({ op: 'i', ts: 'a', o: 'b', ns: 'c' }), '');
    });
    it('should be valid if op is d', function() {
      should.strictEqual(VersionedCollection.invalidOplogItem({ op: 'd', ts: 'a', o: 'b', ns: 'c' }), '');
    });
    it('should not be valid if op is not u, i or d', function() {
      should.strictEqual(VersionedCollection.invalidOplogItem({ op: 'a', ts: 'a', o: 'b', ns: 'c' }), 'invalid item.op');
    });
  });
});
