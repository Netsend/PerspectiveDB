/**
 * Copyright 2014, 2015 Netsend.
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

/*jshint -W068, nonew: false */

var should = require('should');

var ArrayCollection = require('../../../lib/array_collection');
var ConcatMongoStream = require('../../../lib/concat_mongo_stream');
var logger = require('../../../lib/logger');

var silence;

var db;
var databaseName = 'test_concat_mongo_stream';
var Database = require('../../_database');

// open database connection
var database = new Database(databaseName);
before(function(done) {
  logger({ silence: true }, function(err, l) {
    if (err) { throw err; }
    silence = l;
    database.connect(function(err, dbc) {
      db = dbc;
      done(err);
    });
  });
});

after(function(done) {
  silence.close(function(err) {
    if (err) { throw err; }
    database.disconnect(done);
  });
});

describe('ConcatMongoStream', function() {
  var vColl;

  describe('constructor', function() {
    var collName1 = 'contructor1';
    var collName2 = 'contructor2';

    it('should require colls to be an array', function() {
      (function () { new ConcatMongoStream({}); }).should.throwError('colls must be an array');
    });

    it('should require colls to have at least one element', function() {
      (function () { new ConcatMongoStream([]); }).should.throwError('colls must contain at least one element');
    });

    it('should require opts to be an object', function() {
      (function () { new ConcatMongoStream([{}], 1); }).should.throwError('opts must be an object');
    });

    it('should require opts.log to be an object', function() {
      (function () { new ConcatMongoStream([{}], { log: 1 }); }).should.throwError('opts.log must be an object');
    });

    it('should require that all elements in colls are objects', function() {
      (function () { new ConcatMongoStream([{}, 1, {}], { log: silence }); }).should.throwError('colls must only contain objects');
    });

    it('should construct', function() {
      var coll1 = db.collection(collName1);
      var coll2 = db.collection(collName2);
      (function() { vColl = new ConcatMongoStream([coll1, coll2], { log: silence }); }).should.not.throwError();
    });
  });

  describe('stream', function() {
    var collName1 = 'stream';

    var A =  { _id: { _id: 'foo', _v: 'A', _pe: 'bar', _i: 2 }, _m3: { _ack: true }, foo: 'bar' };
    var Ap = { _id: { _id: 'foo', _v: 'A', _pe: 'foo', _i: 1 }, _m3: { _ack: true }, foo: 'bar' };
    var B =  { _id: { _id: 'foo', _v: 'B', _pe: 'bar', _pa: ['A'], _i: 6 }, _m3: { _ack: true }, foo: 'baz' };
    var Bp = { _id: { _id: 'foo', _v: 'B', _pe: 'foo', _pa: ['A'], _i: 5 }, _m3: { _ack: true }, foo: 'baz' };
    var C =  { _id: { _id: 'foo', _v: 'C', _pe: 'bar', _pa: ['B'], _i: 4 }, _m3: { _ack: true }, foo: 'qux' };
    var D =  { _id: { _id: 'foo', _v: 'D', _pe: 'bar', _pa: ['B'], _i: 3 }, _m3: { _ack: false }, foo: 'quux' };

    it('should save DAG', function(done) {
      db.collection(collName1).insert([A, Ap, B, Bp], {w: 1}, done);
    });

    it('should stream asc and append by default', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D], { log: silence });
      var vc = new ConcatMongoStream([coll1, coll2], { log: silence });
      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);
      });

      stream.on('close', function() {
        should.deepEqual(received, [A, Ap, B, Bp, C, D]);
        done();
      });
    });

    it('should stream only once (asc and append by default)', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D], { log: silence });
      var vc = new ConcatMongoStream([coll1, coll2], { log: silence });

      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);
      });

      stream.on('close', function() {
        should.deepEqual(received, [A, Ap, B, Bp, C, D]);

        should.strictEqual(vc.stream(), false);
        done();
      });
    });

    it('should stream only once (desc)', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D], { log: silence });
      var vc = new ConcatMongoStream([coll1, coll2], { log: silence }, [null, { sort: { $natural: -1 } }]);

      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);
      });
      stream.on('close', function() {
        should.equal(received.length, 6);
        should.deepEqual(received[0], D);
        should.deepEqual(received[1], C);
        should.deepEqual(received[2], Bp);
        should.deepEqual(received[3], B);
        should.deepEqual(received[4], Ap);
        should.deepEqual(received[5], A);

        should.strictEqual(vc.stream(), false);
        done();
      });
    });

    it('should stream desc', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D], { log: silence });
      var vc = new ConcatMongoStream([coll1, coll2], { log: silence }, [null, { sort: { $natural: -1 } }]);
      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);
      });
      stream.on('close', function() {
        should.equal(received.length, 6);
        should.deepEqual(received[0], D);
        should.deepEqual(received[1], C);
        should.deepEqual(received[2], Bp);
        should.deepEqual(received[3], B);
        should.deepEqual(received[4], Ap);
        should.deepEqual(received[5], A);
        done();
      });
    });

    it('should stream desc and not have side effects', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D], { log: silence });
      var colls = [coll1, coll2];
      var vc = new ConcatMongoStream(colls, { log: silence }, [null, { sort: { $natural: -1 } }]);
      var stream = vc.stream();

      stream.on('close', function() {
        should.strictEqual(colls.length, 2);
        should.strictEqual(colls[0] === coll1, true);
        should.strictEqual(colls[1] === coll2, true);
        done();
      });
    });

    it('should stream desc (ArrayCollection first)', function(done) {
      var coll1 = new ArrayCollection([C, D], { log: silence });
      var coll2 = db.collection(collName1);
      var vc = new ConcatMongoStream([coll1, coll2], { log: silence }, [null, { sort: { $natural: -1 } }]);
      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);
      });
      stream.on('close', function() {
        should.deepEqual(received, [Bp, B, Ap, A, D, C]);
        done();
      });
    });

    it('should filter', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D], { log: silence });
      var selector = { foo: { $in: [ 'bar', 'quux' ] } };
      var vc = new ConcatMongoStream([coll1, coll2], { log: silence }, [selector]);
      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);
      });

      stream.on('close', function() {
        should.equal(received.length, 3);
        should.deepEqual(received, [A, Ap, D]);
        done();
      });
    });

    it('should filter nested namespaces', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D], { log: silence });
      var selector = { '_id._v': { $in: [ 'B', 'D' ] } };
      var vc = new ConcatMongoStream([coll1, coll2], { log: silence }, [selector]);
      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);
      });

      stream.on('close', function() {
        should.equal(received.length, 3);
        should.deepEqual(received, [B, Bp, D]);
        done();
      });
    });

    describe('sort asc virtual item array', function() {
      it('empty', function(done) {
        var coll1 = db.collection(collName1);
        var coll2 = new ArrayCollection([], { log: silence });
        var vc = new ConcatMongoStream([coll2, coll1], { log: silence }, [null, { sort: { $natural: 1 } }]);
        var stream = vc.stream();

        var received = [];
        stream.on('data', function(item) {
          received.push(item);
        });
        stream.on('close', function() {
          should.deepEqual(received, [A, Ap, B, Bp]);
          done();
        });
      });
    });

    describe('sort desc virtual item array', function() {
      it('empty', function(done) {
        var coll1 = db.collection(collName1);
        var coll2 = new ArrayCollection([], { log: silence });
        var vc = new ConcatMongoStream([coll1, coll2], { log: silence }, [null, { sort: { $natural: -1 } }]);
        var stream = vc.stream();

        var received = [];
        stream.on('data', function(item) {
          received.push(item);
        });
        stream.on('close', function() {
          should.deepEqual(received, [Bp, B, Ap, A]);
          done();
        });
      });
    });

    describe('should use an index', function() {
      it('needs an index on the collection', function(done) {
        // set index on _id._i (from versioned_collection._clearSnapshot)
        db.createIndex(collName1, { '_id._i': -1 }, { name: '_id_i' }, function(err) {
          if (err) { throw err; }
          db.collection(collName1).indexes(done);
        });
      });

      it('should stream asc and sort collection on _id._i', function(done) {
        var coll1 = db.collection(collName1);
        var coll2 = new ArrayCollection([C, D], { log: silence });
        var vc = new ConcatMongoStream([coll1, coll2], { log: silence }, [null, { comment: 'test.index', sortIndex: '_id._i' }]);
        var stream = vc.stream();

        var received = [];
        stream.on('data', function(item) {
          received.push(item);
        });

        stream.on('close', function() {
          should.equal(received.length, 6);
          should.deepEqual(received[0], Ap);
          should.deepEqual(received[1], A);
          should.deepEqual(received[2], Bp);
          should.deepEqual(received[3], B);
          should.deepEqual(received[4], C);
          should.deepEqual(received[5], D);
          done();
        });
      });
    });
  });

  describe('pause', function() {
    var collName1 = 'pause';

    var A = { _id: { _id: 'foo', _v: 'A', _pe: 'bar' }, _m3: { _ack: true } };
    var B = { _id: { _id: 'foo', _v: 'B', _pe: 'bar', _pa: ['A'] }, _m3: { _ack: true } };
    var C = { _id: { _id: 'foo', _v: 'C', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: true } };
    var D = { _id: { _id: 'foo', _v: 'D', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: false } };

    it('should save DAG', function(done) {
      var coll = db.collection(collName1);
      coll.insert([A, B], {w: 1}, done);
    });

    it('should pause and resume virtual items and collection', function(done) {
      var coll1 = new ArrayCollection([C, D], { log: silence });
      var coll2 = db.collection(collName1);
      var vc = new ConcatMongoStream([coll1, coll2], { log: silence }, [null, { sort: { $natural: -1 }}]);

      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'B') {
          stream.pause();
          process.nextTick(function() {
            should.equal(received.length, 1);
            should.deepEqual(received[0], B);
            stream.resume();
          });
        }

        if (item._id._v === 'C') {
          stream.pause();
          process.nextTick(function() {
            should.equal(received.length, 4);
            should.deepEqual(received[0], B);
            should.deepEqual(received[1], A);
            should.deepEqual(received[2], D);
            should.deepEqual(received[3], C);
            stream.resume();
          });
        }
      });

      stream.on('close', function() {
        should.equal(received.length, 4);
        should.deepEqual(received[0], B);
        should.deepEqual(received[1], A);
        should.deepEqual(received[2], D);
        should.deepEqual(received[3], C);
        done();
      });
    });

    it('should pause and resume collection and virtual items', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D], { log: silence });
      var vc = new ConcatMongoStream([coll1, coll2], { log: silence });
      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'B') {
          stream.pause();
          process.nextTick(function() {
            should.deepEqual(received, [A, B]);
            stream.resume();
          });
        }

        if (item._id._v === 'D') {
          stream.pause();
          process.nextTick(function() {
            should.deepEqual(received, [A, B, C, D]);
            stream.resume();
          });
        }
      });

      stream.on('close', function() {
        should.deepEqual(received, [A, B, C, D]);
        done();
      });
    });

    it('should pause and destroy on first collection', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D], { log: silence });
      var vc = new ConcatMongoStream([coll1, coll2], { log: silence });

      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'A') {
          stream.pause();
          process.nextTick(function() {
            stream.destroy();
          });
          return;
        }

        // this should never be called
        done();
      });

      stream.on('close', function() {
        should.deepEqual(received, [A]);
        done();
      });
    });

    it('should pause and destroy on second collection', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D], { log: silence });
      var vc = new ConcatMongoStream([coll1, coll2], { log: silence });
      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'C') {
          stream.pause();
          process.nextTick(function() {
            stream.destroy();
          });
        }
      });

      stream.on('close', function() {
        should.deepEqual(received, [A, B, C]);
        done();
      });
    });

    it('should pause on first collection and stop calling back', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D], { log: silence });
      var vc = new ConcatMongoStream([coll1, coll2], { log: silence });
      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'A') {
          stream.pause();
          stream.on('data', done);
          should.deepEqual(received, [A]);
          done();
        }
      });

      stream.on('close', done);
    });

    it('should pause on second collection and stop calling back', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D], { log: silence });
      var vc = new ConcatMongoStream([coll1, coll2], { log: silence });
      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'C') {
          stream.pause();
          stream.on('data', done);
          should.deepEqual(received, [A, B, C]);
          done();
        }
      });

      stream.on('close', done);
    });
  });

  describe('destroy', function() {
    var collName1 = 'destroy';

    var A = { _id: { _id: 'foo', _v: 'A', _pe: 'bar' }, _m3: { _ack: true } };
    var B = { _id: { _id: 'foo', _v: 'B', _pe: 'bar', _pa: ['A'] }, _m3: { _ack: true } };
    var C = { _id: { _id: 'foo', _v: 'C', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: true } };
    var D = { _id: { _id: 'foo', _v: 'D', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: false } };

    it('should save DAG', function(done) {
      var coll = db.collection(collName1);
      coll.insert([A, B], {w: 1}, done);
    });

    it('should destroy while in first collection', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D], { log: silence });
      var vc = new ConcatMongoStream([coll1, coll2], { log: silence });

      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'B') {
          stream.destroy();
        }
      });

      stream.on('close', function() {
        should.equal(received.length, 2);
        should.deepEqual(received[0], A);
        should.deepEqual(received[1], B);
        stream.resume();
        done();
      });
    });

    it('should destroy while in second collection', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D], { log: silence });
      var vc = new ConcatMongoStream([coll1, coll2], { log: silence });

      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'C') {
          stream.destroy();
        }
      });

      stream.on('close', function() {
        should.equal(received.length, 3);
        should.deepEqual(received[0], A);
        should.deepEqual(received[1], B);
        should.deepEqual(received[2], C);
        stream.resume();
        done();
      });
    });

    it('should destroy and don\'t resume while in database', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D], { log: silence });
      var vc = new ConcatMongoStream([coll1, coll2], { log: silence });

      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'A') {
          stream.destroy();
          stream.resume();
        }
      });

      stream.on('close', function() {
        should.equal(received.length, 1);
        should.deepEqual(received[0], A);
        stream.resume();
        done();
      });
    });

    it('should destroy and don\'t resume while in virtual items', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D], { log: silence });
      var vc = new ConcatMongoStream([coll1, coll2], { log: silence }, [null, { sort: { $natural: -1 } }]);

      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'D') {
          stream.destroy();
          stream.resume();
        }
      });

      stream.on('close', function() {
        should.equal(received.length, 1);
        should.deepEqual(received[0], D);
        stream.resume();
        done();
      });
    });
  });

  describe('_sortDesc', function() {
    it('should return false without parameters', function() {
      should.strictEqual(ConcatMongoStream._sortDesc(), false);
    });

    it('should return false with empty object', function() {
      should.strictEqual(ConcatMongoStream._sortDesc({}), false);
    });

    it('should return false when first key is 1', function() {
      should.strictEqual(ConcatMongoStream._sortDesc({ foo: 1 }), false);
    });

    it('should return true when first key is -1', function() {
      should.strictEqual(ConcatMongoStream._sortDesc({ foo: -1 }), true);
    });

    it('should return false when first key is false', function() {
      should.strictEqual(ConcatMongoStream._sortDesc({ foo: false }), false);
    });

    it('should return false when first key is true', function() {
      should.strictEqual(ConcatMongoStream._sortDesc({ foo: true }), false);
    });

    it('should ignore second key', function() {
      should.strictEqual(ConcatMongoStream._sortDesc({ foo: -1, bar: false }), true);
    });
  });
});
