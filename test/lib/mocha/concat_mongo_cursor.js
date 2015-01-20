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

/*jshint -W068, nonew: false */

var should = require('should');

var ConcatMongoCursor = require('../../../lib/concat_mongo_cursor');
var ConcatMongoStream = require('../../../lib/concat_mongo_stream');
var ArrayCursor = require('../../../lib/array_cursor');

var db;
var databaseName = 'test_concat_mongo_cursor';
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

describe('concat_mongo_cursor', function() {
  describe('constructor', function() {
    var collName1 = 'contructor1';
    var collName2 = 'contructor2';

    it('should require colls to be an array', function() {
      (function () { new ConcatMongoCursor({}); }).should.throwError('colls must be an array');
    });

    it('should require colls to have at least one element', function() {
      (function () { new ConcatMongoCursor([]); }).should.throwError('colls must contain at least one element');
    });

    it('should require opts to be an object', function() {
      (function () { new ConcatMongoCursor([{}], 1); }).should.throwError('opts must be an object');
    });

    it('should require opts.debug to be a boolean', function() {
      (function () { new ConcatMongoCursor([{}], { debug: 1 }); }).should.throwError('opts.debug must be a boolean');
    });

    it('should require opts.hide to be a boolean', function() {
      (function () { new ConcatMongoCursor([{}], { hide: 1 }); }).should.throwError('opts.hide must be a boolean');
    });

    it('should require that all elements in colls are objects', function() {
      (function () { new ConcatMongoCursor([{}, 1, {}], { hide: true }); }).should.throwError('colls must only contain objects');
    });

    it('should construct', function() {
      var coll1 = db.collection(collName1);
      var coll2 = db.collection(collName2);
      var cmc;
      (function() { cmc = new ConcatMongoCursor([coll1, coll2]); }).should.not.throwError();
    });
  });

  describe('findOne', function() {
    var collName1 = 'findOne';

    var A = { _id: { _id: 'foo', _v: 'A', _pe: 'bar' }, _m3: { _ack: true }, foo: 'bar' };
    var Ap = { _id: { _id: 'foo', _v: 'A', _pe: 'foo' }, _m3: { _ack: true }, foo: 'bar' };
    var B = { _id: { _id: 'foo', _v: 'B', _pe: 'bar', _pa: ['A'] }, _m3: { _ack: true }, foo: 'baz' };
    var Bp = { _id: { _id: 'foo', _v: 'B', _pe: 'foo', _pa: ['A'] }, _m3: { _ack: true }, foo: 'baz' };
    var C = { _id: { _id: 'foo', _v: 'C', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: true }, foo: 'qux' };
    var D = { _id: { _id: 'foo', _v: 'D', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: false }, foo: 'quux' };

    it('should save DAG', function(done) {
      var coll = db.collection(collName1);
      coll.insert([A, Ap, B, Bp], {w: 1}, done);
    });

    describe('sort asc', function() {
      it('should find one from second collection', function(done) {
        var coll1 = db.collection(collName1);
        var coll2 = new ArrayCursor([C, D]);
        var vc = new ConcatMongoCursor([coll1, coll2]);

        vc.findOne({ '_id._v': 'C' }, function(err, item) {
          if (err) { throw err; }
          should.deepEqual(item, C);
          done();
        });
      });

      it('should not find any non-existing item', function(done) {
        var coll1 = db.collection(collName1);
        var coll2 = new ArrayCursor([C, D]);
        var vc = new ConcatMongoCursor([coll1, coll2]);

        vc.findOne({ '_id._v': 'X' }, function(err, item) {
          if (err) { throw err; }
          should.deepEqual(item, null);
          done();
        });
      });

      it('should find one from db collection', function(done) {
        var coll1 = db.collection(collName1);
        var coll2 = new ArrayCursor([C, D]);
        var vc = new ConcatMongoCursor([coll1, coll2]);

        vc.findOne({ '_id._v': 'B' }, function(err, item) {
          if (err) { throw err; }
          should.deepEqual(item, B);
          done();
        });
      });
    });

    describe('sort desc', function() {
      it('should find one from second collection', function(done) {
        var coll1 = db.collection(collName1);
        var coll2 = new ArrayCursor([C, D]);
        var vc = new ConcatMongoCursor([coll1, coll2]);

        vc.findOne({ '_id._v': 'C' }, { sort: { $natural: -1 }}, function(err, item) {
          if (err) { throw err; }
          should.deepEqual(item, C);
          done();
        });
      });

      it('should not find any non-existing item', function(done) {
        var coll1 = db.collection(collName1);
        var coll2 = new ArrayCursor([C, D]);
        var vc = new ConcatMongoCursor([coll1, coll2]);

        vc.findOne({ '_id._v': 'X' }, { sort: { $natural: -1 }}, function(err, item) {
          if (err) { throw err; }
          should.deepEqual(item, null);
          done();
        });
      });

      it('should find one from db collection', function(done) {
        var coll1 = db.collection(collName1);
        var coll2 = new ArrayCursor([C, D]);
        var vc = new ConcatMongoCursor([coll1, coll2]);

        vc.findOne({ '_id._v': 'B', '_id._pe': 'foo' }, { sort: { $natural: -1 }}, function(err, item) {
          if (err) { throw err; }
          should.deepEqual(item, Bp);
          done();
        });
      });
    });
  });

  describe('find', function() {
    var collName1 = 'find';

    it('should return a ConcatMongoCursor', function() {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCursor([]);
      var vc = new ConcatMongoCursor([coll1, coll2]);
      var result = vc.find();
      should.equal(result instanceof ConcatMongoCursor, true);
    });
  });

  describe('toArray', function() {
    var collName1 = 'toArray';

    var A = { _id: { _id: 'foo', _v: 'A', _pe: 'bar' }, _m3: { _ack: true }, foo: 'bar' };
    var Ap = { _id: { _id: 'foo', _v: 'A', _pe: 'foo' }, _m3: { _ack: true }, foo: 'bar' };
    var B = { _id: { _id: 'foo', _v: 'B', _pe: 'bar', _pa: ['A'] }, _m3: { _ack: true }, foo: 'baz' };
    var Bp = { _id: { _id: 'foo', _v: 'B', _pe: 'foo', _pa: ['A'] }, _m3: { _ack: true }, foo: 'baz' };
    var C = { _id: { _id: 'foo', _v: 'C', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: true }, foo: 'qux' };
    var D = { _id: { _id: 'foo', _v: 'D', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: false }, foo: 'quux' };

    it('should save DAG', function(done) {
      var coll = db.collection(collName1);
      coll.insert([A, Ap, B, Bp], {w: 1}, done);
    });

    describe('sort asc', function() {
      it('should find one from second collection', function(done) {
        var coll1 = db.collection(collName1);
        var coll2 = new ArrayCursor([C, D]);
        var vc = new ConcatMongoCursor([coll1, coll2]);

        vc.find({ '_id._v': 'C' }).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, [C]);
          done();
        });
      });

      it('should not find any non-existing item', function(done) {
        var coll1 = db.collection(collName1);
        var coll2 = new ArrayCursor([C, D]);
        var vc = new ConcatMongoCursor([coll1, coll2]);

        vc.find({ '_id._v': 'X' }).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, []);
          done();
        });
      });

      it('should find multiple from db collection', function(done) {
        var coll1 = db.collection(collName1);
        var coll2 = new ArrayCursor([C, D]);
        var vc = new ConcatMongoCursor([coll1, coll2]);

        vc.find({ '_id._v': 'B' }).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, [B, Bp]);
          done();
        });
      });

      it('should find multiple with perspective bar', function(done) {
        var coll1 = db.collection(collName1);
        var coll2 = new ArrayCursor([C, D]);
        var vc = new ConcatMongoCursor([coll1, coll2]);

        vc.find({ '_id._pe': 'bar' }).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, [A, B, C, D]);
          done();
        });
      });
    });

    describe('sort desc', function() {
      it('should find one from each collection', function(done) {
        var coll1 = db.collection(collName1);
        var coll2 = new ArrayCursor([C, D]);
        var vc = new ConcatMongoCursor([coll1, coll2]);

        vc.find({ '_id._v': { $in: ['B', 'C'] } }, { sort: { $natural: -1 }}).toArray(function(err, items) {
          if (err) { throw err; }
          should.equal(items.length, 3);
          should.deepEqual(items[0], C);
          should.deepEqual(items[1], Bp);
          should.deepEqual(items[2], B);
          done();
        });
      });

      it('should not find any non-existing item', function(done) {
        var coll1 = db.collection(collName1);
        var coll2 = new ArrayCursor([C, D]);
        var vc = new ConcatMongoCursor([coll1, coll2]);

        vc.find({ '_id._v': 'X' }, { sort: { $natural: -1 }}).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, []);
          done();
        });
      });

      it('should find one from db collection', function(done) {
        var coll1 = db.collection(collName1);
        var coll2 = new ArrayCursor([C, D]);
        var vc = new ConcatMongoCursor([coll1, coll2]);

        vc.find({ '_id._v': 'B', '_id._pe': 'foo' }, { sort: { $natural: -1 }}).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, [Bp]);
          done();
        });
      });

      it('should find multiple from db collection', function(done) {
        var coll1 = db.collection(collName1);
        var coll2 = new ArrayCursor([C, D]);
        var vc = new ConcatMongoCursor([coll1, coll2]);

        vc.find({ '_id._v': 'B' }, { sort: { $natural: -1 }}).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, [Bp, B]);
          done();
        });
      });

      it('should find multiple with perspective bar with second collection first', function(done) {
        var coll1 = db.collection(collName1);
        var coll2 = new ArrayCursor([C, D]);
        var vc = new ConcatMongoCursor([coll2, coll1]);

        vc.find({ '_id._pe': 'bar' }, { sort: { $natural: -1 }}).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, [B, A, D, C]);
          done();
        });
      });
    });
  });

  describe('stream', function() {
    var collName1 = 'stream';

    it('should return a ConcatMongoStream', function() {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCursor([]);
      var vc = new ConcatMongoCursor([coll1, coll2]);
      var stream = vc.stream();
      should.equal(stream instanceof ConcatMongoStream, true);
    });
  });
});
