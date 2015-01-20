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

var ConcatMongoCollection = require('../../../lib/concat_mongo_collection');
var ConcatMongoCursor = require('../../../lib/concat_mongo_cursor');
var ArrayCollection = require('../../../lib/array_collection');

var db;
var databaseName = 'test_concat_mongo_collection';
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

describe('concat_mongo_collection', function() {
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

    it('should require cb to be a function', function() {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D]);
      var vc = new ConcatMongoCollection([coll1, coll2]);
      (function() { vc.findOne(); }).should.throw('cb must be a function');
    });

    it('should find an item in the second collection', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D]);
      var vc = new ConcatMongoCollection([coll1, coll2]);
      vc.findOne({ '_id._v': 'C' }, function(err, item) {
        if (err) { throw err; }
        should.deepEqual(item, C);
        done();
      });
    });

    it('should find an item in the first collection', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D]);
      var vc = new ConcatMongoCollection([coll1, coll2]);
      vc.findOne({ '_id._v': 'A' }, function(err, item) {
        if (err) { throw err; }
        should.deepEqual(item, A);
        done();
      });
    });

    it('should not find an item', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D]);
      var vc = new ConcatMongoCollection([coll1, coll2]);
      vc.findOne({ '_id._v': 'X' }, function(err, item) {
        if (err) { throw err; }
        should.deepEqual(item, null);
        done();
      });
    });
  });

  describe('find', function() {
    var collName1 = 'find';

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

    it('should return a ConcatMongoCursor', function() {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([]);
      var vc = new ConcatMongoCollection([coll1, coll2]);
      var result = vc.find();
      should.equal(result instanceof ConcatMongoCursor, true);
    });

    it('should support find with stream', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D]);
      var vc = new ConcatMongoCollection([coll1, coll2]);
      var s = vc.find().stream();

      var received = [];
      s.on('data', function(item) {
        received.push(item);
      });

      s.on('close', function() {
        should.deepEqual(received, [A, Ap, B, Bp, C, D]);
        done();
      });
    });

    it('should support find with stream, twice', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D]);
      var vc = new ConcatMongoCollection([coll1, coll2]);
      var s = vc.find({}, { sort: { '$natural': -1 } }).stream();

      var received = [];
      s.on('data', function(item) {
        received.push(item);
      });

      s.on('close', function() {
        should.deepEqual(received, [A, Ap, B, Bp, C, D].reverse());

        var s2 = vc.find({}, { sort: { '$natural': -1 } }).stream();

        received = [];
        s2.on('data', function(item) {
          received.push(item);
        });

        s2.on('close', function() {
          should.deepEqual(received, [A, Ap, B, Bp, C, D].reverse());
          done();
        });
      });
    });

    it('should support stream pause and resume on first collection', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D]);
      var vc = new ConcatMongoCollection([coll1, coll2]);
      var s = vc.find({}).stream();

      var received = [];
      s.on('data', function(item) {
        received.push(item);
        if (item._id._v === 'A') {
          s.pause();
          process.nextTick(s.resume.bind(s));
        }
      });

      s.on('close', function() {
        should.deepEqual(received, [A, Ap, B, Bp, C, D]);
        done();
      });
    });

    it('should support stream pause and destroy on first collection', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D]);
      var vc = new ConcatMongoCollection([coll1, coll2]);
      var s = vc.find({}).stream();

      var received = [];
      s.on('data', function(item) {
        received.push(item);
        if (item._id._v === 'A') {
          s.pause();
          process.nextTick(s.destroy.bind(s));
        }
      });

      s.on('close', function() {
        should.deepEqual(received, [A]);
        done();
      });
    });

    it('should support stream pause and resume on second collection', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D]);
      var vc = new ConcatMongoCollection([coll1, coll2]);
      var s = vc.find({}).stream();

      var received = [];
      s.on('data', function(item) {
        received.push(item);
        if (item._id._v === 'C') {
          s.pause();
          process.nextTick(s.resume.bind(s));
        }
      });

      s.on('close', function() {
        should.deepEqual(received, [A, Ap, B, Bp, C, D]);
        done();
      });
    });

    it('should support stream pause and destroy on second collection', function(done) {
      var coll1 = db.collection(collName1);
      var coll2 = new ArrayCollection([C, D]);
      var vc = new ConcatMongoCollection([coll1, coll2]);
      var s = vc.find({}).stream();

      var received = [];
      s.on('data', function(item) {
        received.push(item);
        if (item._id._v === 'C') {
          s.pause();
          process.nextTick(s.destroy.bind(s));
        }
      });

      s.on('close', function() {
        should.deepEqual(received, [A, Ap, B, Bp, C]);
        done();
      });
    });
  });
});
