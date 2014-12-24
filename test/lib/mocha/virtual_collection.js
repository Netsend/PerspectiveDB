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

var VirtualCollection = require('../../../lib/virtual_collection');
var VirtualCursor = require('../../../lib/virtual_cursor');

var db;
var databaseName = 'test_virtual_collection';
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

describe('virtual_collection', function() {
  describe('constructor', function() {
    var collectionName = 'contructor';

    it('should require collection to be a mongodb.Collection', function() {
      (function () { new VirtualCollection(); }).should.throwError('collection must be an instance of mongodb.Collection');
    });

    it('should require virtualItems to be an array', function() {
      var coll = db.collection(collectionName);
      (function() { new VirtualCollection(coll); }).should.throw('virtualItems must be an array');
    });

    it('should construct', function() {
      var coll = db.collection(collectionName);
      (function() { new VirtualCollection(coll, []); }).should.not.throwError();
    });
  });

  describe('findOne', function() {
    var collectionName = 'findOne';

    var A = { _id: { _id: 'foo', _v: 'A', _pe: 'bar' }, _m3: { _ack: true }, foo: 'bar' };
    var Ap = { _id: { _id: 'foo', _v: 'A', _pe: 'foo' }, _m3: { _ack: true }, foo: 'bar' };
    var B = { _id: { _id: 'foo', _v: 'B', _pe: 'bar', _pa: ['A'] }, _m3: { _ack: true }, foo: 'baz' };
    var Bp = { _id: { _id: 'foo', _v: 'B', _pe: 'foo', _pa: ['A'] }, _m3: { _ack: true }, foo: 'baz' };
    var C = { _id: { _id: 'foo', _v: 'C', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: true }, foo: 'qux' };
    var D = { _id: { _id: 'foo', _v: 'D', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: false }, foo: 'quux' };

    it('should save DAG', function(done) {
      var coll = db.collection(collectionName);
      coll.insert([A, Ap, B, Bp], {w: 1}, done);
    });

    it('should require cb to be a function', function() {
      var coll = db.collection(collectionName);
      var vc = new VirtualCollection(coll, [C, D]);
      (function() { vc.findOne(); }).should.throw('cb must be a function');
    });

    it('should find a virtual item', function(done) {
      var coll = db.collection(collectionName);
      var vc = new VirtualCollection(coll, [C, D]);
      vc.findOne({ '_id._v': 'C' }, function(err, item) {
        if (err) { throw err; }
        should.deepEqual(item, C);
        done();
      });
    });

    it('should find a collection item', function(done) {
      var coll = db.collection(collectionName);
      var vc = new VirtualCollection(coll, [C, D]);
      vc.findOne({ '_id._v': 'A' }, function(err, item) {
        if (err) { throw err; }
        should.deepEqual(item, A);
        done();
      });
    });

    it('should not find an item', function(done) {
      var coll = db.collection(collectionName);
      var vc = new VirtualCollection(coll, [C, D]);
      vc.findOne({ '_id._v': 'X' }, function(err, item) {
        if (err) { throw err; }
        should.deepEqual(item, null);
        done();
      });
    });
  });

  describe('find', function() {
    var collectionName = 'find';

    var A = { _id: { _id: 'foo', _v: 'A', _pe: 'bar' }, _m3: { _ack: true }, foo: 'bar' };
    var Ap = { _id: { _id: 'foo', _v: 'A', _pe: 'foo' }, _m3: { _ack: true }, foo: 'bar' };
    var B = { _id: { _id: 'foo', _v: 'B', _pe: 'bar', _pa: ['A'] }, _m3: { _ack: true }, foo: 'baz' };
    var Bp = { _id: { _id: 'foo', _v: 'B', _pe: 'foo', _pa: ['A'] }, _m3: { _ack: true }, foo: 'baz' };
    var C = { _id: { _id: 'foo', _v: 'C', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: true }, foo: 'qux' };
    var D = { _id: { _id: 'foo', _v: 'D', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: false }, foo: 'quux' };

    it('should save DAG', function(done) {
      var coll = db.collection(collectionName);
      coll.insert([A, Ap, B, Bp], {w: 1}, done);
    });

    it('should return a VirtualCursor', function() {
      var coll = db.collection(collectionName);
      var vc = new VirtualCollection(coll, []);
      var result = vc.find();
      should.equal(result instanceof VirtualCursor, true);
    });

    it('should support find with stream', function(done) {
      var coll = db.collection(collectionName);
      var vc = new VirtualCollection(coll, [C, D]);
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
      var coll = db.collection(collectionName);
      var vc = new VirtualCollection(coll, [C, D]);
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

    it('should support stream pause and resume on collection', function(done) {
      var coll = db.collection(collectionName);
      var vc = new VirtualCollection(coll, [C, D]);
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

    it('should support stream pause and destroy on collection', function(done) {
      var coll = db.collection(collectionName);
      var vc = new VirtualCollection(coll, [C, D]);
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
  });
});
