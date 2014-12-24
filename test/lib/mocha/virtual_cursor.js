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

var VirtualCursor = require('../../../lib/virtual_cursor');
var VirtualStream = require('../../../lib/virtual_stream');

var db;
var databaseName = 'test_virtual_cursor';
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

describe('virtual_cursor', function() {
  describe('constructor', function() {
    var collectionName = 'contructor';

    it('should require collection to be a mongodb.Collection', function() {
      (function () { new VirtualCursor(); }).should.throwError('collection must be an instance of mongodb.Collection');
    });

    it('should require virtualItems to be an array', function() {
      var coll = db.collection(collectionName);
      (function() { new VirtualCursor(coll); }).should.throw('virtualItems must be an array');
    });

    it('should construct', function() {
      var coll = db.collection(collectionName);
      (function() { new VirtualCursor(coll, []); }).should.not.throwError();
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

    describe('sort asc', function() {
      it('should find one from virtual items', function(done) {
        var coll = db.collection(collectionName);
        var items = [C, D];
        var vc = new VirtualCursor(coll, items);

        vc.findOne({ '_id._v': 'C' }, function(err, item) {
          if (err) { throw err; }
          should.deepEqual(item, C);
          done();
        });
      });

      it('should not find any non-existing item', function(done) {
        var coll = db.collection(collectionName);
        var items = [C, D];
        var vc = new VirtualCursor(coll, items);

        vc.findOne({ '_id._v': 'X' }, function(err, item) {
          if (err) { throw err; }
          should.deepEqual(item, null);
          done();
        });
      });

      it('should find one from db collection', function(done) {
        var coll = db.collection(collectionName);
        var items = [C, D];
        var vc = new VirtualCursor(coll, items, { debug: false });

        vc.findOne({ '_id._v': 'B' }, function(err, item) {
          if (err) { throw err; }
          should.deepEqual(item, B);
          done();
        });
      });
    });

    describe('sort desc', function() {
      it('should find one from virtual items', function(done) {
        var coll = db.collection(collectionName);
        var items = [C, D];
        var vc = new VirtualCursor(coll, items);

        vc.findOne({ '_id._v': 'C' }, { sort: { $natural: -1 }}, function(err, item) {
          if (err) { throw err; }
          should.deepEqual(item, C);
          done();
        });
      });

      it('should not find any non-existing item', function(done) {
        var coll = db.collection(collectionName);
        var items = [C, D];
        var vc = new VirtualCursor(coll, items);

        vc.findOne({ '_id._v': 'X' }, { sort: { $natural: -1 }}, function(err, item) {
          if (err) { throw err; }
          should.deepEqual(item, null);
          done();
        });
      });

      it('should find one from db collection', function(done) {
        var coll = db.collection(collectionName);
        var items = [C, D];
        var vc = new VirtualCursor(coll, items, { debug: false });

        vc.findOne({ '_id._v': 'B', '_id._pe': 'foo' }, { sort: { $natural: -1 }}, function(err, item) {
          if (err) { throw err; }
          should.deepEqual(item, Bp);
          done();
        });
      });
    });
  });

  describe('find', function() {
    var collectionName = 'find';

    it('should return a VirtualCursor', function() {
      var coll = db.collection(collectionName);
      var vc = new VirtualCursor(coll, []);
      var result = vc.find();
      should.equal(result instanceof VirtualCursor, true);
    });
  });

  describe('toArray', function() {
    var collectionName = 'toArray';

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

    describe('sort asc', function() {
      it('should find one from virtual items', function(done) {
        var coll = db.collection(collectionName);
        var items = [C, D];
        var vc = new VirtualCursor(coll, items);

        vc.find({ '_id._v': 'C' }).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, [C]);
          done();
        });
      });

      it('should not find any non-existing item', function(done) {
        var coll = db.collection(collectionName);
        var items = [C, D];
        var vc = new VirtualCursor(coll, items);

        vc.find({ '_id._v': 'X' }).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, []);
          done();
        });
      });

      it('should find multiple from db collection', function(done) {
        var coll = db.collection(collectionName);
        var items = [C, D];
        var vc = new VirtualCursor(coll, items);

        vc.find({ '_id._v': 'B' }).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, [B, Bp]);
          done();
        });
      });

      it('should find multiple with perspective bar', function(done) {
        var coll = db.collection(collectionName);
        var items = [C, D];
        var vc = new VirtualCursor(coll, items);

        vc.find({ '_id._pe': 'bar' }).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, [A, B, C, D]);
          done();
        });
      });
    });

    describe('sort desc', function() {
      it('should find the two virtual items', function(done) {
        var coll = db.collection(collectionName);
        var items = [C, D];
        var vc = new VirtualCursor(coll, items);

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
        var coll = db.collection(collectionName);
        var items = [C, D];
        var vc = new VirtualCursor(coll, items);

        vc.find({ '_id._v': 'X' }, { sort: { $natural: -1 }}).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, []);
          done();
        });
      });

      it('should find one from db collection', function(done) {
        var coll = db.collection(collectionName);
        var items = [C, D];
        var vc = new VirtualCursor(coll, items);

        vc.find({ '_id._v': 'B', '_id._pe': 'foo' }, { sort: { $natural: -1 }}).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, [Bp]);
          done();
        });
      });

      it('should find multiple from db collection', function(done) {
        var coll = db.collection(collectionName);
        var items = [C, D];
        var vc = new VirtualCursor(coll, items);

        vc.find({ '_id._v': 'B' }, { sort: { $natural: -1 }}).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, [Bp, B]);
          done();
        });
      });

      it('should find multiple with perspective bar with virtual prepended', function(done) {
        var coll = db.collection(collectionName);
        var items = [C, D];
        var vc = new VirtualCursor(coll, items, { prepend: true });

        vc.find({ '_id._pe': 'bar' }, { sort: { $natural: -1 }}).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, [B, A, D, C]);
          done();
        });
      });
    });
  });

  describe('stream', function() {
    var collectionName = 'stream';

    it('should return a VirtualStream', function() {
      var coll = db.collection(collectionName);
      var vc = new VirtualCursor(coll, []);
      var stream = vc.stream();
      should.equal(stream instanceof VirtualStream, true);
    });
  });
});
