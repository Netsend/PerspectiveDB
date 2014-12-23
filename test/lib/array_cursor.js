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

var ArrayCursor = require('../../lib/array_cursor');
var ArrayStream = require('../../lib/array_stream');

describe('ArrayCursor', function() {
  describe('constructor', function() {
    it('should require items to be an array', function() {
      (function() { new ArrayCursor(); }).should.throw('items must be an array');
    });

    it('should construct', function() {
      (function() { new ArrayCursor([]); }).should.not.throwError();
    });
  });

  describe('findOne', function() {
    var C = { _id: { _id: 'foo', _v: 'C', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: true }, foo: 'qux' };
    var D = { _id: { _id: 'foo', _v: 'D', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: false }, foo: 'quux' };

    describe('sort asc', function() {
      it('should find one from items', function(done) {
        var items = [C, D];
        var ac = new ArrayCursor(items);

        ac.findOne({ '_id._v': 'C' }, function(err, item) {
          if (err) { throw err; }
          should.deepEqual(item, C);
          done();
        });
      });

      it('should not find any non-existing item', function(done) {
        var items = [C, D];
        var ac = new ArrayCursor(items);

        ac.findOne({ '_id._v': 'X' }, function(err, item) {
          if (err) { throw err; }
          should.deepEqual(item, null);
          done();
        });
      });
    });

    describe('sort desc', function() {
      it('should find one from items', function(done) {
        var items = [C, D];
        var ac = new ArrayCursor(items, { debug: false });

        ac.findOne({ '_id._v': 'C' }, { sort: { $natural: -1 }}, function(err, item) {
          if (err) { throw err; }
          should.deepEqual(item, C);
          done();
        });
      });

      it('should not find any non-existing item', function(done) {
        var items = [C, D];
        var ac = new ArrayCursor(items);

        ac.findOne({ '_id._v': 'X' }, { sort: { $natural: -1 }}, function(err, item) {
          if (err) { throw err; }
          should.deepEqual(item, null);
          done();
        });
      });
    });
  });

  describe('find', function() {
    it('should return a ArrayCursor', function() {
      var ac = new ArrayCursor([]);
      var result = ac.find();
      should.equal(result instanceof ArrayCursor, true);
    });
  });

  describe('toArray', function() {
    var C = { _id: { _id: 'foo', _v: 'C', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: true }, foo: 'qux' };
    var D = { _id: { _id: 'foo', _v: 'D', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: false }, foo: 'quux' };

    describe('sort asc', function() {
      it('should find one from items', function(done) {
        var items = [C, D];
        var ac = new ArrayCursor(items);

        ac.find({ '_id._v': 'C' }).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, [C]);
          done();
        });
      });

      it('should not find any non-existing item', function(done) {
        var items = [C, D];
        var ac = new ArrayCursor(items);

        ac.find({ '_id._v': 'X' }).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, []);
          done();
        });
      });

      it('should find multiple with nested filter', function(done) {
        var items = [C, D];
        var ac = new ArrayCursor(items);

        ac.find({ '_id._pe': 'bar' }).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, [C, D]);
          done();
        });
      });
    });

    describe('sort desc', function() {
      it('should find item', function(done) {
        var items = [C, D];
        var ac = new ArrayCursor(items);

        ac.find({ '_id._v': { $in: ['B', 'C'] } }, { sort: { $natural: -1 }}).toArray(function(err, items) {
          if (err) { throw err; }
          should.equal(items.length, 1);
          should.deepEqual(items[0], C);
          done();
        });
      });

      it('should not find any non-existing item', function(done) {
        var items = [C, D];
        var ac = new ArrayCursor(items);

        ac.find({ '_id._v': 'X' }, { sort: { $natural: -1 }}).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, []);
          done();
        });
      });

      it('should find multiple with _id._pe bar', function(done) {
        var items = [C, D];
        var ac = new ArrayCursor(items, { prepend: true });

        ac.find({ '_id._pe': 'bar' }, { sort: { $natural: -1 }}).toArray(function(err, items) {
          if (err) { throw err; }
          should.deepEqual(items, [D, C]);
          done();
        });
      });
    });
  });

  describe('stream', function() {
    it('should return an ArrayStream', function() {
      var ac = new ArrayCursor([]);
      var stream = ac.stream();
      should.equal(stream instanceof ArrayStream, true);
    });
  });
});
