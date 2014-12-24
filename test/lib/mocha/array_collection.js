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

var ArrayCollection = require('../../../lib/array_collection');
var ArrayCursor = require('../../../lib/array_cursor');

describe('ArrayCollection', function() {
  describe('constructor', function() {
    it('should require items to be an array', function() {
      (function() { new ArrayCollection(); }).should.throw('items must be an array');
    });

    it('should construct', function() {
      (function() { new ArrayCollection([]); }).should.not.throwError();
    });
  });

  describe('findOne', function() {
    var C = { _id: { _id: 'foo', _v: 'C', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: true }, foo: 'qux' };
    var D = { _id: { _id: 'foo', _v: 'D', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: false }, foo: 'quux' };

    it('should require cb to be a function', function() {
      var vc = new ArrayCollection([C, D]);
      (function() { vc.findOne({}); }).should.throw('cb must be a function');
    });

    it('should find a item', function(done) {
      var vc = new ArrayCollection([C, D]);
      vc.findOne({ '_id._v': 'C' }, function(err, item) {
        if (err) { throw err; }
        should.deepEqual(item, C);
        done();
      });
    });

    it('should not find an item', function(done) {
      var vc = new ArrayCollection([C, D]);
      vc.findOne({ '_id._v': 'X' }, function(err, item) {
        if (err) { throw err; }
        should.deepEqual(item, null);
        done();
      });
    });
  });

  describe('find', function() {
    var C = { _id: { _id: 'foo', _v: 'C', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: true }, foo: 'qux' };
    var D = { _id: { _id: 'foo', _v: 'D', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: false }, foo: 'quux' };

    it('should return an ArrayCursor', function() {
      var vc = new ArrayCollection([]);
      var result = vc.find();
      should.equal(result instanceof ArrayCursor, true);
    });

    it('should support find with stream', function(done) {
      var vc = new ArrayCollection([C, D]);
      var s = vc.find().stream();

      var received = [];
      s.on('data', function(item) {
        received.push(item);
      });

      s.on('close', function() {
        should.deepEqual(received, [C, D]);
        done();
      });
    });

    it('should support find with stream, twice', function(done) {
      var vc = new ArrayCollection([C, D]);
      var s = vc.find({}, { sort: { '$natural': -1 } }).stream();

      var received = [];
      s.on('data', function(item) {
        received.push(item);
      });

      s.on('close', function() {
        should.deepEqual(received, [C, D].reverse());

        var s2 = vc.find({}, { sort: { '$natural': -1 } }).stream();

        received = [];
        s2.on('data', function(item) {
          received.push(item);
        });

        s2.on('close', function() {
          should.deepEqual(received, [C, D].reverse());
          done();
        });
      });
    });

    it('should support stream pause and resume', function(done) {
      var vc = new ArrayCollection([C, D]);
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
        should.deepEqual(received, [C, D]);
        done();
      });
    });

    it('should support stream pause and destroy', function(done) {
      var vc = new ArrayCollection([C, D]);
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
        should.deepEqual(received, [C]);
        done();
      });
    });
  });
});
