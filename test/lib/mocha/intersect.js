/**
 * Copyright 2015 Netsend.
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

var should = require('should');

var intersect = require('../../../lib/intersect');

describe('_intersect', function() {
  it('should require arr1', function(done) {
    intersect(null, null, function(err) {
      should.equal(err.message, 'provide arr1');
      done();
    });
  });

  it('should require arr2', function(done) {
    intersect({}, null, function(err) {
      should.equal(err.message, 'provide arr2');
      done();
    });
  });

  it('should require arr1 to be an array', function(done) {
    intersect({}, {}, function(err) {
      should.equal(err.message, 'arr1 must be an array');
      done();
    });
  });

  it('should require arr2 to be an array', function(done) {
    intersect([], {}, function(err) {
      should.equal(err.message, 'arr2 must be an array');
      done();
    });
  });

  it('should return empty set which are both a subset of each other', function(done) {
    var arr1 = [];
    var arr2 = [];
    intersect(arr1, arr2, function(err, intersection, subset) {
      should.equal(err, null);
      should.deepEqual(intersection, []);
      should.strictEqual(subset, 0);
      done();
    });
  });

  it('should return empty set', function(done) {
    var arr1 = [ 'bar' ];
    var arr2 = [ 'baz' ];
    intersect(arr1, arr2, function(err, intersection, subset) {
      should.equal(err, null);
      should.deepEqual(intersection, []);
      should.strictEqual(subset, false);
      done();
    });
  });

  it('should return element foo', function(done) {
    var arr1 = [ 'bar', 'foo' ];
    var arr2 = [ 'baz', 'foo' ];
    intersect(arr1, arr2, function(err, intersection, subset) {
      should.equal(err, null);
      should.deepEqual(intersection, [ 'foo' ]);
      should.strictEqual(subset, false);
      done();
    });
  });

  it('should find one key and one subset in arr1', function(done) {
    var arr1 = [ 'foo' ];
    var arr2 = [ 'baz', 'foo' ];
    intersect(arr1, arr2, function(err, intersection, subset) {
      should.equal(err, null);
      should.deepEqual(intersection, [ 'foo' ]);
      should.strictEqual(subset, -1);
      done();
    });
  });

  it('should find one key and one subset in arr2', function(done) {
    var arr1 = [ 'bar', 'foo' ];
    var arr2 = [ 'foo' ];
    intersect(arr1, arr2, function(err, intersection, subset) {
      should.equal(err, null);
      should.deepEqual(intersection, [ 'foo' ]);
      should.strictEqual(subset, 1);
      done();
    });
  });

  it('should find one key and both subsets of each other', function(done) {
    var arr1 = [ 'foo' ];
    var arr2 = [ 'foo' ];
    intersect(arr1, arr2, function(err, intersection, subset) {
      should.equal(err, null);
      should.deepEqual(intersection, [ 'foo' ]);
      should.strictEqual(subset, 0);
      done();
    });
  });

  it('should find two keys', function(done) {
    var arr1 = [ 'foo', 'bar', 'quux' ];
    var arr2 = [ 'foo', 'bar', 'qux' ];
    intersect(arr1, arr2, function(err, intersection, subset) {
      should.equal(err, null);
      should.deepEqual(intersection, [ 'foo', 'bar' ]);
      should.strictEqual(subset, false);
      done();
    });
  });
});
