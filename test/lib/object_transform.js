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

/*jshint -W068 */

var should = require('should');

var transform = require('../../lib/object_transform');

describe('transform', function () {
  it('should require obj to be an object', function() {
    (function() { transform(); }).should.throw('obj must be an object');
  });

  it('should require transformer to be a function', function() {
    (function() { transform({}); }).should.throw('transformer must be a function');
  });

  it('should require recurse to be a boolean', function() {
    (function() { transform({}, function(a) { return a; }, []); }).should.throw('recurse must be a boolean');
  });

  it('should work with empty object', function() {
    var obj = {};
    transform(obj, function(a) { return a; });
    should.deepEqual(obj, {});
  });

  it('should use transformation', function() {
    var obj = { $: '$' };
    transform(obj, function() { return 'b'; });
    should.deepEqual(obj, { b: '$' });
  });

  it('should not recurse', function() {
    var obj = { $: '$', foo: { $: '$' } };
    transform(obj, function(a) { return a + 'b'; });
    should.deepEqual(obj, { $b: '$', foob: { $: '$' } });
  });

  it('should pass values as well', function() {
    var obj = { foo: 'bar' };
    transform(obj, function(key, val) { should.strictEqual(val, 'bar'); });
  });

  it('should recurse', function() {
    var obj = { $: '$', foo: { $: '$', bar: { some: 'other' } }, a: 'b' };
    transform(obj, function(a) { return a + 'b'; }, true);
    should.deepEqual(obj, { $b: '$', foob: { $b: '$', barb: { someb: 'other' } } , ab: 'b'});
  });
});
