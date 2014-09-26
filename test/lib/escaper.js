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

/*jshint -W068, -W030 */

var should = require('should');

var escaper = require('../../lib/escaper');

describe('escaper', function () {
  describe('_transform', function () {
    it('should require obj to be an object', function() {
      (function() { escaper._transform(); }).should.throw('obj must be an object');
    });

    it('should require transform to be a function', function() {
      (function() { escaper._transform({}); }).should.throw('transform must be a function');
    });

    it('should require recurse to be a boolean', function() {
      (function() { escaper._transform({}, function(a) { return a; }, []); }).should.throw('recurse must be a boolean');
    });

    it('should work with empty object', function() {
      var obj = {};
      escaper._transform(obj, function(a) { return a; });
      should.deepEqual(obj, {});
    });

    it('should use transformation', function() {
      var obj = { $: '$' };
      escaper._transform(obj, function() { return 'b'; });
      should.deepEqual(obj, { b: '$' });
    });

    it('should not recurse', function() {
      var obj = { $: '$', foo: { $: '$' } };
      escaper._transform(obj, function(a) { return a + 'b'; });
      should.deepEqual(obj, { $b: '$', foob: { $: '$' } });
    });

    it('should recurse', function() {
      var obj = { $: '$', foo: { $: '$', bar: { some: 'other' } }, a: 'b' };
      escaper._transform(obj, function(a) { return a + 'b'; }, true);
      should.deepEqual(obj, { $b: '$', foob: { $b: '$', barb: { someb: 'other' } } , ab: 'b'});
    });
  });

  describe('escape', function () {
    it('should work with empty object', function() {
      var obj = {};
      escaper.escape(obj);
      should.deepEqual(obj, {});
    });

    it('should return original object', function() {
      var obj = {};
      var ret = escaper.escape(obj);
      should.deepEqual(ret, {});
    });

    it('should not recurse', function() {
      var obj = { $: '$', 'foo.bar': { $: '$' } };
      escaper.escape(obj);
      should.deepEqual(obj, { '\uFF04': '$', 'foo\uFF0Ebar': { $: '$' } });
    });

    it('should be idempotent', function() {
      var obj = { $: '$', 'foo.bar': { $: '$' } };
      escaper.escape(obj);
      escaper.escape(obj);
      should.deepEqual(obj, { '\uFF04': '$', 'foo\uFF0Ebar': { $: '$' } });
    });

    it('should recurse', function() {
      var obj = { $: '$', foo: { $: '$', bar: { 'some.foo': 'other' } }, a: 'b' };
      escaper.escape(obj, true);
      should.deepEqual(obj, { '\uFF04': '$', foo: { '\uFF04': '$', bar: { 'some\uFF0Efoo': 'other' } } , a: 'b'});
    });
  });

  describe('unescape', function () {
    it('should work with empty object', function() {
      var obj = {};
      escaper.unescape(obj);
      should.deepEqual(obj, {});
    });

    it('should return original object', function() {
      var obj = {};
      var ret = escaper.unescape(obj);
      should.deepEqual(ret, {});
    });

    it('should not recurse', function() {
      var obj = { '\uFF04': '$', 'foo\uFF0Ebar': { $: '$' } };
      escaper.unescape(obj);
      should.deepEqual(obj, { $: '$', 'foo.bar': { $: '$' } });
    });

    it('should be idempotent', function() {
      var obj = { '\uFF04': '$', 'foo\uFF0Ebar': { $: '$' } };
      escaper.unescape(obj);
      escaper.unescape(obj);
      should.deepEqual(obj, { $: '$', 'foo.bar': { $: '$' } });
    });

    it('should recurse', function() {
      var obj = { '\uFF04': '$', foo: { '\uFF04': '$', bar: { 'some\uFF0Efoo': 'other' } } , a: 'b'};
      escaper.unescape(obj, true);
      should.deepEqual(obj, { $: '$', foo: { $: '$', bar: { 'some.foo': 'other' } }, a: 'b' });
    });
  });
});
