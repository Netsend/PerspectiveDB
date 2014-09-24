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

var nestNamespace = require('../../lib/nest_namespace');

describe('nestNamespace', function () {
  it('should return empty object', function() {
    var result = nestNamespace({});
    should.deepEqual(result, {});
  });

  it('should work with 1d namespace', function() {
    var result = nestNamespace({ foo: 'bar' });
    should.deepEqual(result, { foo: 'bar' });
  });

  it('should work with 2d namespace', function() {
    var result = nestNamespace({ 'foo.baz': 'bar' });
    should.deepEqual(result, { foo: { baz: 'bar' } });
  });

  it('should work with 3d namespace', function() {
    var result = nestNamespace({ 'foo.baz.qux': 'bar' });
    should.deepEqual(result, { foo: { baz: { qux: 'bar' } } });
  });

  it('should work with 1d, 2d and 3d namespace', function() {
    var result = nestNamespace({
      'foo': 'bar',
      'baz.baz.quux': 'baz',
      'bar.qux': 'bar'
    });
    should.deepEqual(result, {
      foo: 'bar',
      baz: { baz: { quux: 'baz' }},
      bar: { qux: 'bar' }
    });
  });

  it('should use last key on duplicate keys', function() {
    var result = nestNamespace({
      'foo.bar.qux': 'bar',
      'foo.bar': 'baz'
    });
    should.deepEqual(result, {
      foo: { bar: 'baz' }
    });
  });

  it('should work with shared keys', function() {
    var result = nestNamespace({
      'foo.bar': 'bar',
      'foo.baz': 'baz'
    });
    should.deepEqual(result, {
      foo: {
        bar: 'bar',
        baz: 'baz'
      }
    });
  });
});
