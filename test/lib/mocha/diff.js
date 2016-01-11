/**
 * Copyright 2016 Netsend.
 *
 * This file is part of PersDB.
 *
 * PersDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PersDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PersDB. If not, see <https://www.gnu.org/licenses/>.
 */

'use strict';

var should = require('should');

var diff = require('../../../lib/diff');

describe('diff', function() {
  it('should not show any differences', function() {
    var x = { foo: 'bar' };
    var y = { foo: 'bar' };
    var result = diff(x, y);
    should.deepEqual(result, { });
  });

  it('should show changed on one attribute', function() {
    var x = { foo: 'bar' };
    var y = { foo: 'baz' };
    var result = diff(x, y);
    should.deepEqual(result, { foo: '~' });
  });

  it('should not conflict on dates', function() {
    var time = new Date();
    var time2 = new Date(time);
    var x = { foo: time };
    var y = { foo: time2 };
    var result = diff(x, y);
    should.deepEqual(result, { });
  });

  it('should process equal object values', function() {
    var x = { foo: { a: 1 } };
    var y = { foo: { a: 1 } };
    var result = diff(x, y);
    should.deepEqual(result, { });
  });

  it('should process different object values', function() {
    var x = { foo: { a: 1 } };
    var y = { foo: { a: 2 } };
    var result = diff(x, y);
    should.deepEqual(result, { foo: '~' });
  });

  it('should process empty items', function() {
    var x = { };
    var y = { };
    var result = diff(x, y);
    should.deepEqual(result, { });
  });

  it('should show deleted keys in item', function() {
    var x = { };
    var y = { foo: 'bar' };
    var result = diff(x, y);
    should.deepEqual(result, { foo: '-' });
  });

  it('should show added keys in item', function() {
    var x = { foo: 'bar' };
    var y = { };
    var result = diff(x, y);
    should.deepEqual(result, { foo: '+' });
  });

  it('should do a more complex diff', function() {
    var x = { foo: 'bar', bar: 'baz', baz: 'qux',                 fubar: { a: 'b', c: 'd' }, foobar: { a: 'b', c: 'e' } };
    var y = {             bar: 'baz', baz: 'quux', qux: 'raboof', fubar: { a: 'b', c: 'e' }, foobar: { a: 'b', c: 'e' } };
    var result = diff(x, y);
    should.deepEqual(result, {
      foo: '+',
      baz: '~',
      qux: '-',
      fubar: '~'
    });
  });

  it('should not alter the original objects', function() {
    var x = { foo: 'bar', bar: 'baz', baz: 'qux'                 };
    var y = {             bar: 'baz', baz: 'quux', qux: 'raboof' };
    var result = diff(x, y);
    should.deepEqual(result, {
      foo: '+',
      baz: '~',
      qux: '-',
    });
    should.deepEqual(x, { foo: 'bar', bar: 'baz', baz: 'qux' });
    should.deepEqual(y, { bar: 'baz', baz: 'quux', qux: 'raboof' });
  });
});
