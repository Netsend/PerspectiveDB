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

var mongodb = require('mongodb');
var nestNamespace = require('../../lib/nest_namespace');
var match = require('../../lib/match');

describe('match', function () {
  it('should require criteria to be an object', function() {
    (function() { match(); }).should.throw('criteria must be an object');
  });

  it('should return true if created with empty filter object and called with empty item', function() {
    var result = match(nestNamespace({}), {});
    should.strictEqual(result, true);
  });

  it('should work with empty search', function() {
    var result = match(nestNamespace({}), { foo: 'bar' });
    should.strictEqual(result, true);
  });

  it('should work with 1d namespace', function() {
    var result = match(nestNamespace({ foo: 'bar' }), { foo: 'bar' });
    should.strictEqual(result, true);
  });

  it('should work with 1d namespace not exists', function() {
    var result = match(nestNamespace({ foo: 'baz' }), { foo: 'bar' });
    should.strictEqual(result, false);
  });

  it('should work with 2d namespace', function() {
    var result = match(nestNamespace({ 'foo.baz': 'bar' }), { foo: { baz: 'bar' }});
    should.strictEqual(result, true);
  });

  it('should work with 2d namespace not exists', function() {
    var result = match(nestNamespace({ 'foo.baz': 'baz' }), { foo: { baz: 'bar' }});
    should.strictEqual(result, false);
  });

  it('should work with 3d namespace', function() {
    var result = match(nestNamespace({ 'foo.baz.qux': 'bar' }), { foo: { baz: { qux:'bar' }}});
    should.strictEqual(result, true);
  });

  it('should work with 3d namespace not exists', function() {
    var result = match(nestNamespace({ 'foo.baz.quux': 'bar' }), { foo: { baz: { qux:'bar' }}});
    should.strictEqual(result, false);
  });

  it('should work with 1d, 2d and 3d namespace', function() {
    var result = match(nestNamespace({
      'foo.foo': 'bar',
      'baz.baz.quux': 'baz',
      'bar.qux': 'bar'
    }), {
      foo: { foo: 'bar' },
      baz: { baz: { quux: 'baz' }},
      bar: { qux: 'bar' }
    });
    should.strictEqual(result, true);
  });

  it('should work with 1d, 2d and 3d namespace not exists', function() {
    var result = match(nestNamespace({
      'foo': 'bar',
      'baz.baz.quux': 'baz',
      'bar.qux': 'bar'
    }), {
      foo: { foo: 'bar' },
      baz: { baz: { quux: 'baz' }},
      bar: { qux: 'bar' }
    });
    should.strictEqual(result, false);
  });

  it('should work with complex non-string values', function() {
    var result = match(nestNamespace({
      'foo': { foo: 'bar' },
      'baz.baz.quux': 'baz',
      'bar.qux': 'bar'
    }), {
      foo: { foo: 'bar' },
      baz: { baz: { quux: 'baz' }},
      bar: { qux: 'bar' }
    });
    should.strictEqual(result, true);
  });

  it('should work with mongodb.ObjectIDs and match', function() {
    var filter = { _id: new mongodb.ObjectID('51114ce9716c200f6ca09041') };
    var obj    = { _id: new mongodb.ObjectID('51114ce9716c200f6ca09041'), foo: 'bar' };

    var result = match(filter, obj);
    should.strictEqual(result, true);
  });

  it('should work with mongodb.ObjectIDs and not match', function() {
    var filter = { _id: new mongodb.ObjectID('51114ce9716c200f6ca09041') };
    var obj    = { _id: new mongodb.ObjectID('51114ce9716c200f6ca09042'), foo: 'bar' };

    var result = match(filter, obj);
    should.strictEqual(result, false);
  });

  it('should work with mongodb.ObjectIDs and string and match', function() {
    var filter = { _id: new mongodb.ObjectID('51114ce9716c200f6ca09041'), foo: 'bar' };
    var obj    = { _id: new mongodb.ObjectID('51114ce9716c200f6ca09041'), foo: 'bar' };

    var result = match(filter, obj);
    should.strictEqual(result, true);
  });

  it('should work with mongodb.ObjectIDs and string and not match on string', function() {
    var filter = { _id: new mongodb.ObjectID('51114ce9716c200f6ca09041'), foo: 'baz' };
    var obj    = { _id: new mongodb.ObjectID('51114ce9716c200f6ca09041'), foo: 'bar' };

    var result = match(filter, obj);
    should.strictEqual(result, false);
  });

  it('should recurse and work with mongodb.ObjectIDs and others and match', function() {
    var id = new mongodb.ObjectID('51114ce9716c200f6ca09041');
    var filter = { _id: { _id: id, _v: { $in: ['B', 'A'] }, _pe: { $in: ['_local']}}};
    var obj =    { _id: { _id: id, _v: 'A', _pe: '_local' }, _m3: { _ack: false }};

    var result = match(filter, obj);
    should.strictEqual(result, true);
  });

  it('should recurse and work with mongodb.ObjectIDs and others and not match on _v', function() {
    var id = new mongodb.ObjectID('51114ce9716c200f6ca09041');
    var filter = { _id: { _id: id, _v: { $in: ['B'] }, _pe: { $in: ['_local']}}};
    var obj =    { _id: { _id: id, _v: 'A', _pe: '_local' }, _m3: { _ack: false }};

    var result = match(filter, obj);
    should.strictEqual(result, false);
  });

  it('should recurse and work with mongodb.ObjectIDs and not match', function() {
    var id = new mongodb.ObjectID('51114ce9716c200f6ca09041');
    var filter = { _id: { _id: id, _v: { $in: ['A'] }, _pe: { $in: ['_local']}}};
    var obj = { _id: { _id: id, _v: 'B', _pe: '_local' }, _m3: { _ack: false }};

    var result = match(filter, obj);
    should.strictEqual(result, false);
  });

  describe('$in', function() {
    it('should require $in values to be of type array', function() {
      var filter = { foo: 'bar', bar: { $in: {} } };
      var obj = { foo: 'bar', bar: {} };
      (function() { match(filter, obj); }).should.throw('$in keys should point to an array');
    });

    it('should work with $in and any match', function() {
      var filter = { foo: 'bar', bar: { $in: [ 'baz' ] } };
      var obj = { foo: 'bar', bar: 'baz' };
      var result = match(filter, obj);
      should.strictEqual(result, true);
    });

    it('should work with $in and no match', function() {
      var filter = { foo: 'bar', bar: { $in: [ 'bar' ] } };
      var obj = { foo: 'bar', bar: 'baz' };
      var result = match(filter, obj);
      should.strictEqual(result, false);
    });

    it('should work only when everything holds', function() {
      var filter = { foo: 'bar', bar: { $in: [ 'baz' ] }, baz: 'qux' };
      var obj = { foo: 'bar', bar: 'baz' };
      var result = match(filter, obj);
      should.strictEqual(result, false);
    });
  });

  describe('regression', function() {
    it('[TypeError: Cannot read property \'_bsontype\' of undefined]', function() {
      var id = new mongodb.ObjectID('51114ce9716c200f6ca09041');
      var filter = { _id: { _id: id, _v: { $in: ['A'] }, _pe: { $in: ['_local']}}};

      var obj = { _id: id, _m3: { _ack: false }};

      var result = match(filter, obj);
      should.strictEqual(result, false);
    });
  });
});
