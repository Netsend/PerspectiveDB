/**
 * Copyright 2014, 2015 Netsend.
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

var threeWayMerge = require('../../../lib/three_way_merge');
var logger = require('../../../lib/logger');

var cons, silence;

before(function(done) {
  logger({ console: true, mask: logger.DEBUG2 }, function(err, l) {
    if (err) { throw err; }
    cons = l;
    logger({ silence: true }, function(err, l) {
      if (err) { throw err; }
      silence = l;
      done();
    });
  });
});

after(function(done) {
  cons.close(function(err) {
    if (err) { throw err; }
    silence.close(done);
  });
});

describe('threeWayMerge', function() {
  it('should keep when key in all three', function() {
    var lca = { foo: 'bar' };
    var x = { foo: 'bar' };
    var y = { foo: 'bar' };
    var mergedItem = threeWayMerge(x, y, lca);
    should.deepEqual(mergedItem, { foo: 'bar' });
  });

  it('should auto-merge when key in both x and y with same value', function() {
    var lca = { };
    var x = { foo: 'bar' };
    var y = { foo: 'bar' };
    var mergedItem = threeWayMerge(x, y, lca);
    should.deepEqual(mergedItem, { foo: 'bar' });
  });

  it('should conflict when key in all three but all with different values', function() {
    var lca = { foo: 'qux' };
    var x = { foo: 'bar' };
    var y = { foo: 'baz' };
    var mergedItem = threeWayMerge(x, y, lca);
    should.deepEqual(mergedItem, [ 'foo' ]);
  });

  it('should conflict when key in x and y but with different values', function() {
    var lca = { };
    var x = { foo: 'bar' };
    var y = { foo: 'baz' };
    var mergedItem = threeWayMerge(x, y, lca);
    should.deepEqual(mergedItem, [ 'foo' ]);
  });

  it('should not conflict on dates', function() {
    var time = new Date();
    var time2 = new Date(time);
    var lca = { };
    var x = { foo: time };
    var y = { foo: time2 };
    var mergedItem = threeWayMerge(x, y, lca);
    should.deepEqual(mergedItem, { foo: time });
  });

  it('should not conflict on objects', function() {
    var lca = { };
    var x = { foo: { a: 1 } };
    var y = { foo: { a: 1 } };
    var mergedItem = threeWayMerge(x, y, lca);
    should.deepEqual(mergedItem, { foo: { a: 1 } });
  });

  it('should merge when key in x has different value', function() {
    var lca = { foo: 'bar' };
    var x = { foo: 'baz' };
    var y = { foo: 'bar' };
    var mergedItem = threeWayMerge(x, y, lca);
    should.deepEqual(mergedItem, { foo: 'baz' });
  });

  it('should merge when key in y has different value', function() {
    var lca = { foo: 'bar' };
    var x = { foo: 'bar' };
    var y = { foo: 'baz' };
    var mergedItem = threeWayMerge(x, y, lca);
    should.deepEqual(mergedItem, { foo: 'baz' });
  });

  it('should delete when key only in lca', function() {
    var lca = { foo: 'bar' };
    var x = { };
    var y = { };
    var mergedItem = threeWayMerge(x, y, lca);
    should.deepEqual(mergedItem, { });
  });

  it('should merge when key in x is deleted', function() {
    var lca = { foo: 'bar' };
    var x = { };
    var y = { foo: 'bar' };
    var mergedItem = threeWayMerge(x, y, lca);
    should.deepEqual(mergedItem, { });
  });

  it('should merge when key in y is deleted', function() {
    var lca = { foo: 'bar' };
    var x = { foo: 'bar' };
    var y = { };
    var mergedItem = threeWayMerge(x, y, lca);
    should.deepEqual(mergedItem, { });
  });

  it('should merge when key is only in x', function() {
    var lca = { };
    var x = { foo: 'bar' };
    var y = { };
    var mergedItem = threeWayMerge(x, y, lca);
    should.deepEqual(mergedItem, { foo: 'bar' });
  });

  it('should merge when key is only in y', function() {
    var lca = { };
    var x = { };
    var y = { foo: 'bar' };
    var mergedItem = threeWayMerge(x, y, lca);
    should.deepEqual(mergedItem, { foo: 'bar' });
  });

  it('should do a more complex merge', function() {
    var lca = { foo: 'bar', baz: 'quz' };
    var x = { foo: 'bar', baz: 'quz', qux: 'raboof' };
    var y = { baz: 'qux' };
    var mergedItem = threeWayMerge(x, y, lca);
    should.deepEqual(mergedItem, {
      baz: 'qux',
      qux: 'raboof'
    });
  });

  it('should use a separate perspective lca', function() {
    var lcaX = { foo: 'bar', baz: 'quz' };
    var lcaY = { foo: 'bar' };
    var x = { foo: 'bar', baz: 'qux', qux: 'raboof' };
    var y = { quux: 'quz' };
    var mergedItem = threeWayMerge(x, y, lcaX, lcaY);
    should.deepEqual(mergedItem, {
      baz: 'qux',
      qux: 'raboof',
      quux: 'quz'
    });
  });

  it('should return all conflicting keys on any conflict', function() {
    var lca = { foo: 'baz' };
    var x = { foo: 'bar', baz: 'quz', qux: 'raboof' };
    var y = { baz: 'qux' };
    var mergedItem = threeWayMerge(x, y, lca);
    should.deepEqual(mergedItem, [ 'baz', 'foo' ]);
  });

  it('first lca should be leading, so foo key should exist', function() {
    var itemX = { _id: 'foo', foo: 'bar', bar: 'baz', qux: 'qux' };
    var itemY = { _id: 'foo', bar: 'raboof', qux: 'quux' };
    var lca0 = { _id: 'foo', foo: 'bar', bar: 'baz', qux: 'qux' };
    var perspectiveBoundLCA = { _id: 'foo', bar: 'baz', qux: 'qux' };

    var mergedItem = threeWayMerge(itemX, itemY, lca0, perspectiveBoundLCA);
    should.deepEqual(mergedItem, { _id: 'foo', foo: 'bar', bar: 'raboof', qux: 'quux' });
  });

  it('first lca should be leading, so yonly key should not exist if not changed', function() {
    var itemX = { _id: 'foo', foo: 'bar', bar: 'baz', qux: 'qux' };
    var itemY = { _id: 'foo', bar: 'raboof', qux: 'quux', yonly: true };
    var lca0 = { _id: 'foo', foo: 'bar', bar: 'baz', qux: 'qux' };
    var perspectiveBoundLCA = { _id: 'foo', bar: 'baz', qux: 'qux', yonly: true };

    var mergedItem = threeWayMerge(itemX, itemY, lca0, perspectiveBoundLCA);
    should.deepEqual(mergedItem, { _id: 'foo', foo: 'bar', bar: 'raboof', qux: 'quux' });
  });

  it('first lca should be leading, but yonly key should be created if changed since lcaB', function() {
    var itemX = { _id: 'foo', foo: 'bar', bar: 'baz', qux: 'qux' };
    var itemY = { _id: 'foo', bar: 'raboof', qux: 'quux', yonly: false };
    var lca0 = { _id: 'foo', foo: 'bar', bar: 'baz', qux: 'qux' };
    var perspectiveBoundLCA = { _id: 'foo', bar: 'baz', qux: 'qux', yonly: true };

    var mergedItem = threeWayMerge(itemX, itemY, lca0, perspectiveBoundLCA);
    should.deepEqual(mergedItem, { _id: 'foo', foo: 'bar', bar: 'raboof', qux: 'quux', yonly: false });
  });

  it('first lca should be leading, but yonly key should be created if created since lcaB', function() {
    var itemX = { _id: 'foo', foo: 'bar', bar: 'baz', qux: 'qux' };
    var itemY = { _id: 'foo', bar: 'raboof', qux: 'quux', yonly: false };
    var lca0 = { _id: 'foo', foo: 'bar', bar: 'baz', qux: 'qux' };
    var perspectiveBoundLCA = { _id: 'foo', bar: 'baz', qux: 'qux' };

    var mergedItem = threeWayMerge(itemX, itemY, lca0, perspectiveBoundLCA);
    should.deepEqual(mergedItem, { _id: 'foo', foo: 'bar', bar: 'raboof', qux: 'quux', yonly: false });
  });

  it('should not conflict on added keys in one perspective with the same value', function() {
    var itemX = { _id: 'foo', foo: 'bar', bar: 'baz' };
    var itemY = { _id: 'foo', foo: 'bar', bar: 'baz' };
    var lcaA = { _id: 'foo', foo: 'bar', bar: 'baz' };
    var lcaB = { _id: 'foo', foo: 'bar' };

    var mergedItem = threeWayMerge(itemX, itemY, lcaA, lcaB);
    should.deepEqual(mergedItem, { _id: 'foo', foo: 'bar', bar: 'baz' });
  });

  it('should conflict on a key added in itemY that was already in lcaA but not in lcaB', function() {
    var itemX = { _id: 'foo', foo: 'bar', bar: 'baz' };
    var itemY = { _id: 'foo', foo: 'bar', bar: 'raboof' };
    var lcaA = { _id: 'foo', foo: 'bar', bar: 'baz' };
    var lcaB = { _id: 'foo', foo: 'bar' };

    var mergedItem = threeWayMerge(itemX, itemY, lcaA, lcaB);
    should.deepEqual(mergedItem, ['bar']);
  });

  it('should conflict on a key added in itemX that was already in lcaB but not in lcaA', function() {
    // since the returned value will be based on lcaA
    var itemX = { _id: 'foo', foo: 'bar', bar: 'raboof' };
    var itemY = { _id: 'foo', foo: 'bar', bar: 'baz' };
    var lcaA = { _id: 'foo', foo: 'bar' };
    var lcaB = { _id: 'foo', foo: 'bar', bar: 'baz' };

    var mergedItem = threeWayMerge(itemX, itemY, lcaA, lcaB);
    should.deepEqual(mergedItem, ['bar']);
  });

  it('should not conflict on a key deleted in itemX that was never in itemY anyway', function() {
    var itemX = { _id: 'foo', foo: 'bar' };
    var itemY = { _id: 'foo', foo: 'bar' };
    var lcaA = { _id: 'foo', foo: 'bar', bar: 'baz' };
    var lcaB = { _id: 'foo', foo: 'bar' };

    var mergedItem = threeWayMerge(itemX, itemY, lcaA, lcaB);
    should.deepEqual(mergedItem, { _id: 'foo', foo: 'bar' });
  });

  it('should conflict on a key deleted in itemX that was add in itemY', function() {
    var itemX = { _id: 'foo', foo: 'bar' };
    var itemY = { _id: 'foo', foo: 'bar', bar: 'baz' };
    var lcaA = { _id: 'foo', foo: 'bar', bar: 'baz' };
    var lcaB = { _id: 'foo', foo: 'bar' };

    var mergedItem = threeWayMerge(itemX, itemY, lcaA, lcaB);
    should.deepEqual(mergedItem, ['bar']);
  });
});
