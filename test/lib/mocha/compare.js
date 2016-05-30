/**
 * Copyright 2014, 2016 Netsend.
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

var tmpdir = require('os').tmpdir;

var level = require('level-packager')(require('leveldown'));
var rimraf = require('rimraf');
var should = require('should');

var Tree = require('../../../lib/tree');
var logger = require('../../../lib/logger');
var compare = require('../../../lib/compare');

var tree1, tree2;

var db, cons, silence;
var dbPath = tmpdir() + '/test_tree';

// open database and trees
before(function(done) {
  logger({ console: true, mask: logger.DEBUG2 }, function(err, l) {
    if (err) { throw err; }
    cons = l;
    logger({ silence: true }, function(err, l) {
      if (err) { throw err; }
      silence = l;
      // ensure a db at start
      rimraf(dbPath, function(err) {
        if (err) { throw err; }
        db = level(dbPath, { keyEncoding: 'binary', valueEncoding: 'binary' });
        tree1 = new Tree(db, 'foo', { log: silence, vSize: 3 });
        tree2 = new Tree(db, 'bar', { log: silence, vSize: 3 });
        done();
      });
    });
  });
});

after(function(done) {
  cons.close(function(err) {
    if (err) { throw err; }
    silence.close(function(err) {
      if (err) { throw err; }
      db.close(function(err) {
        if (err) { throw err; }
        rimraf(dbPath, done);
      });
    });
  });
});

describe('compare', function() {
  it('should require tree1 to be an object', function() {
    (function() { compare(); }).should.throw('tree1 must be an object');
  });

  it('should require tree2 to be an object', function() {
    (function() { compare(tree1); }).should.throw('tree2 must be an object');
  });

  it('should require cb to be a function (without opts)', function() {
    (function() { compare(tree1, tree2); }).should.throw('cb must be a function');
  });

  it('should require cb to be a function (with opts)', function() {
    (function() { compare(tree1, tree2, {}); }).should.throw('cb must be a function');
  });

  it('should require opts to be an object', function() {
    (function() { compare(tree1, tree2, 'foo', function() {}); }).should.throw('opts must be an object');
  });

  it('should require opts.includeAttrs to be an object', function() {
    (function() { compare(tree1, tree2, { includeAttrs: 'foo' }, function() {}); }).should.throw('opts.includeAttrs must be an object');
  });

  it('should require opts.excludeAttrs to be an object', function() {
    (function() { compare(tree1, tree2, { excludeAttrs: 'foo' }, function() {}); }).should.throw('opts.excludeAttrs must be an object');
  });

  it('should require that only includeAttrs or excludeAttrs is provided but not both', function() {
    (function() {
      compare(tree1, tree2, { includeAttrs: { foo: true }, excludeAttrs: { id: true } }, function() {
      }).should.throw('includeAttrs and excludeAttrs can not be combined');
    });
  });

  it('should return object with empty arrays', function(done) {
    compare(tree1, tree2, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [],
        inequal: [],
        equal: [],
        multiple: [],
      });
      done();
    });
  });

  it('needs some objects in both trees for further testing', function(done) {
    var itemIS = { h: { id: 'S', v: 'Aaaa', pa: [], i: 1 }, b: { foo: 'S' } };
    var itemIT = { h: { id: 'T', v: 'Bbbb', pa: [], i: 2 }, b: { foo: 'T', bar: 'baz' } };
    var itemIU = { h: { id: 'U', v: 'Cccc', pa: [], i: 3 }, b: { foo: 'U' } };

    var itemIIS = { h: { id: 'S', v: 'Aaaa', pa: [], i: 1 }, b: { foo: 'S' } };
    var itemIIT = { h: { id: 'T', v: 'Bbbb', pa: [], i: 2 }, b: { foo: 'T' } };
    var itemIIW = { h: { id: 'W', v: 'Cccc', pa: [], i: 3 }, b: { foo: 'W' } };

    tree1.write(itemIS);
    tree1.write(itemIT);
    tree1.write(itemIU, function(err) {
      if (err) { throw err; }

      tree2.write(itemIIS);
      tree2.write(itemIIT);
      tree2.write(itemIIW, done);
    });
  });

  it('should do a basic compare based on all attributes (tree1 leading)', function(done) {
    compare(tree1, tree2, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [
          { h: { id: 'U', v: 'Cccc', pa: [], i: 3 }, b: { foo: 'U' } }
        ],
        inequal: [{
          item1: { h: { id: 'T', v: 'Bbbb', pa: [], i: 2 }, b: { foo: 'T', bar: 'baz' } },
          item2: { h: { id: 'T', v: 'Bbbb', pa: [], i: 2 }, b: { foo: 'T' } }
        }],
        equal: [{
          item1: { h: { id: 'S', v: 'Aaaa', pa: [], i: 1 }, b: { foo: 'S' } },
          item2: { h: { id: 'S', v: 'Aaaa', pa: [], i: 1 }, b: { foo: 'S' } }
        }],
        multiple: [],
      });
      done();
    });
  });

  it('should do a basic compare based on all attributes (tree2 leading)', function(done) {
    compare(tree2, tree1, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [{ h: { id: 'W', v: 'Cccc', pa: [], i: 3 }, b: { foo: 'W' } }],
        inequal: [{
          item1: { h: { id: 'T', v: 'Bbbb', pa: [], i: 2 }, b: { foo: 'T' } },
          item2: { h: { id: 'T', v: 'Bbbb', pa: [], i: 2 }, b: { foo: 'T', bar: 'baz' } },
        }],
        equal: [{
          item1: { h: { id: 'S', v: 'Aaaa', pa: [], i: 1 }, b: { foo: 'S' } },
          item2: { h: { id: 'S', v: 'Aaaa', pa: [], i: 1 }, b: { foo: 'S' } }
        }],
        multiple: [],
      });
      done();
    });
  });

  it('should do a basic compare based on all attributes but bar', function(done) {
    compare(tree1, tree2, { excludeAttrs: { bar: true } }, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [ { h: { id: 'U', v: 'Cccc', pa: [], i: 3 }, b: { foo: 'U' } } ],
        inequal: [],
        equal: [
          {
            item1: { h: { id: 'S', v: 'Aaaa', pa: [], i: 1 }, b: { foo: 'S' } },
            item2: { h: { id: 'S', v: 'Aaaa', pa: [], i: 1 }, b: { foo: 'S' } }
          }, {
            item1: { h: { id: 'T', v: 'Bbbb', pa: [], i: 2 }, b: { foo: 'T', bar: 'baz' } },
            item2: { h: { id: 'T', v: 'Bbbb', pa: [], i: 2 }, b: { foo: 'T' } },
          }
        ],
        multiple: [],
      });
      done();
    });
  });

  it('should do a basic compare based on all attributes but bar (tree2 leading)', function(done) {
    compare(tree2, tree1, { excludeAttrs: { bar: true } }, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [{ h: { id: 'W', v: 'Cccc', pa: [], i: 3 }, b: { foo: 'W' } }],
        inequal: [],
        equal: [
          {
            item1: { h: { id: 'S', v: 'Aaaa', pa: [], i: 1 }, b: { foo: 'S' } },
            item2: { h: { id: 'S', v: 'Aaaa', pa: [], i: 1 }, b: { foo: 'S' } }
          }, {
            item1: { h: { id: 'T', v: 'Bbbb', pa: [], i: 2 }, b: { foo: 'T' } },
            item2: { h: { id: 'T', v: 'Bbbb', pa: [], i: 2 }, b: { foo: 'T', bar: 'baz' } },
          }
        ],
        multiple: [],
      });
      done();
    });
  });

  it('should compare based on "bar"', function(done) {
    compare(tree1, tree2, { includeAttrs: { bar: true } }, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [ { h: { id: 'U', v: 'Cccc', pa: [], i: 3 }, b: { foo: 'U' } } ],
        inequal: [
          {
            item1: { h: { id: 'T', v: 'Bbbb', pa: [], i: 2 }, b: { foo: 'T', bar: 'baz' } },
            item2: { h: { id: 'T', v: 'Bbbb', pa: [], i: 2 }, b: { foo: 'T' } },
          }
        ],
        equal: [
          {
            item1: { h: { id: 'S', v: 'Aaaa', pa: [], i: 1 }, b: { foo: 'S' } },
            item2: { h: { id: 'S', v: 'Aaaa', pa: [], i: 1 }, b: { foo: 'S' } }
          }
        ],
        multiple: [],
      });
      done();
    });
  });

  it('should compare based on everything but "bar"', function(done) {
    compare(tree1, tree2, { excludeAttrs: { bar: true } }, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [{ h: { id: 'U', v: 'Cccc', pa: [], i: 3 }, b: { foo: 'U' } }],
        inequal: [],
        equal: [{
          item1: { h: { id: 'S', v: 'Aaaa', pa: [], i: 1 }, b: { foo: 'S' } },
          item2: { h: { id: 'S', v: 'Aaaa', pa: [], i: 1 }, b: { foo: 'S' } }
        }, {
          item1: { h: { id: 'T', v: 'Bbbb', pa: [], i: 2 }, b: { foo: 'T', bar: 'baz' } },
          item2: { h: { id: 'T', v: 'Bbbb', pa: [], i: 2 }, b: { foo: 'T' } }
        }],
        multiple: [],
      });
      done();
    });
  });

  it('needs multiple heads in tree1, create a fork', function(done) {
    var item1 = { h: { id: 'S', v: 'Dddd', pa: ['Aaaa'], i: 4 }, b: { some: 'S' } };
    var item2 = { h: { id: 'S', v: 'Eeee', pa: ['Aaaa'], i: 5 }, b: { other: 'S' } };
    tree1.write(item1);
    tree1.write(item2, done);
  });

  it('should compare multiple items from tree1 with the same item in tree2', function(done) {
    compare(tree1, tree2, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [
          { h: { id: 'U', v: 'Cccc', pa: [], i: 3 }, b: { foo: 'U' } }
        ],
        inequal: [
          {
            item1: { h: { id: 'S', v: 'Dddd', pa: ['Aaaa'], i: 4 }, b: { some: 'S' } },
            item2: { h: { id: 'S', v: 'Aaaa', pa: [], i: 1 }, b: { foo: 'S' } }
          },
          {
            item1: { h: { id: 'S', v: 'Eeee', pa: ['Aaaa'], i: 5 }, b: { other: 'S' } },
            item2: { h: { id: 'S', v: 'Aaaa', pa: [], i: 1 }, b: { foo: 'S' } }
          },
          {
            item1: { h: { id: 'T', v: 'Bbbb', pa: [], i: 2 }, b: { foo: 'T', bar: 'baz' } },
            item2: { h: { id: 'T', v: 'Bbbb', pa: [], i: 2 }, b: { foo: 'T' } }
          }
        ],
        equal: [],
        multiple: []
      });
      done();
    });
  });

  it('should compare one item from tree2 with multiple items in tree1', function(done) {
    compare(tree2, tree1, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [{ h: { id: 'W', v: 'Cccc', pa: [], i: 3 }, b: { foo: 'W' } }],
        inequal: [{
          item1: { h: { id: 'T', v: 'Bbbb', pa: [], i: 2 }, b: { foo: 'T' } },
          item2: { h: { id: 'T', v: 'Bbbb', pa: [], i: 2 }, b: { foo: 'T', bar: 'baz' } },
        }],
        equal: [],
        multiple: [
          {
            item1: { h: { id: 'S', v: 'Aaaa', pa: [], i: 1 }, b: { foo: 'S' } },
            items2: [
              { h: { id: 'S', v: 'Dddd', pa: ['Aaaa'], i: 4 }, b: { some: 'S' } },
              { h: { id: 'S', v: 'Eeee', pa: ['Aaaa'], i: 5 }, b: { other: 'S' } },
            ]
          }
        ]
      });
      done();
    });
  });
});
