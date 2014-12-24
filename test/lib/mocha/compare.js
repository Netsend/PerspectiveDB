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

var compare = require('../../../lib/compare');

var collName1 = 'compare1';
var collName2 = 'compare2';
var coll1, coll2;

var db;
var databaseName = 'test_compare';
var Database = require('../../_database');

// open database connection
var database = new Database(databaseName);
before(function(done) {
  database.connect(function(err, dbc) {
    if (err) { throw err; }
    db = dbc;
    coll1 = db.collection(collName1);
    coll2 = db.collection(collName2);
    done(err);
  });
});

after(database.disconnect.bind(database));

describe('compare', function() {
  it('should require coll1 to be a mongodb.Collection', function() {
    (function() { compare(); }).should.throw('coll1 must be a mongodb.Collection');
  });

  it('should require coll2 to be a mongodb.Collection', function() {
    (function() { compare(coll1); }).should.throw('coll2 must be a mongodb.Collection');
  });

  it('should require cb to be a function (without opts)', function() {
    (function() { compare(coll1, coll2); }).should.throw('cb must be a function');
  });

  it('should require cb to be a function (with opts)', function() {
    (function() { compare(coll1, coll2, {}); }).should.throw('cb must be a function');
  });

  it('should require opts to be an object', function() {
    (function() { compare(coll1, coll2, 'foo', function() {}); }).should.throw('opts must be an object');
  });

  it('should require opts.includeAttrs to be an object', function() {
    (function() { compare(coll1, coll2, { includeAttrs: 'foo' }, function() {}); }).should.throw('opts.includeAttrs must be an object');
  });

  it('should require opts.excludeAttrs to be an object', function() {
    (function() { compare(coll1, coll2, { excludeAttrs: 'foo' }, function() {}); }).should.throw('opts.excludeAttrs must be an object');
  });

  it('should require that only includeAttrs or excludeAttrs is provided but not both', function() {
    (function() {
      compare(coll1, coll2, { includeAttrs: { foo: true }, excludeAttrs: { _id: true } }, function() {
      }).should.throw('includeAttrs and excludeAttrs can not be combined');
    });
  });

  it('should return object with empty arrays', function(done) {
    compare(coll1, coll2, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [],
        inequal: [],
        equal: [],
        multiple: [],
        unknown: []
      });
      done();
    });
  });

  it('needs some objects in both collections for further testing', function(done) {
    var itemIS = { _id: 'S', foo: 'S' };
    var itemIT = { _id: 'T', foo: 'T', bar: 'baz' };
    var itemIU = { _id: 'U', foo: 'U' };

    var itemIIS = { _id: 'S', foo: 'S' };
    var itemIIT = { _id: 'T', foo: 'T' };
    var itemIIW = { _id: 'W', foo: 'W' };

    coll1.insert([itemIS, itemIT, itemIU], function(err, inserted) {
      if (err) { throw err; }
      should.equal(inserted.length, 3);

      coll2.insert([itemIIS, itemIIT, itemIIW], function(err, inserted) {
        if (err) { throw err; }
        should.equal(inserted.length, 3);
        done();
      });
    });
  });

  it('should do a basic compare based on all attributes', function(done) {
    compare(coll1, coll2, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [{ _id: 'T', foo: 'T', bar: 'baz' }, { _id: 'U', foo: 'U' }],
        inequal: [],
        equal: [{
          item1: { _id: 'S', foo: 'S' },
          item2: { _id: 'S', foo: 'S' }
        }],
        multiple: [],
        unknown: []
      });
      done();
    });
  });

  it('should do a basic compare based on all attributes (first coll2)', function(done) {
    compare(coll2, coll1, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [{ _id: 'W', foo: 'W' }],
        inequal: [{
          item1: { _id: 'T', foo: 'T' },
          item2: { _id: 'T', foo: 'T', bar: 'baz' }
        }],
        equal: [{
          item1: { _id: 'S', foo: 'S' },
          item2: { _id: 'S', foo: 'S' }
        }],
        multiple: [],
        unknown: []
      });
      done();
    });
  });

  it('should do a basic compare based on all attributes but bar', function(done) {
    compare(coll1, coll2, { excludeAttrs: { bar: true } }, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [{ _id: 'U', foo: 'U' }],
        inequal: [],
        equal: [{
          item1: { _id: 'S', foo: 'S' },
          item2: { _id: 'S', foo: 'S' }
        }, {
          item1: { _id: 'T', foo: 'T', bar: 'baz' },
          item2: { _id: 'T', foo: 'T' }
        }],
        multiple: [],
        unknown: []
      });
      done();
    });
  });

  it('should do a basic compare based on all attributes but bar (first coll2)', function(done) {
    compare(coll2, coll1, { excludeAttrs: { bar: true } }, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [{ _id: 'W', foo: 'W' }],
        inequal: [],
        equal: [{
          item1: { _id: 'S', foo: 'S' },
          item2: { _id: 'S', foo: 'S' }
        }, {
          item1: { _id: 'T', foo: 'T' },
          item2: { _id: 'T', foo: 'T', bar: 'baz' }
        }],
        multiple: [],
        unknown: []
      });
      done();
    });
  });

  it('should compare based on "_id"', function(done) {
    compare(coll1, coll2, { includeAttrs: { _id: true } }, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [{ _id: 'U', foo: 'U' }],
        inequal: [],
        equal: [{
          item1: { _id: 'S', foo: 'S' },
          item2: { _id: 'S', foo: 'S' }
        }, {
          item1: { _id: 'T', foo: 'T', bar: 'baz' },
          item2: { _id: 'T', foo: 'T' }
        }],
        multiple: [],
        unknown: []
      });
      done();
    });
  });

  it('should compare on all attributes, matching on "_id"', function(done) {
    compare(coll1, coll2, { matchAttrs: { _id: true } }, function(err, stats) {
      if (err) { throw err; }
      should.strictEqual(Object.keys(stats).length, 5);
      should.deepEqual(stats.missing, [{ _id: 'U', foo: 'U' }]);
      should.deepEqual(stats.inequal, [{
        item1: { _id: 'T', foo: 'T', bar: 'baz' },
        item2: { _id: 'T', foo: 'T' }
      }]);
      should.deepEqual(stats.equal, [{
        item1: { _id: 'S', foo: 'S' },
        item2: { _id: 'S', foo: 'S' }
      }]);
      should.deepEqual(stats.multiple, []);
      should.deepEqual(stats.unknown, []);
      done();
    });
  });

  it('should compare on all attributes, matching on "_id" (first coll2)', function(done) {
    compare(coll2, coll1, { matchAttrs: { _id: true } }, function(err, stats) {
      if (err) { throw err; }
      should.strictEqual(Object.keys(stats).length, 5);
      should.deepEqual(stats.missing, [{ _id: 'W', foo: 'W' }]);
      should.deepEqual(stats.inequal, [{
        item1: { _id: 'T', foo: 'T' },
        item2: { _id: 'T', foo: 'T', bar: 'baz' }
      }]);
      should.deepEqual(stats.equal, [{
        item1: { _id: 'S', foo: 'S' },
        item2: { _id: 'S', foo: 'S' }
      }]);
      should.deepEqual(stats.multiple, []);
      should.deepEqual(stats.unknown, []);
      done();
    });
  });

  it('should compare based on everything but "_id"', function(done) {
    compare(coll1, coll2, { excludeAttrs: { _id: true } }, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [{ _id: 'T', foo: 'T', bar: 'baz' }, { _id: 'U', foo: 'U' }],
        inequal: [],
        equal: [{
          item1: { _id: 'S', foo: 'S' },
          item2: { _id: 'S', foo: 'S' }
        }],
        multiple: [],
        unknown: []
      });
      done();
    });
  });

  it('should compare based on "bar"', function(done) {
    compare(coll1, coll2, { includeAttrs: { bar: true } }, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [{ _id: 'T', foo: 'T', bar: 'baz' }],
        inequal: [],
        equal: [],
        multiple: [],
        unknown: [{ _id: 'S', foo: 'S' }, { _id: 'U', foo: 'U' }]
      });
      done();
    });
  });

  it('should compare based on everything but "bar"', function(done) {
    compare(coll1, coll2, { excludeAttrs: { bar: true } }, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [{ _id: 'U', foo: 'U' }],
        inequal: [],
        equal: [{
          item1: { _id: 'S', foo: 'S' },
          item2: { _id: 'S', foo: 'S' }
        }, {
          item1: { _id: 'T', foo: 'T', bar: 'baz' },
          item2: { _id: 'T', foo: 'T' }
        }],
        multiple: [],
        unknown: []
      });
      done();
    });
  });

  it('needs a duplicate in coll1 if _id is excluded', function(done) {
    var item = { _id: 'V', foo: 'S' };
    coll1.insert(item, done);
  });

  it('should compare and detect if multiple items from coll1 resolve to the same item in coll2', function(done) {
    /*
    var itemIS = { _id: 'S', foo: 'S' };
    var itemIT = { _id: 'T', foo: 'T', bar: 'baz' };
    var itemIU = { _id: 'U', foo: 'U' };
    var itemIV = { _id: 'V', foo: 'S' };

    var itemIIS = { _id: 'S', foo: 'S' };
    var itemIIT = { _id: 'T', foo: 'T' };
    var itemIIW = { _id: 'W', foo: 'W' };
    */

    compare(coll1, coll2, { excludeAttrs: { _id: true } }, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [
          { _id: 'T', foo: 'T', bar: 'baz' },
          { _id: 'U', foo: 'U' }
        ],
        inequal: [],
        equal: [{ item1: { _id: 'S', foo: 'S' }, item2: { _id: 'S', foo: 'S' } }],
        multiple: [{
          item1: { _id: 'V', foo: 'S' },
          items2: [{ _id: 'S', foo: 'S' }]
        }],
        unknown: []
      });
      done();
    });
  });

  it('needs a duplicate in coll2 if _id is excluded', function(done) {
    var item = { _id: 'X', foo: 'S' };
    coll2.insert(item, done);
  });

  it('should compare and detect if items from coll1 resolve to multiple items in coll2', function(done) {
    /*
    var itemIS = { _id: 'S', foo: 'S' };
    var itemIT = { _id: 'T', foo: 'T', bar: 'baz' };
    var itemIU = { _id: 'U', foo: 'U' };
    var itemIV = { _id: 'V', foo: 'S' };

    var itemIIS = { _id: 'S', foo: 'S' };
    var itemIIT = { _id: 'T', foo: 'T' };
    var itemIIW = { _id: 'W', foo: 'W' };
    var itemIIX = { _id: 'X', foo: 'S' };
    */

    compare(coll1, coll2, { excludeAttrs: { _id: true } }, function(err, stats) {
      if (err) { throw err; }
      should.deepEqual(stats, {
        missing: [
          { _id: 'T', foo: 'T', bar: 'baz' },
          { _id: 'U', foo: 'U' }
        ],
        inequal: [],
        equal: [],
        multiple: [{
          item1: { _id: 'S', foo: 'S' },
          items2: [{ _id: 'S', foo: 'S' }, { _id: 'X', foo: 'S' }]
        }, {
          item1: { _id: 'V', foo: 'S' },
          items2: [{ _id: 'S', foo: 'S' }, { _id: 'X', foo: 'S' }]
        }],
        unknown: []
      });
      done();
    });
  });
});
