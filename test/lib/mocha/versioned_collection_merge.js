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
var ObjectID = require('mongodb').ObjectID;

var VersionedCollection = require('../../../lib/versioned_collection');

var db;
var databaseName = 'test_versioned_collection_merge';
var Database = require('../../_database');

// open database connection
var database = new Database(databaseName);
before(function(done) {
  database.connect(function(err, dbc) {
    db = dbc;
    done(err);
  });
});

after(database.disconnect.bind(database));

describe('VersionedCollection._merge', function() {
  var idFoo = new ObjectID('f00000000000000000000000');

  describe('maintain _m3', function() {
    var collectionName = 'maintainM3';

    var A = {
      _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
      _m3: { _ack: true },
      foo: 'bar',
      bar: 'baz',
      qux: 'quux'
    };

    var AII = {
      _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
      _m3: { _ack: true },
      foo: 'bar',
      qux: 'quux'
    };

    var B = {
      _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
      _m3: { _ack: true },
      foo: 'bar',
      bar: 'baz',
      qux: 'qux'
    };

    var C = {
      _id : { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['B'], _d: true },
      _m3: { _ack: true },
      foo: 'bar',
      bar: 'raboof',
      qux: 'quux'
    };

    var D = {
      _id : { _id: idFoo, _v: 'D', _pe: 'I', _pa: ['A'], _d: true },
      _m3: { _ack: true },
      foo: 'bar',
      bar: 'baz',
      qux: 'quux'
    };

    var DII = {
      _id : { _id: idFoo, _v: 'D', _pe: 'II', _pa: ['A'], _d: true },
      _m3: { _ack: true },
      foo: 'bar',
      bar: 'baz'
    };

    // create the following structure:
    //  A---B---C
    //   \
    //    D
    //
    //  AII---DII

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([A, B, C, D, AII, DII], {w: 1}, done);
    });

    it('should err on id mismatch', function(done) {
      var opts = { hide: true };
      var item1 = { _id : { _id: idFoo, _v: 'X', _pe: 'I', _pa: [] }, _m3: { _ack: true } };
      var item2 = { _id : { _id: 'bar', _v: 'Y', _pe: 'I', _pa: [] }, _m3: { _ack: true } };

      var vc = new VersionedCollection(db, collectionName, opts);
      vc._merge(item1, item2, function(err) {
        should.equal(err.message, 'merge id mismatch');
        done();
      });
    });

    it('A and A', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(A, A, function(err, merged) {
        if (err) { throw err; }
        should.strictEqual(merged.length, 1);
        should.deepEqual(merged[0], {
          _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
          foo: 'bar',
          bar: 'baz',
          qux: 'quux',
          _m3: { _ack: true }
        });
        should.strictEqual(merged[0]._id === A._id, true);
        should.strictEqual(merged[0]._m3 === A._m3, true);
        done();
      });
    });

    it('A and B', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(A, B, function(err, merged) {
        if (err) { throw err; }
        should.strictEqual(merged.length, 1);
        should.deepEqual(merged[0], {
          _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
          foo: 'bar',
          bar: 'baz',
          qux: 'qux',
          _m3: { _ack: true }
        });
        should.strictEqual(merged[0]._id === A._id, false);
        should.strictEqual(merged[0]._id === B._id, true);
        should.strictEqual(merged[0]._m3 === A._m3, false);
        should.strictEqual(merged[0]._m3 === B._m3, true);
        done();
      });
    });

    it('A and C', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(A, C, function(err, merged) {
        if (err) { throw err; }
        should.strictEqual(merged.length, 1);
        should.deepEqual(merged[0], {
          _id: { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['B'], _d: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'quux',
          _m3: { _ack: true }
        });
        should.strictEqual(merged[0]._id === A._id, false);
        should.strictEqual(merged[0]._id === C._id, true);
        should.strictEqual(merged[0]._m3 === A._m3, false);
        should.strictEqual(merged[0]._m3 === C._m3, true);
        done();
      });
    });

    it('C and B', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(C, B, function(err, merged) {
        if (err) { throw err; }
        should.strictEqual(merged.length, 1);
        should.deepEqual(merged[0], {
          _id : { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['B'], _d: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'quux',
          _m3: { _ack: true }
        });
        should.strictEqual(merged[0]._id === B._id, false);
        should.strictEqual(merged[0]._id === C._id, true);
        should.strictEqual(merged[0]._m3 === B._m3, false);
        should.strictEqual(merged[0]._m3 === C._m3, true);
        done();
      });
    });

    it('A and D', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(A, D, function(err, merged) {
        if (err) { throw err; }
        should.strictEqual(merged.length, 1);
        should.deepEqual(merged[0], {
          _id : { _id: idFoo, _v: 'D', _pe: 'I', _pa: ['A'], _d: true },
          foo: 'bar',
          bar: 'baz',
          qux: 'quux',
          _m3: { _ack: true }
        });
        should.strictEqual(merged[0]._id === A._id, false);
        should.strictEqual(merged[0]._id === D._id, true);
        should.strictEqual(merged[0]._m3 === A._m3, false);
        should.strictEqual(merged[0]._m3 === D._m3, true);
        done();
      });
    });

    it('B and D, should not conflict and not set _id._d', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc._merge(B, D, function(err, merged) {
        if (err) { throw err; }
        should.strictEqual(merged.length, 1);
        should.deepEqual(merged[0], {
          _id : { _co: 'maintainM3', _id: idFoo, _v: null, _pe: 'I', _pa: ['B', 'D'], _lo: true },
          foo: 'bar',
          bar: 'baz',
          qux: 'qux'
        });
        should.strictEqual(merged[0]._id === B._id, false);
        should.strictEqual(merged[0]._id === D._id, false);
        should.strictEqual(merged[0]._m3 === B._m3, false);
        should.strictEqual(merged[0]._m3 === D._m3, false);
        done();
      });
    });

    it('B and DII, should conflict on quux', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc._merge(B, DII, function(err, merged) {
        should.equal(err.message, 'merge conflict');
        should.deepEqual(merged, [['qux'],['bar','qux']]);
        done();
      });
    });

    it('C and D, should merge', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(C, D, function(err, merged) {
        if (err) { throw err; }
        should.strictEqual(merged.length, 1);
        should.deepEqual(merged[0], {
          _id : { _co: 'maintainM3', _id: idFoo, _v: null, _pe: 'I', _pa: ['C', 'D'], _lo: true, _d: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'quux',
        });
        should.strictEqual(merged[0]._id === C._id, false);
        should.strictEqual(merged[0]._id === D._id, false);
        should.strictEqual(merged[0]._m3 === C._m3, false);
        should.strictEqual(merged[0]._m3 === D._m3, false);
        done();
      });
    });
  });

  describe('one perspective', function() {
    var collectionName = '_mergeOnePerspective';

    var A = {
      _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
      _m3: { _ack: true },
      foo: 'bar',
      bar: 'baz',
      qux: 'quux'
    };

    var B = {
      _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
      _m3: { _ack: true },
      foo: 'bar',
      bar: 'baz',
      qux: 'qux'
    };

    var C = {
      _id : { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['A'] },
      _m3: { _ack: true },
      foo: 'bar',
      bar: 'raboof',
      qux: 'quux'
    };

    var D = {
      _id : { _id: idFoo, _v: 'D', _pe: 'I', _pa: ['B', 'C'] },
      _m3: { _ack: true },
      foo: 'bar',
      bar: 'raboof',
      qux: 'quz'
    };

    var E = {
      _id : { _id: idFoo, _v: 'E', _pe: 'I', _pa: ['C', 'B'] },
      _m3: { _ack: true },
      foo: 'bar',
      bar: 'foobar',
      qux: 'qux'
    };

    var F = {
      _id : { _id: idFoo, _v: 'F', _pe: 'I', _pa: ['D', 'E'] },
      _m3: { _ack: true },
      foo: 'bar',
      bar: 'foobar',
      qux: 'quz'
    };

    // create the following structure:
    //    C <- E
    //   / \ /  \
    //  A   X    F
    //   \ / \  /          
    //    B <- D
    // see http://www.gelato.unsw.edu.au/archives/git/0504/2279.html

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([A, B, C, D, E, F], {w: 1}, done);
    });

    it('A and A = ff to A', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(A, A, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
          _m3: { _ack: true },
          foo: 'bar',
          bar: 'baz',
          qux: 'quux'
        }]);
        should.strictEqual(merged[0]._id === A._id, true);
        should.strictEqual(merged[0]._m3 === A._m3, true);
        done();
      });
    });

    it('B and C = merge', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      console.log(B);
      vc._merge(B, C, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _co: '_mergeOnePerspective', _id: idFoo, _v: null, _pe: 'I', _pa: ['B', 'C'], _lo: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'qux'
        }]);
        should.strictEqual(merged[0]._id === B._id, false);
        should.strictEqual(merged[0]._m3 === B._m3, false);
        should.strictEqual(merged[0]._id === C._id, false);
        should.strictEqual(merged[0]._m3 === C._m3, false);
        done();
      });
    });

    it('E and B = ff to E', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(E, B, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'E', _pe: 'I', _pa: ['C', 'B'] },
          _m3: { _ack: true },
          foo: 'bar',
          bar: 'foobar',
          qux: 'qux'
        }]);
        should.strictEqual(merged[0]._id === B._id, false);
        should.strictEqual(merged[0]._m3 === B._m3, false);
        should.strictEqual(merged[0]._id === E._id, true);
        should.strictEqual(merged[0]._m3 === E._m3, true);
        done();
      });
    });

    it('D and E = merge', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(D, E, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _co: '_mergeOnePerspective', _id: idFoo, _v: null, _pe: 'I', _pa: ['D', 'E'], _lo: true },
          foo: 'bar',
          bar: 'foobar',
          qux: 'quz'
        }]);
        done();
      });
    });

    it('E and D = merge', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(E, D, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _co: '_mergeOnePerspective', _id: idFoo, _v: null, _pe: 'I', _pa: ['E', 'D'], _lo: true },
          foo: 'bar',
          bar: 'foobar',
          qux: 'quz'
        }]);
        should.strictEqual(merged[0]._id === D._id, false);
        should.strictEqual(merged[0]._m3 === D._m3, false);
        should.strictEqual(merged[0]._id === E._id, false);
        should.strictEqual(merged[0]._m3 === E._m3, false);
        done();
      });
    });

    it('E and F = ff to F', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(E, F, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'F', _pe: 'I', _pa: ['D', 'E'] },
          _m3: { _ack: true },
          foo: 'bar',
          bar: 'foobar',
          qux: 'quz'
        }]);
        should.strictEqual(merged[0]._id === F._id, true);
        should.strictEqual(merged[0]._m3 === F._m3, true);
        should.strictEqual(merged[0]._id === E._id, false);
        should.strictEqual(merged[0]._m3 === E._m3, false);
        done();
      });
    });

    it('F and E = ff to F', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(F, E, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'F', _pe: 'I', _pa: ['D', 'E'] },
          _m3: { _ack: true },
          foo: 'bar',
          bar: 'foobar',
          qux: 'quz'
        }]);
        done();
      });
    });

    it('virtual merge vm1 and vm2 = conflict', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      var vm1 = { // add c: 'foo'
        _id : { _id: idFoo, _pe: 'I', _pa: ['A'] },
        foo: 'bar',
        v: true
      };
      var vm2 = { // add c: 'bar'
        _id : { _id: idFoo, _pe: 'I', _pa: ['A'] },
        foo: 'bar',
        v: false
      };

      vc._merge(vm1, vm2, function(err, merged) {
        should.equal(err.message, 'merge conflict');
        should.deepEqual(merged, ['v']);
        done();
      });
    });
  });

  describe('one perspective delete one', function() {
    var collectionName = '_mergeOnePerspectiveDeleteOne';

    var A = {
      _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
      foo: 'bar',
      bar: 'baz',
      qux: 'quux'
    };

    var B = {
      _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
      foo: 'bar',
      bar: 'baz',
      qux: 'qux'
    };

    var Cd = {
      _id : { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['A'], _d: true },
      foo: 'bar',
      bar: 'raboof',
      qux: 'quux'
    };

    // create the following structure:
    //    Cd
    //   /
    //  A
    //   \
    //    B
    // see http://www.gelato.unsw.edu.au/archives/git/0504/2279.html

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([A, B, Cd], {w: 1}, done);
    });

    it('B and C = merge, no delete', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(B, Cd, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _co: '_mergeOnePerspectiveDeleteOne', _id: idFoo, _v: null, _pe: 'I', _pa: ['B', 'C'], _lo: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'qux'
        }]);
        done();
      });
    });

    it('C and B = merge, no delete', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(Cd, B, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _co: '_mergeOnePerspectiveDeleteOne', _id: idFoo, _v: null, _pe: 'I', _pa: ['C', 'B'], _lo: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'qux'
        }]);
        done();
      });
    });
  });

  describe('one perspective delete two', function() {
    var collectionName = '_mergeOnePerspectiveDeleteTwo';

    var A = {
      _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
      foo: 'bar',
      bar: 'baz',
      qux: 'quux'
    };

    var Bd = {
      _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'], _d: true },
      foo: 'bar',
      bar: 'baz',
      qux: 'qux'
    };

    var Cd = {
      _id : { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['A'], _d: true },
      foo: 'bar',
      bar: 'raboof',
      qux: 'quux'
    };

    // create the following structure:
    //    Cd
    //   /
    //  A
    //   \
    //    Bd
    // see http://www.gelato.unsw.edu.au/archives/git/0504/2279.html

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([A, Bd, Cd], {w: 1}, done);
    });

    it('B and C = merge, delete', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(Bd, Cd, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _co: '_mergeOnePerspectiveDeleteTwo', _id: idFoo, _v: null, _pe: 'I', _pa: ['B', 'C'], _lo: true, _d: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'qux'
        }]);
        done();
      });
    });
  });

  describe('two perspectives delete one', function() {
    var collectionName = '_mergeTwoPerspectiveDeleteOne';

    var AI = {
      _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
      foo: 'bar',
      bar: 'baz',
      qux: 'quux',
      some: true
    };

    var BI = {
      _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
      foo: 'bar',
      bar: 'baz',
      qux: 'qux',
      some: true
    };

    var CId = {
      _id : { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['A'], _d: true },
      foo: 'bar',
      bar: 'raboof',
      qux: 'quux',
      some: true
    };

    var AII = {
      _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
      foo: 'bar',
      bar: 'baz',
      qux: 'quux'
    };

    var BII = {
      _id : { _id: idFoo, _v: 'B', _pe: 'II', _pa: ['A'] },
      foo: 'bar',
      bar: 'baz',
      qux: 'qux'
    };

    var CIId = {
      _id : { _id: idFoo, _v: 'C', _pe: 'II', _pa: ['A'], _d: true },
      foo: 'bar',
      bar: 'raboof',
      qux: 'quux'
    };

    // create the following structure:
    //    Cd
    //   /
    //  A
    //   \
    //    B
    // see http://www.gelato.unsw.edu.au/archives/git/0504/2279.html

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([AI, BI, AII, BII, CId, CIId], {w: 1}, done);
    });

    it('BI and CII = merge, no delete', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(BI, CIId, function(err, merged) {
        if (err) { throw err; }
        should.strictEqual(merged.length, 2);
        should.deepEqual(merged[0], {
          _id : { _co: '_mergeTwoPerspectiveDeleteOne', _id: idFoo, _v: null, _pe: 'I', _pa: ['B', 'C'], _lo: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'qux',
          some: true
        });
        should.deepEqual(merged[1], {
          _id : { _co: '_mergeTwoPerspectiveDeleteOne', _id: idFoo, _v: null, _pe: 'II', _pa: ['B', 'C'], _lo: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'qux'
        });
        done();
      });
    });

    it('BII and CI = merge, no delete', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(BII, CId, function(err, merged) {
        if (err) { throw err; }
        should.strictEqual(merged.length, 2);
        should.deepEqual(merged[0], {
          _id : { _co: '_mergeTwoPerspectiveDeleteOne', _id: idFoo, _v: null, _pe: 'II', _pa: ['B', 'C'], _lo: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'qux'
        });
        should.deepEqual(merged[1], {
          _id : { _co: '_mergeTwoPerspectiveDeleteOne', _id: idFoo, _v: null, _pe: 'I', _pa: ['B', 'C'], _lo: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'qux',
          some: true
        });
        done();
      });
    });

    it('CI and BII = merge, no delete', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(CId, BII, function(err, merged) {
        if (err) { throw err; }
        should.strictEqual(merged.length, 2);
        should.deepEqual(merged[0], {
          _id : { _co: '_mergeTwoPerspectiveDeleteOne', _id: idFoo, _v: null, _pe: 'I', _pa: ['C', 'B'], _lo: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'qux',
          some: true
        });
        should.deepEqual(merged[1], {
          _id : { _co: '_mergeTwoPerspectiveDeleteOne', _id: idFoo, _v: null, _pe: 'II', _pa: ['C', 'B'], _lo: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'qux'
        });
        done();
      });
    });

    it('CII and BI = merge, no delete', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(CIId, BI, function(err, merged) {
        if (err) { throw err; }
        should.strictEqual(merged.length, 2);
        should.deepEqual(merged[0], {
          _id : { _co: '_mergeTwoPerspectiveDeleteOne', _id: idFoo, _v: null, _pe: 'II', _pa: ['C', 'B'], _lo: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'qux'
        });
        should.deepEqual(merged[1], {
          _id : { _co: '_mergeTwoPerspectiveDeleteOne', _id: idFoo, _v: null, _pe: 'I', _pa: ['C', 'B'], _lo: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'qux',
          some: true
        });
        done();
      });
    });
  });

  describe('two perspectives delete two', function() {
    var collectionName = '_mergeTwoPerspectiveDeleteTwo';

    var AI = {
      _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
      foo: 'bar',
      bar: 'baz',
      qux: 'quux',
      some: true
    };

    var BId = {
      _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'], _d: true },
      foo: 'bar',
      bar: 'baz',
      qux: 'qux',
      some: true
    };

    var CId = {
      _id : { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['A'], _d: true },
      foo: 'bar',
      bar: 'raboof',
      qux: 'quux',
      some: true
    };

    var AII = {
      _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
      foo: 'bar',
      bar: 'baz',
      qux: 'quux'
    };

    var BIId = {
      _id : { _id: idFoo, _v: 'B', _pe: 'II', _pa: ['A'], _d: true },
      foo: 'bar',
      bar: 'baz',
      qux: 'qux'
    };

    var CIId = {
      _id : { _id: idFoo, _v: 'C', _pe: 'II', _pa: ['A'], _d: true },
      foo: 'bar',
      bar: 'raboof',
      qux: 'quux'
    };

    // create the following structure:
    //    Cd
    //   /
    //  A
    //   \
    //    B
    // see http://www.gelato.unsw.edu.au/archives/git/0504/2279.html

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([AI, BId, AII, BIId, CId, CIId], {w: 1}, done);
    });

    it('BI and CII = merge, delete', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(BId, CIId, function(err, merged) {
        if (err) { throw err; }
        should.strictEqual(merged.length, 2);
        should.deepEqual(merged[0], {
          _id : { _co: '_mergeTwoPerspectiveDeleteTwo', _id: idFoo, _v: null, _pe: 'I', _pa: ['B', 'C'], _lo: true, _d: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'qux',
          some: true
        });
        should.deepEqual(merged[1], {
          _id : { _co: '_mergeTwoPerspectiveDeleteTwo', _id: idFoo, _v: null, _pe: 'II', _pa: ['B', 'C'], _lo: true, _d: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'qux'
        });
        done();
      });
    });

    it('BII and CI = merge, delete', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(BIId, CId, function(err, merged) {
        if (err) { throw err; }
        should.strictEqual(merged.length, 2);
        should.deepEqual(merged[0], {
          _id : { _co: '_mergeTwoPerspectiveDeleteTwo', _id: idFoo, _v: null, _pe: 'II', _pa: ['B', 'C'], _lo: true, _d: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'qux'
        });
        should.deepEqual(merged[1], {
          _id : { _co: '_mergeTwoPerspectiveDeleteTwo', _id: idFoo, _v: null, _pe: 'I', _pa: ['B', 'C'], _lo: true, _d: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'qux',
          some: true
        });
        done();
      });
    });
  });

  describe('different perspectives 1', function() {
    var collectionName = '_mergeDifferentPerspectives1';

    // create DAG where all exported items are imported again

    var AI = {
      _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
      _m3: { _ack: true },
      baz : 'qux',
      bar: 'raboof',
      some: 'secret'
    };

    var BI = {
      _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
      _m3: { _ack: true },
      bar: 'raboof',
      some: 'secret'
    };

    var EI = {
      _id : { _id: idFoo, _v: 'E', _pe: 'I', _pa: ['B'] },
      _m3: { _ack: true },
      bar: 'foo',
      some: 'secret'
    };

    var AII = {
      _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
      _m3: { _ack: false },
      baz : 'qux',
      bar: 'raboof'
    };

    var BII = {
      _id : { _id: idFoo, _v: 'B', _pe: 'II', _pa: ['A'] },
      _m3: { _ack: false },
      bar: 'raboof'
    };

    var CII = {
      _id : { _id: idFoo, _v: 'C', _pe: 'II', _pa: ['A'] },
      _m3: { _ack: false },
      baz : 'qux',
      bar: 'raboof',
      foo: 'bar'
    };

    var DII = {
      _id : { _id: idFoo, _v: 'D', _pe: 'II', _pa: ['C', 'B'] },
      _m3: { _ack: false },
      bar: 'raboof',
      foo: 'bar'
    };

    // create DAG in system I after some interaction between system I and II:
    // I creates AI, syncs to II
    // II creates AI, then CII
    // I creates BI, syncs to II
    // I creates EI
    // II merged BI, with CII, creating DII
    // I syncs from II creating AII, CII, BII and DII
    // note: normally after this I should recreate DII as DI and create a merge between DI and EI

    // resulting DAG in system I:
    //                   AII <-- BII
    //                     \       \
    //                     CII <-- DII
    // AI <-- BI <-- EI

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([AI, BI, EI, AII, CII, BII, DII], {w: 1}, done);
    });

    it('AI and AII = ff to AI, ff to AII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(AI, AII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
          _m3: { _ack: true },
          baz : 'qux',
          bar: 'raboof',
          some: 'secret'
        }, {
          _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
          _m3: { _ack: false },
          baz : 'qux',
          bar: 'raboof'
        }]);
        should.strictEqual(merged[0]._id === AI._id, true);
        should.strictEqual(merged[0]._id === AII._id, false);
        should.strictEqual(merged[0]._m3 === AI._m3, true);
        should.strictEqual(merged[0]._m3 === AII._m3, false);
        should.strictEqual(merged[1]._id === AI._id, false);
        should.strictEqual(merged[1]._id === AII._id, true);
        should.strictEqual(merged[1]._m3 === AI._m3, false);
        should.strictEqual(merged[1]._m3 === AII._m3, true);
        done();
      });
    });

    it('AII and AI = ff to AII, ff to AI', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(AII, AI, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
          _m3: { _ack: false },
          baz : 'qux',
          bar: 'raboof'
        }, {
          _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
          _m3: { _ack: true },
          baz : 'qux',
          bar: 'raboof',
          some: 'secret'
        }]);
        should.strictEqual(merged[0]._id === AI._id, false);
        should.strictEqual(merged[0]._id === AII._id, true);
        should.strictEqual(merged[0]._m3 === AI._m3, false);
        should.strictEqual(merged[0]._m3 === AII._m3, true);
        should.strictEqual(merged[1]._id === AI._id, true);
        should.strictEqual(merged[1]._id === AII._id, false);
        should.strictEqual(merged[1]._m3 === AI._m3, true);
        should.strictEqual(merged[1]._m3 === AII._m3, false);
        done();
      });
    });

    it('BI and DII = merged ff to DI, ff to DII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(BI, DII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'D', _pe: 'I', _pa: ['C', 'B'] },
          bar: 'raboof',
          some: 'secret',
          foo: 'bar'
        }, {
          _id : { _id: idFoo, _v: 'D', _pe: 'II', _pa: ['C', 'B'] },
          _m3: { _ack: false },
          bar: 'raboof',
          foo: 'bar'
        }]);
        should.strictEqual(merged[0]._id === BI._id, false);
        should.strictEqual(merged[0]._id === DII._id, false);
        should.strictEqual(merged[0]._m3 === BI._m3, false);
        should.strictEqual(merged[0]._m3 === DII._m3, false);
        should.strictEqual(merged[1]._id === BI._id, false);
        should.strictEqual(merged[1]._id === DII._id, true);
        should.strictEqual(merged[1]._m3 === BI._m3, false);
        should.strictEqual(merged[1]._m3 === DII._m3, true);
        done();
      });
    });

    it('EI and DII = merges based on BI, BII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(EI, DII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _co: '_mergeDifferentPerspectives1', _id: idFoo, _v: null, _pe: 'I', _pa: ['E', 'D'], _lo: true },
          bar: 'foo',
          some: 'secret',
          foo: 'bar'
        }, {
          _id : { _co: '_mergeDifferentPerspectives1', _id: idFoo, _v: null, _pe: 'II', _pa: ['E', 'D'], _lo: true },
          bar: 'foo',
          foo: 'bar'
        }]);
        should.strictEqual(merged[0]._id === EI._id, false);
        should.strictEqual(merged[0]._id === DII._id, false);
        should.strictEqual(merged[0]._m3 === EI._m3, false);
        should.strictEqual(merged[0]._m3 === DII._m3, false);
        should.strictEqual(merged[1]._id === EI._id, false);
        should.strictEqual(merged[1]._id === DII._id, false);
        should.strictEqual(merged[1]._m3 === EI._m3, false);
        should.strictEqual(merged[1]._m3 === DII._m3, false);
        done();
      });
    });

    it('EI and BI = ff to EI', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(EI, BI, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'E', _pe: 'I', _pa: ['B'] },
          _m3: { _ack: true },
          bar: 'foo',
          some: 'secret'
        }]);
        should.strictEqual(merged[0]._id === EI._id, true);
        should.strictEqual(merged[0]._id === BI._id, false);
        should.strictEqual(merged[0]._m3 === EI._m3, true);
        should.strictEqual(merged[0]._m3 === BI._m3, false);
        done();
      });
    });

    it('BI and CII = unversioned DI and DII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(BI, CII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged[0], {
          _id : { _co: '_mergeDifferentPerspectives1', _id: idFoo, _v: null, _pe: 'I', _pa: ['B', 'C'], _lo: true },
          bar: 'raboof',
          some: 'secret',
          foo: 'bar'
        });
        should.deepEqual(merged[1], {
          _id : { _co: '_mergeDifferentPerspectives1', _id: idFoo, _v: null, _pe: 'II', _pa: ['B', 'C'], _lo: true },
          bar: 'raboof',
          foo: 'bar'
        });
        should.strictEqual(merged[0]._id === BI._id, false);
        should.strictEqual(merged[0]._id === CII._id, false);
        should.strictEqual(merged[0]._m3 === BI._m3, false);
        should.strictEqual(merged[0]._m3 === CII._m3, false);
        should.strictEqual(merged[1]._id === BI._id, false);
        should.strictEqual(merged[1]._id === CII._id, false);
        should.strictEqual(merged[1]._m3 === BI._m3, false);
        should.strictEqual(merged[1]._m3 === CII._m3, false);
        done();
      });
    });

    it('virtual merge vm1 and vm2 = conflict', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      var vm1 = { // add c: 'foo'
        _id : { _id: idFoo, _pe: 'I', _pa: ['A'] },
        foo: 'bar'
      };
      var vm2 = { // add c: 'bar'
        _id : { _id: idFoo, _pe: 'II', _pa: ['A'] },
        foo: 'baz'
      };

      vc._merge(vm1, vm2, function(err, merged) {
        should.equal(err.message, 'merge conflict');
        should.deepEqual(merged, [['foo'], ['foo']]);
        done();
      });
    });

    //////////////////// DELETE AI ON PURPOSE

    it('should error on missing lca', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc._snapshotCollection.remove({ '_id._v': 'A', '_id._pe': 'I' }, {w: 1}, function(err, deleted) {
        if (err) { throw err; }
        should.equal(deleted, 1);
        vc._merge(BI, CII, function(err) {
          should.equal(err.message, 'no lca found');
          done();
        });
      });
    });

    it('should not have had any side effects on merged objects', function() {
      should.deepEqual(AI, {
        _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
        _m3: { _ack: true },
        baz : 'qux',
        bar: 'raboof',
        some: 'secret'
      });

      should.deepEqual(BI, {
        _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
        _m3: { _ack: true },
        bar: 'raboof',
        some: 'secret'
      });

      should.deepEqual(EI, {
        _id : { _id: idFoo, _v: 'E', _pe: 'I', _pa: ['B'] },
        _m3: { _ack: true },
        bar: 'foo',
        some: 'secret'
      });

      should.deepEqual(AII, {
        _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
        _m3: { _ack: false },
        baz : 'qux',
        bar: 'raboof'
      });

      should.deepEqual(BII, {
        _id : { _id: idFoo, _v: 'B', _pe: 'II', _pa: ['A'] },
        _m3: { _ack: false },
        bar: 'raboof'
      });

      should.deepEqual(CII, {
        _id : { _id: idFoo, _v: 'C', _pe: 'II', _pa: ['A'] },
        _m3: { _ack: false },
        baz : 'qux',
        bar: 'raboof',
        foo: 'bar'
      });

      should.deepEqual(DII, {
        _id : { _id: idFoo, _v: 'D', _pe: 'II', _pa: ['C', 'B'] },
        _m3: { _ack: false },
        bar: 'raboof',
        foo: 'bar'
      });
    });
  });

  describe('different perspectives 2', function() {
    var collectionName = '_mergeRecreatedMerge';

    // create DAG with recreated merge CI

    var AI = {
      _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
      _m3: { _ack: true },
      foo: 'bar',
      bar: 'baz',
      qux: 'quux',
      some: 'secret'
    };

    var AII = {
      _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
      _m3: { _ack: false },
      bar: 'baz',
      qux: 'quux'
    };

    var BI = {
      _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
      _m3: { _ack: true },
      foo: 'bar',
      bar: 'baz',
      qux: 'qux',
      some: 'secret'
    };

    var BII = {
      _id : { _id: idFoo, _v: 'B', _pe: 'II', _pa: ['A'] },
      _m3: { _ack: false },
      bar: 'baz',
      qux: 'qux'
    };

    var CII = {
      _id : { _id: idFoo, _v: 'C', _pe: 'II', _pa: ['B'] },
      _m3: { _ack: false },
      bar: 'raboof',
      qux: 'quux'
    };

    var CI = {
      _id : { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['B'] },
      _m3: { _ack: false },
      foo: 'bar',
      bar: 'raboof',
      qux: 'quux',
      some: 'secret'
    };

    // DAG in system I:
    //     AII <-- BII <-- CII
    // 
    //  AI <-- BI <---------- CI

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([AI, AII, BI, BII, CII, CI], {w: 1}, done);
    });

    it('AI and AI = ff to AI', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(AI, AI, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
          _m3: { _ack: true },
          foo: 'bar',
          bar: 'baz',
          qux: 'quux',
          some: 'secret'
        }]);
        done();
      });
    });

    it('AI and BI = ff to BI', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(AI, BI, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
          _m3: { _ack: true },
          foo: 'bar',
          bar: 'baz',
          qux: 'qux',
          some: 'secret'
        }]);
        done();
      });
    });

    it('BI and AI = ff to BI', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(BI, AI, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
          _m3: { _ack: true },
          foo: 'bar',
          bar: 'baz',
          qux: 'qux',
          some: 'secret'
        }]);
        done();
      });
    });

    it('AII and AII = ff to AII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(AII, AII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
          _m3: { _ack: false },
          bar: 'baz',
          qux: 'quux'
        }]);
        done();
      });
    });

    it('AII and BII = ff to BII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(AII, BII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'B', _pe: 'II', _pa: ['A'] },
          _m3: { _ack: false },
          bar: 'baz',
          qux: 'qux'
        }]);
        done();
      });
    });

    it('BII and AII = ff to BII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(BII, AII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'B', _pe: 'II', _pa: ['A'] },
          _m3: { _ack: false },
          bar: 'baz',
          qux: 'qux'
        }]);
        done();
      });
    });

    it('AI and AII = ff to AI, ff to AII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(AI, AII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
          _m3: { _ack: true },
          foo: 'bar',
          bar: 'baz',
          qux: 'quux',
          some: 'secret'
        }, {
          _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
          _m3: { _ack: false },
          bar: 'baz',
          qux: 'quux'
        }]);
        done();
      });
    });

    it('AII and AI = ff to AII, ff to AI', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(AII, AI, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
          _m3: { _ack: false },
          bar: 'baz',
          qux: 'quux'
        }, {
          _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
          _m3: { _ack: true },
          foo: 'bar',
          bar: 'baz',
          qux: 'quux',
          some: 'secret'
        }]);
        done();
      });
    });

    it('AII and BI = merged ff to BII, ff to BI', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(AII, BI, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'B', _pe: 'II', _pa: ['A'] },
          bar: 'baz',
          qux: 'qux'
        }, {
          _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
          _m3: { _ack: true },
          foo: 'bar',
          bar: 'baz',
          qux: 'qux',
          some: 'secret'
        }]);
        done();
      });
    });

    it('BI and AII = ff to BI, merged ff to BII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(BI, AII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
          _m3: { _ack: true },
          foo: 'bar',
          bar: 'baz',
          qux: 'qux',
          some: 'secret'
        }, {
          _id : { _id: idFoo, _v: 'B', _pe: 'II', _pa: ['A'] },
          bar: 'baz',
          qux: 'qux'
        }]);
        done();
      });
    });

    it('BI and CII = merged ff to CI, ff to CII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(BI, CII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['B'] },
          foo: 'bar',
          bar: 'raboof',
          qux: 'quux',
          some: 'secret'
        }, {
          _id : { _id: idFoo, _v: 'C', _pe: 'II', _pa: ['B'] },
          _m3: { _ack: false },
          bar: 'raboof',
          qux: 'quux'
        }]);
        done();
      });
    });

    it('CII and BI = ff to CII, merged ff to CI', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(CII, BI, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'C', _pe: 'II', _pa: ['B'] },
          _m3: { _ack: false },
          bar: 'raboof',
          qux: 'quux'
        }, {
          _id : { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['B'] },
          foo: 'bar',
          bar: 'raboof',
          qux: 'quux',
          some: 'secret'
        }]);
        done();
      });
    });

    it('BII and CII = ff to CII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(BII, CII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'C', _pe: 'II', _pa: ['B'] },
          _m3: { _ack: false },
          bar: 'raboof',
          qux: 'quux'
        }]);
        done();
      });
    });

    it('CII and BII = ff to CII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(CII, BII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'C', _pe: 'II', _pa: ['B'] },
          _m3: { _ack: false },
          bar: 'raboof',
          qux: 'quux'
        }]);
        done();
      });
    });

    it('BI and CI = ff to CI', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(BI, CI, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['B'] },
          _m3: { _ack: false },
          foo: 'bar',
          bar: 'raboof',
          qux: 'quux',
          some: 'secret'
        }]);
        done();
      });
    });

    it('CI and BI = ff to CI', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(CI, BI, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['B'] },
          _m3: { _ack: false },
          foo: 'bar',
          bar: 'raboof',
          qux: 'quux',
          some: 'secret'
        }]);
        done();
      });
    });
  });

  describe('different perspectives 3', function() {
    var collectionName = '_mergeDifferentPerspectives3';

    // create DAG with imported criss-cross merge

    var AI = {
      _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
      _m3: { _ack: true },
      foo: 'bar',
      bar: 'baz',
      qux: 'quux',
      some: 'secret'
    };

    var BI = {
      _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
      _m3: { _ack: true },
      foo: 'bar',
      bar: 'baz',
      qux: 'qux',
      some: 'secret'
    };

    var AII = {
      _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
      _m3: { _ack: false },
      foo: 'bar',
      bar: 'baz',
      qux: 'quux'
    };

    var BII = {
      _id : { _id: idFoo, _v: 'B', _pe: 'II', _pa: ['A'] },
      _m3: { _ack: false },
      foo: 'bar',
      bar: 'baz',
      qux: 'qux'
    };

    var CI = {
      _id : { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['A'] },
      _m3: { _ack: false },
      foo: 'bar',
      bar: 'raboof',
      qux: 'quux',
      some: 'secret'
    };

    var CII = {
      _id : { _id: idFoo, _v: 'C', _pe: 'II', _pa: ['A'] },
      _m3: { _ack: false },
      foo: 'bar',
      bar: 'raboof',
      qux: 'quux'
    };

    var DII = {
      _id : { _id: idFoo, _v: 'D', _pe: 'II', _pa: ['B', 'C'] },
      _m3: { _ack: false },
      foo: 'bar',
      bar: 'raboof',
      qux: 'quz'
    };

    var DI = {
      _id : { _id: idFoo, _v: 'D', _pe: 'I', _pa: ['B', 'C'] },
      _m3: { _ack: false },
      foo: 'bar',
      bar: 'raboof',
      qux: 'quz',
      some: 'secret'
    };

    var EII = {
      _id : { _id: idFoo, _v: 'E', _pe: 'II', _pa: ['C', 'B'] },
      _m3: { _ack: false },
      foo: 'bar',
      bar: 'foobar',
      qux: 'qux'
    };

    var EI = {
      _id : { _id: idFoo, _v: 'E', _pe: 'I', _pa: ['C', 'B'] },
      _m3: { _ack: false },
      foo: 'bar',
      bar: 'foobar',
      qux: 'qux',
      some: 'secret'
    };

    var FII = {
      _id : { _id: idFoo, _v: 'F', _pe: 'II', _pa: ['D', 'E'] },
      _m3: { _ack: false },
      foo: 'bar',
      bar: 'foobar',
      qux: 'quz'
    };

    var FI = {
      _id : { _id: idFoo, _v: 'F', _pe: 'I', _pa: ['D', 'E'] },
      _m3: { _ack: false },
      foo: 'bar',
      bar: 'foobar',
      qux: 'quz',
      some: 'secret'
    };

    // create the following structure:
    //    (plus CI) CII - EII (plus EI)
    //             /   \ /   \
    //           AII    X    FII (plus FI)
    //             \   / \   /          
    //              BII - DII (plus DI)
    //
    // AI <-- BI

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([AI, BI, AII, BII, CI, CII, DII, DI, EII, EI, FII, FI], {w: 1}, done);
    });

    it('AI and AII = ff to AI, ff to AII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(AI, AII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
          _m3: { _ack: true },
          foo: 'bar',
          bar: 'baz',
          qux: 'quux',
          some: 'secret'
        }, {
          _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
          _m3: { _ack: false },
          foo: 'bar',
          bar: 'baz',
          qux: 'quux'
        }]);
        done();
      });
    });

    it('BI and CII = merge', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(BI, CII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _co: '_mergeDifferentPerspectives3', _id: idFoo, _v: null, _pe: 'I', _pa: ['B', 'C'], _lo: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'qux',
          some: 'secret'
        }, {
          _id : { _co: '_mergeDifferentPerspectives3', _id: idFoo, _v: null, _pe: 'II', _pa: ['B', 'C'], _lo: true },
          foo: 'bar',
          bar: 'raboof',
          qux: 'qux'
        }]);
        done();
      });
    });

    it('DI and EII = merge based on BI and CII, merge based on BII and CII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(DI, EII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _co: '_mergeDifferentPerspectives3', _id: idFoo, _v: null, _pe: 'I', _pa: ['D', 'E'], _lo: true },
          foo: 'bar',
          bar: 'foobar',
          qux: 'quz',
          some: 'secret'
        }, {
          _id : { _co: '_mergeDifferentPerspectives3', _id: idFoo, _v: null, _pe: 'II', _pa: ['D', 'E'], _lo: true },
          foo: 'bar',
          bar: 'foobar',
          qux: 'quz'
        }]);
        done();
      });
    });

    it('EII and DI = merge based on BII and CII, merge based on BI and CII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(EII, DI, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _co: '_mergeDifferentPerspectives3', _id: idFoo, _v: null, _pe: 'II', _pa: ['E', 'D'], _lo: true },
          foo: 'bar',
          bar: 'foobar',
          qux: 'quz'
        }, {
          _id : { _co: '_mergeDifferentPerspectives3', _id: idFoo, _v: null, _pe: 'I', _pa: ['E', 'D'], _lo: true },
          foo: 'bar',
          bar: 'foobar',
          qux: 'quz',
          some: 'secret'
        }]);
        done();
      });
    });

    it('EI and FII = merge ff to FI, ff to FII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(EI, FII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'F', _pe: 'I', _pa: ['D', 'E'] },
          foo: 'bar',
          bar: 'foobar',
          qux: 'quz',
          some: 'secret'
        }, {
          _id : { _id: idFoo, _v: 'F', _pe: 'II', _pa: ['D', 'E'] },
          _m3: { _ack: false },
          foo: 'bar',
          bar: 'foobar',
          qux: 'quz'
        }]);
        done();
      });
    });

    it('FII and EI = ff to FII, merge ff to FI', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(FII, EI, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'F', _pe: 'II', _pa: ['D', 'E'] },
          _m3: { _ack: false },
          foo: 'bar',
          bar: 'foobar',
          qux: 'quz'
        }, {
          _id : { _id: idFoo, _v: 'F', _pe: 'I', _pa: ['D', 'E'] },
          foo: 'bar',
          bar: 'foobar',
          qux: 'quz',
          some: 'secret'
        }]);
        done();
      });
    });

    /////////// BREAK THE GRAPH BY DELETING CI ON PURPOSE

    it('should remove CI', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.remove(CI, {w: 1}, function(err, deleted) {
        if (err) { throw err; }
        should.equal(deleted, 1);
        done();
      });
    });

    it('DI and EII = error on missing CI', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc._merge(DI, EII, function(err) {
        should.equal(err.message, 'missing at least one perspective when fetching lca C. perspectives: I, II');
        done();
      });
    });

    it('EII and DI = error on missing CI', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc._merge(EII, DI, function(err) {
        should.equal(err.message, 'missing at least one perspective when fetching lca C. perspectives: II, I');
        done();
      });
    });
  });

  describe('different perspectives 4', function() {
    var collectionName = '_mergeDifferentPerspectives4';

    // create DAG with a merge with n-parents

    var AII = {
      _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
      _m3: { _ack: true },
      foo: 'bar',
      bar: 'baz',
      qux: 'quux'
    };

    var AI = {
      _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
      _m3: { _ack: false },
      foo: 'bar',
      bar: 'baz',
      qux: 'quux',
      some: 'secret'
    };

    var BI = {
      _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
      _m3: { _ack: false },
      foo: 'bar',
      bar: 'baz',
      some: 'secret'
    };

    var CI = {
      _id : { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['B'] },
      _m3: { _ack: false },
      foo: 'bar',
      bar: 'raboof',
      c: true,
      some: 'secret'
    };

    var DI = {
      _id : { _id: idFoo, _v: 'D', _pe: 'I', _pa: ['B'] },
      _m3: { _ack: false },
      foo: 'bar',
      bar: 'raboof',
      d: true,
      some: 'secret'
    };

    var EI = {
      _id : { _id: idFoo, _v: 'E', _pe: 'I', _pa: ['B'] },
      _m3: { _ack: false },
      foo: 'bar',
      bar: 'raboof',
      e: true,
      some: 'secret'
    };

    var FI = {
      _id : { _id: idFoo, _v: 'F', _pe: 'I', _pa: ['C', 'D', 'E'] },
      _m3: { _ack: false },
      foo: 'bar',
      bar: 'raboof',
      c: true,
      d: true,
      e: true,
      some: 'secret'
    };

    // create the following structure:
    //              CI <----
    //             /        \
    //   AI <-- BI <- EI <-- FI
    //             \        /
    //              DI <----
    //
    // AII
    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([AII, AI, BI, CI, DI, EI, FI], {w: 1}, done);
    });

    it('AII and FI = merged ff to FII, ff to FI', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(AII, FI, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'F', _pe: 'II', _pa: ['C', 'D', 'E'] },
          foo: 'bar',
          bar: 'raboof',
          c: true,
          d: true,
          e: true
        }, {
          _id : { _id: idFoo, _v: 'F', _pe: 'I', _pa: ['C', 'D', 'E'] },
          _m3: { _ack: false },
          foo: 'bar',
          bar: 'raboof',
          c: true,
          d: true,
          e: true,
          some: 'secret'
        }]);
        done();
      });
    });

    it('AII and CI = merged ff to CII, ff to CI', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(AII, CI, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'C', _pe: 'II', _pa: ['B'] },
          foo: 'bar',
          bar: 'raboof',
          c: true
        }, {
          _id : { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['B'] },
          _m3: { _ack: false },
          foo: 'bar',
          bar: 'raboof',
          c: true,
          some: 'secret'
        }]);
        done();
      });
    });

    it('AI and AII = ff to AI, ff to AII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(AI, AII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
          _m3: { _ack: false },
          foo: 'bar',
          bar: 'baz',
          qux: 'quux',
          some: 'secret'
        }, {
          _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
          _m3: { _ack: true },
          foo: 'bar',
          bar: 'baz',
          qux: 'quux'
        }]);
        done();
      });
    });
  });

  describe('different perspectives 5', function() {
    var collectionName = '_mergeDifferentPerspectives5';

    // create DAG with imported criss-cross merge with three parents

    var AI = {
      _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
      _m3: { _ack: false },
      a: true,
      some: 'secret'
    };

    var BI = {
      _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      some: 'secret'
    };

    var CI = {
      _id : { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['B'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      c: true,
      some: 'secret'
    };

    var DI = {
      _id : { _id: idFoo, _v: 'D', _pe: 'I', _pa: ['B'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      d: true,
      some: 'secret'
    };

    var EI = {
      _id : { _id: idFoo, _v: 'E', _pe: 'I', _pa: ['B'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      e: true,
      some: 'secret'
    };

    var FI = { // change e
      _id : { _id: idFoo, _v: 'F', _pe: 'I', _pa: ['C', 'D', 'E'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      c: true,
      d: true,
      e: 'foo',
      f: true,
      some: 'secret'
    };

    var GI = { // delete d
      _id : { _id: idFoo, _v: 'G', _pe: 'I', _pa: ['C', 'D', 'E'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      c: true,
      e: true,
      g: true,
      some: 'secret'
    };

    var FIc = { // delete e, change d, conflict with Gc
      _id : { _id: idFoo, _v: 'Fc', _pe: 'I', _pa: ['C', 'D', 'E'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      c: true,
      d: 'foo',
      fc: true,
      some: 'secret'
    };

    var GIc = { // delete d, change e, conflict with Fc
      _id : { _id: idFoo, _v: 'Gc', _pe: 'I', _pa: ['C', 'D', 'E'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      c: true,
      e: 'foo',
      gc: true,
      some: 'secret'
    };

    var HI = {
      _id : { _id: idFoo, _v: 'H', _pe: 'I', _pa: ['F', 'G' ] },
      _m3: { _ack: false },
      a: true,
      b: true,
      c: true,
      d: true,
      e: true,
      f: true,
      g: true,
      h: true,
      some: 'secret'
    };

    var AII = {
      _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
      _m3: { _ack: true },
      a: true,
    };

    var BII = {
      _id : { _id: idFoo, _v: 'B', _pe: 'II', _pa: ['A'] },
      _m3: { _ack: false },
      a: true,
      b: true,
    };

    var CII = {
      _id : { _id: idFoo, _v: 'C', _pe: 'II', _pa: ['B'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      c: true,
    };

    var DII = {
      _id : { _id: idFoo, _v: 'D', _pe: 'II', _pa: ['B'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      d: true,
    };

    var EII = {
      _id : { _id: idFoo, _v: 'E', _pe: 'II', _pa: ['B'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      e: true,
    };

    var FII = { // change e
      _id : { _id: idFoo, _v: 'F', _pe: 'II', _pa: ['C', 'D', 'E'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      c: true,
      d: true,
      e: 'foo',
      f: true,
    };

    var GII = { // delete d
      _id : { _id: idFoo, _v: 'G', _pe: 'II', _pa: ['C', 'D', 'E'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      c: true,
      e: true,
      g: true,
    };

    // create the following structure:
    //                 CI <----- FI (and FIc)
    //                / \  /    /  \
    //               /   \/    /    \
    //              /    /\   /      \
    //             /    /  \ /        \
    //   AI <--- BI <- DI   X         HI
    //             \    \  / \        /
    //              \    \/   \      /
    //               \   /\    \    /
    //                \ /  \    \  /
    //                 EI <----- GI (and GIc)
    //
    //                  CII <------ FII
    //                  /  \  /    /
    //                 /    \/    /
    //                /     /\   /
    //               /     /  \ /
    //   AII <--- BII <- DII   X
    //               \     \  / \
    //                \     \/   \
    //                 \    /\    \
    //                  \  /  \    \
    //                  EII <------ GII
    //
    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([AI, BI, CI, DI, EI, FI, GI, FIc, GIc, HI, AII, BII, CII, DII, EII, FII, GII ], {w: 1}, done);
    });

    it('GI and FI = merge', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(GI, FI, function(err, merged) {
        if (err) { throw err; }
        should.equal(merged.length, 1);
        should.deepEqual(merged, [{
          _id : { _co: '_mergeDifferentPerspectives5', _id: idFoo, _v: null, _pe: 'I', _pa: ['G', 'F'], _lo: true },
          a: true,
          b: true,
          c: true,
          e: 'foo',
          f: true,
          g: true,
          some: 'secret'
        }]);
        done();
      });
    });

    it('GII and FI = merge', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(GII, FI, function(err, merged) {
        if (err) { throw err; }
        should.equal(merged.length, 2);
        should.deepEqual(merged, [{
          _id : { _co: '_mergeDifferentPerspectives5', _id: idFoo, _v: null, _pe: 'II', _pa: ['G', 'F'], _lo: true },
          a: true,
          b: true,
          c: true,
          e: 'foo',
          f: true,
          g: true,
        }, {
          _id : { _co: '_mergeDifferentPerspectives5', _id: idFoo, _v: null, _pe: 'I', _pa: ['G', 'F'], _lo: true },
          a: true,
          b: true,
          c: true,
          e: 'foo',
          f: true,
          g: true,
          some: 'secret'
        }]);
        done();
      });
    });

    it('AII and HI = merged ff to HII, ff to HI', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(AII, HI, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'H', _pe: 'II', _pa: ['F', 'G'] },
          a: true,
          b: true,
          c: true,
          d: true,
          e: true,
          f: true,
          g: true,
          h: true
        }, {
          _id : { _id: idFoo, _v: 'H', _pe: 'I', _pa: ['F', 'G'] },
          _m3: { _ack: false },
          a: true,
          b: true,
          c: true,
          d: true,
          e: true,
          f: true,
          g: true,
          h: true,
          some: 'secret'
        }]);
        done();
      });
    });

    it('GIc and FIc = conflict', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc._merge(GIc, FIc, function(err, merged) {
        should.equal(err.message, 'merge conflict');
        should.deepEqual(merged, ['d', 'e']);
        done();
      });
    });

    describe('recursive lca order', function() {
      // following with both I and II
      //          C <------- F
      //         /  \  /    /
      //        /    \/    /
      //       /     /\   /
      //      /     /  \ /
      //   B <---- D    X
      //      \     \  / \
      //       \     \/   \
      //        \    /\    \
      //         \  /  \    \
      //          E <------- G
      //
      it('save DAG mixing perspectives', function(done) {
        var vc = new VersionedCollection(db, collectionName+'RecursiveLcaOrder');
        vc._snapshotCollection.insert([BI, BII, CII, DII, CI, DI, EI, FI, GI, EII, FII, GII ], {w: 1}, done);
      });

      it('GII and FI = merge', function(done) {
        var vc = new VersionedCollection(db, collectionName+'RecursiveLcaOrder');
        vc._merge(GII, FI, function(err, merged) {
          if (err) { throw err; }
          should.equal(merged.length, 2);
          should.deepEqual(merged, [{
            _id : { _co: '_mergeDifferentPerspectives5RecursiveLcaOrder', _id: idFoo, _v: null, _pe: 'II', _pa: ['G', 'F'], _lo: true },
            a: true,
            b: true,
            c: true,
            e: 'foo',
            f: true,
            g: true,
          }, {
            _id : { _co: '_mergeDifferentPerspectives5RecursiveLcaOrder', _id: idFoo, _v: null, _pe: 'I', _pa: ['G', 'F'], _lo: true },
            a: true,
            b: true,
            c: true,
            e: 'foo',
            f: true,
            g: true,
            some: 'secret'
          }]);
          done();
        });
      });

      it('FII and GI = merge', function(done) {
        var vc = new VersionedCollection(db, collectionName+'RecursiveLcaOrder');
        vc._merge(FI, GII, function(err, merged) {
          if (err) { throw err; }
          should.equal(merged.length, 2);
          should.deepEqual(merged, [{
            _id : { _co: '_mergeDifferentPerspectives5RecursiveLcaOrder', _id: idFoo, _v: null, _pe: 'I', _pa: ['F', 'G'], _lo: true },
            a: true,
            b: true,
            c: true,
            e: 'foo',
            f: true,
            g: true,
            some: 'secret'
          }, {
            _id : { _co: '_mergeDifferentPerspectives5RecursiveLcaOrder', _id: idFoo, _v: null, _pe: 'II', _pa: ['F', 'G'], _lo: true },
            a: true,
            b: true,
            c: true,
            e: 'foo',
            f: true,
            g: true
          }]);
          done();
        });
      });
    });
  });

  describe('double criss-cross three parents', function() {
    // create 2 DAGs with a double criss-cross merge with three parents

    var AI = {
      _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
      _m3: { _ack: false },
      a: true,
      some: 'secret'
    };

    var BI = {
      _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      some: 'secret'
    };

    var CI = {
      _id : { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['B'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      c: true,
      some: 'secret'
    };

    var DI = {
      _id : { _id: idFoo, _v: 'D', _pe: 'I', _pa: ['B'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      d: true,
      some: 'secret'
    };

    var EI = {
      _id : { _id: idFoo, _v: 'E', _pe: 'I', _pa: ['B'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      e: true,
      some: 'secret'
    };

    var FI = { // change c
      _id : { _id: idFoo, _v: 'F', _pe: 'I', _pa: ['C', 'D', 'E'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      c: 'foo',
      d: true,
      e: true,
      f: true,
      some: 'secret'
    };

    var GI = { // change d, delete b
      _id : { _id: idFoo, _v: 'G', _pe: 'I', _pa: ['C', 'D', 'E'] },
      _m3: { _ack: false },
      a: true,
      c: true,
      d: 'bar',
      e: true,
      g: true,
      some: 'secret'
    };

    var HI = { // delete e, delete a
      _id : { _id: idFoo, _v: 'H', _pe: 'I', _pa: ['C', 'D', 'E'] },
      _m3: { _ack: false },
      b: true,
      c: true,
      d: true,
      h: true,
      some: 'secret'
    };

    var II = { // add e again, change d, change h
      _id : { _id: idFoo, _v: 'I', _pe: 'I', _pa: ['F', 'G', 'H'] },
      _m3: { _ack: false },
      c: 'foo',
      d: 'baz',
      e: 'II',
      f: true,
      g: true,
      h: 'II',
      i: true,
      some: 'secret'
    };

    var JI = { // change f, change g
      _id : { _id: idFoo, _v: 'J', _pe: 'I', _pa: ['F', 'G', 'H'] },
      _m3: { _ack: false },
      c: 'foo',
      d: 'bar',
      f: 'JI',
      g: 'JI',
      h: true,
      j: true,
      some: 'secret'
    };

    var FIc = { // delete e, change d, conflict with GIc
      _id : { _id: idFoo, _v: 'Fc', _pe: 'I', _pa: ['C', 'D', 'E'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      c: true,
      d: 'FIc',
      fc: true,
      some: 'secret'
    };

    var GIc = { // delete d, change e, conflict with FIc
      _id : { _id: idFoo, _v: 'Gc', _pe: 'I', _pa: ['C', 'D', 'E'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      c: true,
      e: 'GIc',
      gc: true,
      some: 'secret'
    };

    var IIc = { // conflict with JIc: change f
      _id : { _id: idFoo, _v: 'Ic', _pe: 'I', _pa: ['F', 'G', 'H'] },
      _m3: { _ack: false },
      c: 'foo',
      d: 'bar',
      e: true,
      f: 'IIc',
      g: true,
      h: true,
      ic: true,
      some: 'secret'
    };

    var JIc = { // conflict with IIc: change f, change h
      _id : { _id: idFoo, _v: 'Jc', _pe: 'I', _pa: ['F', 'G', 'H'] },
      _m3: { _ack: false },
      c: 'foo',
      d: 'bar',
      f: 'JIc',
      g: true,
      h: 'JIc',
      jc: true,
      some: 'secret'
    };

    var AII = {
      _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
      _m3: { _ack: false },
      a: true,
    };

    var BII = {
      _id : { _id: idFoo, _v: 'B', _pe: 'II', _pa: ['A'] },
      _m3: { _ack: false },
      a: true,
      b: true,
    };

    var CII = {
      _id : { _id: idFoo, _v: 'C', _pe: 'II', _pa: ['B'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      c: true,
    };

    var DII = {
      _id : { _id: idFoo, _v: 'D', _pe: 'II', _pa: ['B'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      d: true,
    };

    var EII = {
      _id : { _id: idFoo, _v: 'E', _pe: 'II', _pa: ['B'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      e: true,
    };

    var FII = { // change c
      _id : { _id: idFoo, _v: 'F', _pe: 'II', _pa: ['C', 'D', 'E'] },
      _m3: { _ack: false },
      a: true,
      b: true,
      c: 'foo',
      d: true,
      e: true,
      f: true,
    };

    var GII = { // change d, delete b
      _id : { _id: idFoo, _v: 'G', _pe: 'II', _pa: ['C', 'D', 'E'] },
      _m3: { _ack: false },
      a: true,
      c: true,
      d: 'bar',
      e: true,
      g: true,
    };

    var HII = { // delete e, delete a
      _id : { _id: idFoo, _v: 'H', _pe: 'II', _pa: ['C', 'D', 'E'] },
      _m3: { _ack: false },
      b: true,
      c: true,
      d: true,
      h: true,
    };

    var III = { // add e again, change d, change h
      _id : { _id: idFoo, _v: 'I', _pe: 'II', _pa: ['F', 'G', 'H'] },
      _m3: { _ack: false },
      c: 'foo',
      d: 'baz',
      e: 'III',
      f: true,
      g: true,
      h: 'III',
      i: true,
    };

    var JII = { // change f, change g
      _id : { _id: idFoo, _v: 'J', _pe: 'II', _pa: ['F', 'G', 'H'] },
      _m3: { _ack: false },
      c: 'foo',
      d: 'bar',
      f: 'JII',
      g: 'JII',
      h: true,
      j: true,
    };

    // create the following structure:
    //                           (and FIc)
    //                 CI <----- FI <----- II (and IIc)
    //                /\ \ /    / \   /   /
    //               /  \ X    /   \ /   /
    //              /    X \  /     X   /
    //             /    / \ \/     / \ /
    //   AI <--- BI <- DI <----- GI   X
    //             \    \ / \/     \ / \
    //              \    X / \      X   \
    //               \  / X   \    / \   \
    //                \/ / \   \  /   \   \
    //                 EI <----- HI <----- JI (and JIc)
    //                           (and HIc)
    //
    //                           (and F2c)
    //                 C2 <----- F2 <----- I2 (and I2c)
    //                /\ \ /    / \   /   /
    //               /  \ X    /   \ /   /
    //              /    X \  /     X   /
    //             /    / \ \/     / \ /
    //   A2 <--- B2 <- D2 <----- G2   X
    //             \    \ / \/     \ / \
    //              \    X / \      X   \
    //               \  / X   \    / \   \
    //                \/ / \   \  /   \   \
    //                 E2 <----- H2 <----- J2 (and J2c)
    //                           (and H2c)
    //
    describe('one perspective', function() {
      var collectionName = '_mergeDoubleCrissCrossThreeMergesOnePerspective';

      it('should save DAG I', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._snapshotCollection.insert([AI, BI, CI, DI, EI, FI, GI, FIc, GIc, HI, II, JI, IIc, JIc], {w: 1}, done);
      });

      it('FI and GI = merge', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._merge(FI, GI, function(err, merged) {
          if (err) { throw err; }
          should.equal(merged.length, 1);
          should.deepEqual(merged, [{
            _id : { _co: '_mergeDoubleCrissCrossThreeMergesOnePerspective', _id: idFoo, _v: null, _pe: 'I', _pa: ['F', 'G'], _lo: true },
            a: true,
            c: 'foo',
            d: 'bar',
            e: true,
            f: true,
            g: true,
            some: 'secret'
          }]);
          done();
        });
      });

      it('II and JI = merge', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._merge(II, JI, function(err, merged) {
          if (err) { throw err; }
          should.equal(merged.length, 1);
          should.deepEqual(merged, [{
            _id : { _co: '_mergeDoubleCrissCrossThreeMergesOnePerspective', _id: idFoo, _v: null, _pe: 'I', _pa: ['I', 'J'], _lo: true },
            c: 'foo',
            d: 'baz',
            e: 'II',
            f: 'JI',
            g: 'JI',
            h: 'II',
            i: true,
            j: true,
            some: 'secret'
          }]);
          done();
        });
      });

      it('IIc and JIc = conflict', function(done) {
        var vc = new VersionedCollection(db, collectionName, { hide: true });
        vc._merge(IIc, JIc, function(err, merged) {
          should.equal(err.message, 'merge conflict');
          should.equal(merged.length, 1);
          should.deepEqual(merged, ['f']);
          done();
        });
      });
    });

    describe('two perspectives', function() {
      var collectionName = '_mergeDoubleCrissCrossThreeMergesTwoPerspectives';

      it('should save DAG topologically sorted per perspective only', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._snapshotCollection.insert([AII, BII, AI, BI, CI, CII, DII, EII, FII, GII, DI, EI, FI, GI, FIc, GIc, HI, II, HII, III, JII, JI, IIc, JIc], {w: 1}, done);
      });

      it('FII and GI = merge', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._merge(FII, GI, function(err, merged) {
          if (err) { throw err; }
          should.equal(merged.length, 2);
          should.deepEqual(merged, [{
            _id : { _co: '_mergeDoubleCrissCrossThreeMergesTwoPerspectives', _id: idFoo, _v: null, _pe: 'II', _pa: ['F', 'G'], _lo: true },
            a: true,
            c: 'foo',
            d: 'bar',
            e: true,
            f: true,
            g: true,
          }, {
            _id : { _co: '_mergeDoubleCrissCrossThreeMergesTwoPerspectives', _id: idFoo, _v: null, _pe: 'I', _pa: ['F', 'G'], _lo: true },
            a: true,
            c: 'foo',
            d: 'bar',
            e: true,
            f: true,
            g: true,
            some: 'secret'
          }]);
          done();
        });
      });

      it('HI and III = merged ff to II, ff to III', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._merge(HI, III, function(err, merged) {
          if (err) { throw err; }
          should.equal(merged.length, 2);
          should.deepEqual(merged, [{
            _id : { _id: idFoo, _v: 'I', _pe: 'I', _pa: ['F', 'G', 'H'] },
            c: 'foo',
            d: 'baz',
            e: 'III',
            f: true,
            g: true,
            h: 'III',
            i: true,
            some: 'secret'
          }, {
            _id : { _id: idFoo, _v: 'I', _pe: 'II', _pa: ['F', 'G', 'H'] },
            _m3: { _ack: false },
            c: 'foo',
            d: 'baz',
            e: 'III',
            f: true,
            g: true,
            h: 'III',
            i: true,
          }]);
          done();
        });
      });

      it('II and III = ff to II, ff to III', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._merge(II, III, function(err, merged) {
          if (err) { throw err; }
          should.equal(merged.length, 2);
          should.deepEqual(merged, [{
            _id : { _id: idFoo, _v: 'I', _pe: 'I', _pa: ['F', 'G', 'H'] },
            _m3: { _ack: false },
            c: 'foo',
            d: 'baz',
            e: 'II',
            f: true,
            g: true,
            h: 'II',
            i: true,
            some: 'secret'
          }, {
            _id : { _id: idFoo, _v: 'I', _pe: 'II', _pa: ['F', 'G', 'H'] },
            _m3: { _ack: false },
            c: 'foo',
            d: 'baz',
            e: 'III',
            f: true,
            g: true,
            h: 'III',
            i: true,
          }]);
          done();
        });
      });

      it('JIc and III = conflict', function(done) {
        var vc = new VersionedCollection(db, collectionName, { hide: true });
        vc._merge(JIc, III, function(err, merged) {
          should.equal(err.message, 'merge conflict');
          should.equal(merged.length, 2);
          should.deepEqual(merged, [['h'], ['h']]);
          done();
        });
      });

      it('JI and III = merge', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._merge(JI, III, function(err, merged) {
          if (err) { throw err; }
          should.equal(merged.length, 2);
          should.deepEqual(merged, [{
            _id : { _co: '_mergeDoubleCrissCrossThreeMergesTwoPerspectives', _id: idFoo, _v: null, _pe: 'I', _pa: ['J', 'I'], _lo: true },
            c: 'foo',
            d: 'baz',
            e: 'III',
            f: 'JI',
            g: 'JI',
            h: 'III',
            j: true,
            i: true,
            some: 'secret'
          }, {
            _id : { _co: '_mergeDoubleCrissCrossThreeMergesTwoPerspectives', _id: idFoo, _v: null, _pe: 'II', _pa: ['J', 'I'], _lo: true },
            c: 'foo',
            d: 'baz',
            e: 'III',
            f: 'JI',
            g: 'JI',
            h: 'III',
            j: true,
            i: true,
          }]);
          done();
        });
      });
    });
  });

  describe('merge with patches', function() {
    var collectionName = '_mergeMergeWithPatches';

    // create DAG where all exported items are imported again

    var AI = {
      _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
      _m3: { _ack: true },
      baz : 'qux',
      bar: 'raboof',
      some: 'secret'
    };

    var BI = {
      _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
      _m3: { _ack: true },
      bar: 'raboof',
      some: 'secret'
    };

    var EI = {
      _id : { _id: idFoo, _v: 'E', _pe: 'I', _pa: ['B'] },
      _m3: { _ack: true },
      bar: 'foo',
      some: 'secret'
    };

    var AII = {
      _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
      _m3: { _ack: false },
      baz : 'qux',
      bar: 'raboof'
    };

    var BII = {
      _id : { _id: idFoo, _v: 'B', _pe: 'II', _pa: ['A'] },
      _m3: { _ack: false },
      bar: 'raboof'
    };

    var CII = {
      _id : { _id: idFoo, _v: 'C', _pe: 'II', _pa: ['A'] },
      _m3: { _ack: false },
      baz : 'qux',
      bar: 'raboof',
      foo: 'bar'
    };

    var DII = {
      _id : { _id: idFoo, _v: 'D', _pe: 'II', _pa: ['C', 'B'] },
      _m3: { _ack: false },
      bar: 'raboof',
      foo: 'bar',
      d: true
    };

    // resulting DAG in system I:
    //                   AII <-- BII
    //                     \       \
    //                     CII <-- DII
    // AI <-- BI <-- EI

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([AI, AII, CII, BI, BII, EI, DII], {w: 1}, done);
    });

    it('BI and DII = merged ff to DI, ff to DII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(BI, DII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _id: idFoo, _v: 'D', _pe: 'I', _pa: ['C', 'B'] },
          bar: 'raboof',
          some: 'secret',
          foo: 'bar',
          d: true
        }, {
          _id : { _id: idFoo, _v: 'D', _pe: 'II', _pa: ['C', 'B'] },
          _m3: { _ack: false },
          bar: 'raboof',
          foo: 'bar',
          d: true
        }]);
        done();
      });
    });

    it('EI and DII = merges based on BI, BII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(EI, DII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _co: '_mergeMergeWithPatches', _id: idFoo, _v: null, _pe: 'I', _pa: ['E', 'D'], _lo: true },
          bar: 'foo',
          some: 'secret',
          foo: 'bar',
          d: true
        }, {
          _id : { _co: '_mergeMergeWithPatches', _id: idFoo, _v: null, _pe: 'II', _pa: ['E', 'D'], _lo: true },
          bar: 'foo',
          foo: 'bar',
          d: true
        }]);
        done();
      });
    });
  });

  describe('merge with resolved conflict', function() {
    var collectionName = '_mergeMergeWithResolvedConflict';

    // create DAG where all exported items are imported again

    var AI = {
      _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
      _m3: { _ack: true },
      baz : 'qux',
      bar: 'raboof',
      some: 'secret'
    };

    var BI = { // add c: 'foo'
      _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
      _m3: { _ack: true },
      bar: 'raboof',
      c: 'foo',
      some: 'secret'
    };

    var EI = {
      _id : { _id: idFoo, _v: 'E', _pe: 'I', _pa: ['B'] },
      _m3: { _ack: true },
      bar: 'foo',
      c: 'foo',
      e: true,
      some: 'secret'
    };

    var FI = {
      _id : { _id: idFoo, _v: 'F', _pe: 'I', _pa: ['E'] },
      _m3: { _ack: true },
      bar: 'foo',
      c: 'baz',
      e: true,
      f: true,
      some: 'secret'
    };

    var GI = { // conflict with DII
      _id : { _id: idFoo, _v: 'G', _pe: 'I', _pa: ['F'] },
      _m3: { _ack: true },
      bar: 'foo',
      c: 'raboof',
      e: true,
      f: true,
      g: true,
      some: 'secret'
    };

    var AII = {
      _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
      _m3: { _ack: false },
      baz : 'qux',
      bar: 'raboof'
    };

    var BII = { // add c: 'foo'
      _id : { _id: idFoo, _v: 'B', _pe: 'II', _pa: ['A'] },
      _m3: { _ack: false },
      bar: 'raboof',
      c: 'foo'
    };

    var CII = { // add c: 'bar'
      _id : { _id: idFoo, _v: 'C', _pe: 'II', _pa: ['A'] },
      _m3: { _ack: false },
      baz : 'qux',
      bar: 'raboof',
      foo: 'bar',
      c: 'bar'
    };

    var DII = { // resolve conflict: c: 'baz'
      _id : { _id: idFoo, _v: 'D', _pe: 'II', _pa: ['C', 'B'] },
      _m3: { _ack: false },
      bar: 'raboof',
      foo: 'bar',
      c: 'baz',
      d: true
    };

    // resulting DAG in system I:
    //                   AII <-- BII
    //                     \       \
    //                     CII <-- DII
    //
    // AI <-- BI <-- EI <-- FI <-- GI

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._snapshotCollection.insert([AI, AII, CII, BI, BII, EI, FI, DII, GI], {w: 1}, done);
    });

    it('BI and CII = conflict', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc._merge(BI, CII, function(err, merged) {
        should.equal(err.message, 'merge conflict');
        should.deepEqual(merged, [['c'], ['c']]);
        done();
      });
    });

    it('BI and DII = merged ff to DI, ff to DII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(BI, DII, function(err, merged) {
        if (err) { throw err; }
        should.equal(merged.length, 2);
        should.deepEqual(merged[0], {
          _id : { _id: idFoo, _v: 'D', _pe: 'I', _pa: ['C', 'B'] },
          bar: 'raboof',
          some: 'secret',
          foo: 'bar',
          c: 'baz',
          d: true
        });
        should.deepEqual(merged[1], {
          _id : { _id: idFoo, _v: 'D', _pe: 'II', _pa: ['C', 'B'] },
          _m3: { _ack: false },
          bar: 'raboof',
          foo: 'bar',
          c: 'baz',
          d: true
        });
        done();
      });
    });

    it('EI and DII = merges based on BI, BII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(EI, DII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _co: '_mergeMergeWithResolvedConflict', _id: idFoo, _v: null, _pe: 'I', _pa: ['E', 'D'], _lo: true },
          bar: 'foo',
          some: 'secret',
          foo: 'bar',
          c: 'baz',
          e: true,
          d: true
        }, {
          _id : { _co: '_mergeMergeWithResolvedConflict', _id: idFoo, _v: null, _pe: 'II', _pa: ['E', 'D'], _lo: true },
          bar: 'foo',
          foo: 'bar',
          c: 'baz',
          d: true,
          e: true
        }]);
        done();
      });
    });

    it('FI and DII = merges based on BI, BII', function(done) {
      var vc = new VersionedCollection(db, collectionName);
      vc._merge(FI, DII, function(err, merged) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          _id : { _co: '_mergeMergeWithResolvedConflict', _id: idFoo, _v: null, _pe: 'I', _pa: ['F', 'D'], _lo: true },
          bar: 'foo',
          some: 'secret',
          foo: 'bar',
          c: 'baz',
          e: true,
          d: true,
          f: true
        }, {
          _id : { _co: '_mergeMergeWithResolvedConflict', _id: idFoo, _v: null, _pe: 'II', _pa: ['F', 'D'], _lo: true },
          bar: 'foo',
          foo: 'bar',
          c: 'baz',
          d: true,
          e: true,
          f: true
        }]);
        done();
      });
    });

    it('GI and DII = conflict', function(done) {
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc._merge(GI, DII, function(err, merged) {
        should.equal(err.message, 'merge conflict');
        should.deepEqual(merged, [['c'], ['c']]);
        done();
      });
    });
  });

  describe('criss-cross four parents', function() {
    ////// _pe I
    var AI = {
      _id : { _id: idFoo, _v: 'A', _pe: 'I', _pa: [] },
      a: true,
      some: 'secret'
    };

    var BI = {
      _id : { _id: idFoo, _v: 'B', _pe: 'I', _pa: ['A'] },
      a: true,
      b: true,
      some: 'secret'
    };

    var CI = {
      _id : { _id: idFoo, _v: 'C', _pe: 'I', _pa: ['A'] },
      a: true,
      c: true,
      some: 'secret'
    };

    var DI = {
      _id : { _id: idFoo, _v: 'D', _pe: 'I', _pa: ['A'] },
      a: true,
      d: true,
      some: 'secret'
    };

    var EI = {
      _id : { _id: idFoo, _v: 'E', _pe: 'I', _pa: ['A', 'C'] },
      a: true,
      c: 'foo',
      e: true,
      some: 'secret'
    };

    var FI = {
      _id : { _id: idFoo, _v: 'F', _pe: 'I', _pa: ['B', 'C', 'D', 'E'] },
      a: true,
      b: true,
      c: 'foo',
      d: true,
      e: true,
      f: true,
      some: 'secret'
    };

    var GI = {
      _id : { _id: idFoo, _v: 'G', _pe: 'I', _pa: ['B', 'C', 'D', 'E'] },
      a: true,
      b: true,
      c: 'foo',
      d: true,
      e: true,
      g: true,
      some: 'secret'
    };

    ////// _pe II
    var AII = {
      _id : { _id: idFoo, _v: 'A', _pe: 'II', _pa: [] },
      a: true
    };

    var BII = {
      _id : { _id: idFoo, _v: 'B', _pe: 'II', _pa: ['A'] },
      a: true,
      b: true
    };

    var CII = {
      _id : { _id: idFoo, _v: 'C', _pe: 'II', _pa: ['A'] },
      a: true,
      c: true
    };

    var DII = {
      _id : { _id: idFoo, _v: 'D', _pe: 'II', _pa: ['A'] },
      a: true,
      d: true
    };

    var EII = {
      _id : { _id: idFoo, _v: 'E', _pe: 'II', _pa: ['A', 'C'] },
      a: true,
      c: 'foo',
      e: true
    };

    var FII = {
      _id : { _id: idFoo, _v: 'F', _pe: 'II', _pa: ['B', 'C', 'D', 'E'] },
      a: true,
      b: true,
      c: 'foo',
      d: true,
      e: true,
      f: true
    };

    var GII = {
      _id : { _id: idFoo, _v: 'G', _pe: 'II', _pa: ['B', 'C', 'D', 'E'] },
      a: true,
      b: true,
      c: 'foo',
      d: true,
      e: true,
      g: true
    };

    // create the following structure, for _pe I and II:
    //    --- B <---- F (_pa: B, C, D, E)
    //   /     \/ / /
    //  /      /\/ /
    // A <--- C-/\/
    //  \      /\/\
    //   \___ D-/\ \
    //    \    /  \ \
    //     \- E <---- G (_pa: B, C, D, E)
    //      (_pa: A, C)
    describe('one perspective', function() {
      var collectionName = '_mergeCrissCrossFourParentsOnePerspective';

      it('should save DAG I', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._snapshotCollection.insert([AI, BI, CI, DI, EI, FI, GI], {w: 1}, done);
      });

      it('FI and GI = merge', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._merge(FI, GI, function(err, merged) {
          if (err) { throw err; }
          should.equal(merged.length, 1);
          should.deepEqual(merged, [{
            _id : { _co: '_mergeCrissCrossFourParentsOnePerspective', _id: idFoo, _v: null, _pe: 'I', _pa: ['F', 'G'], _lo: true },
            a: true,
            b: true,
            c: 'foo',
            d: true,
            e: true,
            f: true,
            g: true,
            some: 'secret'
          }]);
          done();
        });
      });
    });

    describe('two perspectives', function() {
      var collectionName = '_mergeCrissCrossFourParentsTwoPerspectives';

      it('should save DAG I and II', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._snapshotCollection.insert([AI, BI, CI, AII, BII, DI, CII, EI, DII, EII, FII, GII, FI, GI], {w: 1}, done);
      });

      it('FI and GII = merge', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._merge(FI, GII, function(err, merged) {
          if (err) { throw err; }
          should.equal(merged.length, 2);
          should.deepEqual(merged, [{
            _id : { _co: '_mergeCrissCrossFourParentsTwoPerspectives', _id: idFoo, _v: null, _pe: 'I', _pa: ['F', 'G'], _lo: true },
            a: true,
            b: true,
            c: 'foo',
            d: true,
            e: true,
            f: true,
            g: true,
            some: 'secret'
          }, {
            _id : { _co: '_mergeCrissCrossFourParentsTwoPerspectives', _id: idFoo, _v: null, _pe: 'II', _pa: ['F', 'G'], _lo: true },
            a: true,
            b: true,
            c: 'foo',
            d: true,
            e: true,
            f: true,
            g: true
          }]);
          done();
        });
      });

      it('FII and GI = merge', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._merge(FII, GI, function(err, merged) {
          if (err) { throw err; }
          should.equal(merged.length, 2);
          should.deepEqual(merged, [{
            _id : { _co: '_mergeCrissCrossFourParentsTwoPerspectives', _id: idFoo, _v: null, _pe: 'II', _pa: ['F', 'G'], _lo: true },
            a: true,
            b: true,
            c: 'foo',
            d: true,
            e: true,
            f: true,
            g: true,
          }, {
            _id : { _co: '_mergeCrissCrossFourParentsTwoPerspectives', _id: idFoo, _v: null, _pe: 'I', _pa: ['F', 'G'], _lo: true },
            a: true,
            b: true,
            c: 'foo',
            d: true,
            e: true,
            f: true,
            g: true,
            some: 'secret'
          }]);
          done();
        });
      });

      it('GI and FII = merge', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._merge(GI, FII, function(err, merged) {
          if (err) { throw err; }
          should.equal(merged.length, 2);
          should.deepEqual(merged, [{
            _id : { _co: '_mergeCrissCrossFourParentsTwoPerspectives', _id: idFoo, _v: null, _pe: 'I', _pa: ['G', 'F'], _lo: true },
            a: true,
            b: true,
            c: 'foo',
            d: true,
            e: true,
            f: true,
            g: true,
            some: 'secret'
          }, {
            _id : { _co: '_mergeCrissCrossFourParentsTwoPerspectives', _id: idFoo, _v: null, _pe: 'II', _pa: ['G', 'F'], _lo: true },
            a: true,
            b: true,
            c: 'foo',
            d: true,
            e: true,
            f: true,
            g: true
          }]);
          done();
        });
      });

      it('GII and FI = merge', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._merge(GII, FI, function(err, merged) {
          if (err) { throw err; }
          should.equal(merged.length, 2);
          should.deepEqual(merged, [{
            _id : { _co: '_mergeCrissCrossFourParentsTwoPerspectives', _id: idFoo, _v: null, _pe: 'II', _pa: ['G', 'F'], _lo: true },
            a: true,
            b: true,
            c: 'foo',
            d: true,
            e: true,
            f: true,
            g: true
          }, {
            _id : { _co: '_mergeCrissCrossFourParentsTwoPerspectives', _id: idFoo, _v: null, _pe: 'I', _pa: ['G', 'F'], _lo: true },
            a: true,
            b: true,
            c: 'foo',
            d: true,
            e: true,
            f: true,
            g: true,
            some: 'secret'
          }]);
          done();
        });
      });
    });
  });
});
