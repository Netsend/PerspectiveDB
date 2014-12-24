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

var VersionedCollection = require('../../../lib/versioned_collection');

var db;
var databaseName = 'test_versioned_collection_findlca';
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

describe('VersionedCollection._findLCAs', function() {
  describe('one perspective', function() {
    describe('two merges', function() {
      var collectionName = 'findLCAsTwoMerges';

      var A = { _id : { _id: 'foo', _v: 'A', _pe: 'I', _pa: [] } };
      var B = { _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'] } };
      var C = { _id : { _id: 'foo', _v: 'C', _pe: 'I', _pa: ['B'] } };
      var D = { _id : { _id: 'foo', _v: 'D', _pe: 'I', _pa: ['C'] } };
      var E = { _id : { _id: 'foo', _v: 'E', _pe: 'I', _pa: ['B'] } };
      var F = { _id : { _id: 'foo', _v: 'F', _pe: 'I', _pa: ['E', 'C'] } };
      var G = { _id : { _id: 'foo', _v: 'G', _pe: 'I', _pa: ['F'] } };

      var DAG = [A, B, C, D, E, F, G];

      // create the following structure:
      // A <-- B <-- C <-- D
      //        \     \             
      //         E <-- F <-- G
      it('should save DAG', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._snapshotCollection.insert(DAG, {w: 1}, done);
      });

      it('should require itemX', function(done) {
        var vc = new VersionedCollection(db, collectionName, { hide: true });
        vc._findLCAs(null, null, function(err) {
          should.equal('provide itemX', err.message);
          done();
        });
      });

      it('should require itemY', function(done) {
        var vc = new VersionedCollection(db, collectionName, { hide: true });
        vc._findLCAs({}, null, function(err) {
          should.equal('provide itemY', err.message);
          done();
        });
      });

      it('should require itemX._id', function(done) {
        var vc = new VersionedCollection(db, collectionName, { hide: true });
        vc._findLCAs({}, {}, function(err) {
          should.equal('missing itemX._id', err.message);
          done();
        });
      });

      it('should require itemY._id', function(done) {
        var vc = new VersionedCollection(db, collectionName, { hide: true });
        vc._findLCAs({ _id: {} }, {}, function(err) {
          should.equal('missing itemY._id', err.message);
          done();
        });
      });

      it('should require itemX._id to be an object', function(done) {
        var vc = new VersionedCollection(db, collectionName, { hide: true });
        vc._findLCAs({ _id: 'foo' }, { _id: {} }, function(err) {
          should.equal('itemX._id must be an object', err.message);
          done();
        });
      });

      it('should require itemY._id to be an object', function(done) {
        var vc = new VersionedCollection(db, collectionName, { hide: true });
        vc._findLCAs({ _id: {} }, { _id: 'foo' }, function(err) {
          should.equal('itemY._id must be an object', err.message);
          done();
        });
      });

      it('should require itemX._id._id to equal itemY._id._id', function(done) {
        var vc = new VersionedCollection(db, collectionName, { hide: true });
        vc._findLCAs({ _id: { _id: 1 } }, { _id: { _id: 2 } }, function(err) {
          should.equal('itemX._id._id must equal itemY._id._id', err.message);
          done();
        });
      });

      it('should require itemX._id._pe', function(done) {
        var vc = new VersionedCollection(db, collectionName, { hide: true });
        vc._findLCAs({ _id: { _pe: '' } }, { _id: {} }, function(err) {
          should.equal('missing itemX._id._pe', err.message);
          done();
        });
      });

      it('should require itemY._id._pe', function(done) {
        var vc = new VersionedCollection(db, collectionName, { hide: true });
        vc._findLCAs({ _id: { _pe: 'I' } }, { _id: { _pe: '' } }, function(err) {
          should.equal('missing itemY._id._pe', err.message);
          done();
        });
      });

      it('A and B = A', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(A, B, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [A._id._v]);
          done();
        });
      });

      it('B and B = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(B, B, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('C and D = C', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(C, D, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [C._id._v]);
          done();
        });
      });

      it('D and D = D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(D, D, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [D._id._v]);
          done();
        });
      });

      it('C and E = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(C, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('D and F = C', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(D, F, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [C._id._v]);
          done();
        });
      });

      it('F and G = F', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(F, G, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [F._id._v]);
          done();
        });
      });

      it('F and C = C', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(F, C, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [C._id._v]);
          done();
        });
      });

      it('D and E = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(D, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('E and D = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(E, D, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('G and B = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(G, B, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      var GII = { _id : { _id: 'foo', _v: 'G', _pe: 'II', _pa: ['F'] } };
      var H = { _id : { _id: 'foo', _v: 'H', _pe: 'I', _pa: [] } };

      it('should not find disconnected roots', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        // add not connected nodes
        vc._snapshotCollection.insert([GII, H], { w: 1 }, function(err, inserts) {
          if (err) { throw err; }
          should.equal(inserts.length, 2);
          vc._findLCAs(A, H, function(err, lca) {
            should.equal(err, null);
            should.deepEqual(lca, []);
            done();
          });
        });
      });

      it('should not find disconnected by perspective', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        // add not connected nodes
        vc._findLCAs(GII, C, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, []);
          done();
        });
      });
    });

    describe('two merges one side', function() {
      var collectionName = 'findLCAsTwoMergesOneSide';

      var A = { _id : { _id: 'foo', _v: 'A', _pe: 'I' } };
      var B = { _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'] } };
      var C = { _id : { _id: 'foo', _v: 'C', _pe: 'I', _pa: ['A'] } };
      var D = { _id : { _id: 'foo', _v: 'D', _pe: 'I', _pa: ['B', 'C'] } };
      var E = { _id : { _id: 'foo', _v: 'E', _pe: 'I', _pa: ['C'] } };
      var F = { _id : { _id: 'foo', _v: 'F', _pe: 'I', _pa: ['D', 'E'] } };

      var DAG = [A, B, C, D, E, F];

      // create the following structure:
      // A <-- C <-- E
      //  \     \     \
      //   B <-- D <-- F
      it('should save DAG', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._snapshotCollection.insert(DAG, {w: 1}, done);
      });

      it('D and E = C', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(D, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [C._id._v]);
          done();
        });
      });
    });

    describe('three merges', function() {
      var collectionName = 'findLCAsThreeMerges';

      var A = { _id : { _id: 'foo', _v: 'A', _pe: 'I' } };
      var B = { _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'] } };
      var C = { _id : { _id: 'foo', _v: 'C', _pe: 'I', _pa: ['B', 'E'] } };
      var D = { _id : { _id: 'foo', _v: 'D', _pe: 'I', _pa: ['C'] } };
      var E = { _id : { _id: 'foo', _v: 'E', _pe: 'I', _pa: ['B'] } };
      var F = { _id : { _id: 'foo', _v: 'F', _pe: 'I', _pa: ['E', 'C'] } };
      var G = { _id : { _id: 'foo', _v: 'G', _pe: 'I', _pa: ['F'] } };

      var DAG = [A, B, E, C, D, F, G];

      // create the following structure:
      // A <-- B <-- C <-- D
      //        \  /  \             
      //         E <-- F <-- G
      it('should save DAG', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._snapshotCollection.insert(DAG, {w: 1}, done);
      });

      it('C and E = E', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(C, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [E._id._v]);
          done();
        });
      });

      it('D and F = C', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(D, F, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [C._id._v]);
          done();
        });
      });

      it('F and C = C', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(F, C, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [C._id._v]);
          done();
        });
      });

      it('D and E = E', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(D, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [E._id._v]);
          done();
        });
      });

      it('E and D = E', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(E, D, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [E._id._v]);
          done();
        });
      });

      it('G and B = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(G, B, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });
    });

    describe('criss-cross', function() {
      var collectionName = 'findLCAsCrissCrossMerge';

      var A = { _id : { _id: 'foo', _v: 'A', _pe: 'I', _pa: [] } };
      var B = { _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'] } };
      var C = { _id : { _id: 'foo', _v: 'C', _pe: 'I', _pa: ['B'] } };
      var D = { _id : { _id: 'foo', _v: 'D', _pe: 'I', _pa: ['B'] } };
      var E = { _id : { _id: 'foo', _v: 'E', _pe: 'I', _pa: ['C', 'D'] } };
      var F = { _id : { _id: 'foo', _v: 'F', _pe: 'I', _pa: ['D', 'C'] } };
      var G = { _id : { _id: 'foo', _v: 'G', _pe: 'I', _pa: ['F', 'E'] } };

      // create the following structure:
      //         C <-- E
      //        / \  /   \
      // A <-- B    X     G
      //        \  /  \  /          
      //         D <-- F
      it('should save DAG', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._snapshotCollection.insert([A, B, C, D, E, F, G], {w: 1}, done);
      });

      it('A and B = A', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(A, B, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [A._id._v]);
          done();
        });
      });

      it('B and B = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(B, B, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('C and D = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(C, D, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('D and D = D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(D, D, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [D._id._v]);
          done();
        });
      });

      it('C and E = C', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(C, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [C._id._v]);
          done();
        });
      });

      it('D and F = D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(D, F, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [D._id._v]);
          done();
        });
      });

      it('F and G = F', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(F, G, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [F._id._v]);
          done();
        });
      });

      it('F and C = C', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(F, C, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [C._id._v]);
          done();
        });
      });

      it('D and E = D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(D, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [D._id._v]);
          done();
        });
      });

      it('E and D = D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(E, D, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [D._id._v]);
          done();
        });
      });

      it('G and B = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(G, B, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('E and F = C and D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(E, F, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [C._id._v, D._id._v]);
          done();
        });
      });
    });

    describe('n-parents', function() {
      var collectionName = 'findLCAsNParents';

      var A = { _id : { _id: 'foo', _v: 'A', _pe: 'I', _pa: [] } };
      var B = { _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'] } };
      var C = { _id : { _id: 'foo', _v: 'C', _pe: 'I', _pa: ['B'] } };
      var D = { _id : { _id: 'foo', _v: 'D', _pe: 'I', _pa: ['C'] } };
      var E = { _id : { _id: 'foo', _v: 'E', _pe: 'I', _pa: ['B'] } };
      var F = { _id : { _id: 'foo', _v: 'F', _pe: 'I', _pa: ['E', 'C'] } };
      var G = { _id : { _id: 'foo', _v: 'G', _pe: 'I', _pa: ['F'] } };
      var H = { _id : { _id: 'foo', _v: 'H', _pe: 'I', _pa: ['F'] } };
      var J = { _id : { _id: 'foo', _v: 'J', _pe: 'I', _pa: ['H'] } };
      var K = { _id : { _id: 'foo', _v: 'K', _pe: 'I', _pa: ['J'] } };
      var I = { _id : { _id: 'foo', _v: 'I', _pe: 'I', _pa: ['H', 'G', 'D'] } };
      var L = { _id : { _id: 'foo', _v: 'L', _pe: 'I', _pa: ['I'] } };

      // create the following structure:
      // A <-- B <-- C <----- D
      //        \     \        \
      //         E <-- F <-- G  \
      //                \     \  \      
      //                 H <------- I <-- L
      //                  \
      //                   J <-- K
      it('should save DAG', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._snapshotCollection.insert([A, B, C, D, E, F, G, H, J, K, I, L], {w: 1}, done);
      });

      it('J and K = J', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(J, K, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['J']);
          done();
        });
      });

      it('I and K = H', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(I, K, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['H']);
          done();
        });
      });

      it('I and G = G', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(I, G, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['G']);
          done();
        });
      });

      it('I and D = D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(I, D, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['D']);
          done();
        });
      });

      it('I and L = I', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(I, L, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['I']);
          done();
        });
      });

      it('L and C = C', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(L, C, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['C']);
          done();
        });
      });

      it('L and J = H', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(L, J, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['H']);
          done();
        });
      });
    });

    describe('three parents', function() {
      var collectionName = '_findLCAsThreeParents';

      var A = { _id : { _id: 'foo', _v: 'A', _pe: 'I', _pa: [] } };
      var B = { _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'] } };
      var C = { _id : { _id: 'foo', _v: 'C', _pe: 'I', _pa: ['B'] } };
      var D = { _id : { _id: 'foo', _v: 'D', _pe: 'I', _pa: ['B'] } };
      var E = { _id : { _id: 'foo', _v: 'E', _pe: 'I', _pa: ['C', 'F', 'D'] } };
      var F = { _id : { _id: 'foo', _v: 'F', _pe: 'I', _pa: ['B', 'C', 'D'] } };
      var G = { _id : { _id: 'foo', _v: 'G', _pe: 'I', _pa: ['D', 'F'] } };
      var H = { _id : { _id: 'foo', _v: 'H', _pe: 'I', _pa: ['E'] } };
      var I = { _id : { _id: 'foo', _v: 'I', _pe: 'I', _pa: ['F', 'E', 'G'] } };
      var J = { _id : { _id: 'foo', _v: 'J', _pe: 'I', _pa: ['G', 'E'] } };

      // create the following structure:
      //         C <-- E <-- H
      //        / \ / / \
      //       B <-- F <-- I
      //        \ /   \ / \         
      //         D <-- G <-- J
      it('should save DAG mixed branches', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._snapshotCollection.insert([B, C, D, F, E, H, G, I, J], {w: 1}, done);
      });

      it('J and H = E', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(J, H, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['E']);
          done();
        });
      });

      it('G and E = F', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(G, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['F']);
          done();
        });
      });

      it('F and E = F', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(F, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['F']);
          done();
        });
      });

      it('E and F = F', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(E, F, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['F']);
          done();
        });
      });

      it('J and I = G and E', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(J, I, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['G', 'E']);
          done();
        });
      });

      it('I and J = G and E', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(I, J, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['G', 'E']);
          done();
        });
      });

      it('A and B = no error because A is not in the database, but direct ancestor of B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(A, B, function(err, lcas) {
          should.equal(err, null);
          should.deepEqual(lcas, ['A']);
          done();
        });
      });

      it('B and A = no error because A is not in the database, but direct ancestor of B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(B, A, function(err, lcas) {
          should.equal(err, null);
          should.deepEqual(lcas, ['A']);
          done();
        });
      });

      it('B and B = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(B, B, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('H and B = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(H, B, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('B and H = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(B, H, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('H and E = E', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(H, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [E._id._v]);
          done();
        });
      });

      it('E and H = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(E, H, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [E._id._v]);
          done();
        });
      });

      it('J and D = D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(J, D, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [D._id._v]);
          done();
        });
      });

      it('D and J = D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(D, J, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [D._id._v]);
          done();
        });
      });
    });

    describe('virtual merge', function() {
      var collectionName = '_findLCAsVirtualMerge';

      var BI = { _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'] } };
      var CI = { _id : { _id: 'foo', _v: 'C', _pe: 'I', _pa: ['B'] } };
      var DI = { _id : { _id: 'foo', _v: 'D', _pe: 'I', _pa: ['B'] } };
      var EI = { _id : { _id: 'foo', _v: 'E', _pe: 'I', _pa: ['C', 'F', 'D'] } };
      var FI = { _id : { _id: 'foo', _v: 'F', _pe: 'I', _pa: ['B', 'C', 'D'] } };
      var GI = { _id : { _id: 'foo', _v: 'G', _pe: 'I', _pa: ['D', 'F'] } };
      var HI = { _id : { _id: 'foo', _v: 'H', _pe: 'I', _pa: ['E'] } };
      var II = { _id : { _id: 'foo', _v: 'I', _pe: 'I', _pa: ['F', 'E', 'G'] } };
      var JI = { _id : { _id: 'foo', _v: 'J', _pe: 'I', _pa: ['G', 'E'] } };

      // create the following structure:
      //         C <-- E <-- H
      //        / \ / / \
      //       B <-- F <-- I
      //        \ /   \ / \         
      //         D <-- G <-- J
      it('should save DAG mixed branches and mixed perspectives', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._snapshotCollection.insert([BI, CI, DI, FI, EI, HI, GI, II, JI], {w: 1}, done);
      });

      it('vm1 B and vm2 B = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm1 = { _id : { _id: 'foo', _pe: 'I', _pa: ['B'] } };
        var vm2 = { _id : { _id: 'foo', _pe: 'I', _pa: ['B'] } };
        vc._findLCAs(vm1, vm2, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['B']);
          done();
        });
      });

      it('vm1 B and vm2 A = error because AI is not in the database', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm1 = { _id : { _id: 'foo', _pe: 'I', _pa: ['B'] } };
        var vm2 = { _id : { _id: 'foo', _pe: 'I', _pa: ['A'] } };
        vc._findLCAs(vm1, vm2, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca A. perspectives: I');
          done();
        });
      });

      it('vm1 C, D and vm2 G = C and D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm1 = { _id : { _id: 'foo', _pe: 'I', _pa: ['C', 'D'] } };
        var vm2 = { _id : { _id: 'foo', _pe: 'I', _pa: ['G'] } };
        vc._findLCAs(vm1, vm2, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['D', 'C']);
          done();
        });
      });

      it('two vm\'s without parents = []', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm1 = { _id : { _id: 'foo', _pe: 'I', _pa: [] } };
        var vm2 = { _id : { _id: 'foo', _pe: 'I', _pa: [] } };
        vc._findLCAs(vm1, vm2, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, []);
          done();
        });
      });

      it('vm without parents and GI = []', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm = { _id : { _id: 'foo', _pe: 'II', _pa: [] } };
        vc._findLCAs(vm, GI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, []);
          done();
        });
      });


      it('vm C, D and GI = C and D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm = { _id : { _id: 'foo', _pe: 'I', _pa: ['C', 'D'] } };
        vc._findLCAs(vm, GI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['D', 'C']);
          done();
        });
      });

      it('vm J and II = E, G', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm = { _id : { _id: 'foo', _pe: 'I', _pa: ['J'] } };
        vc._findLCAs(vm, II, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['G', 'E']);
          done();
        });
      });

      it('vm E, F, G and II = E, F, G', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm = { _id : { _id: 'foo', _pe: 'I', _pa: ['E', 'F', 'G'] } };
        vc._findLCAs(vm, II, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['E', 'F', 'G']);
          done();
        });
      });

      it('vm H, I, J and JI = I', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm = { _id : { _id: 'foo', _pe: 'I', _pa: ['H', 'I', 'J'] } };
        vc._findLCAs(vm, II, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['I']);
          done();
        });
      });

      it('vm G, H and II = G and E', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm = { _id : { _id: 'foo', _pe: 'I', _pa: ['G', 'H'] } };
        vc._findLCAs(vm, II, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['G', 'E']);
          done();
        });
      });

      it('vm I, J and FI = F', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm = { _id : { _id: 'foo', _pe: 'I', _pa: ['I', 'J'] } };
        vc._findLCAs(vm, FI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['F']);
          done();
        });
      });
    });
  });

  describe('two perspectives', function() {
    describe('second import', function() {
      var collectionName = '_findLCAsTwoPerspectivesSecondImport';

      var AI  = { _id : { _id: 'foo', _v: 'A', _pe: 'I',  _pa: [] } };
      var BI  = { _id : { _id: 'foo', _v: 'B', _pe: 'I',  _pa: ['A'] } };
      var CI  = { _id : { _id: 'foo', _v: 'C', _pe: 'I',  _pa: ['B'] } };
      var EI  = { _id : { _id: 'foo', _v: 'E', _pe: 'I',  _pa: ['B'] } };
      var FI  = { _id : { _id: 'foo', _v: 'F', _pe: 'I',  _pa: ['E', 'C'] } };
      var GI  = { _id : { _id: 'foo', _v: 'G', _pe: 'I',  _pa: ['F'] } };
      var AII = { _id : { _id: 'foo', _v: 'A', _pe: 'II', _pa: [] } };
      var BII = { _id : { _id: 'foo', _v: 'B', _pe: 'II', _pa: ['A'] } };
      var CII = { _id : { _id: 'foo', _v: 'C', _pe: 'II', _pa: ['B'] } };
      var EII = { _id : { _id: 'foo', _v: 'E', _pe: 'II', _pa: ['B'] } };
      var FII = { _id : { _id: 'foo', _v: 'F', _pe: 'II', _pa: ['E', 'C'] } };
      var GII = { _id : { _id: 'foo', _v: 'G', _pe: 'II', _pa: ['F'] } };
      var HI  = { _id : { _id: 'foo', _v: 'H', _pe: 'I',  _pa: ['G'] } };
      var KI  = { _id : { _id: 'foo', _v: 'K', _pe: 'I',  _pa: ['H'] } };
      var SI  = { _id : { _id: 'foo', _v: 'S', _pe: 'I',  _pa: ['H'] } };
      var RI  = { _id : { _id: 'foo', _v: 'R', _pe: 'I',  _pa: ['K'] } };
      var JI  = { _id : { _id: 'foo', _v: 'J', _pe: 'I',  _pa: ['S'] } };
      var LI  = { _id : { _id: 'foo', _v: 'L', _pe: 'I',  _pa: ['R', 'J'] } };
      var MI  = { _id : { _id: 'foo', _v: 'M', _pe: 'I',  _pa: ['L'] } };
      var HII = { _id : { _id: 'foo', _v: 'H', _pe: 'II', _pa: ['G'] } };
      var KII = { _id : { _id: 'foo', _v: 'K', _pe: 'II', _pa: ['H'] } };
      var SII = { _id : { _id: 'foo', _v: 'S', _pe: 'II', _pa: ['H'] } };
      var RII = { _id : { _id: 'foo', _v: 'R', _pe: 'II', _pa: ['K'] } };
      var JII = { _id : { _id: 'foo', _v: 'J', _pe: 'II', _pa: ['S'] } };
      var LII = { _id : { _id: 'foo', _v: 'L', _pe: 'II', _pa: ['R', 'J'] } };
      var NII = { _id : { _id: 'foo', _v: 'N', _pe: 'II', _pa: ['J'] } };
      var OII = { _id : { _id: 'foo', _v: 'O', _pe: 'II', _pa: ['L', 'N'] } };
      var PII = { _id : { _id: 'foo', _v: 'P', _pe: 'II', _pa: ['N'] } };
      var QII = { _id : { _id: 'foo', _v: 'Q', _pe: 'II', _pa: ['O'] } };

      // create the following structure:
      //                                       EII <-- FII <-- GII <--------------------- HII <-- SII <-- JII <-- NII <-- PII
      //                                       /       /                                    \               \       \
      //                             AII <-- BII <-- CII                                    KII <-- RII <-- LII <-- OII <-- QII
      // AI <-- BI <-- CI                                         KI <-- RI <-- LI <-- MI
      //          \     \                                         /             /
      //          EI <-- FI <-- GI <---------------------------- HI <-- SI <-- JI 
      //
      it('should save DAG', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var DAG = [
          AI, BI, CI, EI, FI, GI,
          AII, BII, CII, EII, FII, GII,
          HI, KI, SI, RI, JI, LI, MI,
          HII, KII, SII, RII, JII, LII, NII, OII, PII, QII
        ];
        vc._snapshotCollection.insert(DAG, {w: 1}, done);
      });

      it('should not find nodes that are not in the DAG', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var item1 = { _id : { _id: 'foo', _v: 'r1', _pe: 'I', _pa: [] } };
        var item2 = { _id : { _id: 'foo', _v: 'r2', _pe: 'II', _pa: [] } };
        vc._findLCAs(item1, item2, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, []);
          done();
        });
      });

      it('GII and RI = G', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(GII, RI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['G']);
          done();
        });
      });

      it('LII and RI = R', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(LII, RI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['R']);
          done();
        });
      });

      it('RII and MI = R', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(RII, MI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['R']);
          done();
        });
      });

      it('LII and MI = L', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(LII, MI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['L']);
          done();
        });
      });

      it('KII and MI = K', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(KII, MI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['K']);
          done();
        });
      });

      it('KII and HI = H', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(KII, HI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['H']);
          done();
        });
      });

      it('HII and HI = H', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(HII, HI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['H']);
          done();
        });
      });

      it('PII and QII = N', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(PII, QII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['N']);
          done();
        });
      });

      it('PII and MI = J', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(PII, MI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['J']);
          done();
        });
      });

      it('QII and MI = L', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(QII, MI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['L']);
          done();
        });
      });

      it('AI and AII = A', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(AI, AII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['A']);
          done();
        });
      });

      it('AII and AI = A', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(AII, AI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['A']);
          done();
        });
      });

      it('BI and BII = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(BI, BII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['B']);
          done();
        });
      });

      it('AI and AII = A', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(AI, AII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['A']);
          done();
        });
      });

      it('AII and AI = A', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(AII, AI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['A']);
          done();
        });
      });

      it('BI and BII = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(BI, BII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['B']);
          done();
        });
      });

      it('BII and BI = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(BII, BI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['B']);
          done();
        });
      });

      it('FI and BII = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(FI, BII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['B']);
          done();
        });
      });

      /////////// BREAK THE GRAPH ON PURPOSE

      it('should remove GII', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._snapshotCollection.remove(GII, {w: 1}, function(err, deleted) {
          if (err) { throw err; }
          should.equal(deleted, 1);
          done();
        });
      });

      it('FI and HII = [] because link GII is missing', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(FI, HII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, []);
          done();
        });
      });
    });

    describe('criss-cross merge import', function() {
      var collectionName = '_findLCAsTwoPerspectivesCrissCrossMergeImport';

      // create DAG with imported criss-cross merge

      var AI = {
        _id : { _id: 'foo', _v: 'A', _pe: 'I', _pa: [] },
        _m3: { _merged: true },
      };

      var BI = {
        _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'] },
        _m3: { _merged: true },
      };

      var AII = {
        _id : { _id: 'foo', _v: 'A', _pe: 'II', _pa: [] },
      };

      var BII = {
        _id : { _id: 'foo', _v: 'B', _pe: 'II', _pa: ['A'] },
      };

      var CII = { _id : { _id: 'foo', _v: 'C', _pe: 'II', _pa: ['A'] } };

      var DII = {
        _id : { _id: 'foo', _v: 'D', _pe: 'II', _pa: ['B', 'C'] },
      };

      var DI = { _id : { _id: 'foo', _v: 'D', _pe: 'I', _pa: ['B', 'C'] } };

      var EII = {
        _id : { _id: 'foo', _v: 'E', _pe: 'II', _pa: ['C', 'B'] },
      };

      var EI = { _id : { _id: 'foo', _v: 'E', _pe: 'I', _pa: ['C', 'B'] } };

      var FII = {
        _id : { _id: 'foo', _v: 'F', _pe: 'II', _pa: ['D', 'E'] },
      };

      var FI = { _id : { _id: 'foo', _v: 'F', _pe: 'I', _pa: ['D', 'E'] } };

      // create the following structure:
      //              CII - EII,EI
      //             /  \ /   \
      //           AII   X    FII,FI
      //             \  / \   /          
      //             BII - DII,DI
      //
      // AI <-- BI

      it('should save DAG', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._snapshotCollection.insert([AI, BI, AII, BII, CII, DII, DI, EII, EI, FII, FI], {w: 1}, done);
      });

      it('EI and DII = error because CI is not in the database', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(EI, DII, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca C. perspectives: I, II');
          done();
        });
      });

      it('EII and DI = error (becaue CI is not in the database', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(EII, DI, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca C. perspectives: II, I');
          done();
        });
      });

      it('DII and EI = error becaue CI is not in the database', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(DII, EI, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca C. perspectives: II, I');
          done();
        });
      });

      it('DI and EII = error becaue CI is not in the database', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(DI, EII, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca C. perspectives: I, II');
          done();
        });
      });

      it('AI and AII = A', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(AI, AII, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['A']);
          done();
        });
      });

      it('BI and CII = A', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(BI, CII, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['A']);
          done();
        });
      });

      it('FI and DII = D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(FI, DII, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['D']);
          done();
        });
      });

      it('FII and DI = D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(FII, DI, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['D']);
          done();
        });
      });

      it('DII and FI = D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(DII, FI, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['D']);
          done();
        });
      });

      it('DI and FII = D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(DI, FII, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['D']);
          done();
        });
      });

      it('FI and EII = E', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(FI, EII, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['E']);
          done();
        });
      });

      it('FII and EI = E', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(FII, EI, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['E']);
          done();
        });
      });

      it('EII and FI = E', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(EII, FI, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['E']);
          done();
        });
      });

      it('EI and FII = E', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(EI, FII, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['E']);
          done();
        });
      });
    });

    describe('criss-cross n-parents', function() {
      var collectionName = '_findLCAsTwoPerspectivesCrissCrossNParents';

      // create DAG with imported criss-cross merge with three parents

      var BI = {
        _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        some: 'secret'
      };

      var CI = {
        _id : { _id: 'foo', _v: 'C', _pe: 'I', _pa: ['B'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        c: true,
        some: 'secret'
      };

      var DI = {
        _id : { _id: 'foo', _v: 'D', _pe: 'I', _pa: ['B'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        d: true,
        some: 'secret'
      };

      var EI = {
        _id : { _id: 'foo', _v: 'E', _pe: 'I', _pa: ['B'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        e: true,
        some: 'secret'
      };

      var FI = { // change e
        _id : { _id: 'foo', _v: 'F', _pe: 'I', _pa: ['C', 'D', 'E'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        c: true,
        d: true,
        e: 'foo',
        f: true,
        some: 'secret'
      };

      var GI = { // delete d
        _id : { _id: 'foo', _v: 'G', _pe: 'I', _pa: ['C', 'D', 'E'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        c: true,
        e: true,
        g: true,
        some: 'secret'
      };

      var BII = {
        _id : { _id: 'foo', _v: 'B', _pe: 'II', _pa: ['A'] },
        _m3: { _merged: false },
        a: true,
        b: true,
      };

      var CII = {
        _id : { _id: 'foo', _v: 'C', _pe: 'II', _pa: ['B'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        c: true,
      };

      var DII = {
        _id : { _id: 'foo', _v: 'D', _pe: 'II', _pa: ['B'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        d: true,
      };

      var EII = {
        _id : { _id: 'foo', _v: 'E', _pe: 'II', _pa: ['B'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        e: true,
      };

      var FII = { // change e
        _id : { _id: 'foo', _v: 'F', _pe: 'II', _pa: ['C', 'D', 'E'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        c: true,
        d: true,
        e: 'foo',
        f: true,
      };

      var GII = { // delete d
        _id : { _id: 'foo', _v: 'G', _pe: 'II', _pa: ['C', 'D', 'E'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        c: true,
        e: true,
        g: true,
      };

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
      it('save the DAG topologically sorted but perspectives mixed', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var DAG = [BI, BII, CII, DII, CI, DI, EI, FI, GI, EII, FII, GII ];
        vc._snapshotCollection.insert(DAG, {w: 1}, function(err, inserts) {
          if (err) { throw err; }
          should.equal(DAG.length, inserts.length);
          done();
        });
      });

      it('GII and FI = C, D, E', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(GII, FI, function(err, lcas) {
          should.equal(err, null);
          should.deepEqual(lcas, ['E', 'C', 'D']);
          done();
        });
      });
    });

    describe('three parents', function() {
      var collectionName = '_findLCAsTwoPerspectivesThreeParents';

      var AI = { _id : { _id: 'foo', _v: 'A', _pe: 'I', _pa: [] } };
      var BI = { _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'] } };
      var CI = { _id : { _id: 'foo', _v: 'C', _pe: 'I', _pa: ['B'] } };
      var DI = { _id : { _id: 'foo', _v: 'D', _pe: 'I', _pa: ['B'] } };
      var EI = { _id : { _id: 'foo', _v: 'E', _pe: 'I', _pa: ['C', 'F', 'D'] } };
      var FI = { _id : { _id: 'foo', _v: 'F', _pe: 'I', _pa: ['B', 'C', 'D'] } };
      var GI = { _id : { _id: 'foo', _v: 'G', _pe: 'I', _pa: ['D', 'F'] } };
      var HI = { _id : { _id: 'foo', _v: 'H', _pe: 'I', _pa: ['E'] } };
      var II = { _id : { _id: 'foo', _v: 'I', _pe: 'I', _pa: ['F', 'E', 'G'] } };
      var JI = { _id : { _id: 'foo', _v: 'J', _pe: 'I', _pa: ['G', 'E'] } };

      var AII = { _id : { _id: 'foo', _v: 'A', _pe: 'II', _pa: [] } };
      var BII = { _id : { _id: 'foo', _v: 'B', _pe: 'II', _pa: ['A'] } };
      var CII = { _id : { _id: 'foo', _v: 'C', _pe: 'II', _pa: ['B'] } };
      var DII = { _id : { _id: 'foo', _v: 'D', _pe: 'II', _pa: ['B'] } };
      var EII = { _id : { _id: 'foo', _v: 'E', _pe: 'II', _pa: ['C', 'F', 'D'] } };
      var FII = { _id : { _id: 'foo', _v: 'F', _pe: 'II', _pa: ['B', 'C', 'D'] } };
      var GII = { _id : { _id: 'foo', _v: 'G', _pe: 'II', _pa: ['D', 'F'] } };
      var HII = { _id : { _id: 'foo', _v: 'H', _pe: 'II', _pa: ['E'] } };
      var III = { _id : { _id: 'foo', _v: 'I', _pe: 'II', _pa: ['F', 'E', 'G'] } };
      var JII = { _id : { _id: 'foo', _v: 'J', _pe: 'II', _pa: ['G', 'E'] } };

      // create the following structure:
      //         C <-- E <-- H
      //        / \ / / \
      //       B <-- F <-- I
      //        \ /   \ / \         
      //         D <-- G <-- J
      it('should save DAG mixed branches and mixed perspectives', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._snapshotCollection.insert([AII, BII, CII, DII, BI, CI, FII, EII, DI, FI, HII, EI, HI, GI, GII, III, JII, II, JI], {w: 1}, done);
      });

      it('JI and HII = E', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(JI, HII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['E']);
          done();
        });
      });

      it('GI and EII = F', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(GI, EII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['F']);
          done();
        });
      });

      it('FI and EII = F', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(FI, EII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['F']);
          done();
        });
      });

      it('EI and FII = F', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(EI, FII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['F']);
          done();
        });
      });

      it('JI and III = G and E', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(JI, III, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['G', 'E']);
          done();
        });
      });

      it('II and JII = G and E', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(II, JII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['G', 'E']);
          done();
        });
      });

      it('AI and BII = error because AI is not in the database', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(AI, BII, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca A. perspectives: I, II');
          done();
        });
      });

      it('BII and AI = error because AI is not in the database', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(BII, AI, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca A. perspectives: II, I');
          done();
        });
      });

      it('AII and BI = error because AI is not in the database', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(AII, BI, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca A. perspectives: II, I');
          done();
        });
      });

      it('BI and AII = error because AII is not in the database', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(BI, AII, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca A. perspectives: I, II');
          done();
        });
      });

      it('BI and BII = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(BI, BII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['B']);
          done();
        });
      });

      it('HI and BII = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(HI, BII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['B']);
          done();
        });
      });

      it('BI and HII = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(BI, HII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['B']);
          done();
        });
      });

      it('HI and EII = E', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(HI, EII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['E']);
          done();
        });
      });

      it('EI and HII = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(EI, HII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['E']);
          done();
        });
      });

      it('JI and DII = D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(JI, DII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['D']);
          done();
        });
      });

      it('DI and JII = D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._findLCAs(DI, JII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['D']);
          done();
        });
      });
    });

    describe('virtual merge', function() {
      var collectionName = '_findLCAsTwoPerspectivesVirtualMerge';

      var BI = { _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'] } };
      var CI = { _id : { _id: 'foo', _v: 'C', _pe: 'I', _pa: ['B'] } };
      var DI = { _id : { _id: 'foo', _v: 'D', _pe: 'I', _pa: ['B'] } };
      var EI = { _id : { _id: 'foo', _v: 'E', _pe: 'I', _pa: ['C', 'F', 'D'] } };
      var FI = { _id : { _id: 'foo', _v: 'F', _pe: 'I', _pa: ['B', 'C', 'D'] } };
      var GI = { _id : { _id: 'foo', _v: 'G', _pe: 'I', _pa: ['D', 'F'] } };
      var HI = { _id : { _id: 'foo', _v: 'H', _pe: 'I', _pa: ['E'] } };
      var II = { _id : { _id: 'foo', _v: 'I', _pe: 'I', _pa: ['F', 'E', 'G'] } };
      var JI = { _id : { _id: 'foo', _v: 'J', _pe: 'I', _pa: ['G', 'E'] } };

      var AII = { _id : { _id: 'foo', _v: 'A', _pe: 'II', _pa: [] } };
      var BII = { _id : { _id: 'foo', _v: 'B', _pe: 'II', _pa: ['A'] } };
      var CII = { _id : { _id: 'foo', _v: 'C', _pe: 'II', _pa: ['B'] } };
      var DII = { _id : { _id: 'foo', _v: 'D', _pe: 'II', _pa: ['B'] } };
      var EII = { _id : { _id: 'foo', _v: 'E', _pe: 'II', _pa: ['C', 'F', 'D'] } };
      var FII = { _id : { _id: 'foo', _v: 'F', _pe: 'II', _pa: ['B', 'C', 'D'] } };
      var GII = { _id : { _id: 'foo', _v: 'G', _pe: 'II', _pa: ['D', 'F'] } };
      var HII = { _id : { _id: 'foo', _v: 'H', _pe: 'II', _pa: ['E'] } };
      var III = { _id : { _id: 'foo', _v: 'I', _pe: 'II', _pa: ['F', 'E', 'G'] } };
      var JII = { _id : { _id: 'foo', _v: 'J', _pe: 'II', _pa: ['G', 'E'] } };

      // create the following structure:
      //         C <-- E <-- H
      //        / \ / / \
      //       B <-- F <-- I
      //        \ /   \ / \         
      //         D <-- G <-- J
      it('should save DAG mixed branches and mixed perspectives', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        vc._snapshotCollection.insert([AII, BII, CII, DII, BI, CI, FII, EII, DI, FI, HII, EI, HI, GI, GII, III, JII, II, JI], {w: 1}, done);
      });

      it('vm1 B and vm2 B = B', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm1 = { _id : { _id: 'foo', _pe: 'II', _pa: ['B'] } };
        var vm2 = { _id : { _id: 'foo', _pe: 'I', _pa: ['B'] } };
        vc._findLCAs(vm1, vm2, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['B']);
          done();
        });
      });

      it('vm1 B and vm2 A = error because AI is not in the database', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm1 = { _id : { _id: 'foo', _pe: 'II', _pa: ['B'] } };
        var vm2 = { _id : { _id: 'foo', _pe: 'I', _pa: ['A'] } };
        vc._findLCAs(vm1, vm2, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca A. perspectives: II, I');
          done();
        });
      });

      it('vm1 C, D and vm2 G = C and D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm1 = { _id : { _id: 'foo', _pe: 'II', _pa: ['C', 'D'] } };
        var vm2 = { _id : { _id: 'foo', _pe: 'I', _pa: ['G'] } };
        vc._findLCAs(vm1, vm2, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['D', 'C']);
          done();
        });
      });

      it('two vm\'s without parents = []', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm1 = { _id : { _id: 'foo', _pe: 'I', _pa: [] } };
        var vm2 = { _id : { _id: 'foo', _pe: 'II', _pa: [] } };
        vc._findLCAs(vm1, vm2, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, []);
          done();
        });
      });

      it('vm without parents and GI = []', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm = { _id : { _id: 'foo', _pe: 'II', _pa: [] } };
        vc._findLCAs(vm, GI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, []);
          done();
        });
      });

      it('vm C, D pe III and GI = []', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm = { _id : { _id: 'foo', _pe: 'III', _pa: ['C', 'D'] } };
        vc._findLCAs(vm, GI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, []);
          done();
        });
      });

      it('vm C, D and GI = C and D', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm = { _id : { _id: 'foo', _pe: 'II', _pa: ['C', 'D'] } };
        vc._findLCAs(vm, GI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['D', 'C']);
          done();
        });
      });

      it('vm J and II = E, G', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm = { _id : { _id: 'foo', _pe: 'II', _pa: ['J'] } };
        vc._findLCAs(vm, II, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['G', 'E']);
          done();
        });
      });

      it('vm E, F, G and II = E, F, G', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm = { _id : { _id: 'foo', _pe: 'II', _pa: ['E', 'F', 'G'] } };
        vc._findLCAs(vm, II, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['E', 'F', 'G']);
          done();
        });
      });

      it('vm H, I, J and JI = I', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm = { _id : { _id: 'foo', _pe: 'II', _pa: ['H', 'I', 'J'] } };
        vc._findLCAs(vm, II, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['I']);
          done();
        });
      });

      it('vm G, H and II = G and E', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm = { _id : { _id: 'foo', _pe: 'II', _pa: ['G', 'H'] } };
        vc._findLCAs(vm, II, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['G', 'E']);
          done();
        });
      });

      it('vm I, J and FI = F', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var vm = { _id : { _id: 'foo', _pe: 'II', _pa: ['I', 'J'] } };
        vc._findLCAs(vm, FI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['F']);
          done();
        });
      });
    });

    describe('regression', function() {
      var collectionName = '_findLCAsTwoPerspectivesRegressions';

      var itemIA =  {'_id':{'_co':'foo','_id':'A','_v':'Hr+ojSYQ','_pa':[],'_pe':'_local','_i':3947}};
      var itemIIA = {'_id':{'_co':'foo','_id':'A','_v':'Hr+ojSYQ','_pa':[],'_pe':'test2'}};
      var itemIB =  {'_id':{'_co':'foo','_id':'A','_v':'p3oGRFGC','_pa':['Hr+ojSYQ'],'_pe':'_local','_i':3948}};
      var itemIIB = {'_id':{'_co':'foo','_id':'A','_v':'p3oGRFGC','_pa':['Hr+ojSYQ'],'_pe':'test2'}};

      it('needs the following items', function(done) {
        var vc = new VersionedCollection(db, collectionName);
        var DAG = [itemIA, itemIB];
        vc._snapshotCollection.insert(DAG, { w: 1 }, done);
      });

      it('should find the version itself to be the lca of two roots from different perspectives with the same version', function(done) {
        var vc = new VersionedCollection(db, collectionName, { debug: false });

        var VirtualCollection = require('../../../lib/virtual_collection');
        var v = new VirtualCollection(vc._snapshotCollection, [itemIIA, itemIIB]);

        var newThis = {
          debug: vc.debug,
          databaseName: vc.databaseName,
          localPerspective: vc.localPerspective,
          versionKey: vc.versionKey,
          collectionName: vc.collectionName,
          _snapshotCollection: v,
          _findLCAs: vc._findLCAs,
          _merge: vc._merge
        };
        newThis._findLCAs(itemIIA, itemIA, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Hr+ojSYQ']);
          done();
        });
      });
    });
  });
});
