/**
 * Copyright 2014-2015 Netsend.
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

var tmpdir = require('os').tmpdir;

var should = require('should');
var rimraf = require('rimraf');
var level = require('level');

var findLCAs = require('../../../lib/find_lcas');
var Tree = require('../../../lib/tree');
//var ConcatMongoCollection = require('../../../lib/concat_mongo_collection');
//var ArrayCollection = require('../../../lib/array_collection');
var logger = require('../../../lib/logger');

var t1, t2, db1, db2, cons, silence;
var dbPath1 = tmpdir() + '/test_find_lcas1';
var dbPath2 = tmpdir() + '/test_find_lcas2';

// open database
before(function(done) {
  logger({ console: true, mask: logger.DEBUG2 }, function(err, l) {
    if (err) { throw err; }
    cons = l;
    logger({ silence: true }, function(err, l) {
      if (err) { throw err; }
      silence = l;
      // ensure a db at start
      rimraf(dbPath1, function(err) {
        if (err) { throw err; }
        db1 = level(dbPath1, { keyEncoding: 'binary', valueEncoding: 'binary' });
        rimraf(dbPath2, function(err) {
          if (err) { throw err; }
          db2 = level(dbPath2, { keyEncoding: 'binary', valueEncoding: 'binary' });
          done();
        });
      });
    });
  });
});

after(function(done) {
  cons.close(function(err) {
    if (err) { throw err; }
    silence.close(function(err) {
      if (err) { throw err; }
      db1.close(function(err) {
        if (err) { throw err; }
        rimraf(dbPath1, function(err) {
          if (err) { throw err; }
          db2.close(function(err) {
            if (err) { throw err; }
            rimraf(dbPath2, done);
          });
        });
      });
    });
  });
});

describe('findLCAs', function() {
  describe('one perspective', function() {
    describe('two merges', function() {
      var name = 'findLCAsTwoMerges';

      var A = { h: { id: 'foo', v: 'Aaaaaaaa', pa: [] } };
      var B = { h: { id: 'foo', v: 'Bbbbbbbb', pa: ['Aaaaaaaa'] } };
      var C = { h: { id: 'foo', v: 'Cccccccc', pa: ['Bbbbbbbb'] } };
      var D = { h: { id: 'foo', v: 'Dddddddd', pa: ['Cccccccc'] } };
      var E = { h: { id: 'foo', v: 'Eeeeeeee', pa: ['Bbbbbbbb'] } };
      var F = { h: { id: 'foo', v: 'Ffffffff', pa: ['Eeeeeeee', 'Cccccccc'] } };
      var G = { h: { id: 'foo', v: 'Gggggggg', pa: ['Ffffffff'] } };

      var DAG = [A, B, C, D, E, F, G];

      // create the following structure:
      // A <-- B <-- C <-- D
      //        \     \             
      //         E <-- F <-- G
      it('should save DAG', function(done) {
        var t = new Tree(db1, name, { log: silence });
        DAG.forEach(function(item) {
          t.write(item)
        });
        t.end(done);
      });

      it('should require itemX', function(done) {
        var t = new Tree(db1, name, { log: silence });
        findLCAs(null, null, function(err) {
          should.equal('provide itemX', err.message);
          done();
        });
      });

      it('should require itemY', function(done) {
        findLCAs({}, null, function(err) {
          should.equal('provide itemY', err.message);
          done();
        });
      });

      it('should require itemX._id', function(done) {
        findLCAs({}, {}, function(err) {
          should.equal('missing itemX._id', err.message);
          done();
        });
      });

      it('should require itemY._id', function(done) {
        findLCAs({ h: {} }, {}, function(err) {
          should.equal('missing itemY._id', err.message);
          done();
        });
      });

      it('should require itemX._id to be an object', function(done) {
        vc._findLCAs({ id: 'foo' }, { h: {} }, function(err) {
          should.equal('itemX._id must be an object', err.message);
          done();
        });
      });

      it('should require itemY._id to be an object', function(done) {
        vc._findLCAs({ h: {} }, { id: 'foo' }, function(err) {
          should.equal('itemY._id must be an object', err.message);
          done();
        });
      });

      it('should require itemX._id._id to equal itemY._id._id', function(done) {
        vc._findLCAs({ h: { id: 1 } }, { h: { id: 2 } }, function(err) {
          should.equal('itemX._id._id must equal itemY._id._id', err.message);
          done();
        });
      });

      it('should require itemX._id._pe', function(done) {
        vc._findLCAs({ h: { pe: '' } }, { h: {} }, function(err) {
          should.equal('missing itemX._id._pe', err.message);
          done();
        });
      });

      it('should require itemY._id._pe', function(done) {
        vc._findLCAs({ h: { pe: 'I' } }, { h: { pe: '' } }, function(err) {
          should.equal('missing itemY._id._pe', err.message);
          done();
        });
      });

      it('A and B = A', function(done) {
        vc._findLCAs(A, B, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [A._id._v]);
          done();
        });
      });

      it('B and B = B', function(done) {
        vc._findLCAs(B, B, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('C and D = C', function(done) {
        vc._findLCAs(C, D, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [C._id._v]);
          done();
        });
      });

      it('D and D = D', function(done) {
        vc._findLCAs(D, D, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [D._id._v]);
          done();
        });
      });

      it('C and E = B', function(done) {
        vc._findLCAs(C, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('D and F = C', function(done) {
        vc._findLCAs(D, F, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [C._id._v]);
          done();
        });
      });

      it('F and G = F', function(done) {
        vc._findLCAs(F, G, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [F._id._v]);
          done();
        });
      });

      it('F and C = C', function(done) {
        vc._findLCAs(F, C, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [C._id._v]);
          done();
        });
      });

      it('D and E = B', function(done) {
        vc._findLCAs(D, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('E and D = B', function(done) {
        vc._findLCAs(E, D, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('G and B = B', function(done) {
        vc._findLCAs(G, B, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      var GII = { h: { id: 'foo', v: 'Gggggggg', pe: 'II', pa: ['Ffffffff'] } };
      var H = { h: { id: 'foo', v: 'H', pe: 'I', pa: [] } };

      it('should not find disconnected roots', function(done) {
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
        // add not connected nodes
        vc._findLCAs(GII, C, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, []);
          done();
        });
      });
    });

    describe('two merges one side', function() {
      var name = 'findLCAsTwoMergesOneSide';

      var A = { h: { id: 'foo', v: 'Aaaaaaaa', pe: 'I' } };
      var B = { h: { id: 'foo', v: 'Bbbbbbbb', pe: 'I', pa: ['Aaaaaaaa'] } };
      var C = { h: { id: 'foo', v: 'Cccccccc', pe: 'I', pa: ['Aaaaaaaa'] } };
      var D = { h: { id: 'foo', v: 'Dddddddd', pe: 'I', pa: ['Bbbbbbbb', 'Cccccccc'] } };
      var E = { h: { id: 'foo', v: 'Eeeeeeee', pe: 'I', pa: ['Cccccccc'] } };
      var F = { h: { id: 'foo', v: 'Ffffffff', pe: 'I', pa: ['Dddddddd', 'Eeeeeeee'] } };

      var DAG = [A, B, C, D, E, F];

      // create the following structure:
      // A <-- C <-- E
      //  \     \     \
      //   B <-- D <-- F
      it('should save DAG', function(done) {
        vc._snapshotCollection.insert(DAG, {w: 1}, done);
      });

      it('D and E = C', function(done) {
        vc._findLCAs(D, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [C._id._v]);
          done();
        });
      });
    });

    describe('three merges', function() {
      var name = 'findLCAsThreeMerges';

      var A = { h: { id: 'foo', v: 'Aaaaaaaa', pe: 'I' } };
      var B = { h: { id: 'foo', v: 'Bbbbbbbb', pe: 'I', pa: ['Aaaaaaaa'] } };
      var C = { h: { id: 'foo', v: 'Cccccccc', pe: 'I', pa: ['Bbbbbbbb', 'Eeeeeeee'] } };
      var D = { h: { id: 'foo', v: 'Dddddddd', pe: 'I', pa: ['Cccccccc'] } };
      var E = { h: { id: 'foo', v: 'Eeeeeeee', pe: 'I', pa: ['Bbbbbbbb'] } };
      var F = { h: { id: 'foo', v: 'Ffffffff', pe: 'I', pa: ['Eeeeeeee', 'Cccccccc'] } };
      var G = { h: { id: 'foo', v: 'Gggggggg', pe: 'I', pa: ['Ffffffff'] } };

      var DAG = [A, B, E, C, D, F, G];

      // create the following structure:
      // A <-- B <-- C <-- D
      //        \  /  \             
      //         E <-- F <-- G
      it('should save DAG', function(done) {
        vc._snapshotCollection.insert(DAG, {w: 1}, done);
      });

      it('C and E = E', function(done) {
        vc._findLCAs(C, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [E._id._v]);
          done();
        });
      });

      it('D and F = C', function(done) {
        vc._findLCAs(D, F, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [C._id._v]);
          done();
        });
      });

      it('F and C = C', function(done) {
        vc._findLCAs(F, C, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [C._id._v]);
          done();
        });
      });

      it('D and E = E', function(done) {
        vc._findLCAs(D, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [E._id._v]);
          done();
        });
      });

      it('E and D = E', function(done) {
        vc._findLCAs(E, D, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [E._id._v]);
          done();
        });
      });

      it('G and B = B', function(done) {
        vc._findLCAs(G, B, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });
    });

    describe('criss-cross', function() {
      var name = 'findLCAsCrissCrossMerge';

      var A = { h: { id: 'foo', v: 'Aaaaaaaa', pe: 'I', pa: [] } };
      var B = { h: { id: 'foo', v: 'Bbbbbbbb', pe: 'I', pa: ['Aaaaaaaa'] } };
      var C = { h: { id: 'foo', v: 'Cccccccc', pe: 'I', pa: ['Bbbbbbbb'] } };
      var D = { h: { id: 'foo', v: 'Dddddddd', pe: 'I', pa: ['Bbbbbbbb'] } };
      var E = { h: { id: 'foo', v: 'Eeeeeeee', pe: 'I', pa: ['Cccccccc', 'Dddddddd'] } };
      var F = { h: { id: 'foo', v: 'Ffffffff', pe: 'I', pa: ['Dddddddd', 'Cccccccc'] } };
      var G = { h: { id: 'foo', v: 'Gggggggg', pe: 'I', pa: ['Ffffffff', 'Eeeeeeee'] } };

      // create the following structure:
      //         C <-- E
      //        / \  /   \
      // A <-- B    X     G
      //        \  /  \  /          
      //         D <-- F
      it('should save DAG', function(done) {
        vc._snapshotCollection.insert([A, B, C, D, E, F, G], {w: 1}, done);
      });

      it('A and B = A', function(done) {
        vc._findLCAs(A, B, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [A._id._v]);
          done();
        });
      });

      it('B and B = B', function(done) {
        vc._findLCAs(B, B, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('C and D = B', function(done) {
        vc._findLCAs(C, D, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('D and D = D', function(done) {
        vc._findLCAs(D, D, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [D._id._v]);
          done();
        });
      });

      it('C and E = C', function(done) {
        vc._findLCAs(C, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [C._id._v]);
          done();
        });
      });

      it('D and F = D', function(done) {
        vc._findLCAs(D, F, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [D._id._v]);
          done();
        });
      });

      it('F and G = F', function(done) {
        vc._findLCAs(F, G, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [F._id._v]);
          done();
        });
      });

      it('F and C = C', function(done) {
        vc._findLCAs(F, C, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [C._id._v]);
          done();
        });
      });

      it('D and E = D', function(done) {
        vc._findLCAs(D, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [D._id._v]);
          done();
        });
      });

      it('E and D = D', function(done) {
        vc._findLCAs(E, D, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [D._id._v]);
          done();
        });
      });

      it('G and B = B', function(done) {
        vc._findLCAs(G, B, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('E and F = C and D', function(done) {
        vc._findLCAs(E, F, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [C._id._v, D._id._v]);
          done();
        });
      });
    });

    describe('n-parents', function() {
      var name = 'findLCAsNParents';

      var A = { h: { id: 'foo', v: 'Aaaaaaaa', pe: 'I', pa: [] } };
      var B = { h: { id: 'foo', v: 'Bbbbbbbb', pe: 'I', pa: ['Aaaaaaaa'] } };
      var C = { h: { id: 'foo', v: 'Cccccccc', pe: 'I', pa: ['Bbbbbbbb'] } };
      var D = { h: { id: 'foo', v: 'Dddddddd', pe: 'I', pa: ['Cccccccc'] } };
      var E = { h: { id: 'foo', v: 'Eeeeeeee', pe: 'I', pa: ['Bbbbbbbb'] } };
      var F = { h: { id: 'foo', v: 'Ffffffff', pe: 'I', pa: ['Eeeeeeee', 'Cccccccc'] } };
      var G = { h: { id: 'foo', v: 'Gggggggg', pe: 'I', pa: ['Ffffffff'] } };
      var H = { h: { id: 'foo', v: 'H', pe: 'I', pa: ['Ffffffff'] } };
      var J = { h: { id: 'foo', v: 'J', pe: 'I', pa: ['H'] } };
      var K = { h: { id: 'foo', v: 'K', pe: 'I', pa: ['J'] } };
      var I = { h: { id: 'foo', v: 'I', pe: 'I', pa: ['H', 'Gggggggg', 'Dddddddd'] } };
      var L = { h: { id: 'foo', v: 'L', pe: 'I', pa: ['I'] } };

      // create the following structure:
      // A <-- B <-- C <----- D
      //        \     \        \
      //         E <-- F <-- G  \
      //                \     \  \      
      //                 H <------- I <-- L
      //                  \
      //                   J <-- K
      it('should save DAG', function(done) {
        vc._snapshotCollection.insert([A, B, C, D, E, F, G, H, J, K, I, L], {w: 1}, done);
      });

      it('J and K = J', function(done) {
        vc._findLCAs(J, K, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['J']);
          done();
        });
      });

      it('I and K = H', function(done) {
        vc._findLCAs(I, K, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['H']);
          done();
        });
      });

      it('I and G = G', function(done) {
        vc._findLCAs(I, G, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Gggggggg']);
          done();
        });
      });

      it('I and D = D', function(done) {
        vc._findLCAs(I, D, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Dddddddd']);
          done();
        });
      });

      it('I and L = I', function(done) {
        vc._findLCAs(I, L, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['I']);
          done();
        });
      });

      it('L and C = C', function(done) {
        vc._findLCAs(L, C, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Cccccccc']);
          done();
        });
      });

      it('L and J = H', function(done) {
        vc._findLCAs(L, J, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['H']);
          done();
        });
      });
    });

    describe('three parents', function() {
      var name = '_findLCAsThreeParents';

      var A = { h: { id: 'foo', v: 'Aaaaaaaa', pe: 'I', pa: [] } };
      var B = { h: { id: 'foo', v: 'Bbbbbbbb', pe: 'I', pa: ['Aaaaaaaa'] } };
      var C = { h: { id: 'foo', v: 'Cccccccc', pe: 'I', pa: ['Bbbbbbbb'] } };
      var D = { h: { id: 'foo', v: 'Dddddddd', pe: 'I', pa: ['Bbbbbbbb'] } };
      var E = { h: { id: 'foo', v: 'Eeeeeeee', pe: 'I', pa: ['Cccccccc', 'Ffffffff', 'Dddddddd'] } };
      var F = { h: { id: 'foo', v: 'Ffffffff', pe: 'I', pa: ['Bbbbbbbb', 'Cccccccc', 'Dddddddd'] } };
      var G = { h: { id: 'foo', v: 'Gggggggg', pe: 'I', pa: ['Dddddddd', 'Ffffffff'] } };
      var H = { h: { id: 'foo', v: 'H', pe: 'I', pa: ['Eeeeeeee'] } };
      var I = { h: { id: 'foo', v: 'I', pe: 'I', pa: ['Ffffffff', 'Eeeeeeee', 'Gggggggg'] } };
      var J = { h: { id: 'foo', v: 'J', pe: 'I', pa: ['Gggggggg', 'Eeeeeeee'] } };

      // create the following structure:
      //         C <-- E <-- H
      //        / \ / / \
      //       B <-- F <-- I
      //        \ /   \ / \         
      //         D <-- G <-- J
      it('should save DAG mixed branches', function(done) {
        vc._snapshotCollection.insert([B, C, D, F, E, H, G, I, J], {w: 1}, done);
      });

      it('J and H = E', function(done) {
        vc._findLCAs(J, H, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Eeeeeeee']);
          done();
        });
      });

      it('G and E = F', function(done) {
        vc._findLCAs(G, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Ffffffff']);
          done();
        });
      });

      it('F and E = F', function(done) {
        vc._findLCAs(F, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Ffffffff']);
          done();
        });
      });

      it('E and F = F', function(done) {
        vc._findLCAs(E, F, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Ffffffff']);
          done();
        });
      });

      it('J and I = G and E', function(done) {
        vc._findLCAs(J, I, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Gggggggg', 'Eeeeeeee']);
          done();
        });
      });

      it('I and J = G and E', function(done) {
        vc._findLCAs(I, J, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Gggggggg', 'Eeeeeeee']);
          done();
        });
      });

      it('A and B = no error because A is not in the database, but direct ancestor of B', function(done) {
        vc._findLCAs(A, B, function(err, lcas) {
          should.equal(err, null);
          should.deepEqual(lcas, ['Aaaaaaaa']);
          done();
        });
      });

      it('B and A = no error because A is not in the database, but direct ancestor of B', function(done) {
        vc._findLCAs(B, A, function(err, lcas) {
          should.equal(err, null);
          should.deepEqual(lcas, ['Aaaaaaaa']);
          done();
        });
      });

      it('B and B = B', function(done) {
        vc._findLCAs(B, B, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('H and B = B', function(done) {
        vc._findLCAs(H, B, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('B and H = B', function(done) {
        vc._findLCAs(B, H, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [B._id._v]);
          done();
        });
      });

      it('H and E = E', function(done) {
        vc._findLCAs(H, E, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [E._id._v]);
          done();
        });
      });

      it('E and H = B', function(done) {
        vc._findLCAs(E, H, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [E._id._v]);
          done();
        });
      });

      it('J and D = D', function(done) {
        vc._findLCAs(J, D, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [D._id._v]);
          done();
        });
      });

      it('D and J = D', function(done) {
        vc._findLCAs(D, J, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, [D._id._v]);
          done();
        });
      });
    });

    describe('virtual merge', function() {
      var name = '_findLCAsVirtualMerge';

      var BI = { h: { id: 'foo', v: 'Bbbbbbbb', pe: 'I', pa: ['Aaaaaaaa'] } };
      var CI = { h: { id: 'foo', v: 'Cccccccc', pe: 'I', pa: ['Bbbbbbbb'] } };
      var DI = { h: { id: 'foo', v: 'Dddddddd', pe: 'I', pa: ['Bbbbbbbb'] } };
      var EI = { h: { id: 'foo', v: 'Eeeeeeee', pe: 'I', pa: ['Cccccccc', 'Ffffffff', 'Dddddddd'] } };
      var FI = { h: { id: 'foo', v: 'Ffffffff', pe: 'I', pa: ['Bbbbbbbb', 'Cccccccc', 'Dddddddd'] } };
      var GI = { h: { id: 'foo', v: 'Gggggggg', pe: 'I', pa: ['Dddddddd', 'Ffffffff'] } };
      var HI = { h: { id: 'foo', v: 'H', pe: 'I', pa: ['Eeeeeeee'] } };
      var II = { h: { id: 'foo', v: 'I', pe: 'I', pa: ['Ffffffff', 'Eeeeeeee', 'Gggggggg'] } };
      var JI = { h: { id: 'foo', v: 'J', pe: 'I', pa: ['Gggggggg', 'Eeeeeeee'] } };

      // create the following structure:
      //         C <-- E <-- H
      //        / \ / / \
      //       B <-- F <-- I
      //        \ /   \ / \         
      //         D <-- G <-- J
      it('should save DAG mixed branches and mixed perspectives', function(done) {
        vc._snapshotCollection.insert([BI, CI, DI, FI, EI, HI, GI, II, JI], {w: 1}, done);
      });

      it('vm1 B and vm2 B = B', function(done) {
        var vm1 = { h: { id: 'foo', pe: 'I', pa: ['Bbbbbbbb'] } };
        var vm2 = { h: { id: 'foo', pe: 'I', pa: ['Bbbbbbbb'] } };
        vc._findLCAs(vm1, vm2, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('vm1 B and vm2 A = error because AI is not in the database', function(done) {
        var vm1 = { h: { id: 'foo', pe: 'I', pa: ['Bbbbbbbb'] } };
        var vm2 = { h: { id: 'foo', pe: 'I', pa: ['Aaaaaaaa'] } };
        vc._findLCAs(vm1, vm2, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca A. perspectives: I');
          done();
        });
      });

      it('vm1 C, D and vm2 G = C and D', function(done) {
        var vm1 = { h: { id: 'foo', pe: 'I', pa: ['Cccccccc', 'Dddddddd'] } };
        var vm2 = { h: { id: 'foo', pe: 'I', pa: ['Gggggggg'] } };
        vc._findLCAs(vm1, vm2, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Dddddddd', 'Cccccccc']);
          done();
        });
      });

      it('two vm\'s without parents = []', function(done) {
        var vm1 = { h: { id: 'foo', pe: 'I', pa: [] } };
        var vm2 = { h: { id: 'foo', pe: 'I', pa: [] } };
        vc._findLCAs(vm1, vm2, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, []);
          done();
        });
      });

      it('vm without parents and GI = []', function(done) {
        var vm = { h: { id: 'foo', pe: 'II', pa: [] } };
        vc._findLCAs(vm, GI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, []);
          done();
        });
      });


      it('vm C, D and GI = C and D', function(done) {
        var vm = { h: { id: 'foo', pe: 'I', pa: ['Cccccccc', 'Dddddddd'] } };
        vc._findLCAs(vm, GI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Dddddddd', 'Cccccccc']);
          done();
        });
      });

      it('vm J and II = E, G', function(done) {
        var vm = { h: { id: 'foo', pe: 'I', pa: ['J'] } };
        vc._findLCAs(vm, II, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Gggggggg', 'Eeeeeeee']);
          done();
        });
      });

      it('vm E, F, G and II = E, F, G', function(done) {
        var vm = { h: { id: 'foo', pe: 'I', pa: ['Eeeeeeee', 'Ffffffff', 'Gggggggg'] } };
        vc._findLCAs(vm, II, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Eeeeeeee', 'Ffffffff', 'Gggggggg']);
          done();
        });
      });

      it('vm H, I, J and JI = I', function(done) {
        var vm = { h: { id: 'foo', pe: 'I', pa: ['H', 'I', 'J'] } };
        vc._findLCAs(vm, II, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['I']);
          done();
        });
      });

      it('vm G, H and II = G and E', function(done) {
        var vm = { h: { id: 'foo', pe: 'I', pa: ['Gggggggg', 'H'] } };
        vc._findLCAs(vm, II, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Gggggggg', 'Eeeeeeee']);
          done();
        });
      });

      it('vm I, J and FI = F', function(done) {
        var vm = { h: { id: 'foo', pe: 'I', pa: ['I', 'J'] } };
        vc._findLCAs(vm, FI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Ffffffff']);
          done();
        });
      });
    });
  });

  describe('two perspectives', function() {
    describe('second import', function() {
      var name = '_findLCAsTwoPerspectivesSecondImport';

      var AI  = { h: { id: 'foo', v: 'Aaaaaaaa', pe: 'I',  pa: [] } };
      var BI  = { h: { id: 'foo', v: 'Bbbbbbbb', pe: 'I',  pa: ['Aaaaaaaa'] } };
      var CI  = { h: { id: 'foo', v: 'Cccccccc', pe: 'I',  pa: ['Bbbbbbbb'] } };
      var EI  = { h: { id: 'foo', v: 'Eeeeeeee', pe: 'I',  pa: ['Bbbbbbbb'] } };
      var FI  = { h: { id: 'foo', v: 'Ffffffff', pe: 'I',  pa: ['Eeeeeeee', 'Cccccccc'] } };
      var GI  = { h: { id: 'foo', v: 'Gggggggg', pe: 'I',  pa: ['Ffffffff'] } };
      var AII = { h: { id: 'foo', v: 'Aaaaaaaa', pe: 'II', pa: [] } };
      var BII = { h: { id: 'foo', v: 'Bbbbbbbb', pe: 'II', pa: ['Aaaaaaaa'] } };
      var CII = { h: { id: 'foo', v: 'Cccccccc', pe: 'II', pa: ['Bbbbbbbb'] } };
      var EII = { h: { id: 'foo', v: 'Eeeeeeee', pe: 'II', pa: ['Bbbbbbbb'] } };
      var FII = { h: { id: 'foo', v: 'Ffffffff', pe: 'II', pa: ['Eeeeeeee', 'Cccccccc'] } };
      var GII = { h: { id: 'foo', v: 'Gggggggg', pe: 'II', pa: ['Ffffffff'] } };
      var HI  = { h: { id: 'foo', v: 'H', pe: 'I',  pa: ['Gggggggg'] } };
      var KI  = { h: { id: 'foo', v: 'K', pe: 'I',  pa: ['H'] } };
      var SI  = { h: { id: 'foo', v: 'S', pe: 'I',  pa: ['H'] } };
      var RI  = { h: { id: 'foo', v: 'R', pe: 'I',  pa: ['K'] } };
      var JI  = { h: { id: 'foo', v: 'J', pe: 'I',  pa: ['S'] } };
      var LI  = { h: { id: 'foo', v: 'L', pe: 'I',  pa: ['R', 'J'] } };
      var MI  = { h: { id: 'foo', v: 'M', pe: 'I',  pa: ['L'] } };
      var HII = { h: { id: 'foo', v: 'H', pe: 'II', pa: ['Gggggggg'] } };
      var KII = { h: { id: 'foo', v: 'K', pe: 'II', pa: ['H'] } };
      var SII = { h: { id: 'foo', v: 'S', pe: 'II', pa: ['H'] } };
      var RII = { h: { id: 'foo', v: 'R', pe: 'II', pa: ['K'] } };
      var JII = { h: { id: 'foo', v: 'J', pe: 'II', pa: ['S'] } };
      var LII = { h: { id: 'foo', v: 'L', pe: 'II', pa: ['R', 'J'] } };
      var NII = { h: { id: 'foo', v: 'N', pe: 'II', pa: ['J'] } };
      var OII = { h: { id: 'foo', v: 'O', pe: 'II', pa: ['L', 'N'] } };
      var PII = { h: { id: 'foo', v: 'P', pe: 'II', pa: ['N'] } };
      var QII = { h: { id: 'foo', v: 'Q', pe: 'II', pa: ['O'] } };

      // create the following structure:
      //                                       EII <-- FII <-- GII <--------------------- HII <-- SII <-- JII <-- NII <-- PII
      //                                       /       /                                    \               \       \
      //                             AII <-- BII <-- CII                                    KII <-- RII <-- LII <-- OII <-- QII
      // AI <-- BI <-- CI                                         KI <-- RI <-- LI <-- MI
      //          \     \                                         /             /
      //          EI <-- FI <-- GI <---------------------------- HI <-- SI <-- JI 
      //
      it('should save DAG', function(done) {
        var DAG = [
          AI, BI, CI, EI, FI, GI,
          AII, BII, CII, EII, FII, GII,
          HI, KI, SI, RI, JI, LI, MI,
          HII, KII, SII, RII, JII, LII, NII, OII, PII, QII
        ];
        vc._snapshotCollection.insert(DAG, {w: 1}, done);
      });

      it('should not find nodes that are not in the DAG', function(done) {
        var item1 = { h: { id: 'foo', v: 'r1', pe: 'I', pa: [] } };
        var item2 = { h: { id: 'foo', v: 'r2', pe: 'II', pa: [] } };
        vc._findLCAs(item1, item2, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, []);
          done();
        });
      });

      it('GII and RI = G', function(done) {
        vc._findLCAs(GII, RI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Gggggggg']);
          done();
        });
      });

      it('LII and RI = R', function(done) {
        vc._findLCAs(LII, RI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['R']);
          done();
        });
      });

      it('RII and MI = R', function(done) {
        vc._findLCAs(RII, MI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['R']);
          done();
        });
      });

      it('LII and MI = L', function(done) {
        vc._findLCAs(LII, MI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['L']);
          done();
        });
      });

      it('KII and MI = K', function(done) {
        vc._findLCAs(KII, MI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['K']);
          done();
        });
      });

      it('KII and HI = H', function(done) {
        vc._findLCAs(KII, HI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['H']);
          done();
        });
      });

      it('HII and HI = H', function(done) {
        vc._findLCAs(HII, HI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['H']);
          done();
        });
      });

      it('PII and QII = N', function(done) {
        vc._findLCAs(PII, QII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['N']);
          done();
        });
      });

      it('PII and MI = J', function(done) {
        vc._findLCAs(PII, MI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['J']);
          done();
        });
      });

      it('QII and MI = L', function(done) {
        vc._findLCAs(QII, MI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['L']);
          done();
        });
      });

      it('AI and AII = A', function(done) {
        vc._findLCAs(AI, AII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Aaaaaaaa']);
          done();
        });
      });

      it('AII and AI = A', function(done) {
        vc._findLCAs(AII, AI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Aaaaaaaa']);
          done();
        });
      });

      it('BI and BII = B', function(done) {
        vc._findLCAs(BI, BII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('AI and AII = A', function(done) {
        vc._findLCAs(AI, AII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Aaaaaaaa']);
          done();
        });
      });

      it('AII and AI = A', function(done) {
        vc._findLCAs(AII, AI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Aaaaaaaa']);
          done();
        });
      });

      it('BI and BII = B', function(done) {
        vc._findLCAs(BI, BII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('BII and BI = B', function(done) {
        vc._findLCAs(BII, BI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('FI and BII = B', function(done) {
        vc._findLCAs(FI, BII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      /////////// BREAK THE GRAPH ON PURPOSE

      it('should remove GII', function(done) {
        vc._snapshotCollection.remove(GII, {w: 1}, function(err, deleted) {
          if (err) { throw err; }
          should.equal(deleted, 1);
          done();
        });
      });

      it('FI and HII = [] because link GII is missing', function(done) {
        vc._findLCAs(FI, HII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, []);
          done();
        });
      });
    });

    describe('criss-cross merge import', function() {
      var name = '_findLCAsTwoPerspectivesCrissCrossMergeImport';

      // create DAG with imported criss-cross merge

      var AI = {
        _id : { id: 'foo', v: 'Aaaaaaaa', pe: 'I', pa: [] },
        _m3: { _merged: true },
      };

      var BI = {
        _id : { id: 'foo', v: 'Bbbbbbbb', pe: 'I', pa: ['Aaaaaaaa'] },
        _m3: { _merged: true },
      };

      var AII = {
        _id : { id: 'foo', v: 'Aaaaaaaa', pe: 'II', pa: [] },
      };

      var BII = {
        _id : { id: 'foo', v: 'Bbbbbbbb', pe: 'II', pa: ['Aaaaaaaa'] },
      };

      var CII = { h: { id: 'foo', v: 'Cccccccc', pe: 'II', pa: ['Aaaaaaaa'] } };

      var DII = {
        _id : { id: 'foo', v: 'Dddddddd', pe: 'II', pa: ['Bbbbbbbb', 'Cccccccc'] },
      };

      var DI = { h: { id: 'foo', v: 'Dddddddd', pe: 'I', pa: ['Bbbbbbbb', 'Cccccccc'] } };

      var EII = {
        _id : { id: 'foo', v: 'Eeeeeeee', pe: 'II', pa: ['Cccccccc', 'Bbbbbbbb'] },
      };

      var EI = { h: { id: 'foo', v: 'Eeeeeeee', pe: 'I', pa: ['Cccccccc', 'Bbbbbbbb'] } };

      var FII = {
        _id : { id: 'foo', v: 'Ffffffff', pe: 'II', pa: ['Dddddddd', 'Eeeeeeee'] },
      };

      var FI = { h: { id: 'foo', v: 'Ffffffff', pe: 'I', pa: ['Dddddddd', 'Eeeeeeee'] } };

      // create the following structure:
      //              CII - EII,EI
      //             /  \ /   \
      //           AII   X    FII,FI
      //             \  / \   /          
      //             BII - DII,DI
      //
      // AI <-- BI

      it('should save DAG', function(done) {
        vc._snapshotCollection.insert([AI, BI, AII, BII, CII, DII, DI, EII, EI, FII, FI], {w: 1}, done);
      });

      it('EI and DII = error because CI is not in the database', function(done) {
        vc._findLCAs(EI, DII, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca C. perspectives: I, II');
          done();
        });
      });

      it('EII and DI = error (becaue CI is not in the database', function(done) {
        vc._findLCAs(EII, DI, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca C. perspectives: II, I');
          done();
        });
      });

      it('DII and EI = error becaue CI is not in the database', function(done) {
        vc._findLCAs(DII, EI, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca C. perspectives: II, I');
          done();
        });
      });

      it('DI and EII = error becaue CI is not in the database', function(done) {
        vc._findLCAs(DI, EII, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca C. perspectives: I, II');
          done();
        });
      });

      it('AI and AII = A', function(done) {
        vc._findLCAs(AI, AII, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['Aaaaaaaa']);
          done();
        });
      });

      it('BI and CII = A', function(done) {
        vc._findLCAs(BI, CII, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['Aaaaaaaa']);
          done();
        });
      });

      it('FI and DII = D', function(done) {
        vc._findLCAs(FI, DII, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['Dddddddd']);
          done();
        });
      });

      it('FII and DI = D', function(done) {
        vc._findLCAs(FII, DI, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['Dddddddd']);
          done();
        });
      });

      it('DII and FI = D', function(done) {
        vc._findLCAs(DII, FI, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['Dddddddd']);
          done();
        });
      });

      it('DI and FII = D', function(done) {
        vc._findLCAs(DI, FII, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['Dddddddd']);
          done();
        });
      });

      it('FI and EII = E', function(done) {
        vc._findLCAs(FI, EII, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['Eeeeeeee']);
          done();
        });
      });

      it('FII and EI = E', function(done) {
        vc._findLCAs(FII, EI, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['Eeeeeeee']);
          done();
        });
      });

      it('EII and FI = E', function(done) {
        vc._findLCAs(EII, FI, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['Eeeeeeee']);
          done();
        });
      });

      it('EI and FII = E', function(done) {
        vc._findLCAs(EI, FII, function(err, merged) {
          should.equal(err, null);
          should.deepEqual(merged, ['Eeeeeeee']);
          done();
        });
      });
    });

    describe('criss-cross n-parents', function() {
      var name = '_findLCAsTwoPerspectivesCrissCrossNParents';

      // create DAG with imported criss-cross merge with three parents

      var BI = {
        _id : { id: 'foo', v: 'Bbbbbbbb', pe: 'I', pa: ['Aaaaaaaa'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        some: 'secret'
      };

      var CI = {
        _id : { id: 'foo', v: 'Cccccccc', pe: 'I', pa: ['Bbbbbbbb'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        c: true,
        some: 'secret'
      };

      var DI = {
        _id : { id: 'foo', v: 'Dddddddd', pe: 'I', pa: ['Bbbbbbbb'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        d: true,
        some: 'secret'
      };

      var EI = {
        _id : { id: 'foo', v: 'Eeeeeeee', pe: 'I', pa: ['Bbbbbbbb'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        e: true,
        some: 'secret'
      };

      var FI = { // change e
        _id : { id: 'foo', v: 'Ffffffff', pe: 'I', pa: ['Cccccccc', 'Dddddddd', 'Eeeeeeee'] },
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
        _id : { id: 'foo', v: 'Gggggggg', pe: 'I', pa: ['Cccccccc', 'Dddddddd', 'Eeeeeeee'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        c: true,
        e: true,
        g: true,
        some: 'secret'
      };

      var BII = {
        _id : { id: 'foo', v: 'Bbbbbbbb', pe: 'II', pa: ['Aaaaaaaa'] },
        _m3: { _merged: false },
        a: true,
        b: true,
      };

      var CII = {
        _id : { id: 'foo', v: 'Cccccccc', pe: 'II', pa: ['Bbbbbbbb'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        c: true,
      };

      var DII = {
        _id : { id: 'foo', v: 'Dddddddd', pe: 'II', pa: ['Bbbbbbbb'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        d: true,
      };

      var EII = {
        _id : { id: 'foo', v: 'Eeeeeeee', pe: 'II', pa: ['Bbbbbbbb'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        e: true,
      };

      var FII = { // change e
        _id : { id: 'foo', v: 'Ffffffff', pe: 'II', pa: ['Cccccccc', 'Dddddddd', 'Eeeeeeee'] },
        _m3: { _merged: false },
        a: true,
        b: true,
        c: true,
        d: true,
        e: 'foo',
        f: true,
      };

      var GII = { // delete d
        _id : { id: 'foo', v: 'Gggggggg', pe: 'II', pa: ['Cccccccc', 'Dddddddd', 'Eeeeeeee'] },
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
        var DAG = [BI, BII, CII, DII, CI, DI, EI, FI, GI, EII, FII, GII ];
        vc._snapshotCollection.insert(DAG, {w: 1}, function(err, inserts) {
          if (err) { throw err; }
          should.equal(DAG.length, inserts.length);
          done();
        });
      });

      it('GII and FI = C, D, E', function(done) {
        vc._findLCAs(GII, FI, function(err, lcas) {
          should.equal(err, null);
          should.deepEqual(lcas, ['Eeeeeeee', 'Cccccccc', 'Dddddddd']);
          done();
        });
      });
    });

    describe('three parents', function() {
      var name = '_findLCAsTwoPerspectivesThreeParents';

      var AI = { h: { id: 'foo', v: 'Aaaaaaaa', pe: 'I', pa: [] } };
      var BI = { h: { id: 'foo', v: 'Bbbbbbbb', pe: 'I', pa: ['Aaaaaaaa'] } };
      var CI = { h: { id: 'foo', v: 'Cccccccc', pe: 'I', pa: ['Bbbbbbbb'] } };
      var DI = { h: { id: 'foo', v: 'Dddddddd', pe: 'I', pa: ['Bbbbbbbb'] } };
      var EI = { h: { id: 'foo', v: 'Eeeeeeee', pe: 'I', pa: ['Cccccccc', 'Ffffffff', 'Dddddddd'] } };
      var FI = { h: { id: 'foo', v: 'Ffffffff', pe: 'I', pa: ['Bbbbbbbb', 'Cccccccc', 'Dddddddd'] } };
      var GI = { h: { id: 'foo', v: 'Gggggggg', pe: 'I', pa: ['Dddddddd', 'Ffffffff'] } };
      var HI = { h: { id: 'foo', v: 'H', pe: 'I', pa: ['Eeeeeeee'] } };
      var II = { h: { id: 'foo', v: 'I', pe: 'I', pa: ['Ffffffff', 'Eeeeeeee', 'Gggggggg'] } };
      var JI = { h: { id: 'foo', v: 'J', pe: 'I', pa: ['Gggggggg', 'Eeeeeeee'] } };

      var AII = { h: { id: 'foo', v: 'Aaaaaaaa', pe: 'II', pa: [] } };
      var BII = { h: { id: 'foo', v: 'Bbbbbbbb', pe: 'II', pa: ['Aaaaaaaa'] } };
      var CII = { h: { id: 'foo', v: 'Cccccccc', pe: 'II', pa: ['Bbbbbbbb'] } };
      var DII = { h: { id: 'foo', v: 'Dddddddd', pe: 'II', pa: ['Bbbbbbbb'] } };
      var EII = { h: { id: 'foo', v: 'Eeeeeeee', pe: 'II', pa: ['Cccccccc', 'Ffffffff', 'Dddddddd'] } };
      var FII = { h: { id: 'foo', v: 'Ffffffff', pe: 'II', pa: ['Bbbbbbbb', 'Cccccccc', 'Dddddddd'] } };
      var GII = { h: { id: 'foo', v: 'Gggggggg', pe: 'II', pa: ['Dddddddd', 'Ffffffff'] } };
      var HII = { h: { id: 'foo', v: 'H', pe: 'II', pa: ['Eeeeeeee'] } };
      var III = { h: { id: 'foo', v: 'I', pe: 'II', pa: ['Ffffffff', 'Eeeeeeee', 'Gggggggg'] } };
      var JII = { h: { id: 'foo', v: 'J', pe: 'II', pa: ['Gggggggg', 'Eeeeeeee'] } };

      // create the following structure:
      //         C <-- E <-- H
      //        / \ / / \
      //       B <-- F <-- I
      //        \ /   \ / \         
      //         D <-- G <-- J
      it('should save DAG mixed branches and mixed perspectives', function(done) {
        vc._snapshotCollection.insert([AII, BII, CII, DII, BI, CI, FII, EII, DI, FI, HII, EI, HI, GI, GII, III, JII, II, JI], {w: 1}, done);
      });

      it('JI and HII = E', function(done) {
        vc._findLCAs(JI, HII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Eeeeeeee']);
          done();
        });
      });

      it('GI and EII = F', function(done) {
        vc._findLCAs(GI, EII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Ffffffff']);
          done();
        });
      });

      it('FI and EII = F', function(done) {
        vc._findLCAs(FI, EII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Ffffffff']);
          done();
        });
      });

      it('EI and FII = F', function(done) {
        vc._findLCAs(EI, FII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Ffffffff']);
          done();
        });
      });

      it('JI and III = G and E', function(done) {
        vc._findLCAs(JI, III, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Gggggggg', 'Eeeeeeee']);
          done();
        });
      });

      it('II and JII = G and E', function(done) {
        vc._findLCAs(II, JII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Gggggggg', 'Eeeeeeee']);
          done();
        });
      });

      it('AI and BII = error because AI is not in the database', function(done) {
        vc._findLCAs(AI, BII, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca A. perspectives: I, II');
          done();
        });
      });

      it('BII and AI = error because AI is not in the database', function(done) {
        vc._findLCAs(BII, AI, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca A. perspectives: II, I');
          done();
        });
      });

      it('AII and BI = error because AI is not in the database', function(done) {
        vc._findLCAs(AII, BI, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca A. perspectives: II, I');
          done();
        });
      });

      it('BI and AII = error because AII is not in the database', function(done) {
        vc._findLCAs(BI, AII, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca A. perspectives: I, II');
          done();
        });
      });

      it('BI and BII = B', function(done) {
        vc._findLCAs(BI, BII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('HI and BII = B', function(done) {
        vc._findLCAs(HI, BII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('BI and HII = B', function(done) {
        vc._findLCAs(BI, HII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('HI and EII = E', function(done) {
        vc._findLCAs(HI, EII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Eeeeeeee']);
          done();
        });
      });

      it('EI and HII = B', function(done) {
        vc._findLCAs(EI, HII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Eeeeeeee']);
          done();
        });
      });

      it('JI and DII = D', function(done) {
        vc._findLCAs(JI, DII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Dddddddd']);
          done();
        });
      });

      it('DI and JII = D', function(done) {
        vc._findLCAs(DI, JII, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Dddddddd']);
          done();
        });
      });
    });

    describe('virtual merge', function() {
      var name = '_findLCAsTwoPerspectivesVirtualMerge';

      var BI = { h: { id: 'foo', v: 'Bbbbbbbb', pe: 'I', pa: ['Aaaaaaaa'] } };
      var CI = { h: { id: 'foo', v: 'Cccccccc', pe: 'I', pa: ['Bbbbbbbb'] } };
      var DI = { h: { id: 'foo', v: 'Dddddddd', pe: 'I', pa: ['Bbbbbbbb'] } };
      var EI = { h: { id: 'foo', v: 'Eeeeeeee', pe: 'I', pa: ['Cccccccc', 'Ffffffff', 'Dddddddd'] } };
      var FI = { h: { id: 'foo', v: 'Ffffffff', pe: 'I', pa: ['Bbbbbbbb', 'Cccccccc', 'Dddddddd'] } };
      var GI = { h: { id: 'foo', v: 'Gggggggg', pe: 'I', pa: ['Dddddddd', 'Ffffffff'] } };
      var HI = { h: { id: 'foo', v: 'H', pe: 'I', pa: ['Eeeeeeee'] } };
      var II = { h: { id: 'foo', v: 'I', pe: 'I', pa: ['Ffffffff', 'Eeeeeeee', 'Gggggggg'] } };
      var JI = { h: { id: 'foo', v: 'J', pe: 'I', pa: ['Gggggggg', 'Eeeeeeee'] } };

      var AII = { h: { id: 'foo', v: 'Aaaaaaaa', pe: 'II', pa: [] } };
      var BII = { h: { id: 'foo', v: 'Bbbbbbbb', pe: 'II', pa: ['Aaaaaaaa'] } };
      var CII = { h: { id: 'foo', v: 'Cccccccc', pe: 'II', pa: ['Bbbbbbbb'] } };
      var DII = { h: { id: 'foo', v: 'Dddddddd', pe: 'II', pa: ['Bbbbbbbb'] } };
      var EII = { h: { id: 'foo', v: 'Eeeeeeee', pe: 'II', pa: ['Cccccccc', 'Ffffffff', 'Dddddddd'] } };
      var FII = { h: { id: 'foo', v: 'Ffffffff', pe: 'II', pa: ['Bbbbbbbb', 'Cccccccc', 'Dddddddd'] } };
      var GII = { h: { id: 'foo', v: 'Gggggggg', pe: 'II', pa: ['Dddddddd', 'Ffffffff'] } };
      var HII = { h: { id: 'foo', v: 'H', pe: 'II', pa: ['Eeeeeeee'] } };
      var III = { h: { id: 'foo', v: 'I', pe: 'II', pa: ['Ffffffff', 'Eeeeeeee', 'Gggggggg'] } };
      var JII = { h: { id: 'foo', v: 'J', pe: 'II', pa: ['Gggggggg', 'Eeeeeeee'] } };

      // create the following structure:
      //         C <-- E <-- H
      //        / \ / / \
      //       B <-- F <-- I
      //        \ /   \ / \         
      //         D <-- G <-- J
      it('should save DAG mixed branches and mixed perspectives', function(done) {
        vc._snapshotCollection.insert([AII, BII, CII, DII, BI, CI, FII, EII, DI, FI, HII, EI, HI, GI, GII, III, JII, II, JI], {w: 1}, done);
      });

      it('vm1 B and vm2 B = B', function(done) {
        var vm1 = { h: { id: 'foo', pe: 'II', pa: ['Bbbbbbbb'] } };
        var vm2 = { h: { id: 'foo', pe: 'I', pa: ['Bbbbbbbb'] } };
        vc._findLCAs(vm1, vm2, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('vm1 B and vm2 A = error because AI is not in the database', function(done) {
        var vm1 = { h: { id: 'foo', pe: 'II', pa: ['Bbbbbbbb'] } };
        var vm2 = { h: { id: 'foo', pe: 'I', pa: ['Aaaaaaaa'] } };
        vc._findLCAs(vm1, vm2, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca A. perspectives: II, I');
          done();
        });
      });

      it('vm1 C, D and vm2 G = C and D', function(done) {
        var vm1 = { h: { id: 'foo', pe: 'II', pa: ['Cccccccc', 'Dddddddd'] } };
        var vm2 = { h: { id: 'foo', pe: 'I', pa: ['Gggggggg'] } };
        vc._findLCAs(vm1, vm2, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Dddddddd', 'Cccccccc']);
          done();
        });
      });

      it('two vm\'s without parents = []', function(done) {
        var vm1 = { h: { id: 'foo', pe: 'I', pa: [] } };
        var vm2 = { h: { id: 'foo', pe: 'II', pa: [] } };
        vc._findLCAs(vm1, vm2, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, []);
          done();
        });
      });

      it('vm without parents and GI = []', function(done) {
        var vm = { h: { id: 'foo', pe: 'II', pa: [] } };
        vc._findLCAs(vm, GI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, []);
          done();
        });
      });

      it('vm C, D pe III and GI = []', function(done) {
        var vm = { h: { id: 'foo', pe: 'III', pa: ['Cccccccc', 'Dddddddd'] } };
        vc._findLCAs(vm, GI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, []);
          done();
        });
      });

      it('vm C, D and GI = C and D', function(done) {
        var vm = { h: { id: 'foo', pe: 'II', pa: ['Cccccccc', 'Dddddddd'] } };
        vc._findLCAs(vm, GI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Dddddddd', 'Cccccccc']);
          done();
        });
      });

      it('vm J and II = E, G', function(done) {
        var vm = { h: { id: 'foo', pe: 'II', pa: ['J'] } };
        vc._findLCAs(vm, II, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Gggggggg', 'Eeeeeeee']);
          done();
        });
      });

      it('vm E, F, G and II = E, F, G', function(done) {
        var vm = { h: { id: 'foo', pe: 'II', pa: ['Eeeeeeee', 'Ffffffff', 'Gggggggg'] } };
        vc._findLCAs(vm, II, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Eeeeeeee', 'Ffffffff', 'Gggggggg']);
          done();
        });
      });

      it('vm H, I, J and JI = I', function(done) {
        var vm = { h: { id: 'foo', pe: 'II', pa: ['H', 'I', 'J'] } };
        vc._findLCAs(vm, II, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['I']);
          done();
        });
      });

      it('vm G, H and II = G and E', function(done) {
        var vm = { h: { id: 'foo', pe: 'II', pa: ['Gggggggg', 'H'] } };
        vc._findLCAs(vm, II, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Gggggggg', 'Eeeeeeee']);
          done();
        });
      });

      it('vm I, J and FI = F', function(done) {
        var vm = { h: { id: 'foo', pe: 'II', pa: ['I', 'J'] } };
        vc._findLCAs(vm, FI, function(err, lca) {
          should.equal(err, null);
          should.deepEqual(lca, ['Ffffffff']);
          done();
        });
      });
    });

    describe('regression', function() {
      var name = '_findLCAsTwoPerspectivesRegressions';

      var itemIA =  {'_id':{'_co':'foo','_id':'Aaaaaaaa','_v':'Hr+ojSYQ','_pa':[],'_pe':'_local','_i':3947}};
      var itemIIA = {'_id':{'_co':'foo','_id':'Aaaaaaaa','_v':'Hr+ojSYQ','_pa':[],'_pe':'test2'}};
      var itemIB =  {'_id':{'_co':'foo','_id':'Aaaaaaaa','_v':'p3oGRFGC','_pa':['Hr+ojSYQ'],'_pe':'_local','_i':3948}};
      var itemIIB = {'_id':{'_co':'foo','_id':'Aaaaaaaa','_v':'p3oGRFGC','_pa':['Hr+ojSYQ'],'_pe':'test2'}};

      it('needs the following items', function(done) {
        var DAG = [itemIA, itemIB];
        vc._snapshotCollection.insert(DAG, { w: 1 }, done);
      });

      it('should find the version itself to be the lca of two roots from different perspectives with the same version', function(done) {
        var ac = new ArrayCollection([itemIIA, itemIIB]);
        vc._virtualCollection = new ConcatMongoCollection([vc._snapshotCollection, ac]);

        var newThis = {
          _log: silence,
          databaseName: vc.databaseName,
          localPerspective: vc.localPerspective,
          versionKey: vc.versionKey,
          name: vc.name,
          _snapshotCollection: vc._virtualCollection,
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

    describe('with virtual collection', function() {
      var name = '_findLCAsRegressionNonSymmetricMultipleLca';

      var AI  = { h: { id: 'foo', v: 'Aaaaaaaa', pe: 'I',  pa: [], _i: 1}, _m3: { _ack: true } };
      var AII = { h: { id: 'foo', v: 'Aaaaaaaa', pe: 'II', pa: [] },       _m3: { _ack: false } };

      var BII = { h: { id: 'foo', v: 'Bbbbbbbb', pe: 'II', pa: ['Aaaaaaaa'] }, _m3: {} };

      // create the following structure, for _pe I and II:
      // _pe I
      //  A
      //
      // _pe II
      //  A
      //
      it('should save DAGs', function(done) {
        vc._snapshotCollection.insert([AI, AII], done);
      });

      it('BII and AI = A', function(done) {
        var ac = new ArrayCollection([BII]);
        vc._virtualCollection = new ConcatMongoCollection([vc._snapshotCollection, ac]);

        // create a new context with _snapshotCollection set to _virtualCollection
        var newThis = {
          _log: silence,
          databaseName: vc.databaseName,
          localPerspective: vc.localPerspective,
          versionKey: vc.versionKey,
          name: vc.name,
          _snapshotCollection: vc._virtualCollection,
          _findLCAs: vc._findLCAs
        };

        newThis._findLCAs(BII, AI, function(err, lcas) {
          if (err) { throw err; }
          should.equal(lcas.length, 1);
          should.deepEqual(lcas, ['Aaaaaaaa']);
          done();
        });
      });

      it('BII and AI = A, should not append to found lcas after callback is called', function(done) {
        var ac = new ArrayCollection([BII]);
        vc._virtualCollection = new ConcatMongoCollection([vc._snapshotCollection, ac]);

        // create a new context with _snapshotCollection set to _virtualCollection
        var newThis = {
          _log: silence,
          databaseName: vc.databaseName,
          localPerspective: vc.localPerspective,
          versionKey: vc.versionKey,
          name: vc.name,
          _snapshotCollection: vc._virtualCollection,
          _findLCAs: vc._findLCAs
        };

        newThis._findLCAs(BII, AI, function(err, lcas) {
          if (err) { throw err; }
          should.equal(lcas.length, 1);
          should.deepEqual(lcas, ['Aaaaaaaa']);

          setTimeout(function() {
            should.equal(lcas.length, 1);
            should.deepEqual(lcas, ['Aaaaaaaa']);
            done();
          }, 10);
        });
      });
    });
  });
});
