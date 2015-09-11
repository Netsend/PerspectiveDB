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

var should = require('should');
var streamify = require('stream-array');

var findLCAs = require('../../../lib/find_lcas');
var logger = require('../../../lib/logger');

var cons, silence;

// open database
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

describe('findLCAs', function() {
  describe('one perspective', function() {
    describe('one merge', function() {
      // create the following structure:
      // A <-- B <-- C <-- D
      //        \     \             
      //         E <-- F <-- G

      var A = { v: 'Aaaaaaaa', pa: [] };
      var B = { v: 'Bbbbbbbb', pa: ['Aaaaaaaa'] };
      var C = { v: 'Cccccccc', pa: ['Bbbbbbbb'] };
      var D = { v: 'Dddddddd', pa: ['Cccccccc'] };
      var E = { v: 'Eeeeeeee', pa: ['Bbbbbbbb'] };
      var F = { v: 'Ffffffff', pa: ['Eeeeeeee', 'Cccccccc'] };
      var G = { v: 'Gggggggg', pa: ['Ffffffff'] };
      var H = { v: 'Hhhhhhhh', pa: [] };


      var DAG = [A, B, C, D, E, F, G, H];

      // create graphs that start at the leaf, might contain multiple roots
      var dA = DAG.slice(0, 1).reverse();
      var dB = DAG.slice(0, 2).reverse();
      var dC = DAG.slice(0, 3).reverse();
      var dD = DAG.slice(0, 4).reverse();
      var dE = DAG.slice(0, 5).reverse();
      var dF = DAG.slice(0, 6).reverse();
      var dG = DAG.slice(0, 7).reverse();
      var dH = DAG.slice(0, 8).reverse();

      it('should require sX to be a stream.Readable', function() {
        (function() { findLCAs(null); }).should.throw('sX must be a stream.Readable');
      });

      it('should require sY to be a stream.Readable', function() {
        (function() { findLCAs({}, null); }).should.throw('sY must be a stream.Readable');
      });

      it('should require cb to be a function (undefined)', function() {
        (function() { findLCAs({}, {}); }).should.throw('cb must be a function');
      });

      it('should require cb to be a function (null)', function() {
        (function() { findLCAs({}, {}, null); }).should.throw('cb must be a function');
      });

      it('should require opts to be an object', function() {
        (function() { findLCAs({}, {}, [], function() {}); }).should.throw('opts must be an object');
      });

      it('A and A = A', function(done) {
        var x = streamify(dA);
        var y = streamify(dA);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [A.v]);
          done();
        });
      });

      it('A and B = A', function(done) {
        var x = streamify(dA);
        var y = streamify(dB);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [A.v]);
          done();
        });
      });

      it('B and B = B', function(done) {
        var x = streamify(dB);
        var y = streamify(dB);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [B.v]);
          done();
        });
      });

      it('C and D = C', function(done) {
        var x = streamify(dC);
        var y = streamify(dD);

        findLCAs(x, y, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [C.v]);
          done();
        });
      });

      it('D and D = D', function(done) {
        var x = streamify(dD);
        var y = streamify(dD);

        findLCAs(x, y, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [D.v]);
          done();
        });
      });

      it('C and E = B', function(done) {
        var x = streamify(dC);
        var y = streamify(dE);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [B.v]);
          done();
        });
      });

      it('D and F = C', function(done) {
        var x = streamify(dD);
        var y = streamify(dF);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [C.v]);
          done();
        });
      });

      it('F and G = F', function(done) {
        var x = streamify(dF);
        var y = streamify(dG);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [F.v]);
          done();
        });
      });

      it('F and C = C', function(done) {
        var x = streamify(dF);
        var y = streamify(dC);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [C.v]);
          done();
        });
      });

      it('D and E = B', function(done) {
        var x = streamify(dD);
        var y = streamify(dE);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [B.v]);
          done();
        });
      });

      it('E and D = B', function(done) {
        var x = streamify(dE);
        var y = streamify(dD);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [B.v]);
          done();
        });
      });

      it('G and B = B', function(done) {
        var x = streamify(dG);
        var y = streamify(dB);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [B.v]);
          done();
        });
      });

      it('should not find disconnected roots', function(done) {
        var x = streamify(dA);
        var y = streamify(dH);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, []);
          done();
        });
      });
    });

    describe('two merges', function() {
      // create the following structure:
      // A <-- C <-- E
      //  \     \     \
      //   B <-- D <-- F

      var A = { v: 'Aaaaaaaa', pa: [] };
      var B = { v: 'Bbbbbbbb', pa: ['Aaaaaaaa'] };
      var C = { v: 'Cccccccc', pa: ['Aaaaaaaa'] };
      var D = { v: 'Dddddddd', pa: ['Bbbbbbbb', 'Cccccccc'] };
      var E = { v: 'Eeeeeeee', pa: ['Cccccccc'] };
      var F = { v: 'Ffffffff', pa: ['Dddddddd', 'Eeeeeeee'] };

      var DAG = [A, B, C, D, E, F];

      // create graphs that start at the leaf, might contain multiple roots
      //var dA = DAG.slice(0, 1).reverse();
      var dB = DAG.slice(0, 2).reverse();
      //var dC = DAG.slice(0, 3).reverse();
      var dD = DAG.slice(0, 4).reverse();
      var dE = DAG.slice(0, 5).reverse();
      var dF = DAG.slice(0, 6).reverse();

      it('B and D = C', function(done) {
        var x = streamify(dB);
        var y = streamify(dD);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [B.v]);
          done();
        });
      });

      it('D and B = C', function(done) {
        var x = streamify(dD);
        var y = streamify(dB);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [B.v]);
          done();
        });
      });

      it('F and E = E', function(done) {
        var x = streamify(dF);
        var y = streamify(dE);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [E.v]);
          done();
        });
      });

      it('E and F = E', function(done) {
        var x = streamify(dE);
        var y = streamify(dF);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [E.v]);
          done();
        });
      });

      it('F and D = D', function(done) {
        var x = streamify(dF);
        var y = streamify(dD);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [D.v]);
          done();
        });
      });

      it('D and F = C', function(done) {
        var x = streamify(dD);
        var y = streamify(dF);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [D.v]);
          done();
        });
      });

      it('E and D = C', function(done) {
        var x = streamify(dE);
        var y = streamify(dD);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [C.v]);
          done();
        });
      });

      it('D and E = C', function(done) {
        var x = streamify(dD);
        var y = streamify(dE);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [C.v]);
          done();
        });
      });
    });

    describe('two merges (closer and different sort)', function() {
      // create the following structure:
      // A <-- B <-- C <-- D
      //        \  /  \             
      //         E <-- F <-- G
      var A = { v: 'Aaaaaaaa', pa: [] };
      var B = { v: 'Bbbbbbbb', pa: ['Aaaaaaaa'] };
      var C = { v: 'Cccccccc', pa: ['Bbbbbbbb', 'Eeeeeeee'] };
      var D = { v: 'Dddddddd', pa: ['Cccccccc'] };
      var E = { v: 'Eeeeeeee', pa: ['Bbbbbbbb'] };
      var F = { v: 'Ffffffff', pa: ['Eeeeeeee', 'Cccccccc'] };
      var G = { v: 'Gggggggg', pa: ['Ffffffff'] };

      var DAG = [A, B, E, C, D, F, G];

      // create graphs that start at the leaf, might contain multiple roots
      //var dA = DAG.slice(0, 1).reverse();
      var dB = DAG.slice(0, 2).reverse();
      var dE = DAG.slice(0, 3).reverse();
      var dC = DAG.slice(0, 4).reverse();
      var dD = DAG.slice(0, 5).reverse();
      var dF = DAG.slice(0, 6).reverse();
      var dG = DAG.slice(0, 6).reverse();

      it('C and E = E', function(done) {
        var x = streamify(dC);
        var y = streamify(dE);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [E.v]);
          done();
        });
      });

      it('D and F = C', function(done) {
        var x = streamify(dD);
        var y = streamify(dF);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [C.v]);
          done();
        });
      });

      it('F and C = C', function(done) {
        var x = streamify(dF);
        var y = streamify(dC);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [C.v]);
          done();
        });
      });

      it('D and E = E', function(done) {
        var x = streamify(dD);
        var y = streamify(dE);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [E.v]);
          done();
        });
      });

      it('E and D = E', function(done) {
        var x = streamify(dE);
        var y = streamify(dD);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [E.v]);
          done();
        });
      });

      it('G and B = B', function(done) {
        var x = streamify(dG);
        var y = streamify(dB);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [B.v]);
          done();
        });
      });
    });

    describe('three merges, criss-cross', function() {
      // create the following structure:
      //         C <-- E
      //        / \  /   \
      // A <-- B    X     G
      //        \  /  \  /          
      //         D <-- F

      var A = { v: 'Aaaaaaaa', pa: [] };
      var B = { v: 'Bbbbbbbb', pa: ['Aaaaaaaa'] };
      var C = { v: 'Cccccccc', pa: ['Bbbbbbbb'] };
      var D = { v: 'Dddddddd', pa: ['Bbbbbbbb'] };
      var E = { v: 'Eeeeeeee', pa: ['Cccccccc', 'Dddddddd'] };
      var F = { v: 'Ffffffff', pa: ['Dddddddd', 'Cccccccc'] };
      var G = { v: 'Gggggggg', pa: ['Ffffffff', 'Eeeeeeee'] };

      var DAG = [A, B, C, D, E, F, G];

      // create graphs that start at the leaf, might contain multiple roots
      var dA = DAG.slice(0, 1).reverse();
      var dB = DAG.slice(0, 2).reverse();
      var dC = DAG.slice(0, 3).reverse();
      var dD = DAG.slice(0, 4).reverse();
      var dE = DAG.slice(0, 5).reverse();
      var dF = DAG.slice(0, 6).reverse();
      var dG = DAG.slice(0, 6).reverse();

      it('A and B = A', function(done) {
        var x = streamify(dA);
        var y = streamify(dB);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [A.v]);
          done();
        });
      });

      it('B and B = B', function(done) {
        var x = streamify(dB);
        var y = streamify(dB);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [B.v]);
          done();
        });
      });

      it('C and D = B', function(done) {
        var x = streamify(dC);
        var y = streamify(dD);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [B.v]);
          done();
        });
      });

      it('D and D = D', function(done) {
        var x = streamify(dD);
        var y = streamify(dD);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [D.v]);
          done();
        });
      });

      it('C and E = C', function(done) {
        var x = streamify(dC);
        var y = streamify(dE);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [C.v]);
          done();
        });
      });

      it('D and F = D', function(done) {
        var x = streamify(dD);
        var y = streamify(dF);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [D.v]);
          done();
        });
      });

      it('F and G = F', function(done) {
        var x = streamify(dF);
        var y = streamify(dG);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [F.v]);
          done();
        });
      });

      it('F and C = C', function(done) {
        var x = streamify(dF);
        var y = streamify(dC);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [C.v]);
          done();
        });
      });

      it('D and E = D', function(done) {
        var x = streamify(dD);
        var y = streamify(dE);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [D.v]);
          done();
        });
      });

      it('E and D = D', function(done) {
        var x = streamify(dE);
        var y = streamify(dD);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [D.v]);
          done();
        });
      });

      it('G and B = B', function(done) {
        var x = streamify(dG);
        var y = streamify(dB);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [B.v]);
          done();
        });
      });

      it('E and F = C and D', function(done) {
        var x = streamify(dE);
        var y = streamify(dF);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [D.v, C.v]);
          done();
        });
      });
    });

    describe('n-parents', function() {
      // create the following structure:
      // A <-- B <-- C <----- D
      //        \     \        \
      //         E <-- F <-- G  \
      //                \     \  \      
      //                 H <------- I <-- L
      //                  \
      //                   J <-- K

      var A = { v: 'Aaaaaaaa', pa: [] };
      var B = { v: 'Bbbbbbbb', pa: ['Aaaaaaaa'] };
      var C = { v: 'Cccccccc', pa: ['Bbbbbbbb'] };
      var D = { v: 'Dddddddd', pa: ['Cccccccc'] };
      var E = { v: 'Eeeeeeee', pa: ['Bbbbbbbb'] };
      var F = { v: 'Ffffffff', pa: ['Eeeeeeee', 'Cccccccc'] };
      var G = { v: 'Gggggggg', pa: ['Ffffffff'] };
      var H = { v: 'Hhhhhhhh', pa: ['Ffffffff'] };
      var J = { v: 'Jjjjjjjj', pa: ['Hhhhhhhh'] };
      var K = { v: 'Kkkkkkkk', pa: ['Jjjjjjjj'] };
      var I = { v: 'Iiiiiiii', pa: ['Hhhhhhhh', 'Gggggggg', 'Dddddddd'] };
      var L = { v: 'Llllllll', pa: ['Iiiiiiii'] };

      var DAG = [A, B, C, D, E, F, G, H, J, K, I, L];

      // create graphs that start at the leaf, might contain multiple roots
      //var dA = DAG.slice(0, 1).reverse();
      //var dB = DAG.slice(0, 2).reverse();
      var dC = DAG.slice(0, 3).reverse();
      var dD = DAG.slice(0, 4).reverse();
      //var dE = DAG.slice(0, 5).reverse();
      //var dF = DAG.slice(0, 6).reverse();
      var dG = DAG.slice(0, 7).reverse();
      //var dH = DAG.slice(0, 8).reverse();
      var dJ = DAG.slice(0, 9).reverse();
      var dK = DAG.slice(0, 10).reverse();
      var dI = DAG.slice(0, 11).reverse();
      var dL = DAG.slice(0, 12).reverse();

      it('J and K = J', function(done) {
        var x = streamify(dJ);
        var y = streamify(dK);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Jjjjjjjj']);
          done();
        });
      });

      it('I and K = H', function(done) {
        var x = streamify(dI);
        var y = streamify(dK);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Hhhhhhhh']);
          done();
        });
      });

      it('I and G = G', function(done) {
        var x = streamify(dI);
        var y = streamify(dG);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Gggggggg']);
          done();
        });
      });

      it('I and D = D', function(done) {
        var x = streamify(dI);
        var y = streamify(dD);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Dddddddd']);
          done();
        });
      });

      it('I and L = I', function(done) {
        var x = streamify(dI);
        var y = streamify(dL);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Iiiiiiii']);
          done();
        });
      });

      it('L and C = C', function(done) {
        var x = streamify(dL);
        var y = streamify(dC);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Cccccccc']);
          done();
        });
      });

      it('L and J = H', function(done) {
        var x = streamify(dL);
        var y = streamify(dJ);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Hhhhhhhh']);
          done();
        });
      });
    });

    describe('three parents (missing root A)', function() {
      // create the following structure:
      //         C <-- E <-- H
      //        / \ / / \
      //       B <-- F <-- I
      //        \ /   \ / \         
      //         D <-- G <-- J

      //var A = { v: 'Aaaaaaaa', pa: [] };
      var B = { v: 'Bbbbbbbb', pa: ['Aaaaaaaa'] };
      var C = { v: 'Cccccccc', pa: ['Bbbbbbbb'] };
      var D = { v: 'Dddddddd', pa: ['Bbbbbbbb'] };
      var E = { v: 'Eeeeeeee', pa: ['Cccccccc', 'Ffffffff', 'Dddddddd'] };
      var F = { v: 'Ffffffff', pa: ['Bbbbbbbb', 'Cccccccc', 'Dddddddd'] };
      var G = { v: 'Gggggggg', pa: ['Dddddddd', 'Ffffffff'] };
      var H = { v: 'Hhhhhhhh', pa: ['Eeeeeeee'] };
      var I = { v: 'Iiiiiiii', pa: ['Ffffffff', 'Eeeeeeee', 'Gggggggg'] };
      var J = { v: 'Jjjjjjjj', pa: ['Gggggggg', 'Eeeeeeee'] };

      var DAG = [B, C, D, F, E, H, G, I, J];

      // create graphs that start at the leaf, might contain multiple roots
      var dB = DAG.slice(0, 1).reverse();
      //var dC = DAG.slice(0, 2).reverse();
      var dD = DAG.slice(0, 3).reverse();
      var dF = DAG.slice(0, 4).reverse();
      var dE = DAG.slice(0, 5).reverse();
      var dH = DAG.slice(0, 6).reverse();
      var dG = DAG.slice(0, 7).reverse();
      var dI = DAG.slice(0, 8).reverse();
      var dJ = DAG.slice(0, 9).reverse();

      it('J and H = E', function(done) {
        var x = streamify(dJ);
        var y = streamify(dH);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Eeeeeeee']);
          done();
        });
      });

      it('G and E = F and D', function(done) {
        var x = streamify(dG);
        var y = streamify(dE);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Ffffffff', 'Dddddddd']);
          done();
        });
      });

      it('F and E = F', function(done) {
        var x = streamify(dF);
        var y = streamify(dE);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Ffffffff']);
          done();
        });
      });

      it('E and F = F', function(done) {
        var x = streamify(dE);
        var y = streamify(dF);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Ffffffff']);
          done();
        });
      });

      it('J and I = G and E', function(done) {
        var x = streamify(dJ);
        var y = streamify(dI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Gggggggg', 'Eeeeeeee']);
          done();
        });
      });

      it('I and J = G and E', function(done) {
        var x = streamify(dI);
        var y = streamify(dJ);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Gggggggg', 'Eeeeeeee']);
          done();
        });
      });

      it('B and B = B', function(done) {
        var x = streamify(dB);
        var y = streamify(dB);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [B.v]);
          done();
        });
      });

      it('H and B = B', function(done) {
        var x = streamify(dH);
        var y = streamify(dB);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [B.v]);
          done();
        });
      });

      it('B and H = B', function(done) {
        var x = streamify(dB);
        var y = streamify(dH);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [B.v]);
          done();
        });
      });

      it('H and E = E', function(done) {
        var x = streamify(dH);
        var y = streamify(dE);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [E.v]);
          done();
        });
      });

      it('E and H = B', function(done) {
        var x = streamify(dE);
        var y = streamify(dH);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [E.v]);
          done();
        });
      });

      it('J and D = D', function(done) {
        var x = streamify(dJ);
        var y = streamify(dD);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [D.v]);
          done();
        });
      });

      it('D and J = D', function(done) {
        var x = streamify(dD);
        var y = streamify(dJ);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, [D.v]);
          done();
        });
      });
    });

    describe('virtual merge', function() {
      // create the following structure:
      //         C <-- E <-- H
      //        / \ / / \
      //       B <-- F <-- I
      //        \ /   \ / \         
      //         D <-- G <-- J

      var BI = { v: 'Bbbbbbbb', pa: ['Aaaaaaaa'] };
      var CI = { v: 'Cccccccc', pa: ['Bbbbbbbb'] };
      var DI = { v: 'Dddddddd', pa: ['Bbbbbbbb'] };
      var EI = { v: 'Eeeeeeee', pa: ['Cccccccc', 'Ffffffff', 'Dddddddd'] };
      var FI = { v: 'Ffffffff', pa: ['Bbbbbbbb', 'Cccccccc', 'Dddddddd'] };
      var GI = { v: 'Gggggggg', pa: ['Dddddddd', 'Ffffffff'] };
      var HI = { v: 'Hhhhhhhh', pa: ['Eeeeeeee'] };
      var II = { v: 'Iiiiiiii', pa: ['Ffffffff', 'Eeeeeeee', 'Gggggggg'] };
      var JI = { v: 'Jjjjjjjj', pa: ['Gggggggg', 'Eeeeeeee'] };

      var DAG = [BI, CI, DI, FI, EI, HI, GI, II, JI];

      // create graphs that start at the leaf, might contain multiple roots
      //var dB = DAG.slice(0, 1).reverse();
      //var dC = DAG.slice(0, 2).reverse();
      //var dD = DAG.slice(0, 3).reverse();
      var dF = DAG.slice(0, 4).reverse();
      //var dE = DAG.slice(0, 5).reverse();
      //var dH = DAG.slice(0, 6).reverse();
      var dG = DAG.slice(0, 7).reverse();
      var dI = DAG.slice(0, 8).reverse();
      var dJ = DAG.slice(0, 9).reverse();

      it('vm1 B and vm2 B = B', function(done) {
        var vm1 = { v: 'r', pa: ['Bbbbbbbb'] };
        var vm2 = { v: 't', pa: ['Bbbbbbbb'] };

        var x = streamify([vm1].concat(dJ));
        var y = streamify([vm2].concat(dJ));

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('vm1 B and vm2 A = []', function(done) {
        var vm1 = { v: 'r', pa: ['Bbbbbbbb'] };
        var vm2 = { v: 't', pa: ['Aaaaaaaa'] };

        var x = streamify([vm1].concat(dJ));
        var y = streamify([vm2].concat(dJ));

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, []);
          done();
        });
      });

      it('vm1 C, D and vm2 G = C and D', function(done) {
        var vm1 = { v: 'r', pa: ['Cccccccc', 'Dddddddd'] };
        var vm2 = { v: 't', pa: ['Gggggggg'] };

        var x = streamify([vm1].concat(dJ));
        var y = streamify([vm2].concat(dJ));

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Dddddddd', 'Cccccccc']);
          done();
        });
      });

      it('two vm\'s without parents = []', function(done) {
        var vm1 = { v: 'r', pa: [] };
        var vm2 = { v: 't', pa: [] };

        var x = streamify([vm1].concat(dJ));
        var y = streamify([vm2].concat(dJ));

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, []);
          done();
        });
      });

      it('vm without parents and G = []', function(done) {
        var vm1 = { v: 'r', pa: [] };

        var x = streamify([vm1].concat(dJ));
        var y = streamify(dG);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, []);
          done();
        });
      });


      it('vm C, D and GI = C and D', function(done) {
        var vm1 = { v: 'r', pa: ['Cccccccc', 'Dddddddd'] };

        var x = streamify([vm1].concat(dJ));
        var y = streamify(dG);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Dddddddd', 'Cccccccc']);
          done();
        });
      });

      it('vm J and II = E, G', function(done) {
        var vm1 = { v: 'r', pa: ['Jjjjjjjj'] };

        var x = streamify([vm1].concat(dJ));
        var y = streamify(dI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Gggggggg', 'Eeeeeeee']);
          done();
        });
      });

      it('vm E, F, G and II = E, F, G', function(done) {
        var vm1 = { v: 'r', pa: ['Eeeeeeee', 'Ffffffff', 'Gggggggg'] };

        var x = streamify([vm1].concat(dJ));
        var y = streamify(dI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Gggggggg', 'Eeeeeeee', 'Ffffffff']);
          done();
        });
      });

      it('vm H, I, J and I = I', function(done) {
        var vm1 = { v: 'r', pa: ['Hhhhhhhh', 'Iiiiiiii', 'Jjjjjjjj'] };

        var x = streamify([vm1].concat(dJ));
        var y = streamify(dI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Iiiiiiii']);
          done();
        });
      });

      it('vm H, I, J and J = J', function(done) {
        var vm1 = { v: 'r', pa: ['Hhhhhhhh', 'Iiiiiiii', 'Jjjjjjjj'] };

        var x = streamify([vm1].concat(dJ));
        var y = streamify(dJ);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Jjjjjjjj']);
          done();
        });
      });

      it('vm G, H and II = G and E', function(done) {
        var vm1 = { v: 'r', pa: ['Gggggggg', 'Hhhhhhhh'] };

        var x = streamify([vm1].concat(dJ));
        var y = streamify(dI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Gggggggg', 'Eeeeeeee']);
          done();
        });
      });

      it('vm I, J and FI = F', function(done) {
        var vm1 = { v: 'r', pa: ['Iiiiiiii', 'Jjjjjjjj'] };

        var x = streamify([vm1].concat(dJ));
        var y = streamify(dF);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Ffffffff']);
          done();
        });
      });
    });
  });

  describe('two perspectives', function() {
    describe('second import', function() {
      // create the following structure:
      //                                       EII <-- FII <-- GII <--------------------- HII <-- SII <-- JII <-- NII <-- PII
      //                                       /       /                                    \               \       \
      //                             AII <-- BII <-- CII                                    KII <-- RII <-- LII <-- OII <-- QII
      // AI <-- BI <-- CI                                         KI <-- RI <-- LI <-- MI
      //          \     \                                         /             /
      //          EI <-- FI <-- GI <---------------------------- HI <-- SI <-- JI 
      //

      var AI  = { v: 'Aaaaaaaa',  pa: [] };
      var BI  = { v: 'Bbbbbbbb',  pa: ['Aaaaaaaa'] };
      var CI  = { v: 'Cccccccc',  pa: ['Bbbbbbbb'] };
      var EI  = { v: 'Eeeeeeee',  pa: ['Bbbbbbbb'] };
      var FI  = { v: 'Ffffffff',  pa: ['Eeeeeeee', 'Cccccccc'] };
      var GI  = { v: 'Gggggggg',  pa: ['Ffffffff'] };
      var AII = { v: 'Aaaaaaaa', pa: [] };
      var BII = { v: 'Bbbbbbbb', pa: ['Aaaaaaaa'] };
      var CII = { v: 'Cccccccc', pa: ['Bbbbbbbb'] };
      var EII = { v: 'Eeeeeeee', pa: ['Bbbbbbbb'] };
      var FII = { v: 'Ffffffff', pa: ['Eeeeeeee', 'Cccccccc'] };
      var GII = { v: 'Gggggggg', pa: ['Ffffffff'] };
      var HI  = { v: 'Hhhhhhhh',  pa: ['Gggggggg'] };
      var KI  = { v: 'Kkkkkkkk',  pa: ['Hhhhhhhh'] };
      var SI  = { v: 'Ssssssss',  pa: ['Hhhhhhhh'] };
      var RI  = { v: 'Rrrrrrrr',  pa: ['Kkkkkkkk'] };
      var JI  = { v: 'Jjjjjjjj',  pa: ['Ssssssss'] };
      var LI  = { v: 'Llllllll',  pa: ['Rrrrrrrr', 'Jjjjjjjj'] };
      var MI  = { v: 'Mmmmmmmm',  pa: ['Llllllll'] };
      var HII = { v: 'Hhhhhhhh', pa: ['Gggggggg'] };
      var KII = { v: 'Kkkkkkkk', pa: ['Hhhhhhhh'] };
      var SII = { v: 'Ssssssss', pa: ['Hhhhhhhh'] };
      var RII = { v: 'Rrrrrrrr', pa: ['Kkkkkkkk'] };
      var JII = { v: 'Jjjjjjjj', pa: ['Ssssssss'] };
      var LII = { v: 'Llllllll', pa: ['Rrrrrrrr', 'Jjjjjjjj'] };
      var NII = { v: 'Nnnnnnnn', pa: ['Jjjjjjjj'] };
      var OII = { v: 'Oooooooo', pa: ['Llllllll', 'Nnnnnnnn'] };
      var PII = { v: 'Pppppppp', pa: ['Nnnnnnnn'] };
      var QII = { v: 'Qqqqqqqq', pa: ['Oooooooo'] };

      var DAGI =  [ AI,  BI,  CI,  EI,  FI,  GI,  HI,  KI,  SI,  RI,  JI,  LI,  MI ];
      var DAGII = [ AII, BII, CII, EII, FII, GII, HII, KII, SII, RII, JII, LII, NII, OII, PII, QII ];

      // create graphs that start at the leaf, might contain multiple roots
      var dAI = DAGI.slice(0, 1).reverse();
      var dBI = DAGI.slice(0, 2).reverse();
      //var dCI = DAGI.slice(0, 3).reverse();
      //var dEI = DAGI.slice(0, 4).reverse();
      var dFI = DAGI.slice(0, 5).reverse();
      //var dGI = DAGI.slice(0, 6).reverse();
      var dHI = DAGI.slice(0, 7).reverse();
      //var dKI = DAGI.slice(0, 8).reverse();
      //var dSI = DAGI.slice(0, 9).reverse();
      var dRI = DAGI.slice(0, 10).reverse();
      //var dJI = DAGI.slice(0, 11).reverse();
      //var dLI = DAGI.slice(0, 12).reverse();
      var dMI = DAGI.slice(0, 13).reverse();

      var dAII = DAGII.slice(0, 1).reverse();
      var dBII = DAGII.slice(0, 2).reverse();
      //var dCII = DAGII.slice(0, 3).reverse();
      //var dEII = DAGII.slice(0, 4).reverse();
      //var dFII = DAGII.slice(0, 5).reverse();
      var dGII = DAGII.slice(0, 6).reverse();
      var dHII = DAGII.slice(0, 7).reverse();
      var dKII = DAGII.slice(0, 8).reverse();
      //var dSII = DAGII.slice(0, 9).reverse();
      var dRII = DAGII.slice(0, 10).reverse();
      //var dJII = DAGII.slice(0, 11).reverse();
      var dLII = DAGII.slice(0, 12).reverse();
      //var dNII = DAGII.slice(0, 13).reverse();
      //var dOII = DAGII.slice(0, 14).reverse();
      var dPII = DAGII.slice(0, 15).reverse();
      var dQII = DAGII.slice(0, 16).reverse();

      it('should not find nodes that are not in the DAG', function(done) {
        var item1 = { v: 'r1', pa: [] };
        var item2 = { v: 'r2', pa: [] };

        var x = streamify([item1]);
        var y = streamify([item2]);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, []);
          done();
        });
      });

      it('GII and RI = G', function(done) {
        var x = streamify(dGII);
        var y = streamify(dRI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Gggggggg']);
          done();
        });
      });

      it('LII and RI = R', function(done) {
        var x = streamify(dLII);
        var y = streamify(dRI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Rrrrrrrr']);
          done();
        });
      });

      it('RII and MI = R', function(done) {
        var x = streamify(dRII);
        var y = streamify(dMI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Rrrrrrrr']);
          done();
        });
      });

      it('LII and MI = L', function(done) {
        var x = streamify(dLII);
        var y = streamify(dMI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Llllllll']);
          done();
        });
      });

      it('KII and MI = K', function(done) {
        var x = streamify(dKII);
        var y = streamify(dMI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Kkkkkkkk']);
          done();
        });
      });

      it('KII and HI = H', function(done) {
        var x = streamify(dKII);
        var y = streamify(dHI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Hhhhhhhh']);
          done();
        });
      });

      it('HII and HI = H', function(done) {
        var x = streamify(dHII);
        var y = streamify(dHI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Hhhhhhhh']);
          done();
        });
      });

      it('PII and QII = N', function(done) {
        var x = streamify(dPII);
        var y = streamify(dQII);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Nnnnnnnn']);
          done();
        });
      });

      it('PII and MI = J', function(done) {
        var x = streamify(dPII);
        var y = streamify(dMI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Jjjjjjjj']);
          done();
        });
      });

      it('QII and MI = L', function(done) {
        var x = streamify(dQII);
        var y = streamify(dMI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Llllllll']);
          done();
        });
      });

      it('AI and AII = A', function(done) {
        var x = streamify(dAI);
        var y = streamify(dAII);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Aaaaaaaa']);
          done();
        });
      });

      it('AII and AI = A', function(done) {
        var x = streamify(dAII);
        var y = streamify(dAI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Aaaaaaaa']);
          done();
        });
      });

      it('BI and BII = B', function(done) {
        var x = streamify(dBI);
        var y = streamify(dBII);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('BII and BI = A', function(done) {
        var x = streamify(dBII);
        var y = streamify(dBI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('FI and BII = B', function(done) {
        var x = streamify(dFI);
        var y = streamify(dBII);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      /////////// BREAK THE GRAPH ON PURPOSE

      it('remove GII and break DAGII on purpose', function() {
        DAGII.splice(5, 1);
      });

      it('FI and HII = [] because link GII is missing', function(done) {
        // reset dHII because of DAGII breakage
        dHII = DAGII.slice(0, 6).reverse();

        var x = streamify(dFI);
        var y = streamify(dHII);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, []);
          done();
        });
      });
    });

    describe('criss-cross merge import', function() {
      // create the following structure:
      //              CII - EII,EI
      //             /  \ /   \
      //           AII   X    FII,FI
      //             \  / \   /          
      //             BII - DII,DI
      //
      // AI <-- BI

      var AI  = { v: 'Aaaaaaaa', pa: [] };
      var BI  = { v: 'Bbbbbbbb', pa: ['Aaaaaaaa'] };
      var AII = { v: 'Aaaaaaaa', pa: [] };
      var BII = { v: 'Bbbbbbbb', pa: ['Aaaaaaaa'] };
      var CII = { v: 'Cccccccc', pa: ['Aaaaaaaa'] };
      var DII = { v: 'Dddddddd', pa: ['Bbbbbbbb', 'Cccccccc'] };
      var DI  = { v: 'Dddddddd', pa: ['Bbbbbbbb', 'Cccccccc'] };
      var EII = { v: 'Eeeeeeee', pa: ['Cccccccc', 'Bbbbbbbb'] };
      var EI  = { v: 'Eeeeeeee', pa: ['Cccccccc', 'Bbbbbbbb'] };
      var FII = { v: 'Ffffffff', pa: ['Dddddddd', 'Eeeeeeee'] };
      var FI  = { v: 'Ffffffff', pa: ['Dddddddd', 'Eeeeeeee'] };

      var DAGI  = [AI,  BI,       DI,  EI,  FI];
      var DAGII = [AII, BII, CII, DII, EII, FII];

      var dAI = DAGI.slice(0, 1).reverse();
      var dBI = DAGI.slice(0, 2).reverse();
      var dDI = DAGI.slice(0, 3).reverse();
      var dEI = DAGI.slice(0, 4).reverse();
      var dFI = DAGI.slice(0, 5).reverse();

      var dAII = DAGII.slice(0, 1).reverse();
      //var dBII = DAGII.slice(0, 2).reverse();
      var dCII = DAGII.slice(0, 3).reverse();
      var dDII = DAGII.slice(0, 4).reverse();
      var dEII = DAGII.slice(0, 5).reverse();
      var dFII = DAGII.slice(0, 6).reverse();

      it('EI and DII = only B because CI is missing', function(done) {
        var x = streamify(dEI);
        var y = streamify(dDII);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('EII and DI = only B because CI is missing', function(done) {
        var x = streamify(dEII);
        var y = streamify(dDI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('DII and EI = only B because CI is missing', function(done) {
        var x = streamify(dDII);
        var y = streamify(dEI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('DI and EII = only B because CI is missing', function(done) {
        var x = streamify(dDI);
        var y = streamify(dEII);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('AI and AII = A', function(done) {
        var x = streamify(dAI);
        var y = streamify(dAII);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Aaaaaaaa']);
          done();
        });
      });

      it('BI and CII = A', function(done) {
        var x = streamify(dBI);
        var y = streamify(dCII);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Aaaaaaaa']);
          done();
        });
      });

      it('FI and DII = D', function(done) {
        var x = streamify(dFI);
        var y = streamify(dDII);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Dddddddd']);
          done();
        });
      });

      it('FII and DI = D', function(done) {
        var x = streamify(dFII);
        var y = streamify(dDI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Dddddddd']);
          done();
        });
      });

      it('DII and FI = D', function(done) {
        var x = streamify(dDII);
        var y = streamify(dFI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Dddddddd']);
          done();
        });
      });

      it('DI and FII = D', function(done) {
        var x = streamify(dDI);
        var y = streamify(dFII);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Dddddddd']);
          done();
        });
      });

      it('FI and EII = E', function(done) {
        var x = streamify(dFI);
        var y = streamify(dEII);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Eeeeeeee']);
          done();
        });
      });

      it('FII and EI = E', function(done) {
        var x = streamify(dFII);
        var y = streamify(dEI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Eeeeeeee']);
          done();
        });
      });

      it('EII and FI = E', function(done) {
        var x = streamify(dEII);
        var y = streamify(dFI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Eeeeeeee']);
          done();
        });
      });

      it('EI and FII = E', function(done) {
        var x = streamify(dEI);
        var y = streamify(dFII);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Eeeeeeee']);
          done();
        });
      });
    });

    describe('criss-cross n-parents', function() {
      // create DAG with imported criss-cross merge with three parents
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

      var BI  = { v: 'Bbbbbbbb', pa: ['Aaaaaaaa'] };
      var CI  = { v: 'Cccccccc', pa: ['Bbbbbbbb'] };
      var DI  = { v: 'Dddddddd', pa: ['Bbbbbbbb'] };
      var EI  = { v: 'Eeeeeeee', pa: ['Bbbbbbbb'] };
      var FI  = { v: 'Ffffffff', pa: ['Cccccccc', 'Dddddddd', 'Eeeeeeee'] };
      var GI  = { v: 'Gggggggg', pa: ['Cccccccc', 'Dddddddd', 'Eeeeeeee'] };
      var BII = { v: 'Bbbbbbbb', pa: ['Aaaaaaaa'] };
      var CII = { v: 'Cccccccc', pa: ['Bbbbbbbb'] };
      var DII = { v: 'Dddddddd', pa: ['Bbbbbbbb'] };
      var EII = { v: 'Eeeeeeee', pa: ['Bbbbbbbb'] };
      var FII = { v: 'Ffffffff', pa: ['Cccccccc', 'Dddddddd', 'Eeeeeeee'] };
      var GII = { v: 'Gggggggg', pa: ['Cccccccc', 'Dddddddd', 'Eeeeeeee'] };

      var DAGI  = [BI,  CI,  DI,  EI,  FI,  GI];
      var DAGII = [BII, CII, DII, EII, FII, GII];

      //var dBI = DAGI.slice(0, 1).reverse();
      //var dCI = DAGI.slice(0, 2).reverse();
      //var dDI = DAGI.slice(0, 3).reverse();
      //var dEI = DAGI.slice(0, 4).reverse();
      var dFI = DAGI.slice(0, 5).reverse();
      //var dGI = DAGI.slice(0, 6).reverse();

      //var dBII = DAGII.slice(0, 1).reverse();
      //var dCII = DAGII.slice(0, 2).reverse();
      //var dDII = DAGII.slice(0, 3).reverse();
      //var dEII = DAGII.slice(0, 4).reverse();
      //var dFII = DAGII.slice(0, 5).reverse();
      var dGII = DAGII.slice(0, 6).reverse();

      it('GII and FI = C, D, E', function(done) {
        var x = streamify(dGII);
        var y = streamify(dFI);

        findLCAs(x, y, { log: silence }, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Eeeeeeee', 'Dddddddd', 'Cccccccc']);
          done();
        });
      });
    });

  /*
    describe('three parents', function() {
      var name = '_findLCAsTwoPerspectivesThreeParents';

      var AI = { h: { id: 'foo', v: 'Aaaaaaaa', pe: 'Iiiiiiii', pa: [] } };
      var BI = { h: { id: 'foo', v: 'Bbbbbbbb', pe: 'Iiiiiiii', pa: ['Aaaaaaaa'] } };
      var CI = { h: { id: 'foo', v: 'Cccccccc', pe: 'Iiiiiiii', pa: ['Bbbbbbbb'] } };
      var DI = { h: { id: 'foo', v: 'Dddddddd', pe: 'Iiiiiiii', pa: ['Bbbbbbbb'] } };
      var EI = { h: { id: 'foo', v: 'Eeeeeeee', pe: 'Iiiiiiii', pa: ['Cccccccc', 'Ffffffff', 'Dddddddd'] } };
      var FI = { h: { id: 'foo', v: 'Ffffffff', pe: 'Iiiiiiii', pa: ['Bbbbbbbb', 'Cccccccc', 'Dddddddd'] } };
      var GI = { h: { id: 'foo', v: 'Gggggggg', pe: 'Iiiiiiii', pa: ['Dddddddd', 'Ffffffff'] } };
      var HI = { h: { id: 'foo', v: 'Hhhhhhhh', pe: 'Iiiiiiii', pa: ['Eeeeeeee'] } };
      var II = { h: { id: 'foo', v: 'Iiiiiiii', pe: 'Iiiiiiii', pa: ['Ffffffff', 'Eeeeeeee', 'Gggggggg'] } };
      var JI = { h: { id: 'foo', v: 'Jjjjjjjj', pe: 'Iiiiiiii', pa: ['Gggggggg', 'Eeeeeeee'] } };

      var AII = { h: { id: 'foo', v: 'Aaaaaaaa', pe: 'II', pa: [] } };
      var BII = { h: { id: 'foo', v: 'Bbbbbbbb', pe: 'II', pa: ['Aaaaaaaa'] } };
      var CII = { h: { id: 'foo', v: 'Cccccccc', pe: 'II', pa: ['Bbbbbbbb'] } };
      var DII = { h: { id: 'foo', v: 'Dddddddd', pe: 'II', pa: ['Bbbbbbbb'] } };
      var EII = { h: { id: 'foo', v: 'Eeeeeeee', pe: 'II', pa: ['Cccccccc', 'Ffffffff', 'Dddddddd'] } };
      var FII = { h: { id: 'foo', v: 'Ffffffff', pe: 'II', pa: ['Bbbbbbbb', 'Cccccccc', 'Dddddddd'] } };
      var GII = { h: { id: 'foo', v: 'Gggggggg', pe: 'II', pa: ['Dddddddd', 'Ffffffff'] } };
      var HII = { h: { id: 'foo', v: 'Hhhhhhhh', pe: 'II', pa: ['Eeeeeeee'] } };
      var III = { h: { id: 'foo', v: 'Iiiiiiii', pe: 'II', pa: ['Ffffffff', 'Eeeeeeee', 'Gggggggg'] } };
      var JII = { h: { id: 'foo', v: 'Jjjjjjjj', pe: 'II', pa: ['Gggggggg', 'Eeeeeeee'] } };

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
        findLCAs(JI, HII, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Eeeeeeee']);
          done();
        });
      });

      it('GI and EII = F', function(done) {
        findLCAs(GI, EII, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Ffffffff']);
          done();
        });
      });

      it('FI and EII = F', function(done) {
        findLCAs(FI, EII, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Ffffffff']);
          done();
        });
      });

      it('EI and FII = F', function(done) {
        findLCAs(EI, FII, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Ffffffff']);
          done();
        });
      });

      it('JI and III = G and E', function(done) {
        findLCAs(JI, III, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Gggggggg', 'Eeeeeeee']);
          done();
        });
      });

      it('II and JII = G and E', function(done) {
        findLCAs(II, JII, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Gggggggg', 'Eeeeeeee']);
          done();
        });
      });

      it('AI and BII = error because AI is not in the database', function(done) {
        findLCAs(AI, BII, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca A. perspectives: I, II');
          done();
        });
      });

      it('BII and AI = error because AI is not in the database', function(done) {
        findLCAs(BII, AI, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca A. perspectives: II, I');
          done();
        });
      });

      it('AII and BI = error because AI is not in the database', function(done) {
        findLCAs(AII, BI, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca A. perspectives: II, I');
          done();
        });
      });

      it('BI and AII = error because AII is not in the database', function(done) {
        findLCAs(BI, AII, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca A. perspectives: I, II');
          done();
        });
      });

      it('BI and BII = B', function(done) {
        findLCAs(BI, BII, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('HI and BII = B', function(done) {
        findLCAs(HI, BII, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('BI and HII = B', function(done) {
        findLCAs(BI, HII, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('HI and EII = E', function(done) {
        findLCAs(HI, EII, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Eeeeeeee']);
          done();
        });
      });

      it('EI and HII = B', function(done) {
        findLCAs(EI, HII, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Eeeeeeee']);
          done();
        });
      });

      it('JI and DII = D', function(done) {
        findLCAs(JI, DII, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Dddddddd']);
          done();
        });
      });

      it('DI and JII = D', function(done) {
        findLCAs(DI, JII, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Dddddddd']);
          done();
        });
      });
    });

    describe('virtual merge', function() {
      var name = '_findLCAsTwoPerspectivesVirtualMerge';

      var BI = { h: { id: 'foo', v: 'Bbbbbbbb', pe: 'Iiiiiiii', pa: ['Aaaaaaaa'] } };
      var CI = { h: { id: 'foo', v: 'Cccccccc', pe: 'Iiiiiiii', pa: ['Bbbbbbbb'] } };
      var DI = { h: { id: 'foo', v: 'Dddddddd', pe: 'Iiiiiiii', pa: ['Bbbbbbbb'] } };
      var EI = { h: { id: 'foo', v: 'Eeeeeeee', pe: 'Iiiiiiii', pa: ['Cccccccc', 'Ffffffff', 'Dddddddd'] } };
      var FI = { h: { id: 'foo', v: 'Ffffffff', pe: 'Iiiiiiii', pa: ['Bbbbbbbb', 'Cccccccc', 'Dddddddd'] } };
      var GI = { h: { id: 'foo', v: 'Gggggggg', pe: 'Iiiiiiii', pa: ['Dddddddd', 'Ffffffff'] } };
      var HI = { h: { id: 'foo', v: 'Hhhhhhhh', pe: 'Iiiiiiii', pa: ['Eeeeeeee'] } };
      var II = { h: { id: 'foo', v: 'Iiiiiiii', pe: 'Iiiiiiii', pa: ['Ffffffff', 'Eeeeeeee', 'Gggggggg'] } };
      var JI = { h: { id: 'foo', v: 'Jjjjjjjj', pe: 'Iiiiiiii', pa: ['Gggggggg', 'Eeeeeeee'] } };

      var AII = { h: { id: 'foo', v: 'Aaaaaaaa', pe: 'II', pa: [] } };
      var BII = { h: { id: 'foo', v: 'Bbbbbbbb', pe: 'II', pa: ['Aaaaaaaa'] } };
      var CII = { h: { id: 'foo', v: 'Cccccccc', pe: 'II', pa: ['Bbbbbbbb'] } };
      var DII = { h: { id: 'foo', v: 'Dddddddd', pe: 'II', pa: ['Bbbbbbbb'] } };
      var EII = { h: { id: 'foo', v: 'Eeeeeeee', pe: 'II', pa: ['Cccccccc', 'Ffffffff', 'Dddddddd'] } };
      var FII = { h: { id: 'foo', v: 'Ffffffff', pe: 'II', pa: ['Bbbbbbbb', 'Cccccccc', 'Dddddddd'] } };
      var GII = { h: { id: 'foo', v: 'Gggggggg', pe: 'II', pa: ['Dddddddd', 'Ffffffff'] } };
      var HII = { h: { id: 'foo', v: 'Hhhhhhhh', pe: 'II', pa: ['Eeeeeeee'] } };
      var III = { h: { id: 'foo', v: 'Iiiiiiii', pe: 'II', pa: ['Ffffffff', 'Eeeeeeee', 'Gggggggg'] } };
      var JII = { h: { id: 'foo', v: 'Jjjjjjjj', pe: 'II', pa: ['Gggggggg', 'Eeeeeeee'] } };

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
        var vm2 = { h: { id: 'foo', pe: 'Iiiiiiii', pa: ['Bbbbbbbb'] } };
        findLCAs(vm1, vm2, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Bbbbbbbb']);
          done();
        });
      });

      it('vm1 B and vm2 A = error because AI is not in the database', function(done) {
        var vm1 = { h: { id: 'foo', pe: 'II', pa: ['Bbbbbbbb'] } };
        var vm2 = { h: { id: 'foo', pe: 'Iiiiiiii', pa: ['Aaaaaaaa'] } };
        findLCAs(vm1, vm2, function(err) {
          should.equal(err.message, 'missing at least one perspective when fetching lca A. perspectives: II, I');
          done();
        });
      });

      it('vm1 C, D and vm2 G = C and D', function(done) {
        var vm1 = { h: { id: 'foo', pe: 'II', pa: ['Cccccccc', 'Dddddddd'] } };
        var vm2 = { h: { id: 'foo', pe: 'Iiiiiiii', pa: ['Gggggggg'] } };
        findLCAs(vm1, vm2, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Dddddddd', 'Cccccccc']);
          done();
        });
      });

      it('two vm\'s without parents = []', function(done) {
        var vm1 = { h: { id: 'foo', pe: 'Iiiiiiii', pa: [] } };
        var vm2 = { h: { id: 'foo', pe: 'II', pa: [] } };
        findLCAs(vm1, vm2, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, []);
          done();
        });
      });

      it('vm without parents and GI = []', function(done) {
        var vm = { h: { id: 'foo', pe: 'II', pa: [] } };
        findLCAs(vm, GI, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, []);
          done();
        });
      });

      it('vm C, D pe III and GI = []', function(done) {
        var vm = { h: { id: 'foo', pe: 'III', pa: ['Cccccccc', 'Dddddddd'] } };
        findLCAs(vm, GI, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, []);
          done();
        });
      });

      it('vm C, D and GI = C and D', function(done) {
        var vm = { h: { id: 'foo', pe: 'II', pa: ['Cccccccc', 'Dddddddd'] } };
        findLCAs(vm, GI, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Dddddddd', 'Cccccccc']);
          done();
        });
      });

      it('vm J and II = E, G', function(done) {
        var vm = { h: { id: 'foo', pe: 'II', pa: ['Jjjjjjjj'] } };
        findLCAs(vm, II, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Gggggggg', 'Eeeeeeee']);
          done();
        });
      });

      it('vm E, F, G and II = E, F, G', function(done) {
        var vm = { h: { id: 'foo', pe: 'II', pa: ['Eeeeeeee', 'Ffffffff', 'Gggggggg'] } };
        findLCAs(vm, II, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Eeeeeeee', 'Ffffffff', 'Gggggggg']);
          done();
        });
      });

      it('vm H, I, J and JI = I', function(done) {
        var vm = { h: { id: 'foo', pe: 'II', pa: ['Hhhhhhhh', 'Iiiiiiii', 'Jjjjjjjj'] } };
        findLCAs(vm, II, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Iiiiiiii']);
          done();
        });
      });

      it('vm G, H and II = G and E', function(done) {
        var vm = { h: { id: 'foo', pe: 'II', pa: ['Gggggggg', 'Hhhhhhhh'] } };
        findLCAs(vm, II, function(err, lca) {
          if (err) { throw err; }
          should.deepEqual(lca, ['Gggggggg', 'Eeeeeeee']);
          done();
        });
      });

      it('vm I, J and FI = F', function(done) {
        var vm = { h: { id: 'foo', pe: 'II', pa: ['Iiiiiiii', 'Jjjjjjjj'] } };
        findLCAs(vm, FI, function(err, lca) {
          if (err) { throw err; }
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
          _findLCAs: findLCAs,
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

      var AI  = { h: { id: 'foo', v: 'Aaaaaaaa', pe: 'Iiiiiiii',  pa: [], _i: 1}, _m3: { _ack: true } };
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
          _findLCAs: findLCAs
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
          _findLCAs: findLCAs
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
    */
  });
});
