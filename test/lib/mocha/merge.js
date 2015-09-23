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
var rimraf = require('rimraf');
var level = require('level');
var async = require('async');

var merge = require('../../../lib/merge');
var Tree = require('../../../lib/tree');
var logger = require('../../../lib/logger');

var db, cons, silence;
var dbPath = require('os').tmpdir() + '/test_merge';

// open database
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

function saveDAG(DAG, tree, cb) {
  DAG.forEach(function(item) {
    tree.write(item);
  });
  tree.end(null, cb);
}

function saveDAGs(DAGI, DAGII, treeI, treeII, cb) {
  saveDAG(DAGI, treeI, function(err) {
    if (err) { throw err; }
    saveDAG(DAGII, treeII, cb);
  });
}

describe('merge', function() {
  var id = 'foo';

  describe('constructor', function() {
    it('should require itemX to be an object', function() {
      (function() { merge(null); }).should.throw('itemX must be an object');
    });

    it('should require itemY to be an object', function() {
      (function() { merge({}, null); }).should.throw('itemY must be an object');
    });

    it('should require treeX to be an object', function() {
      (function() { merge({}, {}, null); }).should.throw('treeX must be an object');
    });

    it('should require treeY to be an object', function() {
      (function() { merge({}, {}, {}, null); }).should.throw('treeY must be an object');
    });

    it('should require cb to be a function', function() {
      (function() { merge({}, {}, {}, {}); }).should.throw('cb must be a function');
    });

    it('should require opts to be an object', function() {
      (function() { merge({}, {}, {}, {}, [], function() {}); }).should.throw('opts must be an object');
    });

    it('should require that ids of itemX and itemY match', function(done) {
      var itemX = { h: { id: 'a' } };
      var itemY = { h: { id: 'b' } };

      var tree = new Tree(db, 'some', { vSize: 3, log: silence });

      merge(itemX, itemY, tree, tree, { log: silence }, function(err) {
        should.equal(err.message, 'id mismatch');
        done();
      });
    });

    it('must require a version on itemX', function(done) {
      var itemX = { h: { id: 'a' } };
      var itemY = { h: { id: 'a' } };

      var tree = new Tree(db, 'some', { vSize: 3, log: silence });

      merge(itemX, itemY, tree, tree, { log: silence }, function(err) {
        should.equal(err.message, 'itemX has no version');
        done();
      });
    });

    it('must require a version on itemY', function(done) {
      var itemX = { h: { id: 'a', v: 's' } };
      var itemY = { h: { id: 'a' } };

      var tree = new Tree(db, 'some', { vSize: 3, log: silence });

      merge(itemX, itemY, tree, tree, { log: silence }, function(err) {
        should.equal(err.message, 'itemY has no version');
        done();
      });
    });
  });

  describe('one perspective (tree)', function() {
    describe('basic', function() {
      var name = 'onePerspective';
      var tree;

      // create the following structure:
      //    C <- E
      //   / \ /  \
      //  A   X    F
      //   \ / \  /
      //    B <- D
      // see http://www.gelato.unsw.edu.au/archives/git/0504/2279.html

      var A = {
        h: { id: id, v: 'Aaaa', pa: [] },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'quux'
        }
      };

      var B = {
        h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'qux'
        }
      };

      var C = {
        h: { id: id, v: 'Cccc', pa: ['Aaaa'] },
        b: {
          foo: 'bar',
          bar: 'raboof',
          qux: 'quux'
        }
      };

      var D = {
        h: { id: id, v: 'Dddd', pa: ['Bbbb', 'Cccc'] },
        b: {
          foo: 'bar',
          bar: 'raboof',
          qux: 'quz'
        }
      };

      var E = {
        h: { id: id, v: 'Eeee', pa: ['Cccc', 'Bbbb'] },
        b: {
          foo: 'bar',
          bar: 'foobar',
          qux: 'qux'
        }
      };

      var F = {
        h: { id: id, v: 'Ffff', pa: ['Dddd', 'Eeee'] },
        b: {
          foo: 'bar',
          bar: 'foobar',
          qux: 'quz'
        }
      };

      it('save DAG', function(done) {
        var DAG = [A, B, C, D, E, F];
        tree = new Tree(db, name, { vSize: 3, log: silence });
        saveDAG(DAG, tree, done);
      });

      it('A and A = original A', function(done) {
        merge(A, A, tree, tree, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Aaaa', pa: [], i: 1 },
            b: {
              foo: 'bar',
              bar: 'baz',
              qux: 'quux'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Aaaa', pa: [], i: 1 },
            b: {
              foo: 'bar',
              bar: 'baz',
              qux: 'quux'
            }
          });
          done();
        });
      });

      it('B and C = merge', function(done) {
        merge(B, C, tree, tree, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Bbbb', 'Cccc'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Bbbb', 'Cccc'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux'
            }
          });
          done();
        });
      });

      it('E and B = ff to E', function(done) {
        merge(E, B, tree, tree, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Eeee', pa: ['Cccc', 'Bbbb'], i: 5 },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'qux'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Eeee', pa: ['Cccc', 'Bbbb'] },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'qux'
            }
          });
          done();
        });
      });

      it('D and E = merge', function(done) {
        merge(D, E, tree, tree, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Dddd', 'Eeee'] },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'quz'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Dddd', 'Eeee'] },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'quz'
            }
          });
          done();
        });
      });

      it('E and D = merge', function(done) {
        merge(E, D, tree, tree, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Eeee', 'Dddd'] },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'quz'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Eeee', 'Dddd'] },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'quz'
            }
          });
          done();
        });
      });

      it('E and F = ff to F', function(done) {
        merge(E, F, tree, tree, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Ffff', pa: ['Dddd', 'Eeee'] },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'quz'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Ffff', pa: ['Dddd', 'Eeee'], i: 6 },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'quz'
            }
          });
          done();
        });
      });

      it('F and E = ff to F', function(done) {
        merge(F, E, tree, tree, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Ffff', pa: ['Dddd', 'Eeee'], i: 6 },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'quz'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Ffff', pa: ['Dddd', 'Eeee'] },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'quz'
            }
          });
          done();
        });
      });

      it('should merge virtual merges', function(done) {
        var vm1 = {
          h: { id: id, v: 'x', pa: ['Aaaa'] },
          b: { a: true }
        };
        var vm2 = {
          h: { id: id, v: 'y', pa: ['Aaaa'] },
          b: { b: true }
        };

        merge(vm1, vm2, tree, tree, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['x', 'y'] },
            b: {
              a: true,
              b: true
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['x', 'y'] },
            b: {
              a: true,
              b: true
            }
          });
          done();
        });
      });

      it('should not merge conflicting virtual merges', function(done) {
        var vm1 = {
          h: { id: id, v: 'x', pa: ['Aaaa'] },
          b: {
            foo: 'bar',
            v: true
          }
        };
        var vm2 = {
          h: { id: id, v: 'y', pa: ['Aaaa'] },
          b: {
            foo: 'bar',
            v: false
          }
        };

        merge(vm1, vm2, tree, tree, { log: silence }, function(err) {
          should.equal(err.message, 'merge conflict');
          should.deepEqual(err.conflict, ['v']);
          done();
        });
      });
    });

    describe('delete one', function() {
      var name = 'onePerspectiveDeleteOne';
      var tree;

      // create the following structure:
      //    Cd
      //   /
      //  A
      //   \
      //    B
      // see http://www.gelato.unsw.edu.au/archives/git/0504/2279.html

      var A = {
        h: { id: id, v: 'Aaaa', pa: [] },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'quux'
        }
      };

      var B = {
        h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'qux'
        }
      };

      var Cd = {
        h: { id: id, v: 'Cccc', pa: ['Aaaa'], d: true },
        b: {
          foo: 'bar',
          bar: 'raboof',
          qux: 'quux'
        }
      };


      it('save DAG', function(done) {
        var DAG = [A, B, Cd];
        tree = new Tree(db, name, { vSize: 3, log: silence });
        saveDAG(DAG, tree, done);
      });

      it('B and C = merge, no delete', function(done) {
        merge(B, Cd, tree, tree, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Bbbb', 'Cccc'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Bbbb', 'Cccc'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux'
            }
          });
          done();
        });
      });

      it('C and B = merge, no delete', function(done) {
        merge(Cd, B, tree, tree, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Cccc', 'Bbbb'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Cccc', 'Bbbb'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux'
            }
          });
          done();
        });
      });
    });

    describe('delete two', function() {
      var name = 'onePerspectiveDeleteTwo';
      var tree;

      // create the following structure:
      //    Cd
      //   /
      //  A
      //   \
      //    Bd
      // see http://www.gelato.unsw.edu.au/archives/git/0504/2279.html

      var A = {
        h: { id: id, v: 'Aaaa', pa: [] },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'quux'
        }
      };

      var Bd = {
        h: { id: id, v: 'Bbbb', pa: ['Aaaa'], d: true },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'qux'
        }
      };

      var Cd = {
        h: { id: id, v: 'Cccc', pa: ['Aaaa'], d: true },
        b: {
          foo: 'bar',
          bar: 'raboof',
          qux: 'quux'
        }
      };


      it('save DAG', function(done) {
        var DAG = [A, Bd, Cd];
        tree = new Tree(db, name, { vSize: 3, log: silence });
        saveDAG(DAG, tree, done);
      });

      it('B and C = merge, delete', function(done) {
        merge(Bd, Cd, tree, tree, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Bbbb', 'Cccc'], d: true },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Bbbb', 'Cccc'], d: true },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux'
            }
          });
          done();
        });
      });

      it('C and B = merge, delete', function(done) {
        merge(Cd, Bd, tree, tree, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Cccc', 'Bbbb'], d: true },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Cccc', 'Bbbb'], d: true },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux'
            }
          });
          done();
        });
      });
    });
  });

  describe('two perspectives', function() {
    describe('basic', function() {
      var nameI  = 'twoPerspectiveI';
      var nameII = 'twoPerspectiveII';
      var treeI;
      var treeII;

      // create DAG in system I after some interaction between system I and II:
      // I creates AI, syncs to II
      // II creates CII
      // I creates BI, syncs to II
      // I creates EI
      // II merged BI, with CII, creating DII

      // DAG:
      //                   AII <-- BII
      //                     \       \
      //                     CII <-- DII
      // AI <-- BI <-- EI

      var AI = {
        h: { id: id, v: 'Aaaa', pa: [] },
        b: {
          baz : 'qux',
          bar: 'raboof',
          some: 'secret'
        }
      };

      var BI = {
        h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
        b: {
          bar: 'raboof',
          some: 'secret'
        }
      };

      var EI = {
        h: { id: id, v: 'Eeee', pa: ['Bbbb'] },
        b: {
          bar: 'foo',
          some: 'secret'
        }
      };

      var AII = {
        h: { id: id, v: 'Aaaa', pa: [] },
        b: {
          baz : 'qux',
          bar: 'raboof'
        }
      };

      var BII = {
        h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
        b: {
          bar: 'raboof'
        }
      };

      var CII = {
        h: { id: id, v: 'Cccc', pa: ['Aaaa'] },
        b: {
          baz : 'qux',
          bar: 'raboof',
          foo: 'bar'
        }
      };

      var DII = {
        h: { id: id, v: 'Dddd', pa: ['Cccc', 'Bbbb'] },
        b: {
          bar: 'raboof',
          foo: 'bar'
        }
      };

      it('save DAG', function(done) {
        var DAGI  = [AI, BI, EI];
        var DAGII = [AII, CII, BII, DII];

        treeI  = new Tree(db, nameI,  { vSize: 3, log: silence });
        treeII = new Tree(db, nameII, { vSize: 3, log: silence });

        saveDAGs(DAGI, DAGII, treeI, treeII, done);
      });

      it('should have added an increment to the saved objects', function() {
        should.deepEqual(AI, {
          h: { id: id, v: 'Aaaa', pa: [], i: 1 },
          b: {
            baz : 'qux',
            bar: 'raboof',
            some: 'secret'
          }
        });

        should.deepEqual(BI, {
          h: { id: id, v: 'Bbbb', pa: ['Aaaa'], i: 2 },
          b: {
            bar: 'raboof',
            some: 'secret'
          }
        });

        should.deepEqual(EI, {
          h: { id: id, v: 'Eeee', pa: ['Bbbb'], i: 3 },
          b: {
            bar: 'foo',
            some: 'secret'
          }
        });

        should.deepEqual(AII, {
          h: { id: id, v: 'Aaaa', pa: [], i: 1 },
          b: {
            baz : 'qux',
            bar: 'raboof'
          }
        });

        should.deepEqual(CII, {
          h: { id: id, v: 'Cccc', pa: ['Aaaa'], i: 2 },
          b: {
            baz : 'qux',
            bar: 'raboof',
            foo: 'bar'
          }
        });

        should.deepEqual(BII, {
          h: { id: id, v: 'Bbbb', pa: ['Aaaa'], i: 3 },
          b: {
            bar: 'raboof'
          }
        });

        should.deepEqual(DII, {
          h: { id: id, v: 'Dddd', pa: ['Cccc', 'Bbbb'], i: 4 },
          b: {
            bar: 'raboof',
            foo: 'bar'
          }
        });
      });

      it('AI and AII = ff to AI, ff to AII', function(done) {
        merge(AI, AII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Aaaa', pa: [], i: 1 },
            b: {
              baz : 'qux',
              bar: 'raboof',
              some: 'secret'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Aaaa', pa: [], i: 1 },
            b: {
              baz : 'qux',
              bar: 'raboof'
            }
          });
          done();
        });
      });

      it('AII and AI = ff to AII, ff to AI', function(done) {
        merge(AII, AI, treeII, treeI, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Aaaa', pa: [], i: 1 },
            b: {
              baz : 'qux',
              bar: 'raboof'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Aaaa', pa: [], i: 1 },
            b: {
              baz : 'qux',
              bar: 'raboof',
              some: 'secret'
            }
          });
          done();
        });
      });

      it('BI and DII = merged ff to DI, ff to DII', function(done) {
        merge(BI, DII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Dddd', pa: ['Cccc', 'Bbbb'] },
            b: {
              bar: 'raboof',
              some: 'secret',
              foo: 'bar'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Dddd', pa: ['Cccc', 'Bbbb'], i: 4 },
            b: {
              bar: 'raboof',
              foo: 'bar'
            }
          });
          done();
        });
      });

      it('EI and DII = merges based on BI, BII', function(done) {
        merge(EI, DII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Eeee', 'Dddd'] },
            b: {
              bar: 'foo',
              some: 'secret',
              foo: 'bar'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Eeee', 'Dddd'] },
            b: {
              bar: 'foo',
              foo: 'bar'
            }
          });
          done();
        });
      });

      it('EI and BII = EI and merge ff EII', function(done) {
        merge(EI, BII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Eeee', pa: ['Bbbb'], i: 3 },
            b: {
              bar: 'foo',
              some: 'secret'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Eeee', pa: ['Bbbb'] },
            b: {
              bar: 'foo'
            }
          });
          done();
        });
      });

      it('BI and CII = merge D-like', function(done) {
        merge(BI, CII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Bbbb', 'Cccc'] },
            b: {
              bar: 'raboof',
              some: 'secret',
              foo: 'bar'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Bbbb', 'Cccc'] },
            b: {
              bar: 'raboof',
              foo: 'bar'
            }
          });
          done();
        });
      });

      it('should not merge conflicting virtual merges', function(done) {
        var vm1 = {
          h: { id: id, v: 'x', pa: ['Aaaa'] },
          b: { foo: 'bar' }
        };
        var vm2 = {
          h: { id: id, v: 'y', pa: ['Aaaa'] },
          b: { foo: 'baz' }
        };

        merge(vm1, vm2, treeI, treeII, { log: silence }, function(err) {
          should.equal(err.message, 'merge conflict');
          should.deepEqual(err.conflict, ['foo']);
          done();
        });
      });
    });

    describe('delete one', function() {
      var nameI  = 'twoPerspectiveDeleteOneI';
      var nameII = 'twoPerspectiveDeleteOneII';
      var treeI;
      var treeII;

      // create the following structure:
      //    Cd
      //   /
      //  A
      //   \
      //    B
      // see http://www.gelato.unsw.edu.au/archives/git/0504/2279.html

      var AI = {
        h: { id: id, v: 'Aaaa', pa: [] },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'quux',
          some: true
        }
      };

      var BI = {
        h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'qux',
          some: true
        }
      };

      var CId = {
        h: { id: id, v: 'Cccc', pa: ['Aaaa'], d: true },
        b: {
          foo: 'bar',
          bar: 'raboof',
          qux: 'quux',
          some: true
        }
      };

      var AII = {
        h: { id: id, v: 'Aaaa', pa: [] },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'quux'
        }
      };

      var BII = {
        h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'qux'
        }
      };

      var CIId = {
        h: { id: id, v: 'Cccc', pa: ['Aaaa'], d: true },
        b: {
          foo: 'bar',
          bar: 'raboof',
          qux: 'quux'
        }
      };

      it('save DAG', function(done) {
        var DAGI = [AI, BI, CId];
        var DAGII = [AII, BII, CIId];

        treeI  = new Tree(db, nameI,  { vSize: 3, log: silence });
        treeII = new Tree(db, nameII, { vSize: 3, log: silence });

        saveDAGs(DAGI, DAGII, treeI, treeII, done);
      });

      it('BI and CII = merge, no delete', function(done) {
        merge(BI, CIId, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Bbbb', 'Cccc'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux',
              some: true
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Bbbb', 'Cccc'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux'
            }
          });
          done();
        });
      });

      it('BII and CI = merge, no delete', function(done) {
        merge(BII, CId, treeII, treeI, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Bbbb', 'Cccc'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Bbbb', 'Cccc'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux',
              some: true
            }
          });
          done();
        });
      });

      it('CI and BII = merge, no delete', function(done) {
        merge(CId, BII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Cccc', 'Bbbb'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux',
              some: true
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Cccc', 'Bbbb'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux'
            }
          });
          done();
        });
      });

      it('CII and BI = merge, no delete', function(done) {
        merge(CIId, BI, treeII, treeI, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Cccc', 'Bbbb'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Cccc', 'Bbbb'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux',
              some: true
            }
          });
          done();
        });
      });
    });

    describe('delete two', function() {
      var nameI  = 'twoPerspectiveDeleteTwoI';
      var nameII = 'twoPerspectiveDeleteTwoII';
      var treeI;
      var treeII;

      // create the following structure:
      //    Cd
      //   /
      //  A
      //   \
      //    B
      // see http://www.gelato.unsw.edu.au/archives/git/0504/2279.html

      var AI = {
        h: { id: id, v: 'Aaaa', pa: [] },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'quux',
          some: true
        }
      };

      var BId = {
        h: { id: id, v: 'Bbbb', pa: ['Aaaa'], d: true },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'qux',
          some: true
        }
      };

      var CId = {
        h: { id: id, v: 'Cccc', pa: ['Aaaa'], d: true },
        b: {
          foo: 'bar',
          bar: 'raboof',
          qux: 'quux',
          some: true
        }
      };

      var AII = {
        h: { id: id, v: 'Aaaa', pa: [] },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'quux'
        }
      };

      var BIId = {
        h: { id: id, v: 'Bbbb', pa: ['Aaaa'], d: true },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'qux'
        }
      };

      var CIId = {
        h: { id: id, v: 'Cccc', pa: ['Aaaa'], d: true },
        b: {
          foo: 'bar',
          bar: 'raboof',
          qux: 'quux'
        }
      };

      it('save DAG', function(done) {
        var DAGI = [AI, BId, CId];
        var DAGII = [AII, BIId, CIId];

        treeI  = new Tree(db, nameI,  { vSize: 3, log: silence });
        treeII = new Tree(db, nameII, { vSize: 3, log: silence });

        saveDAGs(DAGI, DAGII, treeI, treeII, done);
      });

      it('BI and CII = merge, delete', function(done) {
        merge(BId, CIId, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Bbbb', 'Cccc'], d: true },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux',
              some: true
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Bbbb', 'Cccc'], d: true },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux'
            }
          });
          done();
        });
      });

      it('BII and CI = merge, delete', function(done) {
        merge(BIId, CId, treeII, treeI, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Bbbb', 'Cccc'], d: true },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Bbbb', 'Cccc'], d: true },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux',
              some: true
            }
          });
          done();
        });
      });
    });

    describe('criss-cross merge', function() {
      var nameI  = 'twoPerspectiveCrissCrossI';
      var nameII = 'twoPerspectiveCrissCrossII';
      var treeI;
      var treeII;

      // create the following structure:
      //    (plus CI) CII - EII (plus EI)
      //             /   \ /   \
      //           AII    X    FII (plus FI)
      //             \   / \   /          
      //              BII - DII (plus DI)
      //
      // AI <-- BI

      var AI = {
        h: { id: id, v: 'Aaaa', pa: [] },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'quux',
          some: 'secret'
        }
      };

      var BI = {
        h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'qux',
          some: 'secret'
        }
      };

      var AII = {
        h: { id: id, v: 'Aaaa', pa: [] },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'quux'
        }
      };

      var BII = {
        h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'qux'
        }
      };

      var CI = {
        h: { id: id, v: 'Cccc', pa: ['Aaaa'] },
        b: {
          foo: 'bar',
          bar: 'raboof',
          qux: 'quux',
          some: 'secret'
        }
      };

      var CII = {
        h: { id: id, v: 'Cccc', pa: ['Aaaa'] },
        b: {
          foo: 'bar',
          bar: 'raboof',
          qux: 'quux'
        }
      };

      var DII = {
        h: { id: id, v: 'Dddd', pa: ['Bbbb', 'Cccc'] },
        b: {
          foo: 'bar',
          bar: 'raboof',
          qux: 'quz'
        }
      };

      var DI = {
        h: { id: id, v: 'Dddd', pa: ['Bbbb', 'Cccc'] },
        b: {
          foo: 'bar',
          bar: 'raboof',
          qux: 'quz',
          some: 'secret'
        }
      };

      var EII = {
        h: { id: id, v: 'Eeee', pa: ['Cccc', 'Bbbb'] },
        b: {
          foo: 'bar',
          bar: 'foobar',
          qux: 'qux'
        }
      };

      var EI = {
        h: { id: id, v: 'Eeee', pa: ['Cccc', 'Bbbb'] },
        b: {
          foo: 'bar',
          bar: 'foobar',
          qux: 'qux',
          some: 'secret'
        }
      };

      var FII = {
        h: { id: id, v: 'Ffff', pa: ['Dddd', 'Eeee'] },
        b: {
          foo: 'bar',
          bar: 'foobar',
          qux: 'quz'
        }
      };

      var FI = {
        h: { id: id, v: 'Ffff', pa: ['Dddd', 'Eeee'] },
        b: {
          foo: 'bar',
          bar: 'foobar',
          qux: 'quz',
          some: 'secret'
        }
      };

      it('save DAG', function(done) {
        var DAGI  = [AI, BI, CI, DI, EI, FI];
        var DAGII = [AII, BII, CII, DII, EII, FII];

        treeI  = new Tree(db, nameI,  { vSize: 3, log: silence });
        treeII = new Tree(db, nameII, { vSize: 3, log: silence });

        saveDAGs(DAGI, DAGII, treeI, treeII, done);
      });

      it('AI and AII = ff to AI, ff to AII', function(done) {
        merge(AI, AII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Aaaa', pa: [], i: 1 },
            b: {
              foo: 'bar',
              bar: 'baz',
              qux: 'quux',
              some: 'secret'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Aaaa', pa: [], i: 1 },
            b: {
              foo: 'bar',
              bar: 'baz',
              qux: 'quux'
            }
          });
          done();
        });
      });

      it('BI and CII = merge', function(done) {
        merge(BI, CII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Bbbb', 'Cccc'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux',
              some: 'secret'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Bbbb', 'Cccc'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux'
            }
          });
          done();
        });
      });

      it('DI and EII = merge based on BI and CII, merge based on BII and CII', function(done) {
        merge(DI, EII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Dddd', 'Eeee'] },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'quz',
              some: 'secret'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Dddd', 'Eeee'] },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'quz'
            }
          });
          done();
        });
      });

      it('EII and DI = merge based on BII and CII, merge based on BI and CII', function(done) {
        merge(EII, DI, treeII, treeI, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Eeee', 'Dddd'] },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'quz'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Eeee', 'Dddd'] },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'quz',
              some: 'secret'
            }
          });
          done();
        });
      });

      it('EI and FII = merge ff to FI, ff to FII', function(done) {
        merge(EI, FII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Ffff', pa: ['Dddd', 'Eeee'] },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'quz',
              some: 'secret'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Ffff', pa: ['Dddd', 'Eeee'], i: 6 },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'quz'
            }
          });
          done();
        });
      });

      it('FII and EI = ff to FII, merge ff to FI', function(done) {
        merge(FII, EI, treeII, treeI, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Ffff', pa: ['Dddd', 'Eeee'], i: 6 },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'quz'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Ffff', pa: ['Dddd', 'Eeee'] },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'quz',
              some: 'secret'
            }
          });
          done();
        });
      });
    });

    describe('n-parent merge', function() {
      var nameI  = 'twoPerspectiveNparentI';
      var nameII = 'twoPerspectiveNparentII';
      var treeI;
      var treeII;

      // create the following structure:
      //              CI <----
      //             /        \
      //   AI <-- BI <- EI <-- FI
      //             \        /
      //              DI <----
      //
      // AII

      var AII = {
        h: { id: id, v: 'Aaaa', pa: [] },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'quux'
        }
      };

      var AI = {
        h: { id: id, v: 'Aaaa', pa: [] },
        b: {
        foo: 'bar',
        bar: 'baz',
        qux: 'quux',
        some: 'secret'
        }
      };

      var BI = {
        h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
        b: {
        foo: 'bar',
        bar: 'baz',
        some: 'secret'
        }
      };

      var CI = {
        h: { id: id, v: 'Cccc', pa: ['Bbbb'] },
        b: {
        foo: 'bar',
        bar: 'raboof',
        c: true,
        some: 'secret'
        }
      };

      var DI = {
        h: { id: id, v: 'Dddd', pa: ['Bbbb'] },
        b: {
        foo: 'bar',
        bar: 'raboof',
        d: true,
        some: 'secret'
        }
      };

      var EI = {
        h: { id: id, v: 'Eeee', pa: ['Bbbb'] },
        b: {
          foo: 'bar',
          bar: 'raboof',
          e: true,
          some: 'secret'
        }
      };

      var FI = {
        h: { id: id, v: 'Ffff', pa: ['Cccc', 'Dddd', 'Eeee'] },
        b: {
          foo: 'bar',
          bar: 'raboof',
          c: true,
          d: true,
          e: true,
          some: 'secret'
        }
      };

      it('save DAG', function(done) {
        var DAGI  = [AI, BI, CI, DI, EI, FI];
        var DAGII = [AII];

        treeI  = new Tree(db, nameI,  { vSize: 3, log: silence });
        treeII = new Tree(db, nameII, { vSize: 3, log: silence });

        saveDAGs(DAGI, DAGII, treeI, treeII, done);
      });

      it('AII and FI = merged ff to FII, ff to FI', function(done) {
        merge(AII, FI, treeII, treeI, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Ffff', pa: ['Cccc', 'Dddd', 'Eeee'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              c: true,
              d: true,
              e: true
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Ffff', pa: ['Cccc', 'Dddd', 'Eeee'], i: 6 },
            b: {
              foo: 'bar',
              bar: 'raboof',
              c: true,
              d: true,
              e: true,
              some: 'secret'
            }
          });
          done();
        });
      });

      it('AII and CI = merged ff to CII, ff to CI', function(done) {
        merge(AII, CI, treeII, treeI, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Cccc', pa: ['Bbbb'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              c: true
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Cccc', pa: ['Bbbb'], i: 3 },
            b: {
              foo: 'bar',
              bar: 'raboof',
              c: true,
              some: 'secret'
            }
          });
          done();
        });
      });

      it('AI and AII = ff to AI, ff to AII', function(done) {
        merge(AI, AII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Aaaa', pa: [], i: 1 },
            b: {
              foo: 'bar',
              bar: 'baz',
              qux: 'quux',
              some: 'secret'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Aaaa', pa: [], i: 1 },
            b: {
              foo: 'bar',
              bar: 'baz',
              qux: 'quux'
            }
          });
          done();
        });
      });
    });

    describe('criss-cross 3-parent merge', function() {
      var nameI  = 'twoPerspectiveCrissCross3ParentI';
      var nameII = 'twoPerspectiveCrissCross3ParentII';
      var treeI;
      var treeII;

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

      var AI = {
        h: { id: id, v: 'Aaaa', pa: [] },
        b: {
          a: true,
          some: 'secret'
        }
      };

      var BI = {
        h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
        b: {
          a: true,
          b: true,
          some: 'secret'
        }
      };

      var CI = {
        h: { id: id, v: 'Cccc', pa: ['Bbbb'] },
        b: {
          a: true,
          b: true,
          c: true,
          some: 'secret'
        }
      };

      var DI = {
        h: { id: id, v: 'Dddd', pa: ['Bbbb'] },
        b: {
          a: true,
          b: true,
          d: true,
          some: 'secret'
        }
      };

      var EI = {
        h: { id: id, v: 'Eeee', pa: ['Bbbb'] },
        b: {
          a: true,
          b: true,
          e: true,
          some: 'secret'
        }
      };

      var FI = { // change e
        h: { id: id, v: 'Ffff', pa: ['Cccc', 'Dddd', 'Eeee'] },
        b: {
          a: true,
          b: true,
          c: true,
          d: true,
          e: 'foo',
          f: true,
          some: 'secret'
        }
      };

      var GI = { // delete d
        h: { id: id, v: 'Gggg', pa: ['Cccc', 'Dddd', 'Eeee'] },
        b: {
          a: true,
          b: true,
          c: true,
          e: true,
          g: true,
          some: 'secret'
        }
      };

      var FIc = { // delete e, change d, conflict with Gc
        h: { id: id, v: 'Fffc', pa: ['Cccc', 'Dddd', 'Eeee'] },
        b: {
          a: true,
          b: true,
          c: true,
          d: 'foo',
          fc: true,
          some: 'secret'
        }
      };

      var GIc = { // delete d, change e, conflict with Fc
        h: { id: id, v: 'Gggc', pa: ['Cccc', 'Dddd', 'Eeee'] },
        b: {
          a: true,
          b: true,
          c: true,
          e: 'foo',
          gc: true,
          some: 'secret'
        }
      };

      var HI = {
        h: { id: id, v: 'Hhhh', pa: ['Ffff', 'Gggg' ] },
        b: {
          a: true,
          b: true,
          c: true,
          d: true,
          e: true,
          f: true,
          g: true,
          h: true,
          some: 'secret'
        }
      };

      var AII = {
        h: { id: id, v: 'Aaaa', pa: [] },
        b: {
          a: true,
        }
      };

      var BII = {
        h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
        b: {
          a: true,
          b: true,
        }
      };

      var CII = {
        h: { id: id, v: 'Cccc', pa: ['Bbbb'] },
        b: {
          a: true,
          b: true,
          c: true,
        }
      };

      var DII = {
        h: { id: id, v: 'Dddd', pa: ['Bbbb'] },
        b: {
          a: true,
          b: true,
          d: true,
        }
      };

      var EII = {
        h: { id: id, v: 'Eeee', pa: ['Bbbb'] },
        b: {
          a: true,
          b: true,
          e: true,
        }
      };

      var FII = { // change e
        h: { id: id, v: 'Ffff', pa: ['Cccc', 'Dddd', 'Eeee'] },
        b: {
          a: true,
          b: true,
          c: true,
          d: true,
          e: 'foo',
          f: true,
        }
      };

      var GII = { // delete d
        h: { id: id, v: 'Gggg', pa: ['Cccc', 'Dddd', 'Eeee'] },
        b: {
          a: true,
          b: true,
          c: true,
          e: true,
          g: true,
        }
      };

      it('save DAG', function(done) {
        var DAGI  = [AI, BI, CI, DI, EI, FI, GI, FIc, GIc, HI];
        var DAGII = [AII, BII, CII, DII, EII, FII, GII ];

        treeI  = new Tree(db, nameI,  { vSize: 3, log: silence });
        treeII = new Tree(db, nameII, { vSize: 3, log: silence });

        saveDAGs(DAGI, DAGII, treeI, treeII, done);
      });

      it('GI and FI = merge', function(done) {
        merge(GI, FI, treeI, treeI, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Gggg', 'Ffff'] },
            b: {
              a: true,
              b: true,
              c: true,
              e: 'foo',
              f: true,
              g: true,
              some: 'secret'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Gggg', 'Ffff'] },
            b: {
              a: true,
              b: true,
              c: true,
              e: 'foo',
              f: true,
              g: true,
              some: 'secret'
            }
          });
          done();
        });
      });

      it('GII and FI = merge', function(done) {
        merge(GII, FI, treeII, treeI, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Gggg', 'Ffff'] },
            b: {
              a: true,
              b: true,
              c: true,
              e: 'foo',
              f: true,
              g: true,
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Gggg', 'Ffff'] },
            b: {
              a: true,
              b: true,
              c: true,
              e: 'foo',
              f: true,
              g: true,
              some: 'secret'
            }
          });
          done();
        });
      });

      it('FII and GI = merge', function(done) {
        merge(FI, GII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Ffff', 'Gggg'] },
            b: {
              a: true,
              b: true,
              c: true,
              e: 'foo',
              f: true,
              g: true,
              some: 'secret'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Ffff', 'Gggg'] },
            b: {
              a: true,
              b: true,
              c: true,
              e: 'foo',
              f: true,
              g: true
            }
          });
          done();
        });
      });

      it('AII and HI = merged ff to HII, ff to HI', function(done) {
        merge(AII, HI, treeII, treeI, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Hhhh', pa: ['Ffff', 'Gggg'] },
            b: {
              a: true,
              b: true,
              c: true,
              d: true,
              e: true,
              f: true,
              g: true,
              h: true
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Hhhh', pa: ['Ffff', 'Gggg'], i: 10 },
            b: {
              a: true,
              b: true,
              c: true,
              d: true,
              e: true,
              f: true,
              g: true,
              h: true,
              some: 'secret'
            }
          });
          done();
        });
      });

      it('GIc and FIc = conflict', function(done) {
        merge(GIc, FIc, treeI, treeII, { log: silence }, function(err) {
          should.equal(err.message, 'merge conflict');
          should.deepEqual(err.conflict, ['d', 'e']);
          done();
        });
      });
    });
  });

  /*

  describe('double criss-cross three parents', function() {
    // create 2 DAGs with a double criss-cross merge with three parents

    var AI = {
      h: { id: id, v: 'Aaaa', pa: [] },
      b: {
        a: true,
        some: 'secret'
      }
    };

    var BI = {
      h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
      b: {
        a: true,
        b: true,
        some: 'secret'
      }
    };

    var CI = {
      h: { id: id, v: 'Cccc', pa: ['Bbbb'] },
      b: {
        a: true,
        b: true,
        c: true,
        some: 'secret'
      }
    };

    var DI = {
      h: { id: id, v: 'Dddd', pa: ['Bbbb'] },
      b: {
        a: true,
        b: true,
        d: true,
        some: 'secret'
      }
    };

    var EI = {
      h: { id: id, v: 'Eeee', pa: ['Bbbb'] },
      b: {
        a: true,
        b: true,
        e: true,
        some: 'secret'
      }
    };

    var FI = { // change c
      h: { id: id, v: 'Ffff', pa: ['Cccc', 'Dddd', 'Eeee'] },
      b: {
        a: true,
        b: true,
        c: 'foo',
        d: true,
        e: true,
        f: true,
        some: 'secret'
      }
    };

    var GI = { // change d, delete b
      h: { id: id, v: 'Gggg', pa: ['Cccc', 'Dddd', 'Eeee'] },
      b: {
        a: true,
        c: true,
        d: 'bar',
        e: true,
        g: true,
        some: 'secret'
      }
    };

    var HI = { // delete e, delete a
      h: { id: id, v: 'Hhhh', pa: ['Cccc', 'Dddd', 'Eeee'] },
      b: {
        b: true,
        c: true,
        d: true,
        h: true,
        some: 'secret'
      }
    };

    var II = { // add e again, change d, change h
      h: { id: id, v: 'I', pa: ['Ffff', 'Gggg', 'Hhhh'] },
      b: {
        c: 'foo',
        d: 'baz',
        e: 'II',
        f: true,
        g: true,
        h: 'II',
        i: true,
        some: 'secret'
      }
    };

    var JI = { // change f, change g
      h: { id: id, v: 'J', pa: ['Ffff', 'Gggg', 'Hhhh'] },
      b: {
        c: 'foo',
        d: 'bar',
        f: 'JI',
        g: 'JI',
        h: true,
        j: true,
        some: 'secret'
      }
    };

    var FIc = { // delete e, change d, conflict with GIc
      h: { id: id, v: 'Ffff', pa: ['Cccc', 'Dddd', 'Eeee'] },
      b: {
        a: true,
        b: true,
        c: true,
        d: 'FIc',
        fc: true,
        some: 'secret'
      }
    };

    var GIc = { // delete d, change e, conflict with FIc
      h: { id: id, v: 'Gc', pa: ['Cccc', 'Dddd', 'Eeee'] },
      b: {
        a: true,
        b: true,
        c: true,
        e: 'GIc',
        gc: true,
        some: 'secret'
      }
    };

    var IIc = { // conflict with JIc: change f
      h: { id: id, v: 'Ic', pa: ['Ffff', 'Gggg', 'Hhhh'] },
      b: {
        c: 'foo',
        d: 'bar',
        e: true,
        f: 'IIc',
        g: true,
        h: true,
        ic: true,
        some: 'secret'
      }
    };

    var JIc = { // conflict with IIc: change f, change h
      h: { id: id, v: 'Jc', pa: ['Ffff', 'Gggg', 'Hhhh'] },
      b: {
        c: 'foo',
        d: 'bar',
        f: 'JIc',
        g: true,
        h: 'JIc',
        jc: true,
        some: 'secret'
      }
    };

    var AII = {
      h: { id: id, v: 'Aaaa', pa: [] },
      b: {
        a: true,
      }
    };

    var BII = {
      h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
      b: {
        a: true,
        b: true,
      }
    };

    var CII = {
      h: { id: id, v: 'Cccc', pa: ['Bbbb'] },
      b: {
        a: true,
        b: true,
        c: true,
      }
    };

    var DII = {
      h: { id: id, v: 'Dddd', pa: ['Bbbb'] },
      b: {
        a: true,
        b: true,
        d: true,
      }
    };

    var EII = {
      h: { id: id, v: 'Eeee', pa: ['Bbbb'] },
      b: {
        a: true,
        b: true,
        e: true,
      }
    };

    var FII = { // change c
      h: { id: id, v: 'Ffff', pa: ['Cccc', 'Dddd', 'Eeee'] },
      b: {
        a: true,
        b: true,
        c: 'foo',
        d: true,
        e: true,
        f: true,
      }
    };

    var GII = { // change d, delete b
      h: { id: id, v: 'Gggg', pa: ['Cccc', 'Dddd', 'Eeee'] },
      b: {
        a: true,
        c: true,
        d: 'bar',
        e: true,
        g: true,
      }
    };

    var HII = { // delete e, delete a
      h: { id: id, v: 'Hhhh', pa: ['Cccc', 'Dddd', 'Eeee'] },
      b: {
        b: true,
        c: true,
        d: true,
        h: true,
      }
    };

    var III = { // add e again, change d, change h
      h: { id: id, v: 'I', pa: ['Ffff', 'Gggg', 'Hhhh'] },
      b: {
        c: 'foo',
        d: 'baz',
        e: 'III',
        f: true,
        g: true,
        h: 'III',
        i: true,
      }
    };

    var JII = { // change f, change g
      h: { id: id, v: 'J', pa: ['Ffff', 'Gggg', 'Hhhh'] },
      b: {
        c: 'foo',
        d: 'bar',
        f: 'JII',
        g: 'JII',
        h: true,
        j: true,
      }
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
        vc._snapshotCollection.insert([AI, BI, CI, DI, EI, FI, GI, FIc, GIc, HI, II, JI, IIc, JIc], {w: 1}, done);
      });

      it('FI and GI = merge', function(done) {
        merge(FI, GI, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Ffff', 'Gggg'] },
            a: true,
            c: 'foo',
            d: 'bar',
            e: true,
            f: true,
            g: true,
            some: 'secret'
          });
          done();
        });
      });

      it('II and JI = merge', function(done) {
        merge(II, JI, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(merged, [{
            h: { id: id, pa: ['I', 'J'] },
            b: {
              c: 'foo',
              d: 'baz',
              e: 'II',
              f: 'JI',
              g: 'JI',
              h: 'II',
              i: true,
              j: true,
              some: 'secret'
            }
          });
          done();
        });
      });

      it('IIc and JIc = conflict', function(done) {
        merge(IIc, JIc, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          should.equal(err.message, 'merge conflict');
          should.deepEqual(merged, ['f']);
          done();
        });
      });
    });

    describe('two perspectives', function() {
      var collectionName = '_mergeDoubleCrissCrossThreeMergesTwoPerspectives';

      it('should save DAG topologically sorted per perspective only', function(done) {
        vc._snapshotCollection.insert([AII, BII, AI, BI, CI, CII, DII, EII, FII, GII, DI, EI, FI, GI, FIc, GIc, HI, II, HII, III, JII, JI, IIc, JIc], {w: 1}, done);
      });

      it('FII and GI = merge', function(done) {
        merge(FII, GI, treeII, treeI, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(merged, [{
            h: { id: id, pa: ['Ffff', 'Gggg'] },
            a: true,
            c: 'foo',
            d: 'bar',
            e: true,
            f: true,
            g: true,
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Ffff', 'Gggg'] },
            a: true,
            c: 'foo',
            d: 'bar',
            e: true,
            f: true,
            g: true,
            some: 'secret'
          });
          done();
        });
      });

      it('HI and III = merged ff to II, ff to III', function(done) {
        merge(HI, III, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'I', pa: ['Ffff', 'Gggg', 'Hhhh'] },
            b: {
              c: 'foo',
              d: 'baz',
              e: 'III',
              f: true,
              g: true,
              h: 'III',
              i: true,
              some: 'secret'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'I', pa: ['Ffff', 'Gggg', 'Hhhh'] },
            b: {
              c: 'foo',
              d: 'baz',
              e: 'III',
              f: true,
              g: true,
              h: 'III',
              i: true,
            }
          });
          done();
        });
      });

      it('II and III = ff to II, ff to III', function(done) {
        merge(II, III, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'I', pa: ['Ffff', 'Gggg', 'Hhhh'] },
            b: {
              c: 'foo',
              d: 'baz',
              e: 'II',
              f: true,
              g: true,
              h: 'II',
              i: true,
              some: 'secret'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'I', pa: ['Ffff', 'Gggg', 'Hhhh'] },
            b: {
              c: 'foo',
              d: 'baz',
              e: 'III',
              f: true,
              g: true,
              h: 'III',
              i: true,
            }
          });
          done();
        });
      });

      it('JIc and III = conflict', function(done) {
        merge(JIc, III, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          should.equal(err.message, 'merge conflict');
          should.deepEqual(merged, [['h'], ['h']]);
          done();
        });
      });

      it('JI and III = merge', function(done) {
        merge(JI, III, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['J', 'I'] },
            b: {
              c: 'foo',
              d: 'baz',
              e: 'III',
              f: 'JI',
              g: 'JI',
              h: 'III',
              j: true,
              i: true,
              some: 'secret'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['J', 'I'] },
            b: {
              c: 'foo',
              d: 'baz',
              e: 'III',
              f: 'JI',
              g: 'JI',
              h: 'III',
              j: true,
              i: true,
            }
          });
          done();
        });
      });
    });
  });

  describe('merge with patches', function() {
    var collectionName = '_mergeMergeWithPatches';

    // create DAG where all exported items are imported again

    var AI = {
      h: { id: id, v: 'Aaaa', pa: [] },
      b: {
        baz : 'qux',
        bar: 'raboof',
        some: 'secret'
      }
    };

    var BI = {
      h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
      b: {
        bar: 'raboof',
        some: 'secret'
      }
    };

    var EI = {
      h: { id: id, v: 'Eeee', pa: ['Bbbb'] },
      b: {
        bar: 'foo',
        some: 'secret'
      }
    };

    var AII = {
      h: { id: id, v: 'Aaaa', pa: [] },
      b: {
        baz : 'qux',
        bar: 'raboof'
      }
    };

    var BII = {
      h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
      b: {
        bar: 'raboof'
      }
    };

    var CII = {
      h: { id: id, v: 'Cccc', pa: ['Aaaa'] },
      b: {
        baz : 'qux',
        bar: 'raboof',
        foo: 'bar'
      }
    };

    var DII = {
      h: { id: id, v: 'Dddd', pa: ['Cccc', 'Bbbb'] },
      b: {
        bar: 'raboof',
        foo: 'bar',
        d: true
      }
    };

    // resulting DAG in system I:
    //                   AII <-- BII
    //                     \       \
    //                     CII <-- DII
    // AI <-- BI <-- EI

    it('should save DAG', function(done) {
      vc._snapshotCollection.insert([AI, AII, CII, BI, BII, EI, DII], {w: 1}, done);
    });

    it('BI and DII = merged ff to DI, ff to DII', function(done) {
      merge(BI, DII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
        if (err) { throw err; }
        should.deepEqual(mergeX, {
          h: { id: id, v: 'Dddd', pa: ['Cccc', 'Bbbb'] },
          bar: 'raboof',
          some: 'secret',
          foo: 'bar',
          d: true
        });
        should.deepEqual(mergeY, {
          h: { id: id, v: 'Dddd', pa: ['Cccc', 'Bbbb'] },
          b: {
            bar: 'raboof',
            foo: 'bar',
            d: true
          }
        });
        done();
      });
    });

    it('EI and DII = merges based on BI, BII', function(done) {
      merge(EI, DII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
        if (err) { throw err; }
        should.deepEqual(merged, [{
          h: { id: id, pa: ['Eeee', 'Dddd'] },
          b: {
            bar: 'foo',
            some: 'secret',
            foo: 'bar',
            d: true
          }
        });
        should.deepEqual(mergeY, {
          h: { id: id, pa: ['Eeee', 'Dddd'] },
          b: {
            bar: 'foo',
            foo: 'bar',
            d: true
          }
        });
        done();
      });
    });
  });

  describe('merge with resolved conflict', function() {
    var collectionName = '_mergeMergeWithResolvedConflict';

    // create DAG where all exported items are imported again

    var AI = {
      h: { id: id, v: 'Aaaa', pa: [] },
      b: {
        baz : 'qux',
        bar: 'raboof',
        some: 'secret'
      }
    };

    var BI = { // add c: 'foo'
      h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
      b: {
        bar: 'raboof',
        c: 'foo',
        some: 'secret'
      }
    };

    var EI = {
      h: { id: id, v: 'Eeee', pa: ['Bbbb'] },
      b: {
        bar: 'foo',
        c: 'foo',
        e: true,
        some: 'secret'
      }
    };

    var FI = {
      h: { id: id, v: 'Ffff', pa: ['Eeee'] },
      b: {
        bar: 'foo',
        c: 'baz',
        e: true,
        f: true,
        some: 'secret'
      }
    };

    var GI = { // conflict with DII
      h: { id: id, v: 'Gggg', pa: ['Ffff'] },
      b: {
        bar: 'foo',
        c: 'raboof',
        e: true,
        f: true,
        g: true,
        some: 'secret'
      }
    };

    var AII = {
      h: { id: id, v: 'Aaaa', pa: [] },
      b: {
        baz : 'qux',
        bar: 'raboof'
      }
    };

    var BII = { // add c: 'foo'
      h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
      b: {
        bar: 'raboof',
        c: 'foo'
      }
    };

    var CII = { // add c: 'bar'
      h: { id: id, v: 'Cccc', pa: ['Aaaa'] },
      b: {
        baz : 'qux',
        bar: 'raboof',
        foo: 'bar',
        c: 'bar'
      }
    };

    var DII = { // resolve conflict: c: 'baz'
      h: { id: id, v: 'Dddd', pa: ['Cccc', 'Bbbb'] },
      b: {
        bar: 'raboof',
        foo: 'bar',
        c: 'baz',
        d: true
      }
    };

    // resulting DAG in system I:
    //                   AII <-- BII
    //                     \       \
    //                     CII <-- DII
    //
    // AI <-- BI <-- EI <-- FI <-- GI

    it('should save DAG', function(done) {
      vc._snapshotCollection.insert([AI, AII, CII, BI, BII, EI, FI, DII, GI], {w: 1}, done);
    });

    it('BI and CII = conflict', function(done) {
      merge(BI, CII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
        should.equal(err.message, 'merge conflict');
        should.deepEqual(merged, [['c'], ['c']]);
        done();
      });
    });

    it('BI and DII = merged ff to DI, ff to DII', function(done) {
      merge(BI, DII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
        if (err) { throw err; }
        should.deepEqual(merged[0], {
          h: { id: id, v: 'Dddd', pa: ['Cccc', 'Bbbb'] },
          bar: 'raboof',
          some: 'secret',
          foo: 'bar',
          c: 'baz',
          d: true
        });
        should.deepEqual(merged[1], {
          h: { id: id, v: 'Dddd', pa: ['Cccc', 'Bbbb'] },
          b: {
            bar: 'raboof',
            foo: 'bar',
            c: 'baz',
            d: true
          }
        });
        done();
      });
    });

    it('EI and DII = merges based on BI, BII', function(done) {
      merge(EI, DII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
        if (err) { throw err; }
        should.deepEqual(mergeX, {
          h: { id: id, pa: ['Eeee', 'Dddd'] },
          b: {
            bar: 'foo',
            some: 'secret',
            foo: 'bar',
            c: 'baz',
            e: true,
            d: true
          }
        });
        should.deepEqual(mergeY, {
          h: { id: id, pa: ['Eeee', 'Dddd'] },
          b: {
            bar: 'foo',
            foo: 'bar',
            c: 'baz',
            d: true,
            e: true
          }
        });
        done();
      });
    });

    it('FI and DII = merges based on BI, BII', function(done) {
      merge(FI, DII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
        if (err) { throw err; }
        should.deepEqual(mergeX, {
          h: { id: id, pa: ['Ffff', 'Dddd'] },
          b: {
            bar: 'foo',
            some: 'secret',
            foo: 'bar',
            c: 'baz',
            e: true,
            d: true,
            f: true
          }
        });
        should.deepEqual(mergeY, {
          h: { id: id, pa: ['Ffff', 'Dddd'] },
          b: {
            bar: 'foo',
            foo: 'bar',
            c: 'baz',
            d: true,
            e: true,
            f: true
          }
        });
        done();
      });
    });

    it('GI and DII = conflict', function(done) {
      merge(GI, DII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
        should.equal(err.message, 'merge conflict');
        should.deepEqual(merged, [['c'], ['c']]);
        done();
      });
    });
  });

  describe('criss-cross four parents', function() {
    ////// _pe I
    var AI = {
      h: { id: id, v: 'Aaaa', pa: [] },
      a: true,
      some: 'secret'
    };

    var BI = {
      h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
      a: true,
      b: true,
      some: 'secret'
    };

    var CI = {
      h: { id: id, v: 'Cccc', pa: ['Aaaa'] },
      a: true,
      c: true,
      some: 'secret'
    };

    var DI = {
      h: { id: id, v: 'Dddd', pa: ['Aaaa'] },
      a: true,
      d: true,
      some: 'secret'
    };

    var EI = {
      h: { id: id, v: 'Eeee', pa: ['Aaaa', 'Cccc'] },
      a: true,
      c: 'foo',
      e: true,
      some: 'secret'
    };

    var FI = {
      h: { id: id, v: 'Ffff', pa: ['Bbbb', 'Cccc', 'Dddd', 'Eeee'] },
      a: true,
      b: true,
      c: 'foo',
      d: true,
      e: true,
      f: true,
      some: 'secret'
    };

    var GI = {
      h: { id: id, v: 'Gggg', pa: ['Bbbb', 'Cccc', 'Dddd', 'Eeee'] },
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
      h: { id: id, v: 'Aaaa', pa: [] },
      a: true
    };

    var BII = {
      h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
      a: true,
      b: true
    };

    var CII = {
      h: { id: id, v: 'Cccc', pa: ['Aaaa'] },
      a: true,
      c: true
    };

    var DII = {
      h: { id: id, v: 'Dddd', pa: ['Aaaa'] },
      a: true,
      d: true
    };

    var EII = {
      h: { id: id, v: 'Eeee', pa: ['Aaaa', 'Cccc'] },
      a: true,
      c: 'foo',
      e: true
    };

    var FII = {
      h: { id: id, v: 'Ffff', pa: ['Bbbb', 'Cccc', 'Dddd', 'Eeee'] },
      a: true,
      b: true,
      c: 'foo',
      d: true,
      e: true,
      f: true
    };

    var GII = {
      h: { id: id, v: 'Gggg', pa: ['Bbbb', 'Cccc', 'Dddd', 'Eeee'] },
      a: true,
      b: true,
      c: 'foo',
      d: true,
      e: true,
      g: true
    };

    // create the following structure, for _pe I and II:
    //    --- B <---- F (pa: B, C, D, E)
    //   /     \/ / /
    //  /      /\/ /
    // A <--- C-/\/
    //  \      /\/\
    //   \___ D-/\ \
    //    \    /  \ \
    //     \- E <---- G (pa: B, C, D, E)
    //      (pa: A, C)
    describe('one perspective', function() {
      var collectionName = '_mergeCrissCrossFourParentsOnePerspective';

      it('should save DAG I', function(done) {
        vc._snapshotCollection.insert([AI, BI, CI, DI, EI, FI, GI], {w: 1}, done);
      });

      it('FI and GI = merge', function(done) {
        merge(FI, GI, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Ffff', 'Gggg'] },
            a: true,
            b: true,
            c: 'foo',
            d: true,
            e: true,
            f: true,
            g: true,
            some: 'secret'
          });
          done();
        });
      });
    });

    describe('two perspectives', function() {
      var collectionName = '_mergeCrissCrossFourParentsTwoPerspectives';

      it('should save DAG I and II', function(done) {
        vc._snapshotCollection.insert([AI, BI, CI, AII, BII, DI, CII, EI, DII, EII, FII, GII, FI, GI], {w: 1}, done);
      });

      it('FI and GII = merge', function(done) {
        merge(FI, GII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Ffff', 'Gggg'] },
            a: true,
            b: true,
            c: 'foo',
            d: true,
            e: true,
            f: true,
            g: true,
            some: 'secret'
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Ffff', 'Gggg'] },
            a: true,
            b: true,
            c: 'foo',
            d: true,
            e: true,
            f: true,
            g: true
          });
          done();
        });
      });

      it('FII and GI = merge', function(done) {
        merge(FII, GI, treeII, treeI, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Ffff', 'Gggg'] },
            a: true,
            b: true,
            c: 'foo',
            d: true,
            e: true,
            f: true,
            g: true,
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Ffff', 'Gggg'] },
            a: true,
            b: true,
            c: 'foo',
            d: true,
            e: true,
            f: true,
            g: true,
            some: 'secret'
          });
          done();
        });
      });

      it('GI and FII = merge', function(done) {
        merge(GI, FII, treeI, treeII, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Gggg', 'Ffff'] },
            a: true,
            b: true,
            c: 'foo',
            d: true,
            e: true,
            f: true,
            g: true,
            some: 'secret'
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Gggg', 'Ffff'] },
            a: true,
            b: true,
            c: 'foo',
            d: true,
            e: true,
            f: true,
            g: true
          });
          done();
        });
      });

      it('GII and FI = merge', function(done) {
        merge(GII, FI, treeII, treeI, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Gggg', 'Ffff'] },
            a: true,
            b: true,
            c: 'foo',
            d: true,
            e: true,
            f: true,
            g: true
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Gggg', 'Ffff'] },
            a: true,
            b: true,
            c: 'foo',
            d: true,
            e: true,
            f: true,
            g: true,
            some: 'secret'
          });
          done();
        });
      });
    });
  });

  describe('regression', function() {
    describe('non-symmetric multiple lca: error when fetching perspective bound lca\'s 3', function() {
      var collectionName = '_mergeRegressionNonSymmetricMultipleLca';

      ////// _pe I
      var AI = {
        h: { id: 'foo', v: 'Aaaa', pa: [] },
        a: true,
        some: 'secret'
      };

      var BI = {
        h: { id: 'foo', v: 'Bbbb', pa: ['Aaaa'] },
        b: {
          a: 'foo',
          b: true,
          some: 'secret'
        }
      };

      var CI = {
        h: { id: 'foo', v: 'Cccc', pa: ['Aaaa'] },
        b: {
          a: 'foo',
          c: true,
          some: 'secret'
        }
      };

      var EI = {
        h: { id: 'foo', v: 'Eeee', pa: ['Bbbb'] },
        b: {
          a: 'foo',
          b: 'foo',
          e: true,
          some: 'secret'
        }
      };

      var GI = {
        h: { id: 'foo', v: 'Gggg', pa: [ 'Cccc', 'Eeee'] },
        b: {
          a: 'foo',
          b: 'foo',
          c: 'foo',
          e: 'foo',
          g: true,
          some: 'secret'
        }
      };


      ////// _pe II
      var AII = {
        h: { id: 'foo', v: 'Aaaa', pa: [] },
        a: true
      };

      var BII = {
        h: { id: 'foo', v: 'Bbbb', pa: ['Aaaa'] },
        b: {
          a: 'foo',
          b: true
        }
      };

      var CII = {
        h: { id: 'foo', v: 'Cccc', pa: ['Aaaa'] },
        b: {
          a: 'foo',
          c: true
        }
      };

      var DII = {
        h: { id: 'foo', v: 'Dddd', pa: ['Bbbb', 'Cccc'] },
        b: {
          a: 'foo',
          b: 'foo',
          c: 'foo',
          d: true
        }
      };

      var EII = {
        h: { id: 'foo', v: 'Eeee', pa: ['Bbbb'] },
        b: {
          a: 'foo',
          b: 'foo',
          e: true
        }
      };

      var FII = {
        h: { id: 'foo', v: 'Ffff', pa: ['Dddd', 'Eeee'] },
        b: {
          a: 'foo',
          b: 'foo',
          c: 'foo',
          d: 'foo',
          e: 'foo',
          f: true
        }
      };

      var GII = {
        h: { id: 'foo', v: 'Gggg', pa: [ 'Cccc', 'Eeee'] },
        b: {
          a: 'foo',
          b: 'foo',
          c: 'foo',
          e: 'foo',
          g: true
        }
      };

      var HII = {
        h: { id: 'foo', v: 'Hhhh', pa: [ 'Ffff', 'Gggg'] },
        b: {
          a: 'foo',
          b: 'foo',
          c: 'foo',
          d: 'foo',
          e: 'foo',
          f: 'foo',
          g: 'foo',
          h: true
        }
      };

      // create the following structure, for _pe I and II:
      // _pe I
      //          E
      //         / \
      //        /   \
      //       /     \
      //      B       G
      //     /       /
      //    /       /
      //   /       /
      //  A---C----
      //
      // _pe II
      //          E--------
      //         / \       \
      //        /   \       \
      //       /     \       \
      //      B---D---F---H---G
      //     /   /           /
      //    /   /           /
      //   /   /           /
      //  A---C------------
      //
      it('should save DAGs', function(done) {
        vc._snapshotCollection.insert([AI, BI, CI, AII, EI, BII, CII, GI, DII, EII, FII, GII, HII], done);
      });

      it('HII and GI = ff to HI', function(done) {
        merge(HII, GI, treeII, treeI, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(merged[0], {
            h: { id: 'foo', v: 'Hhhh', pa: ['Ffff', 'Gggg'] },
            b: {
              a: 'foo',
              b: 'foo',
              c: 'foo',
              d: 'foo',
              e: 'foo',
              f: 'foo',
              g: 'foo',
              h: true
            }
          });
          should.deepEqual(merged[1], {
            h: { id: 'foo', v: 'Hhhh', pa: ['Ffff', 'Gggg'] },
            b: {
              a: 'foo',
              b: 'foo',
              c: 'foo',
              d: 'foo',
              e: 'foo',
              f: 'foo',
              g: 'foo',
              h: true,
              some: 'secret'
            }
          });
          done();
        });
      });
    });
  });
  */
});
