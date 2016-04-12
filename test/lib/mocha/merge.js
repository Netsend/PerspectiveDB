/**
 * Copyright 2014, 2015, 2015 Netsend.
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
var rimraf = require('rimraf');
var level = require('level-packager')(require('leveldown'));
var streamify = require('stream-array');

// wrapper around streamify that supports "reopen" recursively
function streamifier(dag) {
  var result = streamify(dag);
  result.reopen = function() {
    return streamifier(dag);
  };
  return result;
}

var merge = require('../../../lib/merge');
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

describe('_doMerge', function() {
  var _doMerge = merge._doMerge;

  describe('constructor', function() {
    it('should require itemX to be an object', function() {
      (function() { _doMerge(null); }).should.throw('itemX must be an object');
    });

    it('should require itemY to be an object', function() {
      (function() { _doMerge({}, null); }).should.throw('itemY must be an object');
    });

    it('should require lcaX to be an object', function() {
      (function() { _doMerge({}, {}, null); }).should.throw('lcaX must be an object');
    });

    it('should require lcaY to be an object', function() {
      (function() { _doMerge({}, {}, {}, null); }).should.throw('lcaY must be an object');
    });

    it('should require that versions of itemX and itemY match', function() {
      var itemX = { h: { v: 'a' }, b: {} };
      var lcaX  = { h: { v: 'b' }, b: {} };

      var itemY = { h: { v: 'b' }, b: {} };
      var lcaY  = { h: { v: 'b' }, b: {} };

      try {
        _doMerge(itemX, itemY, lcaX, lcaY);
      } catch(err) {
        should.equal(err.message, 'id mismatch');
      }
    });

    it('should copy itemY.h.d on fast-forward of X to Y', function() {
      var itemX = { h: { id: 'XI', v: 'a' }, b: {} };
      var lcaX  = { h: { id: 'XI', v: 'a' }, b: {} };

      var itemY = { h: { id: 'XI', v: 'b', pa: ['a'], d: true }, b: {} };
      var lcaY  = { h: { id: 'XI', v: 'a' }, b: {} };

      var merge = _doMerge(itemX, itemY, lcaX, lcaY);
      should.deepEqual(merge[0], { h: { id: 'XI', v: 'b', pa: ['a'], d: true }, b: {} });
      should.deepEqual(merge[1], { h: { id: 'XI', v: 'b', pa: ['a'], d: true }, b: {} });
    });

    it('should copy itemX.h.d on fast-forward of Y to X', function() {
      var itemX = { h: { id: 'XI', v: 'b', pa: ['a'], d: true }, b: {} };
      var lcaX  = { h: { id: 'XI', v: 'a' }, b: {} };

      var itemY = { h: { id: 'XI', v: 'a' }, b: {} };
      var lcaY  = { h: { id: 'XI', v: 'a' }, b: {} };

      var merge = _doMerge(itemX, itemY, lcaX, lcaY);
      should.deepEqual(merge[0], { h: { id: 'XI', v: 'b', pa: ['a'], d: true }, b: {} });
      should.deepEqual(merge[1], { h: { id: 'XI', v: 'b', pa: ['a'], d: true }, b: {} });
    });

    describe('regress', function() {
      it('should require that both items be merged', function() {
        var itemX = { h: { id: 'foo',           pa: ['Hhhh','Gggg'] },        b: {                   c: true,  some: 'secret', d: 'bar',                   h: true, g: true } };
        var lcaX  = { h: { id: 'foo',           pa: ['Eeee','Dddd','Cccc'] }, b: { a: true, b: true, c: true,  some: 'secret', d: true,  e: true } };

        var itemY = { h: { id: 'foo', v:'Ffff', pa: ['Cccc','Dddd','Eeee'] }, b: { a: true, b: true, c: 'foo', some: 'secret', d: true,  e: true, f: true } };
        var lcaY  = { h: { id: 'foo',           pa: ['Eeee','Dddd','Cccc'] }, b: { a: true, b: true, c: true,  some: 'secret', d: true,  e: true } };

        // expect:  { h: { id: 'foo',           pa: ['Ffff','Hhhh','Gggg'] }, b: {                   c: 'foo', some: 'secret', d: 'bar',          f: true, h: true, g: true } }

        var merge = _doMerge(itemX, itemY, lcaX, lcaY);
        should.deepEqual(merge[0], {
          h: { id: 'foo', pa: ['Hhhh','Gggg','Ffff'] },
          b: {
            c: 'foo',
            some: 'secret',
            d: 'bar',
            f: true,
            h: true,
            g: true
          }
        });
        should.deepEqual(merge[1], {
          h: { id: 'foo', pa: ['Hhhh','Gggg','Ffff'] },
          b: {
            c: 'foo',
            some: 'secret',
            d: 'bar',
            f: true,
            h: true,
            g: true
          }
        });
      });

      it('should copy itemY.h.d on fast-forward of X to Y (without itemY.b)', function() {
        var itemX = { h: { id: 'XI', v: 'a' }, b: {} };
        var lcaX  = { h: { id: 'XI', v: 'a' }, b: {} };

        var itemY = { h: { id: 'XI', v: 'b', pa: ['a'], d: true } };
        var lcaY  = { h: { id: 'XI', v: 'a' }, b: {} };

        var merge = _doMerge(itemX, itemY, lcaX, lcaY);
        should.deepEqual(merge[0], { h: { id: 'XI', v: 'b', pa: ['a'], d: true }, b: {} });
        should.deepEqual(merge[1], { h: { id: 'XI', v: 'b', pa: ['a'], d: true } });
      });

      it('should copy itemX.h.d on fast-forward of Y to X (without itemX.b)', function() {
        var itemX = { h: { id: 'XI', v: 'b', pa: ['a'], d: true } };
        var lcaX  = { h: { id: 'XI', v: 'a' }, b: {} };

        var itemY = { h: { id: 'XI', v: 'a' }, b: {} };
        var lcaY  = { h: { id: 'XI', v: 'a' }, b: {} };

        var merge = _doMerge(itemX, itemY, lcaX, lcaY);
        should.deepEqual(merge[0], { h: { id: 'XI', v: 'b', pa: ['a'], d: true } });
        should.deepEqual(merge[1], { h: { id: 'XI', v: 'b', pa: ['a'], d: true }, b: {} });
      });

      it('should not copy itemY.h.d on merge of X and Y (without itemY.b)', function() {
        var itemX = { h: { id: 'XI', v: 'c', pa: ['a'] }, b: {} };
        var lcaX  = { h: { id: 'XI', v: 'a' }, b: {} };

        var itemY = { h: { id: 'XI', v: 'b', pa: ['a'], d: true } };
        var lcaY  = { h: { id: 'XI', v: 'a' }, b: {} };


        var merge = _doMerge(itemX, itemY, lcaX, lcaY);
        should.deepEqual(merge[0], { h: { id: 'XI', pa: ['c', 'b'] }, b: {} });
        should.deepEqual(merge[1], { h: { id: 'XI', pa: ['c', 'b'] }, b: {} });
      });

      it('should not copy itemX.h.d on merge of X and Y (without itemX.b)', function() {
        var itemX = { h: { id: 'XI', v: 'c', pa: ['a'], d: true } };
        var lcaX  = { h: { id: 'XI', v: 'a' }, b: {} };

        var itemY = { h: { id: 'XI', v: 'b', pa: ['a'] }, b: {} };
        var lcaY  = { h: { id: 'XI', v: 'a' }, b: {} };


        var merge = _doMerge(itemX, itemY, lcaX, lcaY);
        should.deepEqual(merge[0], { h: { id: 'XI', pa: ['c', 'b'] }, b: {} });
        should.deepEqual(merge[1], { h: { id: 'XI', pa: ['c', 'b'] }, b: {} });
      });
    });
  });
});

describe('merge', function() {
  var id = 'foo';

  describe('constructor', function() {
    it('should require sX to be a stream.Readable', function() {
      (function() { merge(null); }).should.throw('sX must be a stream.Readable');
    });

    it('should require sY to be a stream.Readable', function() {
      (function() { merge({}, null); }).should.throw('sY must be a stream.Readable');
    });

    it('should require cb to be a function', function() {
      (function() { merge({}, {}); }).should.throw('cb must be a function');
    });

    it('should require opts to be an object', function() {
      (function() { merge({}, {}, [], function() {}); }).should.throw('opts must be an object');
    });
  });

  describe('one perspective (tree)', function() {
    describe('basic', function() {
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

      var DAG = [A, B, C, D, E, F];

      // create graphs that start at the leaf, might contain multiple roots
      var dA = DAG.slice(0, 1).reverse();
      var dB = DAG.slice(0, 2).reverse();
      var dC = DAG.slice(0, 3).reverse();
      var dD = DAG.slice(0, 4).reverse();
      var dE = DAG.slice(0, 5).reverse();
      var dF = DAG.slice(0, 6).reverse();

      it('A and A = original A', function(done) {
        var x = streamifier(dA);
        var y = streamifier(dA);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Aaaa', pa: [] },
            b: {
              foo: 'bar',
              bar: 'baz',
              qux: 'quux'
            }
          });
          should.deepEqual(mergeX, mergeY);
          done();
        });
      });


      it('B and C = merge', function(done) {
        var x = streamifier(dB);
        var y = streamifier(dC);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Bbbb', 'Cccc'] },
            b: {
              foo: 'bar',
              bar: 'raboof',
              qux: 'qux'
            }
          });
          should.deepEqual(mergeX, mergeY);
          done();
        });
      });

      it('E and B = ff to E', function(done) {
        var x = streamifier(dE);
        var y = streamifier(dB);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Eeee', pa: ['Cccc', 'Bbbb'] },
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
        var x = streamifier(dD);
        var y = streamifier(dE);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Dddd', 'Eeee'] },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'quz'
            }
          });
          should.deepEqual(mergeX, mergeY);
          done();
        });
      });

      it('E and D = merge', function(done) {
        var x = streamifier(dE);
        var y = streamifier(dD);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Eeee', 'Dddd'] },
            b: {
              foo: 'bar',
              bar: 'foobar',
              qux: 'quz'
            }
          });
          should.deepEqual(mergeX, mergeY);
          done();
        });
      });

      it('E and F = ff to F', function(done) {
        var x = streamifier(dE);
        var y = streamifier(dF);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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

      it('F and E = ff to F', function(done) {
        var x = streamifier(dF);
        var y = streamifier(dE);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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

        var x = streamifier([vm1].concat(dF));
        var y = streamifier([vm2].concat(dF));

        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['x', 'y'] },
            b: {
              a: true,
              b: true
            }
          });
          should.deepEqual(mergeX, mergeY);
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

        var x = streamifier([vm1].concat(dF));
        var y = streamifier([vm2].concat(dF));

        merge(x, y, { log: silence }, function(err) {
          should.equal(err.message, 'merge conflict');
          should.deepEqual(err.conflict, ['v']);
          done();
        });
      });
    });

    describe('delete one', function() {
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
        h: { id: id, v: 'Cccc', pa: ['Aaaa'], d: true }
      };


      var DAG = [A, B, Cd];

      // create graphs that start at the leaf, might contain multiple roots
      //var dA = DAG.slice(0, 1).reverse();
      var dB = DAG.slice(0, 2).reverse();
      var dC = DAG.slice(0, 3).reverse();

      it('B and Cd = conflict (because C deleted body)', function(done) {
        var x = streamifier(dB);
        var y = streamifier(dC);
        merge(x, y, { log: silence }, function(err) {
          should.equal(err.message, 'merge conflict');
          should.deepEqual(err.conflict, ['qux']);
          done();
        });
      });

      it('Cd and B = conflict (because C deleted body)', function(done) {
        var x = streamifier(dC);
        var y = streamifier(dB);
        merge(x, y, { log: silence }, function(err) {
          should.equal(err.message, 'merge conflict');
          should.deepEqual(err.conflict, ['qux']);
          done();
        });
      });
    });

    describe('delete two', function() {
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
        h: { id: id, v: 'Bbbb', pa: ['Aaaa'], d: true }
      };

      var Cd = {
        h: { id: id, v: 'Cccc', pa: ['Aaaa'], d: true }
      };


      var DAG = [A, Bd, Cd];

      // create graphs that start at the leaf, might contain multiple roots
      //var dA = DAG.slice(0, 1).reverse();
      var dB = DAG.slice(0, 2).reverse();
      var dC = DAG.slice(0, 3).reverse();

      it('B and C = merge, delete', function(done) {
        var x = streamifier(dB);
        var y = streamifier(dC);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Bbbb', 'Cccc'], d: true },
            b: {}
          });
          should.deepEqual(mergeX, mergeY);
          done();
        });
      });

      it('C and B = merge, delete', function(done) {
        var x = streamifier(dC);
        var y = streamifier(dB);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Cccc', 'Bbbb'], d: true },
            b: {}
          });
          should.deepEqual(mergeX, mergeY);
          done();
        });
      });
    });

    describe('deleted head and a new root', function() {
      // create the following structure:
      //  A---Bd   C (new root)

      var A = {
        h: { id: id, v: 'Aaaa', pa: [] },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'quux'
        }
      };

      var Bd = {
        h: { id: id, v: 'Bbbb', pa: ['Aaaa'], d: true }
      };

      var C = {
        h: { id: id, v: 'Cccc', pa: [] },
        b: {
          some: 'new'
        }
      };

      var DAG = [A, Bd, C];

      // one graph with a deleted head, one graph with an extra new root
      var dB = DAG.slice(0, 2).reverse();
      var dC = DAG.slice(0, 3).reverse();

      it('Bd and C are two different DAGs, error', function(done) {
        var x = streamifier(dB);
        var y = streamifier(dC);
        merge(x, y, { log: silence }, function(err) {
          should.strictEqual(err.message, 'no lca found');
          done();
        });
      });
    });

    describe('deleted head and a connecting new "root"', function() {
      // create the following structure:
      //  A---Bd   C (new root)

      var A = {
        h: { id: id, v: 'Aaaa', pa: [] },
        b: {
          foo: 'bar',
          bar: 'baz',
          qux: 'quux'
        }
      };

      var Bd = {
        h: { id: id, v: 'Bbbb', pa: ['Aaaa'], d: true }
      };

      var C = {
        h: { id: id, v: 'Cccc', pa: ['Bbbb'] },
        b: {
          some: 'new'
        }
      };

      var DAG = [A, Bd, C];

      // one graph with a deleted head, one graph with an extra new root
      var dB = DAG.slice(0, 2).reverse();
      var dC = DAG.slice(0, 3).reverse();

      it('Bd and C = fast-forward', function(done) {
        var x = streamifier(dB);
        var y = streamifier(dC);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Cccc', pa: ['Bbbb'] },
            b: {
              some: 'new'
            }
          });
          should.deepEqual(mergeX, mergeY);
          done();
        });
      });

      it('C and Bd = fast-forward', function(done) {
        var x = streamifier(dC);
        var y = streamifier(dB);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Cccc', pa: ['Bbbb'] },
            b: {
              some: 'new'
            }
          });
          should.deepEqual(mergeX, mergeY);
          done();
        });
      });
    });
  });

  describe('two perspectives', function() {
    describe('basic', function() {
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

      var DAGI  = [AI, BI, EI];
      var DAGII = [AII, CII, BII, DII];

      // create graphs that start at the leaf, might contain multiple roots
      var dAI = DAGI.slice(0, 1).reverse();
      var dBI = DAGI.slice(0, 2).reverse();
      var dEI = DAGI.slice(0, 3).reverse();

      var dAII = DAGII.slice(0, 1).reverse();
      var dCII = DAGII.slice(0, 2).reverse();
      var dBII = DAGII.slice(0, 3).reverse();
      var dDII = DAGII.slice(0, 4).reverse();

      it('AI and AII = ff to AI, ff to AII', function(done) {
        var x = streamifier(dAI);
        var y = streamifier(dAII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Aaaa', pa: [] },
            b: {
              baz : 'qux',
              bar: 'raboof',
              some: 'secret'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Aaaa', pa: [] },
            b: {
              baz : 'qux',
              bar: 'raboof'
            }
          });
          done();
        });
      });

      it('AII and AI = ff to AII, ff to AI', function(done) {
        var x = streamifier(dAII);
        var y = streamifier(dAI);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Aaaa', pa: [] },
            b: {
              baz : 'qux',
              bar: 'raboof'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Aaaa', pa: [] },
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
        var x = streamifier(dBI);
        var y = streamifier(dDII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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
            h: { id: id, v: 'Dddd', pa: ['Cccc', 'Bbbb'] },
            b: {
              bar: 'raboof',
              foo: 'bar'
            }
          });
          done();
        });
      });

      it('EI and DII = merges based on BI, BII', function(done) {
        var x = streamifier(dEI);
        var y = streamifier(dDII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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
        var x = streamifier(dEI);
        var y = streamifier(dBII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Eeee', pa: ['Bbbb'] },
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
        var x = streamifier(dBI);
        var y = streamifier(dCII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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

        var x = streamifier([vm1].concat(dEI));
        var y = streamifier([vm2].concat(dDII));
        merge(x, y, { log: silence }, function(err) {
          should.equal(err.message, 'merge conflict');
          should.deepEqual(err.conflict, ['foo']);
          done();
        });
      });
    });

    describe('delete one', function() {
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

      var DAGI = [AI, BI, CId];
      var DAGII = [AII, BII, CIId];

      // create graphs that start at the leaf, might contain multiple roots
      //var dAI = DAGI.slice(0, 1).reverse();
      var dBI = DAGI.slice(0, 2).reverse();
      var dCI = DAGI.slice(0, 3).reverse();

      //var dAII = DAGII.slice(0, 1).reverse();
      var dBII = DAGII.slice(0, 2).reverse();
      var dCII = DAGII.slice(0, 3).reverse();

      it('BI and CIId = merge, no delete (because CIId didn\'t change body)', function(done) {
        var x = streamifier(dBI);
        var y = streamifier(dCII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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

      it('BII and CId = merge, no delete (because CId didn\'t change body)', function(done) {
        var x = streamifier(dBII);
        var y = streamifier(dCI);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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
        var x = streamifier(dCI);
        var y = streamifier(dBII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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
        var x = streamifier(dCII);
        var y = streamifier(dBI);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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

      var DAGI = [AI, BId, CId];
      var DAGII = [AII, BIId, CIId];

      // create graphs that start at the leaf, might contain multiple roots
      //var dAI = DAGI.slice(0, 1).reverse();
      var dBI = DAGI.slice(0, 2).reverse();
      var dCI = DAGI.slice(0, 3).reverse();

      //var dAII = DAGII.slice(0, 1).reverse();
      var dBII = DAGII.slice(0, 2).reverse();
      var dCII = DAGII.slice(0, 3).reverse();

      it('BI and CII = merge, delete', function(done) {
        var x = streamifier(dBI);
        var y = streamifier(dCII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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
        var x = streamifier(dBII);
        var y = streamifier(dCI);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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

      var DAGI  = [AI, BI, CI, DI, EI, FI];
      var DAGII = [AII, BII, CII, DII, EII, FII];

      // create graphs that start at the leaf, might contain multiple roots
      var dAI = DAGI.slice(0, 1).reverse();
      var dBI = DAGI.slice(0, 2).reverse();
      //var dCI = DAGI.slice(0, 3).reverse();
      var dDI = DAGI.slice(0, 4).reverse();
      var dEI = DAGI.slice(0, 5).reverse();
      //var dFI = DAGI.slice(0, 6).reverse();

      var dAII = DAGII.slice(0, 1).reverse();
      //var dBII = DAGII.slice(0, 2).reverse();
      var dCII = DAGII.slice(0, 3).reverse();
      //var dDII = DAGII.slice(0, 4).reverse();
      var dEII = DAGII.slice(0, 5).reverse();
      var dFII = DAGII.slice(0, 6).reverse();

      it('AI and AII = ff to AI, ff to AII', function(done) {
        var x = streamifier(dAI);
        var y = streamifier(dAII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Aaaa', pa: [] },
            b: {
              foo: 'bar',
              bar: 'baz',
              qux: 'quux',
              some: 'secret'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Aaaa', pa: [] },
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
        var x = streamifier(dBI);
        var y = streamifier(dCII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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
        var x = streamifier(dDI);
        var y = streamifier(dEII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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
        var x = streamifier(dEII);
        var y = streamifier(dDI);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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
        var x = streamifier(dEI);
        var y = streamifier(dFII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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

      it('FII and EI = ff to FII, merge ff to FI', function(done) {
        var x = streamifier(dFII);
        var y = streamifier(dEI);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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

      var DAGI  = [AI, BI, CI, DI, EI, FI];
      var DAGII = [AII];

      // create graphs that start at the leaf, might contain multiple roots
      var dAI = DAGI.slice(0, 1).reverse();
      //var dBI = DAGI.slice(0, 2).reverse();
      var dCI = DAGI.slice(0, 3).reverse();
      //var dDI = DAGI.slice(0, 4).reverse();
      //var dEI = DAGI.slice(0, 5).reverse();
      var dFI = DAGI.slice(0, 6).reverse();

      var dAII = DAGII.slice(0, 1).reverse();

      it('AII and FI = merged ff to FII, ff to FI', function(done) {
        var x = streamifier(dAII);
        var y = streamifier(dFI);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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
            h: { id: id, v: 'Ffff', pa: ['Cccc', 'Dddd', 'Eeee'] },
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
        var x = streamifier(dAII);
        var y = streamifier(dCI);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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
            h: { id: id, v: 'Cccc', pa: ['Bbbb'] },
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
        var x = streamifier(dAI);
        var y = streamifier(dAII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Aaaa', pa: [] },
            b: {
              foo: 'bar',
              bar: 'baz',
              qux: 'quux',
              some: 'secret'
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, v: 'Aaaa', pa: [] },
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

      var DAGI  = [AI, BI, CI, DI, EI, FI, GI, FIc, GIc, HI];
      var DAGII = [AII, BII, CII, DII, EII, FII, GII ];

      // create graphs that start at the leaf, might contain multiple roots
      //var dAI = DAGI.slice(0, 1).reverse();
      //var dBI = DAGI.slice(0, 2).reverse();
      //var dCI = DAGI.slice(0, 3).reverse();
      //var dDI = DAGI.slice(0, 4).reverse();
      //var dEI = DAGI.slice(0, 5).reverse();
      var dFI = DAGI.slice(0, 6).reverse();
      var dGI = DAGI.slice(0, 7).reverse();
      var dFIc = DAGI.slice(0, 8).reverse();
      var dGIc = DAGI.slice(0, 9).reverse();
      var dHI = DAGI.slice(0, 10).reverse();

      var dAII = DAGII.slice(0, 1).reverse();
      //var dBII = DAGII.slice(0, 2).reverse();
      //var dCII = DAGII.slice(0, 3).reverse();
      //var dDII = DAGII.slice(0, 4).reverse();
      //var dEII = DAGII.slice(0, 5).reverse();
      //var dFII = DAGII.slice(0, 6).reverse();
      var dGII = DAGII.slice(0, 7).reverse();

      it('GI and FI = merge', function(done) {
        var x = streamifier(dGI);
        var y = streamifier(dFI);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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
          should.deepEqual(mergeX, mergeY);
          done();
        });
      });

      it('GII and FI = merge', function(done) {
        var x = streamifier(dGII);
        var y = streamifier(dFI);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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

      it('FI and GII = merge', function(done) {
        var x = streamifier(dFI);
        var y = streamifier(dGII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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
        var x = streamifier(dAII);
        var y = streamifier(dHI);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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
            h: { id: id, v: 'Hhhh', pa: ['Ffff', 'Gggg'] },
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
        var x = streamifier(dGIc);
        var y = streamifier(dFIc);
        merge(x, y, { log: silence }, function(err) {
          should.equal(err.message, 'merge conflict');
          should.deepEqual(err.conflict, ['d', 'e']);
          done();
        });
      });
    });

    describe('double criss-cross 3-parent merge', function() {
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
        h: { id: id, v: 'Iiii', pa: ['Ffff', 'Gggg', 'Hhhh'] },
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
        h: { id: id, v: 'Jjjj', pa: ['Ffff', 'Gggg', 'Hhhh'] },
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
        h: { id: id, v: 'Fffc', pa: ['Cccc', 'Dddd', 'Eeee'] },
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
        h: { id: id, v: 'Gggc', pa: ['Cccc', 'Dddd', 'Eeee'] },
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
        h: { id: id, v: 'Iiic', pa: ['Ffff', 'Gggg', 'Hhhh'] },
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
        h: { id: id, v: 'Jjjc', pa: ['Ffff', 'Gggg', 'Hhhh'] },
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
        h: { id: id, v: 'Iiii', pa: ['Ffff', 'Gggg', 'Hhhh'] },
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
        h: { id: id, v: 'Jjjj', pa: ['Ffff', 'Gggg', 'Hhhh'] },
        b: {
          c: 'foo',
          d: 'bar',
          f: 'JII',
          g: 'JII',
          h: true,
          j: true,
        }
      };

      var DAGI  = [AI, BI, CI, DI, EI, FI, GI, FIc, GIc, HI, II, JI, IIc, JIc];
      var DAGII = [AII, BII, CII, DII, EII, FII, GII, HII, III, JII];

      // create graphs that start at the leaf, might contain multiple roots
      //var dAI = DAGI.slice(0, 1).reverse();
      //var dBI = DAGI.slice(0, 2).reverse();
      //var dCI = DAGI.slice(0, 3).reverse();
      //var dDI = DAGI.slice(0, 4).reverse();
      //var dEI = DAGI.slice(0, 5).reverse();
      var dFI = DAGI.slice(0, 6).reverse();
      var dGI = DAGI.slice(0, 7).reverse();
      //var dFIc = DAGI.slice(0, 8).reverse();
      //var dGIc = DAGI.slice(0, 9).reverse();
      var dHI = DAGI.slice(0, 10).reverse();
      var dII = DAGI.slice(0, 11).reverse();
      var dJI = DAGI.slice(0, 12).reverse();
      var dIIc = DAGI.slice(0, 13).reverse();
      var dJIc = DAGI.slice(0, 14).reverse();

      //var dAII = DAGII.slice(0, 1).reverse();
      //var dBII = DAGII.slice(0, 2).reverse();
      //var dCII = DAGII.slice(0, 3).reverse();
      //var dDII = DAGII.slice(0, 4).reverse();
      //var dEII = DAGII.slice(0, 5).reverse();
      var dFII = DAGII.slice(0, 6).reverse();
      //var dGII = DAGII.slice(0, 7).reverse();
      //var dHII = DAGII.slice(0, 8).reverse();
      var dIII = DAGII.slice(0, 9).reverse();
      //var dJII = DAGII.slice(0, 10).reverse();

      describe('one perspective', function() {
        it('FI and GI = merge', function(done) {
          var x = streamifier(dFI);
          var y = streamifier(dGI);
          merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
            if (err) { throw err; }
            should.deepEqual(mergeX, {
              h: { id: id, pa: ['Ffff', 'Gggg'] },
              b: {
                a: true,
                c: 'foo',
                d: 'bar',
                e: true,
                f: true,
                g: true,
                some: 'secret'
              }
            });
            should.deepEqual(mergeX, mergeY);
            done();
          });
        });

        it('II and JI = merge', function(done) {
          var x = streamifier(dII);
          var y = streamifier(dJI);
          merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
            if (err) { throw err; }
            should.deepEqual(mergeX, {
              h: { id: id, pa: ['Iiii', 'Jjjj'] },
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
            should.deepEqual(mergeX, mergeY);
            done();
          });
        });

        it('IIc and JIc = conflict', function(done) {
          var x = streamifier(dIIc);
          var y = streamifier(dJIc);
          merge(x, y, { log: silence }, function(err) {
            should.equal(err.message, 'merge conflict');
            should.deepEqual(err.conflict, ['f']);
            done();
          });
        });
      });

      describe('two perspectives', function() {
        it('FII and GI = merge', function(done) {
          var x = streamifier(dFII);
          var y = streamifier(dGI);
          merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
            if (err) { throw err; }
            should.deepEqual(mergeX, {
              h: { id: id, pa: ['Ffff', 'Gggg'] },
              b: {
                a: true,
                c: 'foo',
                d: 'bar',
                e: true,
                f: true,
                g: true,
              }
            });
            should.deepEqual(mergeY, {
              h: { id: id, pa: ['Ffff', 'Gggg'] },
              b: {
                a: true,
                c: 'foo',
                d: 'bar',
                e: true,
                f: true,
                g: true,
                some: 'secret'
              }
            });
            done();
          });
        });

        it('HI and III = merged ff to II, ff to III', function(done) {
          var x = streamifier(dHI);
          var y = streamifier(dIII);
          merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
            if (err) { throw err; }
            should.deepEqual(mergeX, {
              h: { id: id, v: 'Iiii', pa: ['Ffff', 'Gggg', 'Hhhh'] },
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
              h: { id: id, v: 'Iiii', pa: ['Ffff', 'Gggg', 'Hhhh'] },
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
          var x = streamifier(dII);
          var y = streamifier(dIII);
          merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
            if (err) { throw err; }
            should.deepEqual(mergeX, {
              h: { id: id, v: 'Iiii', pa: ['Ffff', 'Gggg', 'Hhhh'] },
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
              h: { id: id, v: 'Iiii', pa: ['Ffff', 'Gggg', 'Hhhh'] },
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
          var x = streamifier(dJIc);
          var y = streamifier(dIII);
          merge(x, y, { log: silence }, function(err) {
            should.equal(err.message, 'merge conflict');
            should.deepEqual(err.conflict, ['h']);
            done();
          });
        });

        it('JI and III = merge', function(done) {
          var x = streamifier(dJI);
          var y = streamifier(dIII);
          merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
            if (err) { throw err; }
            should.deepEqual(mergeX, {
              h: { id: id, pa: ['Jjjj', 'Iiii'] },
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
              h: { id: id, pa: ['Jjjj', 'Iiii'] },
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

    describe('DAGs that fork', function() {
      // resulting DAGs:
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
          foo: 'bar',
          d: true
        }
      };

      var DAGI  = [AI, BI, EI];
      var DAGII = [AII, CII, BII, DII];

      // create graphs that start at the leaf, might contain multiple roots
      //var dAI = DAGI.slice(0, 1).reverse();
      var dBI = DAGI.slice(0, 2).reverse();
      var dEI = DAGI.slice(0, 3).reverse();

      //var dAII = DAGII.slice(0, 1).reverse();
      //var dCII = DAGII.slice(0, 2).reverse();
      //var dBII = DAGII.slice(0, 3).reverse();
      var dDII = DAGII.slice(0, 4).reverse();

      it('BI and DII = merged ff to DI, ff to DII', function(done) {
        var x = streamifier(dBI);
        var y = streamifier(dDII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Dddd', pa: ['Cccc', 'Bbbb'] },
            b: {
              bar: 'raboof',
              some: 'secret',
              foo: 'bar',
              d: true
            }
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
        var x = streamifier(dEI);
        var y = streamifier(dDII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
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

    describe('more DAGs that fork', function() {
      // resulting DAGs:
      //                   AII <-- BII
      //                     \       \
      //                     CII <-- DII
      //
      // AI <-- BI <-- EI <-- FI <-- GI


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

      var DAGI  = [AI, BI, EI, FI, GI];
      var DAGII = [AII, CII, BII, DII];

      // create graphs that start at the leaf, might contain multiple roots
      //var dAI = DAGI.slice(0, 1).reverse();
      var dBI = DAGI.slice(0, 2).reverse();
      var dEI = DAGI.slice(0, 3).reverse();
      var dFI = DAGI.slice(0, 4).reverse();
      var dGI = DAGI.slice(0, 5).reverse();

      //var dAII = DAGII.slice(0, 1).reverse();
      var dCII = DAGII.slice(0, 2).reverse();
      //var dBII = DAGII.slice(0, 3).reverse();
      var dDII = DAGII.slice(0, 4).reverse();

      it('BI and CII = conflict', function(done) {
        var x = streamifier(dBI);
        var y = streamifier(dCII);
        merge(x, y, { log: silence }, function(err) {
          should.equal(err.message, 'merge conflict');
          should.deepEqual(err.conflict, ['c']);
          done();
        });
      });

      it('BI and DII = merged ff to DI, ff to DII', function(done) {
        var x = streamifier(dBI);
        var y = streamifier(dDII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, v: 'Dddd', pa: ['Cccc', 'Bbbb'] },
            b: {
              bar: 'raboof',
              some: 'secret',
              foo: 'bar',
              c: 'baz',
              d: true
            }
          });
          should.deepEqual(mergeY, {
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
        var x = streamifier(dEI);
        var y = streamifier(dDII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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
        var x = streamifier(dFI);
        var y = streamifier(dDII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
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
        var x = streamifier(dGI);
        var y = streamifier(dDII);
        merge(x, y, { log: silence }, function(err) {
          should.equal(err.message, 'merge conflict');
          should.deepEqual(err.conflict, ['c']);
          done();
        });
      });
    });

    describe('criss-cross four parents', function() {
      // create the following structure, for pe I and II:
      //    -------B------F (pa: B, C, D, E)
      //   /        \/ / /
      //  /         /\/ /
      // A --------D /\/
      //  \         X /\
      //   \-----C---X  \
      //    \     \ X \  \
      //     \-----E------G (pa: B, C, D, E)
      //      (pa: A, C)

      ////// pe I
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
        h: { id: id, v: 'Cccc', pa: ['Aaaa'] },
        b: {
          a: true,
          c: true,
          some: 'secret'
        }
      };

      var DI = {
        h: { id: id, v: 'Dddd', pa: ['Aaaa'] },
        b: {
          a: true,
          d: true,
          some: 'secret'
        }
      };

      var EI = {
        h: { id: id, v: 'Eeee', pa: ['Aaaa', 'Cccc'] },
        b: {
          a: true,
          c: 'foo',
          e: true,
          some: 'secret'
        }
      };

      var FI = {
        h: { id: id, v: 'Ffff', pa: ['Bbbb', 'Cccc', 'Dddd', 'Eeee'] },
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

      var GI = {
        h: { id: id, v: 'Gggg', pa: ['Bbbb', 'Cccc', 'Dddd', 'Eeee'] },
        b: {
          a: true,
          b: true,
          c: 'foo',
          d: true,
          e: true,
          g: true,
          some: 'secret'
        }
      };

      ////// pe II
      var AII = {
        h: { id: id, v: 'Aaaa', pa: [] },
        b: {
          a: true
        }
      };

      var BII = {
        h: { id: id, v: 'Bbbb', pa: ['Aaaa'] },
        b: {
          a: true,
          b: true
        }
      };

      var CII = {
        h: { id: id, v: 'Cccc', pa: ['Aaaa'] },
        b: {
          a: true,
          c: true
        }
      };

      var DII = {
        h: { id: id, v: 'Dddd', pa: ['Aaaa'] },
        b: {
          a: true,
          d: true
        }
      };

      var EII = {
        h: { id: id, v: 'Eeee', pa: ['Aaaa', 'Cccc'] },
        b: {
          a: true,
          c: 'foo',
          e: true
        }
      };

      var FII = {
        h: { id: id, v: 'Ffff', pa: ['Bbbb', 'Cccc', 'Dddd', 'Eeee'] },
        b: {
          a: true,
          b: true,
          c: 'foo',
          d: true,
          e: true,
          f: true
        }
      };

      var GII = {
        h: { id: id, v: 'Gggg', pa: ['Bbbb', 'Cccc', 'Dddd', 'Eeee'] },
        b: {
          a: true,
          b: true,
          c: 'foo',
          d: true,
          e: true,
          g: true
        }
      };

      var DAGI  = [AI,  BI,  CI,  DI,  EI,  FI,  GI];
      var DAGII = [AII, BII, CII, DII, EII, FII, GII];

      // create graphs that start at the leaf, might contain multiple roots
      //var dAI = DAGI.slice(0, 1).reverse();
      //var dBI = DAGI.slice(0, 2).reverse();
      //var dCI = DAGI.slice(0, 3).reverse();
      //var dDI = DAGI.slice(0, 4).reverse();
      //var dEI = DAGI.slice(0, 5).reverse();
      var dFI = DAGI.slice(0, 6).reverse();
      var dGI = DAGI.slice(0, 7).reverse();

      //var dAII = DAGII.slice(0, 1).reverse();
      //var dBII = DAGII.slice(0, 2).reverse();
      //var dCII = DAGII.slice(0, 3).reverse();
      //var dDII = DAGII.slice(0, 4).reverse();
      //var dEII = DAGII.slice(0, 5).reverse();
      var dFII = DAGII.slice(0, 6).reverse();
      var dGII = DAGII.slice(0, 7).reverse();

      it('FI and GI = merge', function(done) {
        var x = streamifier(dFI);
        var y = streamifier(dGI);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Ffff', 'Gggg'] },
            b: {
              a: true,
              b: true,
              c: 'foo',
              d: true,
              e: true,
              f: true,
              g: true,
              some: 'secret'
            }
          });
          should.deepEqual(mergeX, mergeY);
          done();
        });
      });

      it('FI and GII = merge', function(done) {
        var x = streamifier(dFI);
        var y = streamifier(dGII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Ffff', 'Gggg'] },
            b: {
              a: true,
              b: true,
              c: 'foo',
              d: true,
              e: true,
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
              c: 'foo',
              d: true,
              e: true,
              f: true,
              g: true
            }
          });
          done();
        });
      });

      it('FII and GI = merge', function(done) {
        var x = streamifier(dFII);
        var y = streamifier(dGI);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Ffff', 'Gggg'] },
            b: {
              a: true,
              b: true,
              c: 'foo',
              d: true,
              e: true,
              f: true,
              g: true,
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Ffff', 'Gggg'] },
            b: {
              a: true,
              b: true,
              c: 'foo',
              d: true,
              e: true,
              f: true,
              g: true,
              some: 'secret'
            }
          });
          done();
        });
      });

      it('GI and FII = merge', function(done) {
        var x = streamifier(dGI);
        var y = streamifier(dFII);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Gggg', 'Ffff'] },
            b: {
              a: true,
              b: true,
              c: 'foo',
              d: true,
              e: true,
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
              c: 'foo',
              d: true,
              e: true,
              f: true,
              g: true
            }
          });
          done();
        });
      });

      it('GII and FI = merge', function(done) {
        var x = streamifier(dGII);
        var y = streamifier(dFI);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
            h: { id: id, pa: ['Gggg', 'Ffff'] },
            b: {
              a: true,
              b: true,
              c: 'foo',
              d: true,
              e: true,
              f: true,
              g: true
            }
          });
          should.deepEqual(mergeY, {
            h: { id: id, pa: ['Gggg', 'Ffff'] },
            b: {
              a: true,
              b: true,
              c: 'foo',
              d: true,
              e: true,
              f: true,
              g: true,
              some: 'secret'
            }
          });
          done();
        });
      });
    });
  });

  describe('regression', function() {
    describe('non-symmetric multiple lca: error when fetching perspective bound lca\'s 3', function() {
      // create the following structure, for pe I and II:
      // pe I
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
      // pe II
      //          E--------
      //         / \       \
      //        /   \       \
      //       /     \       \
      //      B---D---F---H---G
      //     /   /           /
      //    /   /           /
      //   /   /           /
      //  A---C------------

      ////// pe I
      var AI = {
        h: { id: 'foo', v: 'Aaaa', pa: [] },
        b: {
          a: true,
          some: 'secret'
        }
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


      ////// pe II
      var AII = {
        h: { id: 'foo', v: 'Aaaa', pa: [] },
        b: {
          a: true
        }
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

      var DAGI  = [AI, BI, CI, EI, GI];
      var DAGII = [AII, BII, CII, DII, EII, FII, GII, HII];

      // create graphs that start at the leaf, might contain multiple roots
      //var dAI = DAGI.slice(0, 1).reverse();
      //var dBI = DAGI.slice(0, 2).reverse();
      //var dCI = DAGI.slice(0, 3).reverse();
      //var dEI = DAGI.slice(0, 4).reverse();
      var dGI = DAGI.slice(0, 5).reverse();

      //var dAII = DAGII.slice(0, 1).reverse();
      //var dBII = DAGII.slice(0, 2).reverse();
      //var dCII = DAGII.slice(0, 3).reverse();
      //var dDII = DAGII.slice(0, 4).reverse();
      //var dEII = DAGII.slice(0, 5).reverse();
      //var dFII = DAGII.slice(0, 6).reverse();
      //var dGII = DAGII.slice(0, 7).reverse();
      var dHII = DAGII.slice(0, 8).reverse();

      it('HII and GI = ff to HI', function(done) {
        var x = streamifier(dHII);
        var y = streamifier(dGI);
        merge(x, y, { log: silence }, function(err, mergeX, mergeY) {
          if (err) { throw err; }
          should.deepEqual(mergeX, {
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
          should.deepEqual(mergeY, {
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
});
