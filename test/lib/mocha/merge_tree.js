/**
 * Copyright 2015 Netsend.
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

var should = require('should');
var rimraf = require('rimraf');
var level = require('level');

var MergeTree = require('../../../lib/merge_tree');
var Tree = require('../../../lib/tree');
var logger = require('../../../lib/logger');

var db, cons, silence;
var dbPath = tmpdir() + '/test_merge_tree';

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

describe('MergeTree', function() {
  describe('constructor', function() {
    var mtree;

    it('should require db to be an object', function() {
      (function () { mtree = new MergeTree(); }).should.throwError('db must be an object');
    });

    it('should require opts to be an object', function() {
      (function() { mtree = new MergeTree(db, []); }).should.throw('opts must be an object');
    });

    it('should require opts.vSize to be a number', function() {
      (function() { mtree = new MergeTree(db, { vSize: {} }); }).should.throw('opts.vSize must be a number');
    });

    it('should require opts.iSize to be a number', function() {
      (function() { mtree = new MergeTree(db, { iSize: {} }); }).should.throw('opts.iSize must be a number');
    });

    it('should require opts.vSize to be between 0 and 6', function() {
      (function() { mtree = new MergeTree(db, { vSize: 7 }); }).should.throw('opts.vSize must be between 0 and 6');
    });

    it('should require opts.iSize to be between 0 and 6', function() {
      (function() { mtree = new MergeTree(db, { iSize: 7 }); }).should.throw('opts.iSize must be between 0 and 6');
    });

    it('should construct', function() {
      (function() { mtree = new MergeTree(db); }).should.not.throwError();
    });
  });

  describe('_copyTo', function() {
    var sname = '_copyTo_foo';
    var dname = '_copyTo_bar';

    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa', pa: [] }, b: { some: 'body' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'] }, b: { more: 'body' } };
    var item3 = { h: { id: 'XI', v: 'Cccc', pa: ['Aaaa'] }, b: { more2: 'body' } };
    var item4 = { h: { id: 'XI', v: 'Dddd', pa: ['Cccc'] }, b: { more3: 'b' } };

    it('should require stree to be an object', function() {
      (function() { MergeTree._copyTo(); }).should.throw('stree must be an object');
    });

    it('should require dtree to be an object', function() {
      (function() { MergeTree._copyTo({}); }).should.throw('dtree must be an object');
    });

    it('should require cb to be a function', function() {
      (function() { MergeTree._copyTo({}, {}, {}); }).should.throw('cb must be a function');
    });

    it('should require opts to be an object', function() {
      (function() { MergeTree._copyTo({}, {}, 1, function() {}); }).should.throw('opts must be an object');
    });

    it('should copy an empty database to an empty database', function(done) {
      var stree = new Tree(db, sname, { log: silence });
      var dtree = new Tree(db, dname, { log: silence });
      MergeTree._copyTo(stree, dtree, function(err) {
        if (err) { throw err; }
        stree.iterateInsertionOrder(function() {
          throw new Error('stree: no new item expected');
        }, function(err) {
          if (err) { throw err; }
          dtree.iterateInsertionOrder(function() {
            throw new Error('dtree: no new item expected');
          }, done);
        });
      });
    });

    it('needs item1 in stree', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      stree.end(item1, done);
    });

    it('should copy stree to dtree with one item', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      var dtree = new Tree(db, dname, { vSize: 3, log: silence });

      var streeIt = 0;
      var dtreeIt = 0;

      MergeTree._copyTo(stree, dtree, function(err) {
        if (err) { throw err; }
        stree.iterateInsertionOrder(function(item, next) {
          should.deepEqual(item, item1);
          streeIt++;
          next();
        }, function(err) {
          if (err) { throw err; }
          dtree.iterateInsertionOrder(function(item, next) {
            should.deepEqual(item, item1);
            dtreeIt++;
            next();
          }, function(err) {
            if (err) { throw err; }
            should.strictEqual(streeIt, 1);
            should.strictEqual(dtreeIt, 1);
            done();
          });
        });
      });
    });

    it('needs item2 in stree', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      var streeIt = 0;
      stree.end(item2, function(err) {
        if (err) { throw err; }
        stree.iterateInsertionOrder(function(item, next) {
          if (streeIt === 0) {
            should.deepEqual(item, item1);
          }
          if (streeIt > 0) {
            should.deepEqual(item, item2);
          }
          streeIt++;
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(streeIt, 2);
          done();
        });
      });
    });

    it('should err when trying to copy an existing item', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      var dtree = new Tree(db, dname, { vSize: 3, log: silence });

      MergeTree._copyTo(stree, dtree, function(err) {
        should.strictEqual(err.message, 'not a valid new item');
        done();
      });
    });

    it('should copy stree to dtree starting at item1, exclusive', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      var dtree = new Tree(db, dname, { vSize: 3, log: silence });

      var streeIt = 0;
      var dtreeIt = 0;

      var opts = { first: item1.h.v, excludeFirst: true };

      MergeTree._copyTo(stree, dtree, opts, function(err) {
        if (err) { throw err; }
        stree.iterateInsertionOrder(function(item, next) {
          if (streeIt === 0) {
            should.deepEqual(item, item1);
          }
          if (streeIt > 0) {
            should.deepEqual(item, item2);
          }
          streeIt++;
          next();
        }, function(err) {
          if (err) { throw err; }
          dtree.iterateInsertionOrder(function(item, next) {
            if (dtreeIt === 0) {
              should.deepEqual(item, item1);
            }
            if (dtreeIt > 0) {
              should.deepEqual(item, item2);
            }
            dtreeIt++;
            next();
          }, function(err) {
            if (err) { throw err; }
            should.strictEqual(streeIt, 2);
            should.strictEqual(dtreeIt, 2);
            done();
          });
        });
      });
    });

    it('needs item3 and item4', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      var streeIt = 0;
      stree.write(item3);
      stree.end(item4, function(err) {
        if (err) { throw err; }
        stree.iterateInsertionOrder(function(item, next) {
          if (streeIt === 0) {
            should.deepEqual(item, item1);
          }
          if (streeIt === 1) {
            should.deepEqual(item, item2);
          }
          if (streeIt === 2) {
            should.deepEqual(item, item3);
          }
          if (streeIt > 2) {
            should.deepEqual(item, item4);
          }
          streeIt++;
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(streeIt, 4);
          done();
        });
      });
    });

    it('should err when copying non-connecting, starting at item3, exclusive', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      var dtree = new Tree(db, dname, { vSize: 3, log: silence });

      var streeIt = 0;
      var dtreeIt = 0;

      var opts = { first: item3.h.v, excludeFirst: true };

      MergeTree._copyTo(stree, dtree, opts, function(err) {
        should.strictEqual(err.message, 'not a valid new item');
        stree.iterateInsertionOrder(function(item, next) {
          streeIt++;
          next();
        }, function(err) {
          if (err) { throw err; }
          dtree.iterateInsertionOrder(function(item, next) {
            dtreeIt++;
            next();
          }, function(err) {
            if (err) { throw err; }
            should.strictEqual(streeIt, 4);
            should.strictEqual(dtreeIt, 2);
            done();
          });
        });
      });
    });

    it('should copy stree to dtree starting at item3, inclusive', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      var dtree = new Tree(db, dname, { vSize: 3, log: silence });

      var streeIt = 0;
      var dtreeIt = 0;

      var opts = { first: item3.h.v, excludeFirst: false };

      MergeTree._copyTo(stree, dtree, opts, function(err) {
        if (err) { throw err; }
        stree.iterateInsertionOrder(function(item, next) {
          if (streeIt === 0) {
            should.deepEqual(item, item1);
          }
          if (streeIt === 1) {
            should.deepEqual(item, item2);
          }
          if (streeIt === 2) {
            should.deepEqual(item, item3);
          }
          if (streeIt > 2) {
            should.deepEqual(item, item4);
          }
          streeIt++;
          next();
        }, function(err) {
          if (err) { throw err; }
          dtree.iterateInsertionOrder(function(item, next) {
            if (dtreeIt === 0) {
              should.deepEqual(item, item1);
            }
            if (dtreeIt === 1) {
              should.deepEqual(item, item2);
            }
            if (dtreeIt === 2) {
              should.deepEqual(item, item3);
            }
            if (dtreeIt > 2) {
              should.deepEqual(item, item4);
            }
            dtreeIt++;
            next();
          }, function(err) {
            if (err) { throw err; }
            should.strictEqual(streeIt, 4);
            should.strictEqual(dtreeIt, 4);
            done();
          });
        });
      });
    });
  });

  describe('_iterateMissing', function() {
    var sname = '_iterateMissing_foo';
    var dname = '_iterateMissing_bar';

    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] }, b: { some: 'body' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'] }, b: { more: 'body' } };
    var item3 = { h: { id: 'XI', v: 'Cccc', pe: sname, pa: ['Aaaa'] }, b: { more2: 'body' } };

    it('should require stree to be an object', function() {
      (function() { MergeTree._iterateMissing(); }).should.throw('stree must be an object');
    });

    it('should require dtree to be an object', function() {
      (function() { MergeTree._iterateMissing({}); }).should.throw('dtree must be an object');
    });

    it('should require iterator to be a function', function() {
      (function() { MergeTree._iterateMissing({}, {}, {}); }).should.throw('iterator must be a function');
    });

    it('should require cb to be a function', function() {
      (function() { MergeTree._iterateMissing({}, {}, function() {}); }).should.throw('cb must be a function');
    });

    it('should iterate over an empty database', function(done) {
      var stree = new Tree(db, sname, { log: silence });
      var dtree = new Tree(db, dname, { log: silence });
      var i = 0;
      MergeTree._iterateMissing(stree, dtree, function() {
        i++;
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 0);
        done();
      });
    });

    it('needs item1 in stree', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      stree.end(item1, done);
    });

    it('should iterate over item1 because it misses in dtree', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      var dtree = new Tree(db, dname, { vSize: 3, log: silence });
      var i = 0;
      MergeTree._iterateMissing(stree, dtree, function(item, next) {
        i++;
        should.deepEqual(item, item1);
        next();
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        done();
      });
    });

    it('needs item1 in dtree', function(done) {
      var dtree = new Tree(db, dname, { vSize: 3, log: silence });
      dtree.end(item1, done);
    });

    it('should not iterate over item1 because it is already in dtree', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      var dtree = new Tree(db, dname, { vSize: 3, log: silence });
      var i = 0;
      MergeTree._iterateMissing(stree, dtree, function() {
        i++;
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 0);
        done();
      });
    });

    it('needs item2 in stree', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      stree.end(item2, done);
    });

    it('should iterate over item2 in stree since item1 is already in dtree', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      var dtree = new Tree(db, dname, { vSize: 3, log: silence });

      var i = 0;
      MergeTree._iterateMissing(stree, dtree, function(item, next) {
        should.deepEqual(item, item2);
        i++;
        next();
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        done();
      });
    });

    it('needs item3 in stree', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      stree.end(item3, done);
    });

    it('should iterate over item2 and item3 in stree since item1 is already in dtree', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      var dtree = new Tree(db, dname, { vSize: 3, log: silence });

      var i = 0;
      MergeTree._iterateMissing(stree, dtree, function(item, next) {
        if (i === 0) {
          should.deepEqual(item, item2);
        }
        if (i === 1) {
          should.deepEqual(item, item3);
        }
        i++;
        next();
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 2);
        done();
      });
    });
  });

  describe('_mergeTrees', function() {
    describe('parameter checking', function() {
      it('should require stree to be an object', function() {
        (function() { MergeTree._mergeTrees(); }).should.throw('stree must be an object');
      });

      it('should require dtree to be an object', function() {
        (function() { MergeTree._mergeTrees({}); }).should.throw('dtree must be an object');
      });

      it('should require iterator to be a function', function() {
        (function() { MergeTree._mergeTrees({}, {}, {}); }).should.throw('iterator must be a function');
      });

      it('should require cb to be a function', function() {
        (function() { MergeTree._mergeTrees({}, {}, function() {}); }).should.throw('cb must be a function');
      });
    });

    describe('empty databases and root merge', function() {
      var sname = '_mergeTreesEmptyDatabases_foo';
      var dname = '_mergeTreesEmptyDatabases_bar';

      // use 24-bit version numbers (base 64)
      var sitem1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] },       b: { some: 'body' } };
      var sitem2 = { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'] }, b: { more2: 'body' } };

      var ditem1 = { h: { id: 'XI', v: 'Aaaa', pe: dname, pa: [] },       b: { some: 'body' } };
      var ditem2 = { h: { id: 'XI', v: 'Bbbb', pe: dname, pa: ['Aaaa'] }, b: { more2: 'body' } };

      it('should call back without calling the iterator when the database is empty', function(done) {
        var stree = new Tree(db, sname, { log: silence });
        var dtree = new Tree(db, dname, { log: silence });
        var i = 0;
        MergeTree._mergeTrees(stree, dtree, function() {
          i++;
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 0);
          done();
        });
      });

      it('write sitem1', function(done) {
        var stree = new Tree(db, sname, { vSize: 3, log: silence });
        stree.write(sitem1, done);
      });

      it('should iterate over sitem1 (fast-forward) because it misses in dtree', function(done) {
        var stree = new Tree(db, sname, { vSize: 3, log: silence });
        var dtree = new Tree(db, dname, { vSize: 3, log: silence });
        var i = 0;
        MergeTree._mergeTrees(stree, dtree, function(smerge, dmerge, shead, dhead, next) {
          i++;
          should.deepEqual(smerge, null);
          should.deepEqual(dmerge, null);
          should.deepEqual(shead, sitem1);
          should.deepEqual(dhead, null);
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 1);
          done();
        });
      });

      it('write ditem1', function(done) {
        var dtree = new Tree(db, dname, { vSize: 3, log: silence });
        dtree.write(ditem1, done);
      });

      it('should merge sitem1 and ditem1 since this perspective is not written to dtree', function(done) {
        // this happens when a new remote is added that is a subset of the existing items
        var stree = new Tree(db, sname, { vSize: 3, log: silence });
        var dtree = new Tree(db, dname, { vSize: 3, log: silence });
        var i = 0;
        MergeTree._mergeTrees(stree, dtree, function(smerge, dmerge, shead, dhead, next) {
          i++;
          should.deepEqual(smerge, null);
          should.deepEqual(dmerge, null);
          should.deepEqual(shead, sitem1);
          should.deepEqual(dhead, ditem1);
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 1);
          done();
        });
      });

      it('write sitem2 and ditem2', function(done) {
        var stree = new Tree(db, sname, { vSize: 3, log: silence });
        var dtree = new Tree(db, dname, { vSize: 3, log: silence });
        dtree.write(ditem2, function(err) {
        if (err) { throw err; }
          stree.write(sitem2, done);
        });
      });

      it('write sitem2 to dtree (should update last by perspective)', function(done) {
        var dtree = new Tree(db, dname, { vSize: 3, log: silence });
        dtree.write(sitem2, done);
      });

      it('should not iterate over sitem1 or sitem2 since sitem2 is written to dtree (perspective update)', function(done) {
        var stree = new Tree(db, sname, { vSize: 3, log: silence });
        var dtree = new Tree(db, dname, { vSize: 3, log: silence });
        var i = 0;
        MergeTree._mergeTrees(stree, dtree, function(smerge, dmerge, shead, dhead, next) {
          i++;
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 0);
          done();
        });
      });
    });

    describe('merge one and two heads in stree', function() {
      var sname = '_mergeTreesOneTwoHeads_foo';
      var dname = '_mergeTreesOneTwoHeads_bar';

      // use 24-bit version numbers (base 64)
      var sitem1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] },       b: { more1: 'body' } };
      var sitem2 = { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'] }, b: { more2: 'body' } };
      var sitem3 = { h: { id: 'XI', v: 'Cccc', pe: sname, pa: ['Aaaa'] }, b: { more3: 'body' } };

      var ditem1 = { h: { id: 'XI', v: 'Aaaa', pe: dname, pa: [] },       b: { more1: 'body' } };
      var ditem2 = { h: { id: 'XI', v: 'Bbbb', pe: dname, pa: ['Aaaa'] }, b: { more2: 'body' } };

      it('write sitem1, sitem2', function(done) {
        var stree = new Tree(db, sname, { vSize: 3, log: silence });
        stree.write(sitem1);
        stree.write(sitem2);
        stree.end(done);
      });

      it('write ditem1, ditem2', function(done) {
        var dtree = new Tree(db, dname, { vSize: 3, log: silence });
        dtree.write(ditem1);
        dtree.write(ditem2);
        dtree.end(done);
      });

      it('write sitem1 to dtree', function(done) {
        var dtree = new Tree(db, dname, { vSize: 3, log: silence });
        dtree.write(sitem1);
        dtree.end(done);
      });

      it('should merge sitem2 with ditem2 (fast-forward to existing items)', function(done) {
        var stree = new Tree(db, sname, { vSize: 3, log: silence });
        var dtree = new Tree(db, dname, { vSize: 3, log: silence });

        var i = 0;
        MergeTree._mergeTrees(stree, dtree, function(smerge, dmerge, shead, dhead, next) {
          i++;
          should.deepEqual(smerge, null);
          should.deepEqual(dmerge, null);
          should.deepEqual(shead, sitem2);
          should.deepEqual(dhead, ditem2);
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 1);
          done();
        });
      });

      it('write sitem3', function(done) {
        var stree = new Tree(db, sname, { vSize: 3, log: silence });
        stree.write(sitem3, done);
      });

      it('should merge sitem2 with ditem2 (ff) and sitem3 with ditem2 (merge)', function(done) {
        var stree = new Tree(db, sname, { vSize: 3, log: silence });
        var dtree = new Tree(db, dname, { vSize: 3, log: silence });

        var i = 0;
        MergeTree._mergeTrees(stree, dtree, function(smerge, dmerge, shead, dhead, next) {
          i++;
          if (i === 1) {
            should.deepEqual(smerge, null);
            should.deepEqual(dmerge, null);
            should.deepEqual(shead, sitem2);
            should.deepEqual(dhead, ditem2);
          }
          if (i === 2) {
            // should add content based version number to merges
            should.deepEqual(smerge, {
              h: { id: 'XI', v: 'CkiF', pa: ['Cccc', 'Bbbb'] },
              b: { more2: 'body', more3: 'body' }
            });
            should.deepEqual(dmerge, {
              h: { id: 'XI', v: 'CkiF', pa: ['Cccc', 'Bbbb'] },
              b: { more2: 'body', more3: 'body' }
            });
            should.deepEqual(shead, sitem3);
            should.deepEqual(dhead, ditem2);
          }
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 2);
          done();
        });
      });
    });

    describe('merge two heads one deleted in stree', function() {
      var sname = '_mergeTreesTwoHeadsOneDeleted_foo';
      var dname = '_mergeTreesTwoHeadsOneDeleted_bar';

      // use 24-bit version numbers (base 64)
      var sitem1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] },                b: { some: 'body' } };
      var sitem2 = { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], d: true }, b: { more2: 'body' } };
      var sitem3 = { h: { id: 'XI', v: 'Cccc', pe: sname, pa: ['Aaaa'] },          b: { more3: 'body' } };

      var ditem1 = { h: { id: 'XI', v: 'Aaaa', pe: dname, pa: [] },                b: { some: 'body' } };
      var ditem2 = { h: { id: 'XI', v: 'Bbbb', pe: dname, pa: ['Aaaa'], d: true }, b: { more2: 'body' } };

      it('write sitem1, sitem2', function(done) {
        var stree = new Tree(db, sname, { vSize: 3, log: silence });
        stree.write(sitem1);
        stree.write(sitem2);
        stree.end(done);
      });

      it('write ditem1', function(done) {
        var dtree = new Tree(db, dname, { vSize: 3, log: silence });
        dtree.write(ditem1);
        dtree.end(done);
      });

      it('write sitem1 to dtree', function(done) {
        var dtree = new Tree(db, dname, { vSize: 3, log: silence });
        dtree.write(sitem1);
        dtree.end(done);
      });

      it('should merge sitem2 with ditem1 (merge ff in dtree)', function(done) {
        var stree = new Tree(db, sname, { vSize: 3, log: silence });
        var dtree = new Tree(db, dname, { vSize: 3, log: silence });

        var i = 0;
        MergeTree._mergeTrees(stree, dtree, function(smerge, dmerge, shead, dhead, next) {
          i++;
          should.deepEqual(smerge, {
            h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], d: true, i: 2 },
            b: { more2: 'body' }
          });
          should.deepEqual(dmerge, {
            h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], d: true },
            b: { more2: 'body' }
          });
          should.deepEqual(shead, sitem2);
          should.deepEqual(dhead, ditem1);
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 1);
          done();
        });
      });

      it('write ditem2', function(done) {
        var dtree = new Tree(db, dname, { vSize: 3, log: silence });
        dtree.write(ditem2);
        dtree.end(done);
      });

      it('write sitem3', function(done) {
        var stree = new Tree(db, sname, { vSize: 3, log: silence });
        stree.write(sitem3);
        stree.end(done);
      });

      it('should merge sitem2 (ff) and sitem3 with ditem2 (merge, no deleted flag)', function(done) {
        var stree = new Tree(db, sname, { vSize: 3, log: silence });
        var dtree = new Tree(db, dname, { vSize: 3, log: silence });

        var i = 0;
        MergeTree._mergeTrees(stree, dtree, function(smerge, dmerge, shead, dhead, next) {
          i++;
          if (i === 1) {
            // sitem2 with ditem2
            should.deepEqual(smerge, null);
            should.deepEqual(dmerge, null);
            should.deepEqual(shead, sitem2);
            should.deepEqual(dhead, ditem2);
          }

          if (i === 2) {
            // sitem3 with ditem2
            should.deepEqual(smerge, {
              h: { id: 'XI', v: 'CkiF', pa: ['Cccc', 'Bbbb'] },
              b: { more2: 'body', more3: 'body' }
            });
            should.deepEqual(dmerge, {
              h: { id: 'XI', v: 'CkiF', pa: ['Cccc', 'Bbbb'] },
              b: { more2: 'body', more3: 'body' }
            });
            should.deepEqual(shead, sitem3);
            should.deepEqual(dhead, ditem2);
          }
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 2);
          done();
        });
      });
    });

    describe('merge with conflict', function() {
      var sname = '_mergeTreesWithConflict_foo';
      var dname = '_mergeTreesWithConflict_bar';

      // use 24-bit version numbers (base 64)
      var sitem1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] },       b: { some: 'body' } };
      var sitem2 = { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'] }, b: { more2: 'body' } };

      var ditem1 = { h: { id: 'XI', v: 'Aaaa', pe: dname, pa: [] },       b: { some: 'body' } };
      var ditem2 = { h: { id: 'XI', v: 'Cccc', pe: dname, pa: ['Aaaa'] }, b: { more2: 'other' } };

      it('write sitem1, sitem2', function(done) {
        var stree = new Tree(db, sname, { vSize: 3, log: silence });
        stree.write(sitem1);
        stree.write(sitem2);
        stree.end(done);
      });

      it('write ditem1, ditem2', function(done) {
        var dtree = new Tree(db, dname, { vSize: 3, log: silence });
        dtree.write(ditem1);
        dtree.write(ditem2);
        dtree.end(done);
      });

      it('write sitem1 to dtree', function(done) {
        var dtree = new Tree(db, dname, { vSize: 3, log: silence });
        dtree.write(sitem1);
        dtree.end(done);
      });

      it('should merge sitem2 with ditem2, conflict on "more2"', function(done) {
        var stree = new Tree(db, sname, { vSize: 3, log: silence });
        var dtree = new Tree(db, dname, { vSize: 3, log: silence });

        var i = 0;
        MergeTree._mergeTrees(stree, dtree, function(smerge, dmerge, shead, dhead, next) {
          i++;
          should.deepEqual(smerge, ['more2']);
          should.deepEqual(dmerge, null);
          should.deepEqual(shead, sitem2);
          should.deepEqual(dhead, ditem2);
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 1);
          done();
        });
      });
    });
  });

  describe('mergeWithLocal', function() {
    var sname = 'mergeWithLocal_foo';

    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] }, b: { some: 'body' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'] }, b: { more: 'body' } };
    var item3 = { h: { id: 'XI', v: 'Cccc', pe: sname, pa: ['Aaaa'] }, b: { more2: 'body' } };

    it('should require stree to be an object', function() {
      var mt = new MergeTree(db);
      (function() { mt.mergeWithLocal(); }).should.throw('stree must be an object');
    });

    it('should require cb to be a function', function() {
      var mt = new MergeTree(db);
      (function() { mt.mergeWithLocal({}); }).should.throw('cb must be a function');
    });

    it('should merge an empty database', function(done) {
      var mt = new MergeTree(db, { log: silence });
      var stree = new Tree(db, sname, { log: silence });
      var i = 0;
      mt.mergeWithLocal(stree, function(err) {
        if (err) { throw err; }
        stree.iterateInsertionOrder(function(item, next) {
          i++;
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 0);
          done();
        });
      });
    });

    it('item1 in stree', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      stree.write(item1, done);
    });

    it('should merge one item with the local database, by putting it in stage', function(done) {
      var mergeHandler = function(newHead, prevHead, next) {
        should.deepEqual(newHead, item1);
        should.deepEqual(prevHead, null);
        next();
      };
      var opts = {
        vSize: 3,
        log: silence,
        mergeHandler: mergeHandler
      };
      var mt = new MergeTree(db, opts);
      var stree = new Tree(db, sname, opts);
      var stage = mt._stage;
      var i = 0;
      mt.mergeWithLocal(stree, function(err) {
        if (err) { throw err; }
        // inspect staging
        stage.iterateInsertionOrder(function(item, next) {
          i++;
          should.deepEqual(item, item1);
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 1);
          done();
        });
      });
    });

    it('item2 in stree', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      stree.write(item2, done);
    });

    it('should merge ff with previous head in stage', function(done) {
      var mergeHandler = function(newHead, prevHead, next) {
        should.deepEqual(newHead, item2);
        should.deepEqual(prevHead, item1);
        next();
      };
      var opts = {
        vSize: 3,
        log: silence,
        mergeHandler: mergeHandler
      };
      var mt = new MergeTree(db, opts);
      var stree = new Tree(db, sname, opts);
      var stage = mt._stage;
      var i = 0;
      mt.mergeWithLocal(stree, function(err) {
        if (err) { throw err; }
        // inspect staging
        stage.iterateInsertionOrder(function(item, next) {
          i++;
          if (i === 1) {
            should.deepEqual(item, item1);
          }
          if (i === 2) {
            should.deepEqual(item, item2);
          }
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 2);
          done();
        });
      });
    });

    it('item3 in stree', function(done) {
      var tree = new Tree(db, sname, { vSize: 3, log: silence });
      tree.write(item3, done);
    });

    it('should merge with previous head in stage (merge ff (again) + merge = 3 new items)', function(done) {
      var j = 0;
      var mergeHandler = function(newHead, prevHead, next) {
        j++;
        // the previously created merge in stage is created again
        if (j === 1) {
          should.deepEqual(newHead, item2);
          should.deepEqual(prevHead, item2);
        }
        if (j > 1) {
          should.deepEqual(newHead, {
            h: { id: 'XI', v: 'C8I/', pe: sname, pa: ['Cccc', 'Bbbb'], i: 4 },
            b: { more: 'body', more2: 'body' }
          });
          should.deepEqual(prevHead, item2);
        }
        next();
      };
      var mt = new MergeTree(db, {
        vSize: 3,
        log: silence,
        mergeHandler: mergeHandler
      });
      var stree = new Tree(db, sname, { vSize: 3, log: silence});
      var stage = mt._stage;
      mt.mergeWithLocal(stree, function(err) {
        if (err) { throw err; }
        // inspect staging
        var i = 0;
        stage.iterateInsertionOrder(function(item, next) {
          i++;
          if (i === 1) {
            should.deepEqual(item, item1);
          }
          if (i === 2) {
            should.deepEqual(item, item2);
          }
          if (i === 3) {
            should.deepEqual(item, item3);
          }
          if (i > 3) {
            should.deepEqual(item, {
              h: { id: 'XI', v: 'C8I/', pe: sname, pa: ['Cccc', 'Bbbb'], i: 4 },
              b: { more: 'body', more2: 'body' }
            });
          }
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 4);
          done();
        });
      });
    });
  });

  describe('createRemoteWriteStream', function() {
    var pe = 'other';

    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', pe: pe, v: 'Aaaa', pa: [] }, b: { some: 'body' } };
    var item2 = { h: { id: 'XI', pe: pe, v: 'Bbbb', pa: ['Aaaa'] }, b: { more: 'body' } };
    var item3 = { h: { id: 'XI', pe: pe, v: 'Cccc', pa: ['Aaaa'] }, b: { more2: 'body' } };
    var item4 = { h: { id: 'XI', pe: pe, v: 'Dddd', pa: ['Cccc'] }, b: { more3: 'b' } };

    it('should require item.h.pe to be a string', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      t.createRemoteWriteStream().write({}, function(err) {
        should.strictEqual(err.message, 'item.h.pe must be a string');
        done();
      });
    });

    it('should require item.h.pe to be a configured perspective', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      t.createRemoteWriteStream().write({ h: { pe: pe } }, function(err) {
        should.strictEqual(err.message, 'perspective not found');
        done();
      });
    });

    it('should require item.h.pe to differ from local name', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      t.createRemoteWriteStream().write({ h: { pe: '_local' } }, function(err) {
        should.strictEqual(err.message, 'perspective should differ from local name');
        done();
      });
    });

    it('should not accept non-connecting items (item2 in empty database)', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence, perspectives: [pe] });
      t.createRemoteWriteStream().write(item2, function(err) {
        should.strictEqual(err.message, 'not a valid new item');
        done();
      });
    });

    it('should accept new root (item1)', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence, perspectives: [pe] });
      t.createRemoteWriteStream().write(item1, function(err) {
        if (err) { throw err; }
        done();
      });
    });

    it('should accept item2, item3 and item4', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence, perspectives: [pe] });
      var s = t.createRemoteWriteStream();
      s.on('error', function(err) { throw err; });
      s.write(item2);
      s.write(item3);
      s.write(item4);
      s.end(done);
    });
  });

  describe('createLocalWriteStream', function() {
    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa' }, b: { some: 'body' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb' }, b: { more: 'body' } };
    var item3 = { h: { id: 'XI', v: 'Cccc' }, b: { more2: 'body' } };
    var item4 = { h: { id: 'XI', v: 'Dddd', pa: ['Cccc'] }, b: { more3: 'b' } };

    it('should require item to be an object', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      t.createLocalWriteStream().write([], function(err) {
        should.strictEqual(err.message, 'item must be an object');
        done();
      });
    });

    it('should require item.h to be an object', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      t.createLocalWriteStream().write({}, function(err) {
        should.strictEqual(err.message, 'item.h must be an object');
        done();
      });
    });

    it('should require item.h.id to be defined', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      t.createLocalWriteStream().write({ h: { id: null } }, function(err) {
        should.strictEqual(err.message, 'item.h.id must be defined');
        done();
      });
    });

    it('should require item.h.pe to be a string', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      t.createLocalWriteStream().write({ h: { id: 'some' } }, function(err) {
        should.strictEqual(err.message, 'item.h.pe must be a string');
        done();
      });
    });

    it('should require item.h.pe to be a configured perspective', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      t.createLocalWriteStream().write({ h: { id: 'some', pe: 'some' } }, function(err) {
        should.strictEqual(err.message, 'perspective not found');
        done();
      });
    });

    it('should not accept non-connecting items', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      t.createLocalWriteStream().write(item2, function(err) {
        should.strictEqual(err.message, 'item is not a new leaf');
        done();
      });
    });

    it('should accept a new root and a new leaf in an empty database and increment i twice', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      var i = 0;
      t.on('data', function(obj) {
        i++;
        should.strictEqual(obj._h.i, i);
      });
      t.on('finish', function() {
        should.strictEqual(i, 2);
        done();
      });
      t.createLocalWriteStream().write(item1);
      t.createLocalWriteStream().write(item2);
      t.end();
    });

    it('inspect keys: should have created two dskeys with incremented i values', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getDsKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = MergeTree.parseKey(obj.key, 'utf8');
        var val = obj.value;

        if (i === 1) {
          should.strictEqual(key.type, 0x01);
          should.strictEqual(key.id, 'XI');
          should.strictEqual(key.i, 1);
          should.deepEqual(val, { _h: { id: 'XI', v: 'Aaaa', pa: [], i: 1 }, _b: { some: 'body' } });
        }
        if (i === 2) {
          should.strictEqual(key.type, 0x01);
          should.strictEqual(key.id, 'XI');
          should.strictEqual(key.i, 2);
          should.deepEqual(val, { _h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], i: 2 }, _b: { more: 'body' } });
        }
      });

      s.on('end', function() {
        should.strictEqual(i, 2);
        done();
      });
    });

    it('inspect keys: should have one head key with the latest version and corresponding head value', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getHeadKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = MergeTree.parseKey(obj.key, 'utf8', 'base64');
        var val = MergeTree.parseHeadVal(obj.value);

        should.strictEqual(key.type, 0x03);
        should.strictEqual(key.id, 'XI');
        should.strictEqual(key.v, 'Bbbb');

        should.strictEqual(val.i, 2);
      });

      s.on('end', function() {
        should.strictEqual(i, 1);
        done();
      });
    });

    it('inspect keys: should have two ikeys with the corresponding headkey as value', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getIKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = MergeTree.parseKey(obj.key, 'utf8');
        var val = MergeTree.parseKey(obj.value, 'utf8', 'base64');

        if (i === 1) {
          should.strictEqual(key.type, 0x02);
          should.strictEqual(key.i, 1);

          should.strictEqual(val.type, 0x03);
          should.strictEqual(val.id, 'XI');
          should.strictEqual(val.v, 'Aaaa');
        }
        if (i === 2) {
          should.strictEqual(key.type, 0x02);
          should.strictEqual(key.i, 2);

          should.strictEqual(val.type, 0x03);
          should.strictEqual(val.id, 'XI');
          should.strictEqual(val.v, 'Bbbb');
        }
      });

      s.on('end', function() {
        should.strictEqual(i, 2);
        done();
      });
    });

    it('inspect keys: should have two vkeys with the corresponding dskey as value', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getVKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = MergeTree.parseKey(obj.key, 'utf8', 'base64');
        var val = MergeTree.parseKey(obj.value, 'utf8');

        if (i === 1) {
          should.strictEqual(key.type, 0x04);
          should.strictEqual(key.v, 'Aaaa');

          should.strictEqual(val.type, 0x01);
          should.strictEqual(val.id, 'XI');
          should.strictEqual(val.i, 1);
        }
        if (i === 2) {
          should.strictEqual(key.type, 0x04);
          should.strictEqual(key.v, 'Bbbb');

          should.strictEqual(val.type, 0x01);
          should.strictEqual(val.id, 'XI');
          should.strictEqual(val.i, 2);
        }
      });

      s.on('end', function() {
        should.strictEqual(i, 2);
        done();
      });
    });

    it('should not accept an existing root', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      t.on('error', function(err) {
        should.strictEqual(err.message, 'item is not a new leaf');
        done();
      });
      t.end(item1);
      // expect that streams do not emit a finish after an error occurred
      t.on('finish', done);
      // nor should a data event happen
      t.on('data', done);
    });

    it('should not accept an existing non-root', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      t.on('error', function(err) {
        should.strictEqual(err.message, 'item is not a new leaf');
        done();
      });
      t.end(item2);
      // expect that streams do not emit a finish after an error occurred
      t.on('finish', done);
      // nor should a data event happen
      t.on('data', done);
    });

    it('should accept new item (fork)', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      t.on('data', function(obj) {
        should.strictEqual(obj._h.i, 3);
        done();
      });
      t.createLocalWriteStream().write(item3);
    });

    it('should accept new item (fast-forward)', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      t.on('data', function(obj) {
        should.strictEqual(obj._h.i, 4);
        done();
      });
      t.createLocalWriteStream().write(item4);
    });

    it('inspect keys: should have created four dskeys with incremented i values', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getDsKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = MergeTree.parseKey(obj.key, 'utf8');
        var val = obj.value;

        if (i === 1) {
          should.strictEqual(key.type, 0x01);
          should.strictEqual(key.id, 'XI');
          should.strictEqual(key.i, 1);
          should.deepEqual(val, { _h: { id: 'XI', v: 'Aaaa', pa: [], i: 1 }, _b: { some: 'body' } });
        }
        if (i === 2) {
          should.strictEqual(key.type, 0x01);
          should.strictEqual(key.id, 'XI');
          should.strictEqual(key.i, 2);
          should.deepEqual(val, { _h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], i: 2 }, _b: { more: 'body' } });
        }
        if (i === 3) {
          should.strictEqual(key.type, 0x01);
          should.strictEqual(key.id, 'XI');
          should.strictEqual(key.i, 3);
          should.deepEqual(val, { _h: { id: 'XI', v: 'Cccc', pa: ['Aaaa'], i: 3 }, _b: { more2: 'body' } });
        }
        if (i === 4) {
          should.strictEqual(key.type, 0x01);
          should.strictEqual(key.id, 'XI');
          should.strictEqual(key.i, 4);
          should.deepEqual(val, { _h: { id: 'XI', v: 'Dddd', pa: ['Cccc'], i: 4 }, _b: { more3: 'b' } });
        }
      });

      s.on('end', function() {
        should.strictEqual(i, 4);
        done();
      });
    });

    it('inspect keys: should have two headkey with the latest versions and corresponding value', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getHeadKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = MergeTree.parseKey(obj.key, 'utf8', 'base64');
        var val = MergeTree.parseHeadVal(obj.value, 'utf8');

        if (i === 1) {
          should.strictEqual(key.type, 0x03);
          should.strictEqual(key.id, 'XI');
          should.strictEqual(key.v, 'Bbbb');

          should.strictEqual(val.i, 2);
        }

        if (i === 2) {
          should.strictEqual(key.type, 0x03);
          should.strictEqual(key.id, 'XI');
          should.strictEqual(key.v, 'Dddd');

          should.strictEqual(val.i, 4);
        }
      });

      s.on('end', function() {
        should.strictEqual(i, 2);
        done();
      });
    });

    it('inspect keys: should have two headKeys with the latest versions and corresponding value', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getHeadKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = MergeTree.parseKey(obj.key, 'utf8', 'base64');
        var val = MergeTree.parseHeadVal(obj.value, 'utf8');

        if (i === 1) {
          should.strictEqual(key.type, 0x03);
          should.strictEqual(key.id, 'XI');
          should.strictEqual(key.v, 'Bbbb');

          should.strictEqual(val.i, 2);
        }

        if (i === 2) {
          should.strictEqual(key.type, 0x03);
          should.strictEqual(key.id, 'XI');
          should.strictEqual(key.v, 'Dddd');

          should.strictEqual(val.i, 4);
        }
      });
      s.on('close', function() {
        should.strictEqual(i, 2);
        done();
      });
    });

    it('inspect keys: should have four ikeys with the corresponding headkey as value', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getIKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = MergeTree.parseKey(obj.key, 'utf8');
        var val = MergeTree.parseKey(obj.value, 'utf8', 'base64');

        if (i === 1) {
          should.strictEqual(key.type, 0x02);
          should.strictEqual(key.i, 1);

          should.strictEqual(val.type, 0x03);
          should.strictEqual(val.id, 'XI');
          should.strictEqual(val.v, 'Aaaa');
        }
        if (i === 2) {
          should.strictEqual(key.type, 0x02);
          should.strictEqual(key.i, 2);

          should.strictEqual(val.type, 0x03);
          should.strictEqual(val.id, 'XI');
          should.strictEqual(val.v, 'Bbbb');
        }
        if (i === 3) {
          should.strictEqual(key.type, 0x02);
          should.strictEqual(key.i, 3);

          should.strictEqual(val.type, 0x03);
          should.strictEqual(val.id, 'XI');
          should.strictEqual(val.v, 'Cccc');
        }
        if (i === 4) {
          should.strictEqual(key.type, 0x02);
          should.strictEqual(key.i, 4);

          should.strictEqual(val.type, 0x03);
          should.strictEqual(val.id, 'XI');
          should.strictEqual(val.v, 'Dddd');
        }
      });

      s.on('end', function() {
        should.strictEqual(i, 4);
        done();
      });
    });

    it('inspect keys: should have four vkeys with the corresponding dskey as value', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getVKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = MergeTree.parseKey(obj.key, 'utf8', 'base64');
        var val = MergeTree.parseKey(obj.value, 'utf8');

        if (i === 1) {
          should.strictEqual(key.type, 0x04);
          should.strictEqual(key.v, 'Aaaa');

          should.strictEqual(val.type, 0x01);
          should.strictEqual(val.id, 'XI');
          should.strictEqual(val.i, 1);
        }
        if (i === 2) {
          should.strictEqual(key.type, 0x04);
          should.strictEqual(key.v, 'Bbbb');

          should.strictEqual(val.type, 0x01);
          should.strictEqual(val.id, 'XI');
          should.strictEqual(val.i, 2);
        }
        if (i === 3) {
          should.strictEqual(key.type, 0x04);
          should.strictEqual(key.v, 'Cccc');

          should.strictEqual(val.type, 0x01);
          should.strictEqual(val.id, 'XI');
          should.strictEqual(val.i, 3);
        }
        if (i === 4) {
          should.strictEqual(key.type, 0x04);
          should.strictEqual(key.v, 'Dddd');

          should.strictEqual(val.type, 0x01);
          should.strictEqual(val.id, 'XI');
          should.strictEqual(val.i, 4);
        }
      });

      s.on('end', function() {
        should.strictEqual(i, 4);
        done();
      });
    });
  });
});
