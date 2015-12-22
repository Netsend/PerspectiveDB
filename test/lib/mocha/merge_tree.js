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

  describe('lastByPerspective', function() {
    var name = 'lastByPerspective';
    var stageName = '_stage_lastByPerspective';
    var ldb;
    var ldbPath = tmpdir() + '/test_merge_tree_lastByPerspective';

    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa', pe: name, pa: [] }, b: { some: 'body' } };

    before('should open a new db for lastByPerspective tests only', function(done) {
      // ensure a db at start
      rimraf(ldbPath, function(err) {
        if (err) { throw err; }
        level(ldbPath, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, adb) {
          if (err) { throw err; }
          ldb = adb;
          done();
        });
      });
    });

    after('should destroy this db', function(done) {
      rimraf(ldbPath, done);
    });

    it('should require pe to be a buffer or a string', function() {
      var opts = { stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(ldb, opts);
      (function() { mt.lastByPerspective(); }).should.throw('pe must be a buffer or a string');
    });

    it('should require cb to be a function', function() {
      var opts = { stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(ldb, opts);
      (function() { mt.lastByPerspective(''); }).should.throw('cb must be a function');
    });

    it('should return with non-existing database', function(done) {
      var opts = { stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(ldb, opts);
      mt.lastByPerspective('some', function(err, v) {
        if (err) { throw err; }
        should.strictEqual(v, null);
        done();
      });
    });

    it('save item1 from pe "lastByPerspective"', function(done) {
      var opts = { stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(ldb, opts);
      mt._local.write(item1, done);
    });

    it('should return with last saved item as a buffer by default', function(done) {
      var opts = { stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(ldb, opts);
      mt.lastByPerspective(name, 'base64', function(err, v) {
        if (err) { throw err; }
        should.strictEqual(v.toString('base64'), 'Aaaa');
        done();
      });
    });

    it('should return with last saved item, decoded in base64', function(done) {
      var opts = { stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(ldb, opts);
      mt.lastByPerspective(name, 'base64', function(err, v) {
        if (err) { throw err; }
        should.strictEqual(v, 'Aaaa');
        done();
      });
    });
  });

  describe('mergeWithLocal', function() {
    var sname = 'mergeWithLocal_foo';
    var stageName = '_stage_mergeWithLocal';

    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] }, b: { some: 'body' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'] }, b: { more: 'body' } };
    var item3 = { h: { id: 'XI', v: 'Cccc', pe: sname, pa: ['Aaaa'] }, b: { more2: 'body' } };

    it('should require stree to be an object', function() {
      var opts = { stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);
      (function() { mt.mergeWithLocal(); }).should.throw('stree must be an object');
    });

    it('should require cb to be a function', function() {
      var opts = { stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);
      (function() { mt.mergeWithLocal({}); }).should.throw('cb must be a function');
    });

    it('should merge an empty database', function(done) {
      var opts = { stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);
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
        stage: stageName,
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
        stage: stageName,
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

    it('should merge with previous head in stage (merge ff (like previous test) + merge = 3 new items)', function(done) {
      var j = 0;
      var mergeHandler = function(newHead, prevHead, next) {
        j++;
        // the previously created merge in stage is created again
        if (j === 1) {
          should.deepEqual(newHead, {
            h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], i: 3 },
            b: { more: 'body' }
          });
          should.deepEqual(prevHead, {
            h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], i: 2 },
            b: { more: 'body' }
          });
        }
        if (j > 1) {
          should.deepEqual(newHead, {
            h: { id: 'XI', v: 'HFFj', pa: ['Cccc', 'Bbbb'], i: 5 },
            b: { more: 'body', more2: 'body' }
          });
          should.deepEqual(prevHead, {
            h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], i: 3 },
            b: { more: 'body' }
          });
        }
        next();
      };
      var mt = new MergeTree(db, {
        stage: stageName,
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
            should.deepEqual(item, {
              h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], i: 3 },
              b: { more: 'body' }
            });
          }
          if (i === 4) {
            should.deepEqual(item, {
              h: { id: 'XI', v: 'Cccc', pe: sname, pa: ['Aaaa'], i: 4 },
              b: { more2: 'body' }
            });
          }
          if (i > 4) {
            should.deepEqual(item, {
              h: { id: 'XI', v: 'HFFj', pa: ['Cccc', 'Bbbb'], i: 5 },
              b: { more: 'body', more2: 'body' }
            });
          }
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 5);
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

    it('should require remote to be a string', function() {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      (function() { t.createRemoteWriteStream(); }).should.throw('remote must be a string');
    });

    it('should require remote to be a configured perspective', function() {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      (function() { t.createRemoteWriteStream('foo'); }).should.throw('perspective not found');
    });

    it('should require remote to differ from local name', function() {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      (function() { t.createRemoteWriteStream('_local'); }).should.throw('perspective should differ from local name');
    });

    it('should not accept non-connecting items (item2 in empty database)', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence, perspectives: [pe] });
      t.createRemoteWriteStream(pe).write(item2, function(err) {
        should.strictEqual(err.message, 'not a valid new item');
        done();
      });
    });

    it('should accept new root (item1)', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence, perspectives: [pe] });
      t.createRemoteWriteStream(pe).write(item1, function(err) {
        if (err) { throw err; }
        done();
      });
    });

    it('should accept item2 and item3', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence, perspectives: [pe] });
      var s = t.createRemoteWriteStream(pe);
      s.on('error', function(err) { throw err; });
      s.write(item2);
      s.write(item3);
      s.end(done);
    });

    it('should run hook on item4', function(done) {
      var mt = new MergeTree(db, { vSize: 3, log: silence, perspectives: [pe] });
      var opts = {};
      opts.hooks = [
        function(db, item, o, cb) {
          item.b.seen = true;
          cb(null, item);
        }
      ];
      var s = mt.createRemoteWriteStream(pe, opts);
      s.on('error', function(err) { throw err; });
      s.write(item4);
      s.end(function() {
        var item4Found;
        var rs = mt._pe[pe].createReadStream();
        rs.on('data', function(item) {
          if (item.h.v === item4.h.v) {
            should.deepEqual({
              h: { id: 'XI', v: 'Dddd', pa: ['Cccc'], pe: pe, i: 4 },
              b: { more3: 'b', seen: true }
            }, item);
            item4Found = true;
          }
        });
        rs.on('end', function() {
          should.ok(item4Found);
          done();
        });
      });
    });
  });

  describe('createLocalWriteStream', function() {
    var stageName = '_stage_createLocalWriteStream';

    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa' }, b: { some: 'body' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb' }, b: { more: 'body' } };
    var item3 = { h: { id: 'XI', v: 'Cccc' }, b: { more2: 'body' } };
    var item4 = { h: { id: 'XI', v: 'Dddd' }, b: { more3: 'b' } };

    it('should require item to be an object', function(done) {
      var mt = new MergeTree(db, { stage: stageName, vSize: 3, log: silence });
      mt.createLocalWriteStream().write(1, function(err) {
        should.strictEqual(err.message, 'item must be an object');
        done();
      });
    });

    it('should require item.h to be an object', function(done) {
      var mt = new MergeTree(db, { stage: stageName, vSize: 3, log: silence });
      mt.createLocalWriteStream().write({}, function(err) {
        should.strictEqual(err.message, 'item.h must be an object');
        done();
      });
    });

    it('should require item.h.id to be a buffer, a string or implement "toString"', function(done) {
      var mt = new MergeTree(db, { stage: stageName, vSize: 3, log: silence });
      mt.createLocalWriteStream().write({ h: { id: null } }, function(err) {
        should.strictEqual(err.message, 'item.h.id must be a buffer, a string or implement "toString"');
        done();
      });
    });

    it('should require items to not have h.pa defined', function(done) {
      var mt = new MergeTree(db, { stage: stageName, vSize: 3, log: silence });
      var lws = mt.createLocalWriteStream();
      lws.write({ h: { id: 'XI', pa: ['Aaaa'] } }, function(err) {
        should.strictEqual(err.message, 'did not expect local item to have a parent defined');
        done();
      });
    });

    it('should accept a new item, create parents and i', function(done) {
      var mt = new MergeTree(db, { stage: stageName, vSize: 3, log: silence });
      var lws = mt.createLocalWriteStream();
      lws.write(item1);

      var i = 0;
      lws.end(function() {
        mt._local.iterateInsertionOrder(function(item, next) {
          i++;
          should.deepEqual(item, {
            h: { id: 'XI', v: 'Aaaa', pa: [], i: 1 },
            b: { some: 'body' }
          });
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 1);
          done();
        });
      });
    });

    it('should accept a new item and point parents to the previously created head', function(done) {
      var mt = new MergeTree(db, { stage: stageName, vSize: 3, log: silence });
      var lws = mt.createLocalWriteStream();
      lws.write(item2);

      var i = 0;
      lws.end(function() {
        mt._local.iterateInsertionOrder(function(item, next) {
          i++;
          if (i > 1) {
            should.deepEqual(item, {
              h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], i: 2 },
              b: { more: 'body' }
            });
          }
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 2);
          done();
        });
      });
    });

    it('should not accept an existing version', function(done) {
      var mt = new MergeTree(db, { stage: stageName, vSize: 3, log: silence });
      var lws = mt.createLocalWriteStream();
      lws.write(item1, function(err) {
        should.strictEqual(err.message, 'not a valid new item');
        done();
      });
    });

    it('should accept item3 (fast-forward)', function(done) {
      var mt = new MergeTree(db, { stage: stageName, vSize: 3, log: silence });
      var lws = mt.createLocalWriteStream();
      lws.write(item3);
      lws.end(function() {
        var i = 0;
        mt._local.iterateInsertionOrder(function(item, next) {
          i++;
          if (i === 1) {
            should.deepEqual(item, {
              h: { id: 'XI', v: 'Aaaa', pa: [], i: 1 },
              b: { some: 'body' }
            });
          }
          if (i === 2) {
            should.deepEqual(item, {
              h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], i: 2 },
              b: { more: 'body' }
            });
          }
          if (i > 2) {
            should.deepEqual(item, {
              h: { id: 'XI', v: 'Cccc', pa: ['Bbbb'], i: 3 },
              b: { more2: 'body' }
            });
          }
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 3);
          done();
        });
      });
    });

    it('preload item4 in stage with parent set to of Aaaa (fork)', function(done) {
      var mt = new MergeTree(db, { stage: stageName, vSize: 3, log: silence });
      mt._stage.write({
        h: { id: 'XI', v: 'Dddd', pa: ['Aaaa'] },
        b: { more3: 'b' }
      }, done);
    });

    it('write item4 (fork, use item from stage)', function(done) {
      var mt = new MergeTree(db, { stage: stageName, vSize: 3, log: silence });
      var lws = mt.createLocalWriteStream();
      lws.write(item4);
      lws.end(function() {
        var i = 0;
        mt._local.iterateInsertionOrder(function(item, next) {
          i++;
          if (i === 1) {
            should.deepEqual(item, {
              h: { id: 'XI', v: 'Aaaa', pa: [], i: 1 },
              b: { some: 'body' }
            });
          }
          if (i === 2) {
            should.deepEqual(item, {
              h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], i: 2 },
              b: { more: 'body' }
            });
          }
          if (i === 3) {
            should.deepEqual(item, {
              h: { id: 'XI', v: 'Cccc', pa: ['Bbbb'], i: 3 },
              b: { more2: 'body' }
            });
          }
          if (i > 3) {
            should.deepEqual(item, {
              h: { id: 'XI', v: 'Dddd', pa: ['Aaaa'], i: 4 },
              b: { more3: 'b' }
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

    it('stage should be empty, since item4 is copied to local', function(done) {
      var mt = new MergeTree(db, { stage: stageName, vSize: 3, log: silence });
      var i = 0;
      mt._stage.iterateInsertionOrder(function(item, next) {
        i++;
        next();
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 0);
        done();
      });
    });
  });

  describe('scheduleMerge', function() {
    var stageName = '_stage_scheduleMerge';
    var pe = 'some';
    var ldb;
    var ldbPath = tmpdir() + '/test_merge_tree_scheduleMerge';

    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa', pe: pe, pa: [] }, b: { some: 'body' } };

    before('should open a new db for scheduleMerge tests only', function(done) {
      // ensure a db at start
      rimraf(ldbPath, function(err) {
        if (err) { throw err; }
        level(ldbPath, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, adb) {
          if (err) { throw err; }
          ldb = adb;
          done();
        });
      });
    });

    after('should destroy this db', function(done) {
      rimraf(ldbPath, done);
    });

    it('should require timeout to be a number', function() {
      var opts = { stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(ldb, opts);
      (function() { mt.scheduleMerge(); }).should.throw('timeout must be a number');
    });

    it('should return right away if no dbs are updated', function(done) {
      var opts = { stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(ldb, opts);
      mt.scheduleMerge(0, done);
    });

    it('should set updated flag if written to remote stream', function(done) {
      var opts = { stage: stageName, vSize: 3, log: silence, perspectives: [pe] };
      var mt = new MergeTree(ldb, opts);
      should.equal(mt._updatedPerspectives[pe], undefined);
      mt.createRemoteWriteStream(pe).write(item1, function(err) {
        if (err) { throw err; }
        should.equal(mt._updatedPerspectives[pe], true);
        done();
      });
    });

    it('should call the merge handler and write to the local database', function(done) {
      var opts = {
        stage: stageName,
        vSize: 3,
        log: silence,
        perspectives: [pe]
      };
      var mt = new MergeTree(ldb, opts);
      // make sure the local db is empty before testing
      var rs = mt.createReadStream();
      rs.on('data', function() {
        throw new Error('db not empty');
      });
      rs.on('end', function() {
        // signal update and use previously written item
        mt._updatedPerspectives[pe] = true;
        mt.scheduleMerge(0, function(err) {
          if (err) { throw err; }
          mt.createReadStream().on('data', function(item) {
            should.deepEqual({
              h: { id: 'XI', v: 'Aaaa', pa: [] },
              b: { some: 'body' }
            }, item);
            done();
          });
        });
      });
    });
  });

  describe('close', function() {
    it('should close and callback', function(done) {
      var mt = new MergeTree(db, { vSize: 3, log: silence });
      mt.close(done);
    });
  });

  describe('_versionContent', function() {
    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa', pa: [] }, b: { some: 'body' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'] }, b: { more: 'body' } };

    it('should require item to be an object', function() {
      (function() { MergeTree._versionContent(); }).should.throw('item must be an object');
    });

    it('should require vSize to be >0 bytes', function() {
      (function() { MergeTree._versionContent(item1, 0); }).should.throw('version too small');
    });

    it('should try to read first parent if vSize is omitted', function() {
      (function() { MergeTree._versionContent(item1); }).should.throw('cannot determine vSize');
    });

    it('should version item1', function() {
      var v = MergeTree._versionContent(item1, 3);
      should.deepEqual(v, 'lYSZ');
    });

    it('should version item2 with vSize provided', function() {
      var v = MergeTree._versionContent(item2, 3);
      should.deepEqual(v, 'nujg');
    });

    it('should version item2 and determine vSize automatically', function() {
      var v = MergeTree._versionContent(item2);
      should.deepEqual(v, 'nujg');
    });
  });
});
