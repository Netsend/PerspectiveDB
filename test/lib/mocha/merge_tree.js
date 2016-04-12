/**
 * Copyright 2015, 2016 Netsend.
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

var tmpdir = require('os').tmpdir;

var should = require('should');
var rimraf = require('rimraf');
var level = require('level-packager')(require('leveldown'));

var MergeTree = require('../../../lib/merge_tree');
var Tree = require('../../../lib/tree');
var logger = require('../../../lib/logger');

var db, cons, silence;
var dbPath = tmpdir() + '/test_merge_tree';

// open database
before(function(done) {
  logger({ console: true, mask: logger.DEBUG }, function(err, l) {
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
    it('should require db to be an object', function() {
      (function () { new MergeTree(); }).should.throwError('db must be an object');
    });

    it('should require opts to be an object', function() {
      (function() { new MergeTree(db, []); }).should.throw('opts must be an object');
    });

    it('should require opts.vSize to be a number', function() {
      (function() { new MergeTree(db, { vSize: {} }); }).should.throw('opts.vSize must be a number');
    });

    it('should require opts.iSize to be a number', function() {
      (function() { new MergeTree(db, { iSize: {} }); }).should.throw('opts.iSize must be a number');
    });

    it('should require opts.vSize to be between 0 and 6', function() {
      (function() { new MergeTree(db, { vSize: 7 }); }).should.throw('opts.vSize must be between 0 and 6');
    });

    it('should require opts.iSize to be between 0 and 6', function() {
      (function() { new MergeTree(db, { iSize: 7 }); }).should.throw('opts.iSize must be between 0 and 6');
    });

    it('should construct', function() {
      (function() { new MergeTree(db); }).should.not.throwError();
    });

    it('should set updated flag for remotes on init', function() {
      var pe = 'foo';
      var opts = { log: silence, perspectives: [pe] };
      var mt = new MergeTree(db, opts);
      should.equal(mt._updatedPerspectives[pe], true);
    });
  });

  describe('_ensureConflictHandler', function() {
    it('should set default conflict handler on construction', function() {
      var opts = { vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);

      should.strictEqual(typeof mt._conflictHandler, 'function');
    });

    it('should set given conflict handler', function() {
      var opts = { vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);

      var mh = function() {};

      should.strictEqual(typeof mt._conflictHandler, 'function');
      should.strictEqual(mt._conflictHandler === mh, false);
      mt._ensureConflictHandler(mh);
      should.strictEqual(mt._conflictHandler === mh, true);
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

  describe('_copyMissingToStage', function() {
    var sname = '_copyMissingToStage_foo';
    var dname = '_copyMissingToStage_bar';
    var stageName = '_stage_copyMissingToStage';

    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa', pa: [] }, b: { some: 'body' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'] }, b: { more: 'body' } };
    var item3 = { h: { id: 'XI', v: 'Cccc', pa: ['Aaaa'] }, b: { more2: 'body' } };
    var item4 = { h: { id: 'XI', v: 'Dddd', pa: ['Cccc'] }, b: { more3: 'body' } };

    it('should require stree to be an object', function() {
      var opts = { stage: stageName, perspectives: [ sname, dname ], vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);
      (function() { mt._copyMissingToStage(); }).should.throw('stree must be an object');
    });

    it('should require dtree to be an object', function() {
      var opts = { stage: stageName, perspectives: [ sname, dname ], vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);
      (function() { mt._copyMissingToStage({}); }).should.throw('dtree must be an object');
    });

    it('should require cb to be a function', function() {
      var opts = { stage: stageName, perspectives: [ sname, dname ], vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);
      (function() { mt._copyMissingToStage({}, {}); }).should.throw('cb must be a function');
    });

    it('should iterate over an empty database', function(done) {
      var opts = { stage: stageName, perspectives: [ sname, dname ], vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      var dtree = new Tree(db, sname, { vSize: 3, log: silence });
      mt._copyMissingToStage(stree, dtree, function(err) {
        if (err) { throw err; }
        // inspect stage
        mt.stats(function(err, stats) {
          if (err) { throw err; }
          should.deepEqual(stats, {
            local: { heads: { count: 0, conflict: 0, deleted: 0 } },
            stage: { heads: { count: 0, conflict: 0, deleted: 0 } },
            '_copyMissingToStage_bar': { heads: { count: 0, conflict: 0, deleted: 0 } },
            '_copyMissingToStage_foo': { heads: { count: 0, conflict: 0, deleted: 0 } }
          });
          done();
        });
      });
    });

    it('needs item1 in stree', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      stree.end(item1, done);
    });

    it('should copy item1 to stage', function(done) {
      var opts = { stage: stageName, perspectives: [ sname, dname ], vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      var dtree = new Tree(db, sname, { vSize: 3, log: silence });
      mt._copyMissingToStage(stree, dtree, function(err) {
        if (err) { throw err; }
        // inspect stage
        mt.stats(function(err, stats) {
          if (err) { throw err; }
          should.deepEqual(stats, {
            local: { heads: { count: 0, conflict: 0, deleted: 0 } },
            stage: { heads: { count: 1, conflict: 0, deleted: 0 } },
            '_copyMissingToStage_foo': { heads: { count: 1, conflict: 0, deleted: 0 } },
            '_copyMissingToStage_bar': { heads: { count: 0, conflict: 0, deleted: 0 } }
          });
          done();
        });
      });
    });

    it('needs item1 and item2 in dtree', function(done) {
      var tree = new Tree(db, dname, { vSize: 3, log: silence });

      tree.write(item1);
      tree.write(item2);
      tree.end(null, done);
    });

    it('should not copy anything to stage', function(done) {
      var opts = { stage: stageName, perspectives: [ sname, dname ], vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      var dtree = new Tree(db, sname, { vSize: 3, log: silence });
      mt._copyMissingToStage(stree, dtree, function(err) {
        if (err) { throw err; }
        // inspect stage
        mt.stats(function(err, stats) {
          if (err) { throw err; }
          should.deepEqual(stats, {
            local: { heads: { count: 0, conflict: 0, deleted: 0 } },
            stage: { heads: { count: 1, conflict: 0, deleted: 0 } },
            '_copyMissingToStage_foo': { heads: { count: 1, conflict: 0, deleted: 0 } },
            '_copyMissingToStage_bar': { heads: { count: 1, conflict: 0, deleted: 0 } }
          });
          done();
        });
      });
    });

    it('needs item2 and item3 in stree', function(done) {
      var tree = new Tree(db, sname, { vSize: 3, log: silence });

      tree.write(item2);
      tree.write(item3);
      tree.end(null, done);
    });

    it('should copy item2 and item3 to stage (and skip item1 that is already in stage)', function(done) {
      var opts = { stage: stageName, perspectives: [ sname, dname ], vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      var dtree = new Tree(db, sname, { vSize: 3, log: silence });
      var i = 0;
      mt._copyMissingToStage(stree, dtree, function(err) {
        if (err) { throw err; }
        mt._stage.iterateInsertionOrder(function(item, next) {
          i++;
          if (i === 1) {
            should.deepEqual(item, { h: { id: 'XI', v: 'Aaaa', pa: [], i: 1, pe: sname }, b: { some: 'body' } });
          }
          if (i === 2) {
            should.deepEqual(item, { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], i: 2, pe: sname }, b: { more: 'body' } });
          }
          if (i > 2) {
            should.deepEqual(item, { h: { id: 'XI', v: 'Cccc', pa: ['Aaaa'], i: 3, pe: sname }, b: { more2: 'body' } });
          }
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 3);
          done();
        });
      });
    });

    it('needs item4 stree', function(done) {
      var tree = new Tree(db, sname, { vSize: 3, log: silence });

      tree.write(item4);
      tree.end(null, done);
    });

    it('should transform and copy item4 to stage', function(done) {
      var transform = function(item, cb) {
        item.b.transformed = true;
        cb(null, item);
      };

      var opts = { transform: transform, stage: stageName, perspectives: [ sname, dname ], vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      var dtree = new Tree(db, sname, { vSize: 3, log: silence });
      var i = 0;
      mt._copyMissingToStage(stree, dtree, function(err) {
        if (err) { throw err; }
        mt._stage.iterateInsertionOrder(function(item, next) {
          i++;
          if (i === 1) {
            should.deepEqual(item, { h: { id: 'XI', v: 'Aaaa', pa: [], i: 1, pe: sname }, b: { some: 'body' } });
          }
          if (i === 2) {
            should.deepEqual(item, { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], i: 2, pe: sname }, b: { more: 'body' } });
          }
          if (i === 3) {
            should.deepEqual(item, { h: { id: 'XI', v: 'Cccc', pa: ['Aaaa'], i: 3, pe: sname }, b: { more2: 'body' } });
          }
          if (i > 3) {
            should.deepEqual(item, { h: { id: 'XI', v: 'Dddd', pa: ['Cccc'], i: 4, pe: sname }, b: { more3: 'body', transformed: true } });
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

  describe('_mergeStageWithLocal', function() {
    var sname = '_mergeStageWithLocal_foo';
    var localName = '_local_mergeStageWithLocal';
    var stageName = '_stage_mergeStageWithLocal';

    it('should require cb to be a function', function() {
      var opts = { local: localName, stage: stageName, perspectives: [ sname ], vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);
      (function() { mt._mergeStageWithLocal(); }).should.throw('cb must be a function');
    });

    describe('empty databases and root merge', function() {
      // use 24-bit version numbers (base 64)
      var sitem1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] },       b: { some: 'body' } };

      var litem1 = { h: { id: 'XI', v: 'Aaaa', pa: [] },       b: { some: 'body' } };

      it('should return and do nothing if both local and stage are empty', function(done) {
        var opts = { local: localName, stage: stageName, perspectives: [ sname ], vSize: 3, log: silence };
        var mt = new MergeTree(db, opts);
        mt._mergeStageWithLocal(function(err) {
          if (err) { throw err; }
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.deepEqual(stats, {
              local: { heads: { count: 0, conflict: 0, deleted: 0 } },
              stage: { heads: { count: 0, conflict: 0, deleted: 0 } },
              '_mergeStageWithLocal_foo': { heads: { count: 0, conflict: 0, deleted: 0 } }
            });
            done();
          });
        });
      });

      it('write sitem1 to stage', function(done) {
        var tree = new Tree(db, stageName, { vSize: 3, log: silence });
        tree.write(sitem1, done);
      });

      it('should call merge handler with sitem1 (fast-forward) because it misses in local', function(done) {
        var i = 0;
        var mergeHandler = function(merged, lhead, next) {
          i++;
          should.deepEqual(merged, { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [], i: 1 }, b: { some: 'body' } });
          next();
        };
        var opts = {
          local: localName,
          stage: stageName,
          perspectives: [ sname ],
          vSize: 3,
          log: silence,
          mergeHandler: mergeHandler
        };
        var mt = new MergeTree(db, opts);
        mt._mergeStageWithLocal(function(err) {
          if (err) { throw err; }
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.strictEqual(i, 1);
            should.deepEqual(stats, {
              local: { heads: { count: 0, conflict: 0, deleted: 0 } },
              stage: { heads: { count: 1, conflict: 0, deleted: 0 } },
              '_mergeStageWithLocal_foo': { heads: { count: 0, conflict: 0, deleted: 0 } }
            });
            done();
          });
        });
      });

      it('write litem1', function(done) {
        var tree = new Tree(db, localName, { vSize: 3, log: silence });
        tree.write(litem1, done);
      });

      it('should not call merge handler with sitem1 since litem1 is in local (although perspective is not written to local)', function(done) {
        // this happens when a new remote is added that is a subset of the existing items
        var i = 0;
        var mergeHandler = function(merged, lhead, next) {
          i++;
          next();
        };
        var opts = {
          local: localName,
          stage: stageName,
          perspectives: [ sname ],
          vSize: 3,
          log: silence,
          mergeHandler: mergeHandler
        };
        var mt = new MergeTree(db, opts);
        mt._mergeStageWithLocal(function(err) {
          if (err) { throw err; }
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.strictEqual(i, 0);
            should.deepEqual(stats, {
              local: { heads: { count: 1, conflict: 0, deleted: 0 } },
              stage: { heads: { count: 1, conflict: 0, deleted: 0 } },
              '_mergeStageWithLocal_foo': { heads: { count: 0, conflict: 0, deleted: 0 } }
            });
            done();
          });
        });
      });
    });

    describe('merge one and two heads in stree', function() {
      var sname = '_mergeStageWithLocalOneTwoHeads_foo';
      var localName = '_local_oneTwoHeadsMergeStageWithLocal';
      var stageName = '_stage_oneTwoHeadsMergeStageWithLocal';

      // use 24-bit version numbers (base 64)
      var sitem1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] },       b: { more1: 'body' } };
      var sitem2 = { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'] }, b: { more2: 'body' } };
      var sitem3 = { h: { id: 'XI', v: 'Cccc', pe: sname, pa: ['Aaaa'] }, b: { more3: 'body' } };

      var litem1 = { h: { id: 'XI', v: 'Aaaa', pa: [] },       b: { more1: 'body' } };

      it('write sitem1, sitem2', function(done) {
        var tree = new Tree(db, stageName, { vSize: 3, log: silence });
        tree.write(sitem1);
        tree.write(sitem2);
        tree.end(done);
      });

      it('write litem1', function(done) {
        var tree = new Tree(db, localName, { vSize: 3, log: silence });
        tree.write(litem1);
        tree.end(done);
      });

      it('write sitem1 to ltree to update perspective', function(done) {
        var tree = new Tree(db, localName, { vSize: 3, log: silence });
        tree.end(sitem1, done);
      });

      it('should merge sitem2 with local (fast-forward)', function(done) {
        var i = 0;
        var mergeHandler = function(merge, lhead, next) {
          i++;
          should.deepEqual(merge, { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], i: 2 }, b: { more2: 'body' } });
          should.deepEqual(lhead, { h: { id: 'XI', v: 'Aaaa', pa: [], i: 1 }, b: { more1: 'body' } });
          next();
        };
        var opts = {
          local: localName,
          stage: stageName,
          perspectives: [ sname ],
          vSize: 3,
          log: silence,
          mergeHandler: mergeHandler
        };
        var mt = new MergeTree(db, opts);
        mt._mergeStageWithLocal(function(err) {
          if (err) { throw err; }
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.strictEqual(i, 1);
            should.deepEqual(stats, {
              local: { heads: { count: 1, conflict: 0, deleted: 0 } },
              stage: { heads: { count: 1, conflict: 0, deleted: 0 } },
              '_mergeStageWithLocalOneTwoHeads_foo': { heads: { count: 0, conflict: 0, deleted: 0 } }
            });
            done();
          });
        });
      });

      it('write sitem3', function(done) {
        var tree = new Tree(db, stageName, { vSize: 3, log: silence });
        tree.write(sitem3, done);
      });

      it('should merge sitem2 with litem2 (ff), sitem3 litem2 (merge) (return false) and retry sitem3 with litem2 (merge)', function(done) {
        var local = new Tree(db, localName, { vSize: 3, log: silence });

        var i = 0;
        var firstMerge;
        var mergeHandler = function(merge, lhead, next) {
          i++;
          if (i === 1) {
            should.deepEqual(merge, { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], i: 2 }, b: { more2: 'body' } });
            should.deepEqual(lhead, { h: { id: 'XI', v: 'Aaaa', pa: [], i: 1 }, b: { more1: 'body' } });

            // ref this merge so in next iteration it can be saved to the local tree
            firstMerge = merge;
            next();
          }
          if (i === 2) {
            // update the version in local like a normal client would do at some point, using the previous merge
            local.write(firstMerge, function(err) {
              if (err) { throw err; }

              should.deepEqual(merge, { h: { id: 'XI', v: 'Cccc', pe: sname, pa: ['Aaaa'], i: 3 }, b: { more3: 'body' } });
              should.deepEqual(lhead, { h: { id: 'XI', v: 'Aaaa', pa: [], i: 1 }, b: { more1: 'body' } });

              // break iteration as if lhead does not match anymore
              // should break iteration since lhead !== the last head in local
              next(false);
            });
          }
          if (i > 2) {
            // this iteration should start after _mergeStageWithLocal is invoked again
            // should merge and use a content based version number (parents must be sorted)
            should.deepEqual(merge, { h: { id: 'XI', v: 'xeaV', pa: ['Bbbb', 'Cccc'], i: 4 }, b: { more2: 'body', more3: 'body' } });
            should.deepEqual(lhead, { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], i: 2 }, b: { more2: 'body' } });
            next();
          }
        };
        var opts = {
          local: localName,
          stage: stageName,
          perspectives: [ sname ],
          vSize: 3,
          log: silence,
          mergeHandler: mergeHandler
        };
        var mt = new MergeTree(db, opts);
        mt._mergeStageWithLocal(function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 2);
          mt._mergeStageWithLocal(function(err) {
            if (err) { throw err; }
            should.strictEqual(i, 3);
            mt.stats(function(err, stats) {
              if (err) { throw err; }
              should.deepEqual(stats, {
                local: { heads: { count: 1, conflict: 0, deleted: 0 } },
                stage: { heads: { count: 1, conflict: 0, deleted: 0 } },
                '_mergeStageWithLocalOneTwoHeads_foo': { heads: { count: 0, conflict: 0, deleted: 0 } }
              });
              done();
            });
          });
        });
      });
    });

    describe('merge two heads one deleted in stree', function() {
      var sname = '_mergeStageWithLocalTwoHeadsOneDelete_foo';
      var localName = '_local_twoHeadsOneDeleteMergeStageWithLocal';
      var stageName = '_stage_twoHeadsOneDeleteMergeStageWithLocal';

      // use 24-bit version numbers (base 64)
      var sitem1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] },                b: { some: 'body' } };
      var sitem2 = { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], d: true }, b: { more2: 'body' } };
      var sitem3 = { h: { id: 'XI', v: 'Cccc', pe: sname, pa: ['Aaaa'] },          b: { more3: 'body' } };

      var litem1 = { h: { id: 'XI', v: 'Aaaa', pa: [] },                b: { some: 'body' } };
      var litem2 = { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], d: true }, b: { more2: 'body' } };

      it('write sitem1, sitem2', function(done) {
        var tree = new Tree(db, stageName, { vSize: 3, log: silence });
        tree.write(sitem1);
        tree.write(sitem2);
        tree.end(done);
      });

      it('write litem1 and sitem1 to local', function(done) {
        var tree = new Tree(db, localName, { vSize: 3, log: silence });
        tree.write(litem1);
        tree.write(sitem1);
        tree.end(done);
      });

      it('should merge sitem2 with litem1 (merge ff in ltree)', function(done) {
        var i = 0;
        var mergeHandler = function(merge, lhead, next) {
          i++;
          should.deepEqual(merge, { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], d: true, i: 2 }, b: { more2: 'body' } });
          should.deepEqual(lhead, { h: { id: 'XI', v: 'Aaaa', pa: [], i: 1 }, b: { some: 'body' } });
          next();
        };
        var opts = {
          local: localName,
          stage: stageName,
          perspectives: [ sname ],
          vSize: 3,
          log: silence,
          mergeHandler: mergeHandler
        };
        var mt = new MergeTree(db, opts);
        mt._mergeStageWithLocal(function(err) {
          if (err) { throw err; }
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.strictEqual(i, 1);
            should.deepEqual(stats, {
              local: { heads: { count: 1, conflict: 0, deleted: 0 } },
              stage: { heads: { count: 1, conflict: 0, deleted: 1 } },
              '_mergeStageWithLocalTwoHeadsOneDelete_foo': { heads: { count: 0, conflict: 0, deleted: 0 } }
            });
            done();
          });
        });
      });


      it('write litem2', function(done) {
        var tree = new Tree(db, localName, { vSize: 3, log: silence });
        tree.write(litem2);
        tree.end(done);
      });

      it('write sitem3', function(done) {
        var tree = new Tree(db, stageName, { vSize: 3, log: silence });
        tree.write(sitem3);
        tree.end(done);
      });

      it('should merge sitem3 with litem2 (merge, no deleted flag)', function(done) {
        var i = 0;
        var mergeHandler = function(merge, lhead, next) {
          i++;
          should.deepEqual(merge, { h: { id: 'XI', v: 'xeaV', pa: ['Bbbb', 'Cccc'], i: 4 }, b: { more2: 'body', more3: 'body' } });
          should.deepEqual(lhead, { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], d: true, i: 2 }, b: { more2: 'body' } });
          next();
        };
        var opts = {
          local: localName,
          stage: stageName,
          perspectives: [ sname ],
          vSize: 3,
          log: silence,
          mergeHandler: mergeHandler
        };
        var mt = new MergeTree(db, opts);
        mt._mergeStageWithLocal(function(err) {
          if (err) { throw err; }
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.strictEqual(i, 1);
            should.deepEqual(stats, {
              local: { heads: { count: 1, conflict: 0, deleted: 1 } },
              stage: { heads: { count: 1, conflict: 0, deleted: 0 } },
              '_mergeStageWithLocalTwoHeadsOneDelete_foo': { heads: { count: 0, conflict: 0, deleted: 0 } }
            });
            done();
          });
        });
      });
    });

    describe('merge new connecting root after delete', function() {
      var sname = '_mergeStageWithLocalNewRootAfterDelete_foo';
      var localName = '_local_newRootAfterDeleteMergeStageWithLocal';
      var stageName = '_stage_newRootAfterDeleteMergeStageWithLocal';

      // use 24-bit version numbers (base 64)
      var sitem1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] },                b: { some: 'body' } };
      var sitem2 = { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], d: true }, b: { more2: 'body' } };
      var sitem3 = { h: { id: 'XI', v: 'Cccc', pe: sname, pa: ['Bbbb'] },          b: { more3: 'body' } }; // the "root"

      var litem1 = { h: { id: 'XI', v: 'Aaaa', pa: [] },                b: { some: 'body' } };
      var litem2 = { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], d: true }, b: { more2: 'body' } };

      it('write sitem1, sitem2 and sitem3', function(done) {
        var tree = new Tree(db, stageName, { vSize: 3, log: silence });
        tree.write(sitem1);
        tree.write(sitem2);
        tree.write(sitem3);
        tree.end(done);
      });

      it('write litem1 and litem2 to local', function(done) {
        var tree = new Tree(db, localName, { vSize: 3, log: silence });
        tree.write(litem1);
        tree.write(litem2);
        tree.end(done);
      });

      it('should have one deleted head in local and one head in stage', function(done) {
        var opts = {
          local: localName,
          stage: stageName,
          vSize: 3,
          log: silence
        };
        var mt = new MergeTree(db, opts);
        mt.stats(function(err, stats) {
          if (err) { throw err; }
          should.deepEqual(stats, {
            local: { heads: { count: 1, conflict: 0, deleted: 1 } },
            stage: { heads: { count: 1, conflict: 0, deleted: 0 } }
          });
          done();
        });
      });

      it('should merge item3 (new "root")', function(done) {
        var i = 0;
        var mergeHandler = function(merge, lhead, next) {
          i++;
          should.deepEqual(merge, { h: { id: 'XI', v: 'Cccc', pe: sname, pa: ['Bbbb'], i: 3 }, b: { more3: 'body' } });
          should.deepEqual(null);
          next();
        };
        var opts = {
          local: localName,
          stage: stageName,
          perspectives: [ sname ],
          vSize: 3,
          log: silence,
          mergeHandler: mergeHandler
        };
        var mt = new MergeTree(db, opts);
        mt._mergeStageWithLocal(function(err) {
          if (err) { throw err; }
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.strictEqual(i, 1);
            should.deepEqual(stats, {
              local: { heads: { count: 1, conflict: 0, deleted: 1 } },
              stage: { heads: { count: 1, conflict: 0, deleted: 0 } },
              '_mergeStageWithLocalNewRootAfterDelete_foo': { heads: { count: 0, conflict: 0, deleted: 0 } }
            });
            done();
          });
        });
      });
    });

    describe('merge with conflict (unresolved)', function() {
      var sname = '_mergeStageWithLocalTwoHeadsOneConflict_foo';
      var localName = '_local_twoHeadsOneConflictMergeStageWithLocal';
      var stageName = '_stage_twoHeadsOneConflictMergeStageWithLocal';

      // use 24-bit version numbers (base 64)
      var sitem1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] },       b: { some: 'body' } };
      var sitem2 = { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'] }, b: { more2: 'body' } };

      var litem1 = { h: { id: 'XI', v: 'Aaaa', pa: [] },                  b: { some: 'body' } };
      var litem2 = { h: { id: 'XI', v: 'Cccc', pa: ['Aaaa'] },            b: { more2: 'other' } };

      it('write sitem1, sitem2', function(done) {
        var tree = new Tree(db, stageName, { vSize: 3, log: silence });
        tree.write(sitem1);
        tree.write(sitem2);
        tree.end(done);
      });

      it('write litem1, litem2, sitem1 to local', function(done) {
        var tree = new Tree(db, localName, { vSize: 3, log: silence });
        tree.write(litem1);
        tree.write(litem2);
        tree.write(sitem1);
        tree.end(done);
      });

      it('should merge sitem2 with litem2, conflict on "more2" (unresolved, mark in conflict)', function(done) {
        var i = 0;
        var j = 0;
        var conflictHandler = function(attrs, shead, lhead, next) {
          i++;
          should.deepEqual(attrs, ['more2']);
          should.deepEqual(shead, { more2: 'body' });
          should.deepEqual(lhead, { more2: 'other' });
          next();
        };
        var mergeHandler = function(merge, lhead, next) {
          j++;
          next();
        };
        var opts = {
          local: localName,
          stage: stageName,
          perspectives: [ sname ],
          vSize: 3,
          log: silence,
          conflictHandler: conflictHandler,
          mergeHandler: mergeHandler
        };
        var mt = new MergeTree(db, opts);
        mt._mergeStageWithLocal(function(err) {
          if (err) { throw err; }
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.strictEqual(i, 1); // expect one call to conflict handler
            should.strictEqual(j, 0); // expect no merge handler
            should.deepEqual(stats, {
              local: { heads: { count: 1, conflict: 0, deleted: 0 } },
              stage: { heads: { count: 1, conflict: 1, deleted: 0 } }, // should have marked in conflict
              '_mergeStageWithLocalTwoHeadsOneConflict_foo': { heads: { count: 0, conflict: 0, deleted: 0 } }
            });

            // invoke twice to make sure old conflicts don't bubble up again
            mt._mergeStageWithLocal(function(err) {
              if (err) { throw err; }
              mt.stats(function(err, stats) {
                if (err) { throw err; }
                should.strictEqual(i, 1); // expect no extra calls to conflict handler
                should.strictEqual(j, 0); // expect no merge handler
                should.deepEqual(stats, {
                  local: { heads: { count: 1, conflict: 0, deleted: 0 } },
                  stage: { heads: { count: 1, conflict: 1, deleted: 0 } }, // should have marked in conflict
                  '_mergeStageWithLocalTwoHeadsOneConflict_foo': { heads: { count: 0, conflict: 0, deleted: 0 } }
                });
                done();
              });
            });
          });
        });
      });
    });

    describe('merge with conflict (resolved)', function() {
      var sname = '_mergeStageWithLocalTwoHeadsOneConflictResolved_foo';
      var localName = '_local_twoHeadsOneConflictResolvedMergeStageWithLocal';
      var stageName = '_stage_twoHeadsOneConflictResolvedMergeStageWithLocal';

      // use 24-bit version numbers (base 64)
      var sitem1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] },       b: { some: 'body' } };
      var sitem2 = { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'] }, b: { more2: 'body' } };

      var litem1 = { h: { id: 'XI', v: 'Aaaa', pa: [] },                  b: { some: 'body' } };
      var litem2 = { h: { id: 'XI', v: 'Cccc', pa: ['Aaaa'] },            b: { more2: 'other' } };

      it('write sitem1, sitem2', function(done) {
        var tree = new Tree(db, stageName, { vSize: 3, log: silence });
        tree.write(sitem1);
        tree.write(sitem2);
        tree.end(done);
      });

      it('write litem1, litem2, sitem1 to local', function(done) {
        var tree = new Tree(db, localName, { vSize: 3, log: silence });
        tree.write(litem1);
        tree.write(litem2);
        tree.write(sitem1);
        tree.end(done);
      });

      it('should merge sitem2 with litem2, conflict on "more2" (resolved)', function(done) {
        var i = 0;
        var j = 0;
        var conflictHandler = function(attrs, shead, lhead, next) {
          i++;
          should.deepEqual(attrs, ['more2']);
          should.deepEqual(shead, { more2: 'body' });
          should.deepEqual(lhead, { more2: 'other' });
          next({ mergedManually: true });
        };
        // should be called with new resolved conflict if _mergeStageWithLocal is invoked twice
        var mergeHandler = function(merge, lhead, next) {
          j++;
          should.deepEqual(merge, { h: { id: 'XI', v: 'f1l4', pa: ['Bbbb', 'Cccc'], i: 3 }, b: { mergedManually: true} });
          next();
        };
        var opts = {
          local: localName,
          stage: stageName,
          perspectives: [ sname ],
          vSize: 3,
          log: silence,
          conflictHandler: conflictHandler,
          mergeHandler: mergeHandler
        };
        var mt = new MergeTree(db, opts);
        mt._mergeStageWithLocal(function(err) {
          if (err) { throw err; }
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.strictEqual(i, 1); // expect one call to conflict handler
            should.strictEqual(j, 0); // expect no merge handler
            should.deepEqual(stats, {
              local: { heads: { count: 1, conflict: 0, deleted: 0 } },
              stage: { heads: { count: 1, conflict: 0, deleted: 0 } }, // should have marked in conflict
              '_mergeStageWithLocalTwoHeadsOneConflictResolved_foo': { heads: { count: 0, conflict: 0, deleted: 0 } }
            });

            // invoke twice to process resolved merge conflict
            mt._mergeStageWithLocal(function(err) {
              if (err) { throw err; }
              mt.stats(function(err, stats) {
                if (err) { throw err; }
                should.strictEqual(i, 1); // expect no extra calls to conflict handler
                should.strictEqual(j, 1); // expect no one call to merge handler
                should.deepEqual(stats, {
                  local: { heads: { count: 1, conflict: 0, deleted: 0 } },
                  stage: { heads: { count: 1, conflict: 0, deleted: 0 } }, // should have marked in conflict
                  '_mergeStageWithLocalTwoHeadsOneConflictResolved_foo': { heads: { count: 0, conflict: 0, deleted: 0 } }
                });
                done();
              });
            });
          });
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

    it('should return with last saved item as a number by default', function(done) {
      var opts = { stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(ldb, opts);
      mt.lastByPerspective(name, function(err, v) {
        if (err) { throw err; }
        // version number Aaaa base64 is 108186
        should.strictEqual(v, 108186);
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

    describe('stats', function() {
      it('should return stats', function(done) {
        var opts = { stage: stageName, vSize: 3, log: silence };
        var mt = new MergeTree(ldb, opts);
        mt.stats(function(err, stats) {
          if (err) { throw err; }
          should.deepEqual(stats, {
            local: { heads: { count: 1, conflict: 0, deleted: 0 } },
            stage: { heads: { count: 0, conflict: 0, deleted: 0 } }
          });
          done();
        });
      });
    });
  });

  describe('lastReceivedFromRemote', function() {
    var name = 'lastReceivedFromRemote';
    var stageName = '_stage_lastReceivedFromRemote';
    var ldb;
    var ldbPath = tmpdir() + '/test_merge_tree_lastReceivedFromRemote';

    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa', pe: name, pa: [] }, b: { some: 'body' } };

    before('should open a new db for lastReceivedFromRemote tests only', function(done) {
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

    it('should require remote to be a string', function() {
      var opts = { stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(ldb, opts);
      (function() { mt.lastReceivedFromRemote(); }).should.throw('remote must be a string');
    });

    it('should require cb to be a function', function() {
      var opts = { stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(ldb, opts);
      (function() { mt.lastReceivedFromRemote(''); }).should.throw('cb must be a function');
    });

    it('should throw with non-existing perspective', function() {
      var opts = { stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(ldb, opts);
      (function() { mt.lastReceivedFromRemote(name, function() {}); }).should.throw('remote not found');
    });

    it('should return with empty database', function(done) {
      var opts = { perspectives: [name], stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(ldb, opts);
      mt.lastReceivedFromRemote(name, function(err, v) {
        if (err) { throw err; }
        should.strictEqual(v, null);
        done();
      });
    });

    it('save item1 from remote "lastReceivedFromRemote"', function(done) {
      var opts = { perspectives: [name], stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(ldb, opts);
      mt._pe[name].write(item1, done);
    });

    it('should return with last saved version as a number by default', function(done) {
      var opts = { perspectives: [name], stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(ldb, opts);
      mt.lastReceivedFromRemote(name, function(err, v) {
        if (err) { throw err; }
        // version number Aaaa base64 is 108186
        should.strictEqual(v, 108186);
        done();
      });
    });

    it('should return with last saved version, decoded in base64', function(done) {
      var opts = { perspectives: [name], stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(ldb, opts);
      mt.lastReceivedFromRemote(name, 'base64', function(err, v) {
        if (err) { throw err; }
        should.strictEqual(v, 'Aaaa');
        done();
      });
    });
  });

  describe('mergeWithLocal', function() {
    var sname = 'mergeWithLocal_foo';
    var localName = '_local_mergeWithLocal';
    var stageName = '_stage_mergeWithLocal';

    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] },       b: { some: 'body' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'] }, b: { more: 'body' } };
    var item3 = { h: { id: 'XI', v: 'Cccc', pe: sname, pa: ['Aaaa'] }, b: { more2: 'body' } };

    var litem1 = { h: { id: 'XI', v: 'Aaaa', pa: [] }, b: { some: 'body' } };

    it('should require stree to be an object', function() {
      var opts = { local: localName, stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);
      (function() { mt.mergeWithLocal(); }).should.throw('stree must be an object');
    });

    it('should require cb to be a function', function() {
      var opts = { local: localName, stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);
      (function() { mt.mergeWithLocal({}); }).should.throw('cb must be a function');
    });

    it('should merge an empty database', function(done) {
      var opts = { local: localName, stage: stageName, vSize: 3, log: silence };
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
      var i = 0;
      var mergeHandler = function(newHead, lhead, next) {
        i++;
        should.deepEqual(newHead, item1);
        should.deepEqual(lhead, null);
        next();
      };
      var opts = {
        local: localName,
        stage: stageName,
        vSize: 3,
        log: silence,
        mergeHandler: mergeHandler
      };
      var mt = new MergeTree(db, opts);
      var stree = new Tree(db, sname, opts);
      var stage = mt._stage;
      mt.mergeWithLocal(stree, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);

        // inspect staging
        var j = 0;
        stage.iterateInsertionOrder(function(item, next) {
          j++;
          should.deepEqual(item, item1);
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 1);
          should.strictEqual(j, 1);
          done();
        });
      });
    });

    it('should be the same as previous run (idempotence)', function(done) {
      var i = 0;
      var mergeHandler = function(newHead, lhead, next) {
        i++;
        should.deepEqual(newHead, item1);
        should.deepEqual(lhead, null);
        next();
      };
      var opts = {
        local: localName,
        stage: stageName,
        vSize: 3,
        log: silence,
        mergeHandler: mergeHandler
      };
      var mt = new MergeTree(db, opts);
      var stree = new Tree(db, sname, opts);
      var stage = mt._stage;
      mt.mergeWithLocal(stree, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);

        // inspect staging
        var j = 0;
        stage.iterateInsertionOrder(function(item, next) {
          j++;
          should.deepEqual(item, item1);
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(j, 1);
          done();
        });
      });
    });

    it('item2 in stree', function(done) {
      var tree = new Tree(db, sname, { vSize: 3, log: silence });
      tree.write(item2, done);
    });

    it('should merge ff with current head in local', function(done) {
      var i = 0;
      var mergeHandler = function(newHead, lhead, next) {
        i++;
        should.deepEqual(newHead, item2);
        should.deepEqual(lhead, null);
        next();
      };
      var opts = {
        local: localName,
        stage: stageName,
        vSize: 3,
        log: silence,
        mergeHandler: mergeHandler
      };
      var mt = new MergeTree(db, opts);
      var stree = new Tree(db, sname, opts);
      var stage = mt._stage;
      mt.mergeWithLocal(stree, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        // inspect staging
        var j = 0;
        stage.iterateInsertionOrder(function(item, next) {
          j++;
          if (j === 1) {
            should.deepEqual(item, item1);
          }
          if (j > 1) {
            should.deepEqual(item, item2);
          }
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(j, 2);
          done();
        });
      });
    });

    it('should be the same as previous run (idempotence)', function(done) {
      var i = 0;
      var mergeHandler = function(newHead, lhead, next) {
        i++;
        should.deepEqual(newHead, item2);
        should.deepEqual(lhead, null);
        next();
      };
      var opts = {
        local: localName,
        stage: stageName,
        vSize: 3,
        log: silence,
        mergeHandler: mergeHandler
      };
      var mt = new MergeTree(db, opts);
      var stree = new Tree(db, sname, opts);
      var stage = mt._stage;
      mt.mergeWithLocal(stree, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        // inspect staging
        var j = 0;
        stage.iterateInsertionOrder(function(item, next) {
          j++;
          if (j === 1) {
            should.deepEqual(item, item1);
          }
          if (j > 1) {
            should.deepEqual(item, item2);
          }
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(j, 2);
          done();
        });
      });
    });

    it('item3 in stree', function(done) {
      var tree = new Tree(db, sname, { vSize: 3, log: silence });
      tree.write(item3, done);
    });

    it('litem1 in local', function(done) {
      var tree = new Tree(db, localName, { vSize: 3, log: silence });
      tree.write(litem1, done);
    });

    it('should only add item3 to stage and merge both heads with local head (write first merge to local)', function(done) {
      var prevMerge;
      var j = 0;
      var mergeHandler = function(merge, lhead, next) {
        j++;
        // the previously created merge in stage is used
        if (j === 1) {
          should.deepEqual(merge, { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], i: 2 }, b: { more: 'body' } });
          should.deepEqual(lhead, { h: { id: 'XI', v: 'Aaaa', pa: [], i: 1 },                  b: { some: 'body' } });
          prevMerge = merge;
          next(); // accept
        }
        if (j > 1) {
          should.deepEqual(merge, { h: { id: 'XI', v: 'Cccc', pe: sname, pa: ['Aaaa'], i: 3 }, b: { more2: 'body' } });
          should.deepEqual(lhead, { h: { id: 'XI', v: 'Aaaa', pa: [], i: 1 },                  b: { some: 'body' } });

          // write previous merge to local for next test
          mt._local.write(prevMerge, function(err) {
            if (err) { throw err; }
            next(false); // don't accept this new head
          });
        }
      };
      var mt = new MergeTree(db, {
        local: localName,
        stage: stageName,
        vSize: 3,
        log: silence,
        mergeHandler: mergeHandler
      });
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
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
          if (i > 2) {
            should.deepEqual(item, item3);
          }
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 3);
          should.strictEqual(j, 2);
          done();
        });
      });
    });

    it('should merge item3 again, but now there is a head in local from previous test', function(done) {
      var j = 0;
      var mergeHandler = function(merge, lhead, next) {
        j++;
        // the previously created merge in stage is used (parents are sorted)
        if (j > 0) {
          should.deepEqual(merge, { h: { id: 'XI', v: 'QBPL', pa: ['Bbbb', 'Cccc'], i: 4 }, b: { more: 'body', more2: 'body' } });
          should.deepEqual(lhead, { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], i: 2 }, b: { more: 'body' } });
        }
        next();
      };
      var mt = new MergeTree(db, {
        local: localName,
        stage: stageName,
        perspectives: [sname],
        vSize: 3,
        log: silence,
        mergeHandler: mergeHandler
      });
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
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
            should.deepEqual(item, { h: { id: 'XI', v: 'QBPL', pa: ['Bbbb', 'Cccc'], i: 4 }, b: { more: 'body', more2: 'body' } });
          }
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 4);
          should.strictEqual(j, 1);
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.deepEqual(stats, {
              local: { heads: { count: 1, conflict: 0, deleted: 0 } },
              stage: { heads: { count: 1, conflict: 0, deleted: 0 } },
              'mergeWithLocal_foo': { heads: { count: 2, conflict: 0, deleted: 0 } }
            });
            done();
          });
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
    var v1;
    var item1 = { h: { id: 'XI'            }, b: { some: 'body' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb' }, b: { more: 'body' } };
    var item3 = { h: { id: 'XI', v: 'Cccc' }, b: { more2: 'body' } };
    var item4 = { h: { id: 'XI', v: 'Dddd' }, b: { more3: 'b' } };
    var item5 = { h: { id: 'XI', v: 'Eeee', d: true } };

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
      lws.write({ h: { id: 'XI', pa: [v1] } }, function(err) {
        should.strictEqual(err.message, 'did not expect local item to have a parent defined');
        done();
      });
    });

    it('should accept a new item, version, create parents and i', function(done) {
      var mt = new MergeTree(db, { stage: stageName, vSize: 3, log: silence });
      var lws = mt.createLocalWriteStream();
      lws.write(item1);

      var i = 0;
      lws.end(function() {
        mt._local.iterateInsertionOrder(function(item, next) {
          i++;
          v1 = item.h.v;
          should.deepEqual(item, {
            h: { id: 'XI', v: v1, pa: [], i: 1 },
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
              h: { id: 'XI', v: 'Bbbb', pa: [v1], i: 2 },
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
      lws.write(item2, function(err) {
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
              h: { id: 'XI', v: v1, pa: [], i: 1 },
              b: { some: 'body' }
            });
          }
          if (i === 2) {
            should.deepEqual(item, {
              h: { id: 'XI', v: 'Bbbb', pa: [v1], i: 2 },
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

    it('preload item4 in stage with parent set to v1 (fork)', function(done) {
      var mt = new MergeTree(db, { stage: stageName, vSize: 3, log: silence });
      mt._stage.write({
        h: { id: 'XI', v: 'Dddd', pa: [v1] },
        b: { more3: 'b' }
      }, done);
    });

    it('write item4 (fork, move item from stage)', function(done) {
      var mt = new MergeTree(db, { stage: stageName, vSize: 3, log: silence });
      var lws = mt.createLocalWriteStream();
      lws.write(item4);
      lws.end(function() {
        var i = 0;
        mt._local.iterateInsertionOrder(function(item, next) {
          i++;
          if (i === 1) {
            should.deepEqual(item, {
              h: { id: 'XI', v: v1, pa: [], i: 1 },
              b: { some: 'body' }
            });
          }
          if (i === 2) {
            should.deepEqual(item, {
              h: { id: 'XI', v: 'Bbbb', pa: [v1], i: 2 },
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
              h: { id: 'XI', v: 'Dddd', pa: [v1], i: 4 },
              b: { more3: 'b' }
            });
          }
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 4);
          // stage should be empty, since item4 is copied to local
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.deepEqual(stats, {
              local: { heads: { count: 2, conflict: 0, deleted: 0 } },
              stage: { heads: { count: 0, conflict: 0, deleted: 0 } }
            });
            done();
          });
        });
      });
    });

    it('should not accept items as long as multiple non-conflicting parents exist', function(done) {
      var mt = new MergeTree(db, { stage: stageName, vSize: 3, log: silence });
      var lws = mt.createLocalWriteStream();

      lws.on('error', function(err) {
        should.strictEqual(err.message, 'more than one non-conflicting head in local tree');
        done();
      });

      lws.write(item5);
    });

    describe('new root after delete', function() {
      var localName = '_local_createLocalWriteStreamNewRootAfterDelete';
      var stageName = '_stage_createLocalWriteStreamNewRootAfterDelete';

      // use 24-bit version numbers (base 64)
      var item1 = { h: { id: 'XI', v: 'Aaaa' },         b: { some: 'body' } };
      var item2 = { h: { id: 'XI', v: 'Bbbb', d: true } };
      var item3 = { h: { id: 'XI', v: 'Cccc' },         b: { more3: 'b' } };  // new "root" (should piont to item5)

      it('write items, new root should point to last delete', function(done) {
        var mt = new MergeTree(db, { local: localName, stage: stageName, vSize: 3, log: silence });
        var lws = mt.createLocalWriteStream();
        lws.write(item1);
        lws.write(item2);
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
                h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], i: 2, d: true }
              });
            }
            if (i === 3) {
              should.deepEqual(item, {
                h: { id: 'XI', v: 'Cccc', pa: ['Bbbb'], i: 3 },
                b: { more3: 'b' }
              });
            }
            next();
          }, function(err) {
            if (err) { throw err; }
            should.strictEqual(i, 3);
            // stage should be empty, since item4 is copied to local
            mt.stats(function(err, stats) {
              if (err) { throw err; }
              should.deepEqual(stats, {
                local: { heads: { count: 1, conflict: 0, deleted: 0 } },
                stage: { heads: { count: 0, conflict: 0, deleted: 0 } }
              });
              done();
            });
          });
        });
      });
    });
  });

  describe('ensureMergeHandler', function() {
    it('should set default merge handler on construction', function() {
      var opts = { vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);

      should.strictEqual(typeof mt._mergeHandler, 'function');
    });

    it('should set given merge handler', function() {
      var opts = { vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);

      var mh = function() {};

      should.strictEqual(typeof mt._mergeHandler, 'function');
      should.strictEqual(mt._mergeHandler === mh, false);
      mt.ensureMergeHandler(mh);
      should.strictEqual(mt._mergeHandler === mh, true);
    });
  });

  describe('mergeAll', function() {
    var stageName = '_stage_mergeAll';
    var pe = 'some';
    var ldb;
    var ldbPath = tmpdir() + '/test_merge_tree_mergeAll';

    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa', pe: pe, pa: [] }, b: { some: 'body' } };

    before('should open a new db for mergeAll tests only', function(done) {
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

    it('should return right away if no dbs are updated', function(done) {
      var opts = { stage: stageName, vSize: 3, log: silence };
      var mt = new MergeTree(ldb, opts);
      mt.mergeAll({ done: done });
    });

    it('should set updated flag if written to remote stream', function(done) {
      var opts = { stage: stageName, vSize: 3, log: silence, perspectives: [pe] };
      var mt = new MergeTree(ldb, opts);
      should.equal(mt._updatedPerspectives[pe], true); // constructor should set true on init
      mt._updatedPerspectives[pe] = false;
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
        mt.mergeAll({
          done: function(err) {
            if (err) { throw err; }
            mt.createReadStream().on('data', function(item) {
              should.deepEqual({
                h: { id: 'XI', v: 'Aaaa', pa: [] },
                b: { some: 'body' }
              }, item);
              done();
            });
          }
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

  describe('_generateRandomVersion', function() {
    it('should generate a new id of 5 bytes', function() {
      MergeTree._generateRandomVersion(5).length.should.equal(8);
    });
    it('should encode in base64', function() {
      MergeTree._generateRandomVersion(3).should.match(/^[a-z0-9/+A-Z]{4}$/);
    });
    it('should generate a new id of 6 bytes default', function() {
      MergeTree._generateRandomVersion().should.match(/^[a-z0-9/+A-Z]{8}$/);
    });
    it('should error with unsupported sizes', function() {
      (function() { MergeTree._generateRandomVersion('a'); }).should.throwError('size must be a number >= 0');
    });
  });

  describe('getLocalTree', function() {
    it('should return the local tree', function() {
      var mt = new MergeTree(db);
      var tree = mt.getLocalTree();
      should.strictEqual(tree, mt._local);
    });
  });

  describe('getStageTree', function() {
    it('should return the stage tree', function() {
      var mt = new MergeTree(db);
      var tree = mt.getStageTree();
      should.strictEqual(tree, mt._stage);
    });
  });

  describe('getRemoteTrees', function() {
    it('should return an empty object', function() {
      var mt = new MergeTree(db);
      var trees = mt.getRemoteTrees();
      should.deepEqual(Object.keys(trees), []);
    });

    it('should return an empty object', function() {
      var mt = new MergeTree(db, { perspectives: ['foo'] });
      var trees = mt.getRemoteTrees();
      should.deepEqual(Object.keys(trees), ['foo']);
    });
  });

  describe('proxy', function() {
    it('should proxy createReadStream', function() {
      var mt = new MergeTree(db);
      if (typeof mt.createReadStream !== 'function') {
        throw new Error('createReadStream is not a function');
      }
    });

    it('should proxy getByVersion', function() {
      var mt = new MergeTree(db);
      if (typeof mt.getByVersion !== 'function') {
        throw new Error('getByVersion is not a function');
      }
    });
  });
});
