/**
 * Copyright 2015, 2016 Netsend.
 *
 * This file is part of PerspectiveDB.
 *
 * PerspectiveDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PerspectiveDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PerspectiveDB. If not, see <https://www.gnu.org/licenses/>.
 */

'use strict';

var tmpdir = require('os').tmpdir;

var bson = require('bson');
var should = require('should');
var rimraf = require('rimraf');
var level = require('level-packager')(require('leveldown'));
var xtend = require('xtend');

var MergeTree = require('../../../lib/merge_tree');
var Tree = require('../../../lib/tree');
var logger = require('../../../lib/logger');

var db, cons, silence;
var dbPath = tmpdir() + '/test_merge_tree';

var BSON = new bson.BSONPure.BSON();

function cloneItem(item) {
  // clone object to prevent side effects on the header
  var copy = xtend(item);
  copy.h = xtend(item.h);
  return copy;
}

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
  });

  describe('_createMergeStream', function() {
    var sname = '_createMergeStream_foo';
    var localName = '_local_createMergeStream';

    describe('empty databases and root merge', function() {
      // use 24-bit version numbers (base 64)
      var sitem1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [], i: 1 },       b: { some: 'body' } };

      var litem1 = { h: { id: 'XI', v: 'Aaaa', pa: [], i: 1 },       b: { some: 'body' } };

      it('should return and do nothing if local and remote are empty', function(done) {
        var opts = { local: localName + '1', perspectives: [ sname ], vSize: 3, log: silence };
        var mt = new MergeTree(db, opts);
        var ms = mt._createMergeStream();
        ms.on('error', done);
        ms.on('data', function() {}); // get stream in flowing mode so that end is emitted
        ms.end();
        ms.on('end', function() {
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.deepEqual(stats, {
              local: { heads: { count: 0, conflict: 0, deleted: 0 } },
              stage: { heads: { count: 0, conflict: 0, deleted: 0 } },
              '_createMergeStream_foo': { heads: { count: 0, conflict: 0, deleted: 0 } }
            });
            done();
          });
        });
      });

      it('should save remote items in remote tree', function(done) {
        var opts = { perspectives: [ sname ], vSize: 3, log: silence };
        var mt = new MergeTree(db, opts);
        var stree = mt._pe[sname];
        stree.write(sitem1, done);
      });

      it('should merge sitem1 (fast-forward) because it misses in local', function(done) {
        var opts = { local: localName + '2', perspectives: [ sname ], vSize: 3, log: silence };
        var mt = new MergeTree(db, opts);
        var ms = mt._createMergeStream();
        ms.on('error', done);
        ms.end(sitem1);

        var i = 0;
        ms.on('readable', function() {
          var obj = this.read();

          if (!obj) { // end of stream
            return;
          }

          should.deepEqual(obj, {
            n: { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [], i: 1 }, b: { some: 'body' } },
            l: null,
            lcas: [],
            pe: '_createMergeStream_foo',
            c: null
          });
          i++;
        });

        ms.on('end', function() {
          should.strictEqual(i, 1);
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.deepEqual(stats, {
              local: { heads: { count: 0, conflict: 0, deleted: 0 } },
              stage: { heads: { count: 0, conflict: 0, deleted: 0 } },
              '_createMergeStream_foo': { heads: { count: 1, conflict: 0, deleted: 0 } }
            });
            done();
          });
          return;
        });
      });

      it('should update last version in local to sitem1 if litem1 is already in local', function(done) {
        // this happens when a new remote is added that is a subset of the existing items
        var opts = { local: localName + '3', perspectives: [ sname ], vSize: 3, log: silence };
        var mt = new MergeTree(db, opts);

        var ms = mt._createMergeStream();
        ms.on('error', done);

        var i = 0;
        ms.on('data', function() {
          i++;
        });

        // write litem1 to ltree
        // write sitem1 to merge stream
        mt.getLocalTree().write(litem1, function(err) {
          if (err) { throw err; }
          ms.end(sitem1);
        });

        ms.on('end', function() {
          should.strictEqual(i, 0);
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.deepEqual(stats, {
              local: { heads: { count: 1, conflict: 0, deleted: 0 } },
              stage: { heads: { count: 0, conflict: 0, deleted: 0 } },
              '_createMergeStream_foo': { heads: { count: 1, conflict: 0, deleted: 0 } }
            });
            done();
          });
        });
      });
    });

    describe('merge one and two heads in stree', function() {
      var sname = '_createMergeStreamOneTwoHeads_foo';
      var localName = '_local_oneTwoHeadsMergeStageWithLocal';

      // use 24-bit version numbers (base 64)
      var sitem1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] },       b: { more1: 'body' } };
      var sitem2 = { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'] }, b: { more2: 'body' } };
      var sitem3 = { h: { id: 'XI', v: 'Cccc', pe: sname, pa: ['Aaaa'] }, b: { more3: 'body' } };

      var litem1 = { h: { id: 'XI', v: 'Aaaa', pa: [] },       b: { more1: 'body' } };

      it('should save remote items in remote tree', function(done) {
        var opts = { perspectives: [ sname ], vSize: 3, log: silence };
        var mt = new MergeTree(db, opts);
        var stree = mt._pe[sname];
        stree.write(sitem1);
        stree.write(sitem2);
        stree.write(sitem3, done);
      });

      it('should merge sitem2 with local (fast-forward)', function(done) {
        var opts = { local: localName + '1', perspectives: [ sname ], vSize: 3, log: silence };
        var mt = new MergeTree(db, opts);
        var ms = mt._createMergeStream();
        ms.on('error', done);

        // write sitem1 to ltree to update perspective
        // write sitem1 to merge stream
        // write sitem2 to merge stream
        var local = mt.getLocalTree();
        local.write(litem1, function(err) {
          if (err) { throw err; }
          local.write(sitem1, function(err) {
            if (err) { throw err; }
            ms.write(sitem1);
            ms.write(sitem2);
            ms.end();
          });
        });

        var i = 0;
        ms.on('data', function(obj) {
          should.deepEqual(obj, {
            n: { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], i: 2 }, b: { more2: 'body' } }, // h.i is from stage
            l: { h: { id: 'XI', v: 'Aaaa', pa: [], i: 1 }, b: { more1: 'body' } },
            lcas: ['Aaaa'],
            pe: '_createMergeStreamOneTwoHeads_foo',
            c: null
          });
          i++;
        });

        ms.on('end', function() {
          should.strictEqual(i, 1);
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.deepEqual(stats, {
              local: { heads: { count: 1, conflict: 0, deleted: 0 } },
              stage: { heads: { count: 0, conflict: 0, deleted: 0 } },
              '_createMergeStreamOneTwoHeads_foo': { heads: { count: 2, conflict: 0, deleted: 0 } }
            });
            done();
          });
        });
      });

      it('should merge sitem2 with litem2 (ff), sitem3 litem2 (merge)', function(done) {
        var opts = { local: localName + '2', perspectives: [ sname ], vSize: 3, log: silence };
        var mt = new MergeTree(db, opts);

        var ms = mt._createMergeStream();
        ms.on('error', done);

        // write litem1 to ltree
        // write sitem1 to ltree to update perspective
        // write sitem1 to merge stream
        // write sitem2 to merge stream
        // write sitem3 to merge stream
        var local = mt.getLocalTree();
        local.write(litem1, function(err) {
          if (err) { throw err; }
          local.write(sitem1, function(err) {
            if (err) { throw err; }
            ms.write(sitem1);
            ms.write(sitem2);
            ms.write(sitem3);
            ms.end();
          });
        });

        var i = 0;
        ms.on('data', function(obj) {
          i++;
          if (i === 1) {
            should.deepEqual(obj, {
              n: { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], i: 2 }, b: { more2: 'body' } },
              l: { h: { id: 'XI', v: 'Aaaa', pa: [], i: 1 }, b: { more1: 'body' } },
              lcas: ['Aaaa'],
              pe: '_createMergeStreamOneTwoHeads_foo',
              c: null
            });
          }
          if (i === 2) {
            should.deepEqual(obj, {
              n: { h: { id: 'XI', v: 'xeaV', pa: ['Bbbb', 'Cccc'] }, b: { more2: 'body', more3: 'body' } },
              l: { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], i: 2 }, b: { more2: 'body' } },
              lcas: ['Aaaa'],
              pe: '_createMergeStreamOneTwoHeads_foo',
              c: null
            });
          }
        });

        ms.on('end', function() {
          should.strictEqual(i, 2);
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.deepEqual(stats, {
              local: { heads: { count: 1, conflict: 0, deleted: 0 } },
              stage: { heads: { count: 0, conflict: 0, deleted: 0 } },
              '_createMergeStreamOneTwoHeads_foo': { heads: { count: 2, conflict: 0, deleted: 0 } }
            });
            done();
          });
        });
      });
    });

    describe('merge two heads one deleted', function() {
      var sname = '_createMergeStreamTwoHeadsOneDelete_foo';
      var localName = '_local_twoHeadsOneDeleteMergeStageWithLocal';
      var stageName = '_stage_twoHeadsOneDeleteMergeStageWithLocal';

      // use 24-bit version numbers (base 64)
      var sitem1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] },                b: { some: 'body' } };
      var sitem2 = { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], d: true }, b: { more2: 'body' } };
      var sitem3 = { h: { id: 'XI', v: 'Cccc', pe: sname, pa: ['Aaaa'] },          b: { more3: 'body' } };

      var litem1 = { h: { id: 'XI', v: 'Aaaa', pa: [] },                b: { some: 'body' } };
      var litem2 = { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], d: true }, b: { more2: 'body' } };

      it('should save remote items in remote tree', function(done) {
        var opts = { perspectives: [ sname ], vSize: 3, log: silence };
        var mt = new MergeTree(db, opts);
        var stree = mt._pe[sname];
        stree.write(sitem1);
        stree.write(sitem2);
        stree.write(sitem3, done);
      });

      it('should merge sitem2 with litem1 (merge ff in ltree)', function(done) {
        var opts = { local: localName + '1', stage: stageName + '1', perspectives: [ sname ], vSize: 3, log: silence };
        var mt = new MergeTree(db, opts);
        var ms = mt._createMergeStream();
        ms.on('error', done);

        // write litem1 to ltree
        // write sitem1 to ltree to update perspective
        // write sitem1 to merge stream
        // write sitem2 to merge stream
        var local = mt.getLocalTree();
        local.write(litem1, function(err) {
          if (err) { throw err; }
          local.write(sitem1, function(err) {
            if (err) { throw err; }
            ms.write(sitem1);
            ms.write(sitem2);
            ms.end();
          });
        });

        var i = 0;
        ms.on('data', function(obj) {
          i++;
          should.deepEqual(obj, {
            n: { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], d: true, i: 2 }, b: { more2: 'body' } },
            l: { h: { id: 'XI', v: 'Aaaa', pa: [], i: 1 }, b: { some: 'body' } },
            lcas: ['Aaaa'],
            pe: '_createMergeStreamTwoHeadsOneDelete_foo',
            c: null
          });
        });

        ms.on('end', function() {
          should.strictEqual(i, 1);
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.deepEqual(stats, {
              local: { heads: { count: 1, conflict: 0, deleted: 0 } },
              stage: { heads: { count: 0, conflict: 0, deleted: 0 } },
              '_createMergeStreamTwoHeadsOneDelete_foo': { heads: { count: 2, conflict: 0, deleted: 1 } }
            });
            done();
          });
        });
      });


      it('should merge sitem3 with litem2 (merge, no deleted flag)', function(done) {
        var opts = { local: localName + '2', stage: stageName + '2', perspectives: [ sname ], vSize: 3, log: silence };
        var mt = new MergeTree(db, opts);
        var ms = mt._createMergeStream();
        ms.on('error', done);

        // write litem1 to ltree
        // write litem2 to ltree
        // write sitem1 to ltree to update perspective
        // write sitem1 to merge stream
        // write sitem2 to merge stream
        // write sitem3 to merge stream
        var local = mt.getLocalTree();
        local.write(litem1);
        local.write(litem2, function(err) {
          if (err) { throw err; }
          local.write(sitem1, function(err) {
            if (err) { throw err; }
            ms.write(sitem1);
            ms.write(sitem2);
            ms.write(sitem3);
            ms.end();
          });
        });

        var i = 0;
        ms.on('data', function(obj) {
          i++;
          should.deepEqual(obj, {
            n: { h: { id: 'XI', v: 'xeaV', pa: ['Bbbb', 'Cccc'] }, b: { more2: 'body', more3: 'body' } },
            l: { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], d: true, i: 2 }, b: { more2: 'body' } },
            lcas: ['Aaaa'], // XXX: shouldn't this be Bbbb?
            pe: '_createMergeStreamTwoHeadsOneDelete_foo',
            c: null
          });
        });
        ms.on('end', function() {
          should.strictEqual(i, 1);
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.deepEqual(stats, {
              local: { heads: { count: 1, conflict: 0, deleted: 1 } },
              stage: { heads: { count: 0, conflict: 0, deleted: 0 } },
              '_createMergeStreamTwoHeadsOneDelete_foo': { heads: { count: 2, conflict: 0, deleted: 1 } }
            });
            done();
          });
        });
      });
    });

    describe('merge new connecting root after delete', function() {
      var sname = '_createMergeStreamNewRootAfterDelete_foo';
      var localName = '_local_newRootAfterDeleteMergeStageWithLocal';
      var stageName = '_stage_newRootAfterDeleteMergeStageWithLocal';

      // use 24-bit version numbers (base 64)
      var sitem1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] },                b: { some: 'body' } };
      var sitem2 = { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], d: true }, b: { more2: 'body' } };
      var sitem3 = { h: { id: 'XI', v: 'Cccc', pe: sname, pa: ['Bbbb'] },          b: { more3: 'body' } }; // the "root"

      var litem1 = { h: { id: 'XI', v: 'Aaaa', pa: [] },                b: { some: 'body' } };
      var litem2 = { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], d: true }, b: { more2: 'body' } };

      it('should save remote items in remote tree', function(done) {
        var opts = { perspectives: [ sname ], vSize: 3, log: silence };
        var mt = new MergeTree(db, opts);
        var stree = mt._pe[sname];
        stree.write(sitem1);
        stree.write(sitem2);
        stree.write(sitem3, done);
      });

      it('should have one deleted head in local and one head in stage, merge sitem3 (new root)', function(done) {
        var opts = { local: localName + '1', stage: stageName + '1', perspectives: [ sname ], vSize: 3, log: silence };
        var mt = new MergeTree(db, opts);
        var ms = mt._createMergeStream();
        ms.on('error', done);

        // write litem1 to ltree
        // write litem2 to ltree
        // write sitem1 to merge stream
        // write sitem2 to merge stream
        // write sitem3 to merge stream
        var local = mt.getLocalTree();
        local.write(litem1);
        local.write(litem2, function(err) {
          if (err) { throw err; }
          ms.write(sitem1);
          ms.write(sitem2);
          ms.write(sitem3);
          ms.end();
        });

        var i = 0;
        ms.on('data', function(obj) {
          should.deepEqual(obj, {
            n: { h: { id: 'XI', v: 'Cccc', pe: sname, pa: ['Bbbb'], i: 3 }, b: { more3: 'body' } },
            l: { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], d: true, i: 2 }, b: { more2: 'body' } },
            lcas: ['Bbbb'],
            pe: '_createMergeStreamNewRootAfterDelete_foo',
            c: null
          });
          i++;
        });

        ms.on('end', function() {
          should.strictEqual(i, 1);
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.deepEqual(stats, {
              local: { heads: { count: 1, conflict: 0, deleted: 1 } },
              stage: { heads: { count: 0, conflict: 0, deleted: 0 } },
              _createMergeStreamNewRootAfterDelete_foo: { heads: { count: 1, conflict: 0, deleted: 0 } }
            });
            done();
          });
        });
      });
    });

    describe('merge with conflict (unresolved)', function() {
      var sname = '_createMergeStreamTwoHeadsOneConflict_foo';
      var localName = '_local_twoHeadsOneConflictMergeStageWithLocal';
      var stageName = '_stage_twoHeadsOneConflictMergeStageWithLocal';

      // use 24-bit version numbers (base 64)
      var sitem1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] },       b: { some: 'body' } };
      var sitem2 = { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'] }, b: { more2: 'body' } };

      var litem1 = { h: { id: 'XI', v: 'Aaaa', pa: [] },                  b: { some: 'body' } };
      var litem2 = { h: { id: 'XI', v: 'Cccc', pa: ['Aaaa'] },            b: { more2: 'other' } };

      it('should save remote items in remote tree', function(done) {
        var opts = { perspectives: [ sname ], vSize: 3, log: silence };
        var mt = new MergeTree(db, opts);
        var stree = mt._pe[sname];
        stree.write(sitem1);
        stree.write(sitem2, done);
      });

      it('should merge sitem2 with litem2, conflict on "more2"', function(done) {
        var opts = { local: localName + '1', stage: stageName + '1', perspectives: [ sname ], vSize: 3, log: silence };
        var mt = new MergeTree(db, opts);
        var ms = mt._createMergeStream();
        ms.on('error', done);

        // write litem1 to ltree
        // write litem2 to ltree
        // write sitem1 to merge stream
        // write sitem2 to merge stream
        var local = mt.getLocalTree();
        local.write(litem1);
        local.write(litem2, function(err) {
          if (err) { throw err; }
          ms.write(sitem1);
          ms.write(sitem2);
          ms.end();
        });

        var i = 0;
        ms.on('data', function(obj) {
          should.deepEqual(obj, {
            n: { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'], i: 2 }, b: { more2: 'body' } },
            l: { h: { id: 'XI', v: 'Cccc', pa: ['Aaaa'], i: 2 },            b: { more2: 'other' } },
            lcas: ['Aaaa'],
            pe: '_createMergeStreamTwoHeadsOneConflict_foo',
            c: ['more2']
          });
          i++;
        });

        ms.on('end', function() {
          should.strictEqual(i, 1);
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.deepEqual(stats, {
              local: { heads: { count: 1, conflict: 0, deleted: 0 } },
              stage: { heads: { count: 0, conflict: 0, deleted: 0 } }, // should have marked in conflict
              '_createMergeStreamTwoHeadsOneConflict_foo': { heads: { count: 1, conflict: 0, deleted: 0 } }
            });
            done();
          });
        });
      });
    });

    // TODO: resolve via local write stream
    xdescribe('merge with conflict (resolved)', function() {
      var sname = '_createMergeStreamTwoHeadsOneConflictResolved_foo';
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
        // should be called with new resolved conflict if _createMergeStream is invoked twice
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

  describe('startMerge', function() {
    var sname = 'startMerge_foo';
    var localName = '_localstartMerge';
    var stageName = '_stagestartMerge';

    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa', pe: sname, pa: [] },       b: { some: 'body' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'] }, b: { more: 'body' } };
    var item3 = { h: { id: 'XI', v: 'Cccc', pe: sname, pa: ['Aaaa'] }, b: { more2: 'body' } };

    var litem1 = { h: { id: 'XI', v: 'Aaaa', pa: [] }, b: { some: 'body' } };
    var litem2 = { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'] }, b: { more: 'body' } };

    it('should merge an empty database', function(done) {
      var opts = { local: localName, stage: stageName, perspectives: [sname], vSize: 3, log: silence };
      var mt = new MergeTree(db, opts);
      var stree = mt._pe[sname];
      var i = 0;
      mt.startMerge({ ready: function(err) {
        if (err) { throw err; }
        var r = stree.insertionOrderStream();
        r.on('data', function() {
          i++;
        });
        r.on('end', function() {
          should.strictEqual(i, 0);
          done();
        });
      }});
    });

    it('item1 in stree', function(done) {
      var stree = new Tree(db, sname, { vSize: 3, log: silence });
      stree.write(cloneItem(item1), done);
    });

    it('should merge one item with the local database', function(done) {
      var opts = { local: localName, stage: stageName, vSize: 3, log: silence, perspectives: [sname] };
      var mt = new MergeTree(db, opts);

      var i = 0;
      var rs = mt.startMerge({ tail: false });

      rs.on('data', function(obj) {
        i++;
        should.deepEqual(obj, {
          n: item1,
          l: null,
          lcas: [],
          c: null,
          pe: sname
        });
      });

      rs.on('end', function() {
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should be the same as previous run (idempotence)', function(done) {
      var opts = { local: localName, stage: stageName, vSize: 3, log: silence, perspectives: [sname] };
      var mt = new MergeTree(db, opts);

      var i = 0;
      var rs = mt.startMerge({ tail: false });

      rs.on('data', function(obj) {
        i++;
        should.deepEqual(obj, {
          n: item1,
          l: null,
          lcas: [],
          c: null,
          pe: sname
        });
      });

      rs.on('end', function() {
        should.strictEqual(i, 1);
        done();
      });
    });

    it('item2 in stree', function(done) {
      var tree = new Tree(db, sname, { vSize: 3, log: silence });
      tree.write(cloneItem(item2), done);
    });

    it('should merge ff with current head in local', function(done) {
      var opts = { local: localName, stage: stageName, vSize: 3, log: silence, perspectives: [sname] };
      var mt = new MergeTree(db, opts);

      var i = 0;
      var rs = mt.startMerge({ tail: false });

      rs.on('data', function(obj) {
        i++;
        if (i === 1) {
          should.deepEqual(obj, {
            n: item1,
            l: null,
            lcas: [],
            c: null,
            pe: sname
          });
        }
        if (i > 1) {
          should.deepEqual(obj, {
            n: item2,
            l: item1,
            lcas: ['Aaaa'],
            c: null,
            pe: sname
          });
        }
      });

      rs.on('end', function() {
        should.strictEqual(i, 2);
        done();
      });
    });

    it('should be the same as previous run (idempotence)', function(done) {
      var opts = { local: localName, stage: stageName, vSize: 3, log: silence, perspectives: [sname] };
      var mt = new MergeTree(db, opts);

      var i = 0;
      var rs = mt.startMerge({ tail: false });

      rs.on('data', function(obj) {
        i++;
        if (i === 1) {
          should.deepEqual(obj, {
            n: item1,
            l: null,
            c: null,
            lcas: [],
            pe: sname
          });
        }
        if (i > 1) {
          should.deepEqual(obj, {
            n: item2,
            l: item1,
            c: null,
            lcas: ['Aaaa'],
            pe: sname
          });
        }
      });

      rs.on('end', function() {
        should.strictEqual(i, 2);
        done();
      });
    });

    it('item3 in stree', function(done) {
      var tree = new Tree(db, sname, { vSize: 3, log: silence });
      tree.write(cloneItem(item3), done);
    });

    it('litem1 in local', function(done) {
      var tree = new Tree(db, localName, { vSize: 3, log: silence });
      tree.write(cloneItem(litem1), done);
    });

    it('should update last by perspective to sitem1, copy 2 and 3 to stage and merge stage with local head', function(done) {
      var opts = { local: localName, stage: stageName, vSize: 3, log: silence, perspectives: [sname] };
      var mt = new MergeTree(db, opts);

      var i = 0;
      var rs = mt.startMerge({ tail: false });

      rs.on('data', function(obj) {
        i++;
        if (i === 1) {
          should.deepEqual(obj, {
            n: { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'] }, b: { more: 'body' } },
            l: { h: { id: 'XI', v: 'Aaaa', pa: [], i: 1 },            b: { some: 'body' } },
            c: null,
            lcas: ['Aaaa'],
            pe: sname
          });
        }
        if (i > 1) {
          should.deepEqual(obj, {
            n: { h: { id: 'XI', v: 'QBPL', pa: ['Bbbb', 'Cccc'] }, b: { more: 'body', more2: 'body' } }, // h.i is from stage
            l: { h: { id: 'XI', v: 'Bbbb', pe: sname, pa: ['Aaaa'] }, b: { more: 'body' } },
            c: null,
            lcas: ['Aaaa'],
            pe: sname
          });
        }
      });

      rs.on('end', function() {
        should.strictEqual(i, 2);
        // should have updated last version in local by fast-forward of item1 from stree
        mt._local.lastByPerspective(sname, 'base64', function(err, v) {
          if (err) { throw err; }
          should.strictEqual(v, 'Aaaa');
          done();
        });
      });
    });

    it('litem2 in local', function(done) {
      var tree = new Tree(db, localName, { vSize: 3, log: silence });
      tree.write(litem2, done);
    });

    it('should update last by perspective to sitem2, copy 3 to stage and merge stage with local head', function(done) {
      var opts = { local: localName, stage: stageName, vSize: 3, log: silence, perspectives: [sname] };
      var mt = new MergeTree(db, opts);

      var i = 0;
      var rs = mt.startMerge({ tail: false });

      rs.on('data', function(obj) {
        i++;
        if (i > 0) {
          should.deepEqual(obj, {
            n: { h: { id: 'XI', v: 'QBPL', pa: ['Bbbb', 'Cccc'] }, b: { more: 'body', more2: 'body' } }, // h.i is from stage
            l: { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], i: 2 }  , b: { more: 'body' } },
            c: null,
            lcas: ['Aaaa'],
            pe: sname
          });
        }
      });

      rs.on('end', function() {
        should.strictEqual(i, 1);

        // should have updated last version in local by fast-forward of item1 from stree
        mt._local.lastByPerspective(sname, 'base64', function(err, v) {
          if (err) { throw err; }
          should.strictEqual(v, 'Bbbb');

          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.deepEqual(stats, {
              local: { heads: { count: 1, conflict: 0, deleted: 0 } },
              stage: { heads: { count: 0, conflict: 0, deleted: 0 } },
              'startMerge_foo': { heads: { count: 2, conflict: 0, deleted: 0 } }
            });
            done();
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
      t.createRemoteWriteStream(pe).write(xtend(item1), function(err) {
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
    var pe = '_pe_createLocalWriteStream';

    // use 24-bit version numbers (base 64)
    var v1;
    var item1 = { n: { h: { id: 'XI'            }, b: { some: 'body' } } };
    var item2 = { n: { h: { id: 'XI', v: 'Bbbb' }, b: { more: 'body' } } };
    var item3 = { n: { h: { id: 'XI', v: 'Cccc' }, b: { more2: 'body' } } };

    var oitem1 = { h: { id: 'XII', v: 'Oooo', pa: []       }, b: { osome:  'body' } };
    var oitem2 = { h: { id: 'XII', v: 'Pppp', pa: ['Oooo'] }, b: { omore:  'body' } };
    var oitem3 = { h: { id: 'XII', v: 'Qqqq', pa: ['Pppp'] }, b: { omore2: 'body' } };
    var oitem4 = { h: { id: 'XII', v: 'Xxxx', pa: ['Qqqq'] }, b: { omore3: 'b'    } };

    it('should require item.h.id to be a buffer, a string or implement "toString"', function(done) {
      var mt = new MergeTree(db, { vSize: 3, log: silence });
      mt.createLocalWriteStream().write({ n: { h: { id: null } } }, function(err) {
        should.strictEqual(err.message, 'item.h.id must be a buffer, a string or implement "toString"');
        done();
      });
    });

    it('should require items to not have h.pa defined', function(done) {
      var mt = new MergeTree(db, { vSize: 3, log: silence });
      var lws = mt.createLocalWriteStream();
      lws.write({ n: { h: { id: 'XI', pa: [v1] } } }, function(err) {
        should.strictEqual(err.message, 'did not expect local item to have a parent defined');
        done();
      });
    });

    it('should accept a new item, version, create parents and i', function(done) {
      var mt = new MergeTree(db, { vSize: 3, log: silence });
      var lws = mt.createLocalWriteStream();
      lws.write(item1);

      var i = 0;
      lws.end(function() {
        var r = mt._local.insertionOrderStream();
        r.on('data', function(item) {
          i++;
          v1 = item.h.v;
          should.deepEqual(item, {
            h: { id: 'XI', v: v1, pa: [], i: 1 },
            b: { some: 'body' }
          });
        });
        r.on('end', function() {
          should.strictEqual(i, 1);
          done();
        });
      });
    });

    it('should accept a new item and point parents to the previously created head', function(done) {
      var mt = new MergeTree(db, { vSize: 3, log: silence });
      var lws = mt.createLocalWriteStream();
      lws.write(item2);

      var i = 0;
      lws.end(function() {
        var r = mt._local.insertionOrderStream();
        r.on('data', function(item) {
          i++;
          if (i > 1) {
            should.deepEqual(item, {
              h: { id: 'XI', v: 'Bbbb', pa: [v1], i: 2 },
              b: { more: 'body' }
            });
          }
        });
        r.on('end', function() {
          should.strictEqual(i, 2);
          done();
        });
      });
    });

    it('should not accept an existing version', function(done) {
      var mt = new MergeTree(db, { vSize: 3, log: silence });
      var lws = mt.createLocalWriteStream();
      lws.write(item2, function(err) {
        should.strictEqual(err.message, 'not a valid new item');
        done();
      });
    });

    it('should accept item3 (fast-forward)', function(done) {
      var mt = new MergeTree(db, { vSize: 3, log: silence });
      var lws = mt.createLocalWriteStream();
      lws.write(item3);
      lws.end(function() {
        var i = 0;
        var r = mt._local.insertionOrderStream();
        r.on('data', function(item) {
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
        });
        r.on('end', function() {
          should.strictEqual(i, 3);
          done();
        });
      });
    });

    it('write oitem1 to oitem4 in other perspective tree', function(done) {
      var mt = new MergeTree(db, { perspectives: [pe], vSize: 3, log: silence });
      mt._pe[pe].write([oitem1, oitem2, oitem3, oitem4], done);
    });

    xit('write oitem2, fast-forward from other perspective', function(done) {
      var mt = new MergeTree(db, { perspectives: [pe], vSize: 3, log: silence });
      var lws = mt.createLocalWriteStream();
      var i2 = xtend(oitem2); // set perspective on the item
      i2.h = xtend(i2.h, { pe: pe });
      lws.end({ n: i2, l: null, lcas: [], pe: pe }, function() {
        var i = 0;
        var r = mt._local.insertionOrderStream();
        r.on('data', function(item) {
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

          ////////////////////////////
          if (i === 4) {
            should.deepEqual(item, {
              h: { id: 'XII', v: 'Oooo', pa: [], i: 4 },
              b: { osome: 'body' }
            });
          }

          if (i > 4) {
            should.deepEqual(item, {
              h: { id: 'XII', v: 'Pppp', pa: ['Oooo'], i: 5 },
              b: { omore: 'body' }
            });
          }
        });
        r.on('end', function() {
          should.strictEqual(i, 5);
          mt.stats(function(err, stats) {
            if (err) { throw err; }
            should.deepEqual(stats, {
              local: { heads: { count: 2, conflict: 0, deleted: 0 } },
              stage: { heads: { count: 0, conflict: 0, deleted: 0 } },
              _pe_createLocalWriteStream: { heads: { count: 1, conflict: 0, deleted: 0 } }
            });
            done();
          });
        });
      });
    });

    xit('should not accept item if given local head does not match the current local head', function(done) {
      var mt = new MergeTree(db, { perspectives: [pe], vSize: 3, log: silence });
      var lws = mt.createLocalWriteStream();
      var i3 = xtend(oitem3); // set perspective on the item
      i3.h = xtend(i3.h, { pe: pe });
      lws.on('error', function(err) {
        should.strictEqual(err.message, 'not a valid new item');
        done();
      });
      lws.write({
        n: i3,
        l: null,
        lcas: [],
        c: null,
        pe: pe
      });
    });

    describe('merge with a remote', function() {
      var aitem1 = { h: { id: 'XIII', v: 'Kkkk', pa: []       }, b: { asome:  'body' } };
      var aitem2 = { h: { id: 'XIII', v: 'Llll', pa: ['Kkkk'] }, b: { amore:  'body' } };
      var aitem3 = { h: { id: 'XIII', v: 'Mmmm', pa: ['Llll'] }, b: { amore2: 'body' } };
      var aitem4 = { h: { id: 'XIII', v: 'Nnnn', pa: ['Llll'] }, b: { amore3: 'b'    } };
      // merge item 3 and 4
      var aitem5 = { h: { id: 'XIII', v: 'Ffff', pa: ['Mmmm', 'Nnnn'] }, b: { amor: 'c' } };

      it('save items in local and remote tree', function(done) {
        var mt = new MergeTree(db, { perspectives: [pe], vSize: 3, log: silence });
        mt._local.write([aitem1, aitem2, aitem3]);
        mt._pe[pe].end([aitem1, aitem2, aitem4], function(err) {
          if (err) { throw err; }
          mt._local.end(done);
        });
      });

      xit('should accept a merge between a remote and local', function(done) {
        var mt = new MergeTree(db, { perspectives: [pe], vSize: 3, log: silence });
        var lws = mt.createLocalWriteStream();
        lws.end({ n: aitem5, l: aitem3, lcas: ['Llll'], pe: pe }, function() {
          var i = 0;
          mt._local.createReadStream().on('data', function(item) {
            i++;

            ///////////////////////
            if (i === 6) {
              should.deepEqual(item, {
                h: { id: 'XIII', v: 'Kkkk', pa: [], i: 6 },
                b: { asome: 'body' }
              });
            }
            if (i === 7) {
              should.deepEqual(item, {
                h: { id: 'XIII', v: 'Llll', pa: ['Kkkk'], i: 7 },
                b: { amore: 'body' }
              });
            }
            if (i === 8) {
              should.deepEqual(item, {
                h: { id: 'XIII', v: 'Mmmm', pa: ['Llll'], i: 8 },
                b: { amore2: 'body' }
              });
            }
            if (i === 9) {
              should.deepEqual(item, {
                h: { id: 'XIII', v: 'Nnnn', pa: ['Llll'], i: 9 },
                b: { amore3: 'b' }
              });
            }
            if (i > 9) {
              should.deepEqual(item, {
                h: { id: 'XIII', v: 'Ffff', pa: ['Mmmm', 'Nnnn'], i: 10 },
                b: { amor: 'c' }
              });
            }
          }).on('end', function() {
            should.strictEqual(i, 10);
            mt.stats(function(err, stats) {
              if (err) { throw err; }
              should.deepEqual(stats, {
                local: { heads: { count: 3, conflict: 0, deleted: 0 } },
                stage: { heads: { count: 0, conflict: 0, deleted: 0 } },
                _pe_createLocalWriteStream: { heads: { count: 2, conflict: 0, deleted: 0 } }
              });
              done();
            });
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

  describe('generateRandomVersion', function() {
    it('should generate a new id of 5 bytes', function() {
      MergeTree.generateRandomVersion(5).length.should.equal(8);
    });
    it('should encode in base64', function() {
      MergeTree.generateRandomVersion(3).should.match(/^[a-z0-9/+A-Z]{4}$/);
    });
    it('should generate a new id of 6 bytes default', function() {
      MergeTree.generateRandomVersion().should.match(/^[a-z0-9/+A-Z]{8}$/);
    });
    it('should error with unsupported sizes', function() {
      (function() { MergeTree.generateRandomVersion('a'); }).should.throwError('size must be a number >= 0');
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

  describe('createReadStream', function() {
    var localName = '_local_createReadStream';
    var stageName = '_stage_createReadStream';
    var mt;
    var perspective = 'I';

    var A = {
      h: { id: 'foo', v: 'Aaaa', pe: 'I', pa: [], i: 1 },
      b: { baz : 'qux' }
    };

    var B = {
      h: { id: 'foo', v: 'Bbbb', pe: 'I', pa: ['Aaaa'], i: 2 },
      b: { foo: 'bar' }
    };

    var C = {
      h: { id: 'foo', v: 'Cccc', pe: 'I', pa: ['Bbbb'], i: 3 },
      b: {
        baz : 'mux',
        foo: 'bar'
      }
    };

    var D = {
      h: { id: 'foo', v: 'Dddd', pe: 'I', pa: ['Cccc'], i: 4 },
      b: {
        baz : 'qux' }
    };

    var E = {
      h: { id: 'foo', v: 'Eeee', pe: 'I', pa: ['Bbbb'], i: 5 },
      b: { }
    };

    var F = {
      h: { id: 'foo', v: 'Ffff', pe: 'I', pa: ['Eeee', 'Cccc'], i: 6 },
      b: {
        foo: 'bar' }
    };

    var G = {
      h: { id: 'foo', v: 'Gggg', pe: 'I', pa: ['Ffff'], i: 7 },
      b: { baz : 'qux' }
    };

    // same but without h.pe
    var rA = {
      h: { id: 'foo', v: 'Aaaa', pa: [] },
      b: { baz : 'qux' }
    };
    var rB = {
      h: { id: 'foo', v: 'Bbbb', pa: ['Aaaa'] },
      b: { foo: 'bar' }
    };
    var rC = {
      h: { id: 'foo', v: 'Cccc', pa: ['Bbbb'] },
      b: {
        baz : 'mux',
        foo: 'bar'
      }
    };
    var rD = {
      h: { id: 'foo', v: 'Dddd', pa: ['Cccc'] },
      b: { baz : 'qux' }
    };
    var rE = {
      h: { id: 'foo', v: 'Eeee', pa: ['Bbbb'] },
      b: {}
    };
    var rF = {
      h: { id: 'foo', v: 'Ffff', pa: ['Eeee', 'Cccc'] },
      b: { foo: 'bar' }
    };
    var rG = {
      h: { id: 'foo', v: 'Gggg', pa: ['Ffff'] },
      b: { baz : 'qux' }
    };

    // create the following structure:
    // A <-- B <-- C <-- D
    //        \     \
    //         E <-- F <-- G

    it('should create a MergeTree', function() {
      mt = new MergeTree(db, {
        local: localName,
        stage: stageName,
        vSize: 3,
        log: silence
      });
    });

    it('should require opts.bson to be a boolean', function() {
      (function() {
        mt.createReadStream({ bson: {} });
      }).should.throw('opts.bson must be a boolean');
    });

    it('should require opts.filter to be an object', function() {
      (function() {
        mt.createReadStream({ filter: 'foo' });
      }).should.throw('opts.filter must be an object');
    });

    it('should require opts.first to be a base64 string or a number', function() {
      (function() {
        mt.createReadStream({ first: {} });
      }).should.throw('opts.first must be a base64 string or a number');
    });

    it('should require opts.hooks to be an array', function() {
      (function() {
        mt.createReadStream({ hooks: {} });
      }).should.throw('opts.hooks must be an array');
    });

    it('should require opts.hooksOpts to be an object', function() {
      (function() {
        mt.createReadStream({ hooksOpts: 'foo' });
      }).should.throw('opts.hooksOpts must be an object');
    });

    it('should work with empty DAG', function(done) {
      var smt = mt.createReadStream();
      smt.on('data', function() { throw Error('no data should be emitted'); });
      smt.on('end', done);
    });

    it('should save DAG', function(done) {
      var ws = mt._local;
      ws.write(A);
      ws.write(B);
      ws.write(C);
      ws.write(D);
      ws.write(E);
      ws.write(F);
      ws.write(G);
      ws.end(done);
    });

    it('should return all elements', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var smt = mt.createReadStream({ local: perspective });
      var docs = [];

      smt.on('data', function(doc) {
        docs.push(doc);
      });

      smt.on('end', function() {
        should.equal(docs.length, 7);
        should.deepEqual(docs, [rA, rB, rC, rD, rE, rF, rG]);
        done();
      });
    });

    it('should tail and not close', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var smt = mt.createReadStream({
        local: perspective,
        tail: true,
        tailRetry: 10
      });
      var docs = [];

      smt.on('data', function(doc) {
        docs.push(doc);
      });

      var endCalled = false;
      setTimeout(function() {
        smt.end();
        endCalled = true;
      }, 50);

      smt.on('end', function() {
        should.equal(docs.length, 7);
        should.deepEqual(docs, [rA, rB, rC, rD, rE, rF, rG]);
        should.ok(endCalled);
        done();
      });
    });

    it('should return bson buffer instances', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var smt = mt.createReadStream({
        local: perspective,
        bson: true
      });
      var docs = [];

      smt.on('data', function(doc) {
        docs.push(BSON.deserialize(doc));
      });

      smt.on('end', function() {
        should.equal(docs.length, 7);
        should.deepEqual(docs, [rA, rB, rC, rD, rE, rF, rG]);
        done();
      });
    });

    it('should return only the last element if that is the offset', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var smt = mt.createReadStream({
        local: perspective,
        first: G.h.v
      });
      var docs = [];

      smt.on('data', function(doc) {
        docs.push(doc);
      });

      smt.on('end', function() {
        should.equal(docs.length, 1);
        should.deepEqual(docs, [rG]);
        done();
      });
    });

    it('should return from offset E', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var smt = mt.createReadStream({
        local: perspective,
        first: E.h.v
      });
      var docs = [];

      smt.on('data', function(doc) {
        docs.push(doc);
      });

      smt.on('end', function() {
        should.equal(docs.length, 3);
        should.deepEqual(docs[0], rE);
        should.deepEqual(docs[1], rF);
        should.deepEqual(docs[2], rG);
        done();
      });
    });

    it('should return everything since offset C (including E)', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var smt = mt.createReadStream({
        local: perspective,
        first: C.h.v
      });
      var docs = [];

      smt.on('data', function(doc) {
        docs.push(doc);
      });

      smt.on('end', function() {
        should.equal(docs.length, 5);
        should.deepEqual(docs[0], rC);
        should.deepEqual(docs[1], rD);
        should.deepEqual(docs[2], rE);
        should.deepEqual(docs[3], rF);
        should.deepEqual(docs[4], rG);
        done();
      });
    });

    it('should return the complete DAG if filter is empty', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var smt = mt.createReadStream({
        local: perspective,
        first: A.h.v
      });
      var docs = [];

      smt.on('data', function(doc) {
        docs.push(doc);
      });

      smt.on('end', function() {
        should.equal(docs.length, 7);
        should.deepEqual(docs[0], rA);
        should.deepEqual(docs[1], rB);
        should.deepEqual(docs[2], rC);
        should.deepEqual(docs[3], rD);
        should.deepEqual(docs[4], rE);
        should.deepEqual(docs[5], rF);
        should.deepEqual(docs[6], rG);
        done();
      });
    });

    it('should not endup with two same parents A for G since F is a merge but not selected', function(done) {
      // should not find A twice for merge F
      var smt = mt.createReadStream({
        local: perspective,
        first: A.h.v,
        filter: { baz: 'qux' }
      });
      var docs = [];

      smt.on('data', function(doc) {
        docs.push(doc);
      });

      smt.on('end', function() {
        should.equal(docs.length, 3);
        should.deepEqual(docs[0], { h: { id: 'foo', v: 'Aaaa', pa: [] }, b: { baz : 'qux' } });
        should.deepEqual(docs[1], { h: { id: 'foo', v: 'Dddd', pa: ['Aaaa'] },  b: { baz : 'qux' } });
        should.deepEqual(docs[2], { h: { id: 'foo', v: 'Gggg', pa: ['Aaaa'] },  b: { baz : 'qux' } });
        done();
      });
    });

    it('should return only attrs with baz = mug and change root to C', function(done) {
      // should not find A twice for merge F
      var smt = mt.createReadStream({
        local: perspective,
        first: A.h.v,
        filter: { baz: 'mux' }
      });
      var docs = [];

      smt.on('data', function(doc) {
        docs.push(doc);
      });

      smt.on('end', function() {
        should.equal(docs.length, 1);
        should.deepEqual(docs[0], { h: { id: 'foo', v: 'Cccc', pa: [] }, b: { baz : 'mux', foo: 'bar' } });
        done();
      });
    });

    it('should return only attrs with foo = bar and change root to B and alter subsequent parents', function(done) {
      // should not find A twice for merge F
      var smt = mt.createReadStream({
        local: perspective,
        first: A.h.v,
        filter: { foo: 'bar' }
      });
      var docs = [];

      smt.on('data', function(doc) {
        docs.push(doc);
      });

      smt.on('end', function() {
        should.equal(docs.length, 3);
        should.deepEqual(docs[0], { h: { id: 'foo', v: 'Bbbb', pa: [] }, b: { foo: 'bar' } });
        should.deepEqual(docs[1], { h: { id: 'foo', v: 'Cccc', pa: ['Bbbb'] }, b: { baz: 'mux', foo: 'bar' } });
        should.deepEqual(docs[2], { h: { id: 'foo', v: 'Ffff', pa: ['Bbbb', 'Cccc'] }, b: { foo: 'bar' } });
        done();
      });
    });

    it('should return nothing if filters don\'t match any item', function(done) {
      // should not find A twice for merge F
      var smt = mt.createReadStream({
        local: perspective,
        first: A.h.v,
        filter: { some: 'none' }
      });
      var docs = [];

      smt.on('data', function(doc) {
        docs.push(doc);
      });

      smt.on('end', function() {
        should.equal(docs.length, 0);
        done();
      });
    });

    it('should execute each hook', function(done) {
      // should not find A twice for merge F
      function transform(db, obj, opts, cb) {
        delete obj.b.baz;
        obj.b.checked = true;
        if (i === 1) {
          obj.b.checked = false;
        }
        i++;
        cb(null, obj);
      }
      function hook1(db, obj, opts, cb) {
        obj.b.hook1 = true;
        if (obj.h.v === 'Gggg') { obj.b.hook1g = 'foo'; }
        cb(null, obj);
      }
      function hook2(db, obj, opts, cb) {
        if (obj.b.hook1) {
          obj.b.hook2 = true;
        }
        cb(null, obj);
      }

      var smt = mt.createReadStream({
        local: perspective,
        first: A.h.v,
        filter: { baz: 'qux' },
        hooks: [transform, hook1, hook2]
      });
      var i = 0;

      var docs = [];

      smt.on('data', function(doc) {
        docs.push(doc);
      });

      smt.on('end', function() {
        should.equal(docs.length, 3);
        should.deepEqual(docs[0], { h: { id: 'foo', v: 'Aaaa', pa: [] }, b: { checked : true, hook1: true, hook2: true } });
        should.deepEqual(docs[1], { h: { id: 'foo', v: 'Dddd', pa: ['Aaaa'] }, b: { checked : false, hook1: true, hook2: true} });
        should.deepEqual(docs[2], { h: { id: 'foo', v: 'Gggg', pa: ['Aaaa'] }, b: { checked : true, hook1g: 'foo', hook1: true, hook2: true} });
        done();
      });
    });

    it('should cancel hook execution and skip item if one hook filters', function(done) {
      // should not find A twice for merge F

      function transform(db, obj, opts, callback) {
        delete obj.b.baz;
        obj.b.transformed = true;
        callback(null, obj);
      }
      // filter F. G should get parents of F which are E and C
      function hook1(db, obj, opts, callback) {
        if (obj.h.v === 'Ffff') {
          return callback(null, null);
        }
        callback(null, obj);
      }
      function hook2(db, obj, opts, callback) {
        obj.b.hook2 = true;
        callback(null, obj);
      }

      var smt = mt.createReadStream({
        local: perspective,
        first: E.h.v,
        hooks: [transform, hook1, hook2]
      });

      var docs = [];

      smt.on('data', function(doc) {
        docs.push(doc);
      });

      smt.on('end', function() {
        should.equal(docs.length, 2);
        should.deepEqual(docs[0], { h: { id: 'foo', v: 'Eeee', pa: ['Bbbb'] }, b: { transformed : true, hook2: true } });
        should.deepEqual(docs[1], { h: { id: 'foo', v: 'Gggg', pa: ['Eeee', 'Cccc'] }, b: { transformed : true, hook2: true} });
        done();
      });
    });

    it('should return only attrs with foo = bar and change root to B and alter subsequent parents, filtered by hook', function(done) {
      function hook(db, obj, opts, callback) {
        if (obj.b.foo === 'bar') {
          return callback(null, obj);
        }
        callback(null, null);
      }

      var smt = mt.createReadStream({
        local: perspective,
        first: A.h.v,
        hooks: [hook]
      });
      var docs = [];

      smt.on('data', function(doc) {
        docs.push(doc);
      });

      smt.on('end', function() {
        should.equal(docs.length, 3);
        should.deepEqual(docs[0], { h: { id: 'foo', v: 'Bbbb', pa: [] }, b: { foo: 'bar' } });
        should.deepEqual(docs[1], { h: { id: 'foo', v: 'Cccc', pa: ['Bbbb'] }, b: { baz: 'mux', foo: 'bar' } });
        should.deepEqual(docs[2], { h: { id: 'foo', v: 'Ffff', pa: ['Bbbb', 'Cccc'] }, b: { foo: 'bar' } });
        done();
      });
    });

    it('should return only attrs with foo = bar and change root to B and alter subsequent parents, filtered by hook and offset = B', function(done) {
      function hook(db, obj, opts, callback) {
        if (obj.b.foo === 'bar') {
          return callback(null, obj);
        }
        callback(null, null);
      }

      var smt = mt.createReadStream({
        local: perspective,
        first: B.h.v,
        hooks: [hook]
      });

      var docs = [];

      smt.on('data', function(doc) {
        docs.push(doc);
      });

      smt.on('end', function() {
        should.equal(docs.length, 3);
        should.deepEqual(docs[0], { h: { id: 'foo', v: 'Bbbb', pa: [] }, b: { foo: 'bar' } });
        should.deepEqual(docs[1], { h: { id: 'foo', v: 'Cccc', pa: ['Bbbb'] }, b: { baz: 'mux', foo: 'bar' } });
        should.deepEqual(docs[2], { h: { id: 'foo', v: 'Ffff', pa: ['Bbbb', 'Cccc'] }, b: { foo: 'bar' } });
        done();
      });
    });
  });

  describe('proxy', function() {
    it('should proxy getByVersion', function() {
      var mt = new MergeTree(db);
      if (typeof mt.getByVersion !== 'function') {
        throw new Error('getByVersion is not a function');
      }
    });
  });
});
