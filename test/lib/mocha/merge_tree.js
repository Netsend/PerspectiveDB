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
    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa', pa: [] }, b: { some: 'body' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'] }, b: { more: 'body' } };
    var item3 = { h: { id: 'XI', v: 'Cccc', pa: ['Aaaa'] }, b: { more2: 'body' } };
    var item4 = { h: { id: 'XI', v: 'Dddd', pa: ['Cccc'] }, b: { more3: 'b' } };

    it('should require dtree to be an object', function() {
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
      var stree = new Tree(db, 'foo', { log: silence });
      var dtree = new Tree(db, 'bar', { log: silence });
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
      var stree = new Tree(db, 'foo', { vSize: 3, log: silence });
      stree.end(item1, done);
    });

    it('should copy stree to dtree with one item', function(done) {
      var stree = new Tree(db, 'foo', { vSize: 3, log: silence });
      var dtree = new Tree(db, 'bar', { vSize: 3, log: silence });

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
      var stree = new Tree(db, 'foo', { vSize: 3, log: silence });
      var streeIt = 0;
      stree.end(item2, function(err) {
        stree.iterateInsertionOrder(function(item, next) {
          if (err) { throw err; }

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
      var stree = new Tree(db, 'foo', { vSize: 3, log: silence });
      var dtree = new Tree(db, 'bar', { vSize: 3, log: silence });

      MergeTree._copyTo(stree, dtree, function(err) {
        should.strictEqual(err.message, 'item is not a new child');
        done();
      });
    });

    it('should copy stree to dtree starting at item1, exclusive', function(done) {
      var stree = new Tree(db, 'foo', { vSize: 3, log: silence });
      var dtree = new Tree(db, 'bar', { vSize: 3, log: silence });

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
      var stree = new Tree(db, 'foo', { vSize: 3, log: silence });
      var streeIt = 0;
      stree.write(item3);
      stree.end(item4, function(err) {
        stree.iterateInsertionOrder(function(item, next) {
          if (err) { throw err; }

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
      var stree = new Tree(db, 'foo', { vSize: 3, log: silence });
      var dtree = new Tree(db, 'bar', { vSize: 3, log: silence });

      var streeIt = 0;
      var dtreeIt = 0;

      var opts = { first: item3.h.v, excludeFirst: true };

      MergeTree._copyTo(stree, dtree, opts, function(err) {
        should.strictEqual(err.message, 'item is not a new child');
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
      var stree = new Tree(db, 'foo', { vSize: 3, log: silence });
      var dtree = new Tree(db, 'bar', { vSize: 3, log: silence });

      var streeIt = 0;
      var dtreeIt = 0;

      var opts = { first: item3.h.v, excludeFirst: false };

      MergeTree._copyTo(stree, dtree, opts, function(err) {
        if (err) { throw err; }
        stree.iterateInsertionOrder(function(item, next) {
          if (err) { throw err; }

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
            if (err) { throw err; }

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

  describe('createRemoteWriteStream', function() {
    var pe = 'other';

    // use 24-bit version numbers (base 64)
    var item1 = { m: { pe: pe }, h: { id: 'XI', v: 'Aaaa', pa: [] }, b: { some: 'body' } };
    var item2 = { m: { pe: pe }, h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'] }, b: { more: 'body' } };
    var item3 = { m: { pe: pe }, h: { id: 'XI', v: 'Cccc', pa: ['Aaaa'] }, b: { more2: 'body' } };
    var item4 = { m: { pe: pe }, h: { id: 'XI', v: 'Dddd', pa: ['Cccc'] }, b: { more3: 'b' } };

    it('should require item.m.pe to be a string', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      t.createRemoteWriteStream().write({}, function(err) {
        should.strictEqual(err.message, 'item.m.pe must be a string');
        done();
      });
    });

    it('should require item.h.pe to be a configured perspective', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      t.createRemoteWriteStream().write({ m: { pe: pe } }, function(err) {
        should.strictEqual(err.message, 'perspective not found');
        done();
      });
    });

    it('should require item.h.pe to differ from local name', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence });
      t.createRemoteWriteStream().write({ m: { pe: '_local' } }, function(err) {
        should.strictEqual(err.message, 'perspective should differ from local name');
        done();
      });
    });

    it('should not accept non-connecting items (item2 in empty database)', function(done) {
      var t = new MergeTree(db, { vSize: 3, log: silence, perspectives: [pe] });
      t.createRemoteWriteStream().write(item2, function(err) {
        should.strictEqual(err.message, 'item is not a new leaf');
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
