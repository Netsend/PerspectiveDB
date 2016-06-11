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
var level = require('level-packager')(require('leveldown'));

var Tree = require('../../../lib/tree');
var StreamTree = require('../../../lib/_stream_tree');
var logger = require('../../../lib/logger');

var db, cons, silence;
var dbPath = tmpdir() + '/test_tree';

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

describe('StreamTree', function() {
  var name = 'streamInsertionOrder';

  var item1 = { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } };
  var item2 = { h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } };
  var item3 = { h: { id: 'XI', v: 'Dddd', i: 3, pa: ['Aaaa'] }, b: { some: 'other' } };
  var item4 = { h: { id: 'foo', v: 'Eeee', i: 4, pa: [] }, b: { som3: 'other' } };

  it('should end directly with an empty database', function(done) {
    var t = new Tree(db, name, { vSize: 3, log: silence });
    var s = new StreamTree(t);
    s.on('data', done); // need handler to start flowing, should not emit
    s.on('end', done);
  });

  it('item1', function(done) {
    var t = new Tree(db, name, { vSize: 3, log: silence });
    t.write(item1, done);
  });

  it('should emit bson buffers', function(done) {
    var t = new Tree(db, name, { vSize: 3, log: silence });

    var i = 0;
    var s = new StreamTree(t, { bson: true });
    s.on('data', function(obj) {
      i++;
      should.strictEqual(Buffer.isBuffer(obj), true);
    });

    s.on('end', function() {
      should.strictEqual(i, 1);
      done();
    });
  });

  it('should stream over item1', function(done) {
    var t = new Tree(db, name, { vSize: 3, log: silence });

    var i = 0;
    var s = new StreamTree(t);

    s.on('data', function(obj) {
      i++;
      should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
    });

    s.on('end', function() {
      should.strictEqual(i, 1);
      done();
    });
  });

  it('should not emit items that are written after the stream is opened', function(done) {
    var t = new Tree(db, name, { vSize: 3, log: silence });

    var i = 0;
    var s = new StreamTree(t);
    var writeDone, ended;
    t.write(item2, function(err) {
      if (err) { throw err; }
      writeDone = true;
      if (ended) { done(); }
    });
    s.on('data', function(obj) {
      i++;
      if (i > 0) {
        should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
      }
    });

    s.on('end', function() {
      should.strictEqual(i, 1);
      ended = true;
      if (writeDone) { done(); }
    });
  });

  it('should end after last data event, even while paused (node stream behavior)', function(done) {
    var t = new Tree(db, name, { vSize: 3, log: silence });

    var ended;
    var i = 0;
    var s = new StreamTree(t);
    s.on('data', function() {
      s.pause();
      i++;
      if (i === 1) {
        setTimeout(function() {
          s.resume();
        }, 10);
      } else {
        setTimeout(function() {
          should.strictEqual(ended, true);
          done();
        }, 10);
      }
    });

    s.on('end', function() {
      ended = true;
      should.strictEqual(i, 2);
    });
  });

  it('should use opts.first and start emitting at offset 1', function(done) {
    var t = new Tree(db, name, { vSize: 3, log: silence });

    var i = 0;
    var s = new StreamTree(t, { first: 'Aaaa' });
    s.on('data', function(obj) {
      i++;
      if (i === 1) {
        should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
      }

      if (i > 1) {
        should.deepEqual({ h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } }, obj);
      }
    });

    s.on('end', function() {
      should.strictEqual(i, 2);
      done();
    });
  });

  it('should use opts.first and start emitting at offset 2 (excludeFirst)', function(done) {
    var t = new Tree(db, name, { vSize: 3, log: silence });

    var i = 0;
    var s = new StreamTree(t, { first: 'Aaaa', excludeFirst: true });
    s.on('data', function(obj) {
      i++;
      if (i > 0) {
        should.deepEqual({ h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } }, obj);
      }
    });

    s.on('end', function() {
      should.strictEqual(i, 1);
      done();
    });
  });

  it('should emit the existing item but no new items that are added while the stream is running the first time of many', function(done) {
    var t = new Tree(db, name, { vSize: 3, log: silence });

    var i = 0;
    var s = new StreamTree(t);
    var writeDone, ended;
    t.write(item3, function(err) {
      if (err) { throw err; }
      writeDone = true;
      if (ended) { done(); }
    });
    s.on('data', function(obj) {
      i++;
      if (i === 1) {
        should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
      }
      if (i > 1) {
        should.deepEqual({ h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } }, obj);
      }
    });

    s.on('end', function() {
      should.strictEqual(i, 2);
      ended = true;
      if (writeDone) { done(); }
    });
  });

  it('item4, different DAG', function(done) {
    var t = new Tree(db, name, { vSize: 3, log: silence });
    t.write(item4, done);
  });

  it('should stream over DAG foo only (item4)', function(done) {
    var t = new Tree(db, name, { vSize: 3, log: silence });
    var i = 0;
    var s = new StreamTree(t, { id: 'foo' });
    s.on('data', function(obj) {
      i++;
      if (i > 0) {
        should.deepEqual({ h: { id: 'foo', v: 'Eeee', i: 4, pa: [] }, b: { som3: 'other' } }, obj);
      }
    });

    s.on('end', function() {
      should.strictEqual(i, 1);
      done();
    });
  });

  it('should reopen a new stream', function(done) {
    var t = new Tree(db, name, { vSize: 3, log: silence });

    var i = 0;
    var s = new StreamTree(t);

    s.on('data', function() {
      i++;
    });

    s.on('end', function() {
      should.strictEqual(i, 4);

      var u = s.reopen();

      u.on('data', function() {
        i++;
      });

      u.on('end', function() {
        should.strictEqual(i, 8);
        done();
      });
    });
  });

  // depends on all 4 previous inserts
  it('should drain if reading more items than fit in the buffer', function(done) {
    var t = new Tree(db, name, { vSize: 3, log: silence });

    var s = new StreamTree(t, { highWaterMark: 1 });

    var reads = 0;
    var drains = 0;

    s.on('drain', function() {
      drains++;
    });

    // slow reader
    s.on('readable', function() {
      setTimeout(function() {
        while (s.read()) {
          reads++;
        }
      }, 10);
    });

    s.on('end', function(err) {
      if (err) { throw err; }

      should.strictEqual(reads, 4); // the number of items written in previous tests
      should.strictEqual(drains, 3); // must drain after each item

      done();
    });
  });
});
