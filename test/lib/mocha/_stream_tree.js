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

  it('should emit raw buffers', function(done) {
    var t = new Tree(db, name, { vSize: 3, log: silence });

    var i = 0;
    var s = new StreamTree(t, { raw: true });
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

  it('should wait with end before all items are emitted', function(done) {
    var t = new Tree(db, name, { vSize: 3, log: silence });

    var i = 0;
    var s = new StreamTree(t);
    s.on('data', function() {
      s.pause();
      i++;
      setTimeout(function() {
        s.resume();
      }, 10);
    });

    s.on('end', function() {
      should.strictEqual(i, 2);
      done();
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
});
