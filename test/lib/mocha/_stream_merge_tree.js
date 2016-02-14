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

var fs = require('fs');
var Readable = require('stream').Readable;
var tmpdir = require('os').tmpdir;

var should = require('should');
var bson = require('bson');
var BSON = new bson.BSONPure.BSON();
var rimraf = require('rimraf');
var level = require('level-packager')(require('leveldown'));

var StreamMergeTree = require('../../../lib/_stream_merge_tree');
var MergeTree = require('../../../lib/merge_tree');
var logger = require('../../../lib/logger');

var cons, silence;

var dbPath = tmpdir() + '/test_merge_tree';

// open database
before(function(done) {
  logger({ console: true, mask: logger.DEBUG2 }, function(err, l) {
    if (err) { throw err; }
    cons = l;
    logger({ silence: true }, function(err, l) {
      if (err) { throw err; }
      silence = l;
      // ensure empty dbPath
      rimraf(dbPath, function(err) {
        if (err) { throw err; }
        fs.mkdir(dbPath, 0o755, done);
      });
    });
  });
});

after(function(done) {
  cons.close(function(err) {
    if (err) { throw err; }
    silence.close(function(err) {
      if (err) { throw err; }
      // ensure empty dbPath
      rimraf(dbPath, done);
    });
  });
});

describe('StreamMergeTree', function() {

  describe('constructor', function() {
    var mt, smt;

    it('create MergeTree', function(done) {
      var path = dbPath + '/constructor';
      fs.mkdir(path, 0o755, function(err) {
        if (err) { throw err; }
        level(path, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, db) {
          if (err) { throw err; }
          mt = new MergeTree(db, { log: silence });
          done();
        });
      });
    });

    it('should require mt to be an object', function() {
      (function () { smt = new StreamMergeTree(); }).should.throw('mt must be an object');
    });

    it('should require opts.log to be an object', function() {
      (function() {
        smt = new StreamMergeTree(mt, { log: '' });
      }).should.throw('opts.log must be an object');
    });

    it('should require opts.bson to be a boolean', function() {
      (function() {
        smt = new StreamMergeTree(mt, { bson: {}, log: silence });
      }).should.throw('opts.bson must be a boolean');
    });

    it('should require opts.filter to be an object', function() {
      (function() {
        smt = new StreamMergeTree(mt, { filter: 'foo', log: silence });
      }).should.throw('opts.filter must be an object');
    });

    it('should require opts.first to be a base64 string or a number', function() {
      (function() {
        smt = new StreamMergeTree(mt, { first: {} });
      }).should.throw('opts.first must be a base64 string or a number');
    });

    it('should require opts.hooks to be an array', function() {
      (function() {
        smt = new StreamMergeTree(mt, { hooks: {}, log: silence  });
      }).should.throw('opts.hooks must be an array');
    });

    it('should require opts.hooksOpts to be an object', function() {
      (function() {
        smt = new StreamMergeTree(mt, { hooksOpts: 'foo', log: silence  });
      }).should.throw('opts.hooksOpts must be an object');
    });

    it('should construct', function() {
      smt = new StreamMergeTree(mt, { log: silence });
    });
  });

  describe('stream', function() {
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

    it('create MergeTree', function(done) {
      var path = dbPath + '/stream';
      fs.mkdir(path, 0o755, function(err) {
        if (err) { throw err; }
        level(path, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, db) {
          if (err) { throw err; }
          mt = new MergeTree(db, { vSize: 3, log: silence });
          done();
        });
      });
    });

    it('should work with empty DAG', function(done) {
      var smt = new StreamMergeTree(mt, { log: silence });
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
      var smt = new StreamMergeTree(mt, { local: perspective, log: silence });
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
      var smt = new StreamMergeTree(mt, { local: perspective, log: silence, tail: true, tailRetry: 1 });
      var docs = [];

      smt.on('data', function(doc) {
        docs.push(doc);
      });

      var closeCalled;
      setTimeout(function() {
        smt.close();
        closeCalled = true;
      }, 50);

      smt.on('end', function() {
        should.equal(docs.length, 7);
        should.deepEqual(docs, [rA, rB, rC, rD, rE, rF, rG]);
        should.strictEqual(closeCalled, true);
        done();
      });
    });

    it('should reopen and return all elements again', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var smt = new StreamMergeTree(mt, { local: perspective, log: silence });
      var docs = [], docs2 = [];

      smt.on('data', function(doc) {
        docs.push(doc);
      });

      smt.on('end', function() {
        should.equal(docs.length, 7);
        should.deepEqual(docs, [rA, rB, rC, rD, rE, rF, rG]);

        var smt2 = smt.reopen();
        smt2.on('data', function(doc) {
          docs2.push(doc);
        });

        smt2.on('end', function() {
          should.equal(docs2.length, 7);
          should.deepEqual(docs2, [rA, rB, rC, rD, rE, rF, rG]);

          done();
        });
      });
    });

    it('should return bson buffer instances', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var smt = new StreamMergeTree(mt, {
        local: perspective,
        log: silence,
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
      var smt = new StreamMergeTree(mt, {
        local: perspective,
        log: silence,
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
      var smt = new StreamMergeTree(mt, {
        local: perspective,
        log: silence,
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
      var smt = new StreamMergeTree(mt, {
        local: perspective,
        log: silence,
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
      var smt = new StreamMergeTree(mt, {
        local: perspective,
        log: silence,
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
      var smt = new StreamMergeTree(mt, {
        local: perspective,
        log: silence,
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
      var smt = new StreamMergeTree(mt, {
        local: perspective,
        log: silence,
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
      var smt = new StreamMergeTree(mt, {
        local: perspective,
        log: silence,
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
      var smt = new StreamMergeTree(mt, {
        local: perspective,
        log: silence,
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

      var smt = new StreamMergeTree(mt, {
        local: perspective,
        log: silence,
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

      var smt = new StreamMergeTree(mt, {
        local: perspective,
        log: silence,
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

      var smt = new StreamMergeTree(mt, {
        local: perspective,
        log: silence,
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

      var smt = new StreamMergeTree(mt, {
        local: perspective,
        log: silence,
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

    it('should be a readable stream', function(done) {
      var smt = new StreamMergeTree(mt, { log: silence });
      should.strictEqual(smt instanceof Readable, true);
      done();
    });
  });
});
