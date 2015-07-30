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

var should = require('should');
var rimraf = require('rimraf');
var level = require('level');

var Tree = require('../../../lib/tree');
var logger = require('../../../lib/logger');

var db, cons, silence;
var dbPath = '/tmp/test_tree';

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
        db = level(dbPath, { keyEncoding: 'binary' });
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

describe('Tree', function() {
  describe('constructor', function() {
    var tree;
    it('should require db to be an object', function() {
      (function () { tree = new Tree(); }).should.throwError('db must be an object');
    });

    it('should require name to be a string', function() {
      (function() { tree = new Tree(db, {}); }).should.throw('name must be a string');
    });

    it('should require name to not contain 0x00', function() {
      (function() { tree = new Tree(db, 'foo\x00bar'); }).should.throw('name must not contain 0x00');
    });

    it('should require name to not contain 0xff', function() {
      (function() { tree = new Tree(db, 'foo\xffbar'); }).should.throw('name must not contain 0xff');
    });

    it('should require opts to be an object', function() {
      (function() { tree = new Tree(db, 'foo', []); }).should.throw('opts must be an object');
    });

    it('should require opts.vSize to be a number', function() {
      (function() { tree = new Tree(db, 'foo', { vSize: {} }); }).should.throw('opts.vSize must be a number');
    });

    it('should require opts.iSize to be a number', function() {
      (function() { tree = new Tree(db, 'foo', { iSize: {} }); }).should.throw('opts.iSize must be a number');
    });

    it('should require opts.vSize to be between 0 and 6', function() {
      (function() { tree = new Tree(db, 'foo', { vSize: 7 }); }).should.throw('opts.vSize must be between 0 and 6');
    });

    it('should require opts.iSize to be between 0 and 6', function() {
      (function() { tree = new Tree(db, 'foo', { iSize: 7 }); }).should.throw('opts.iSize must be between 0 and 6');
    });

    it('should construct', function() {
      (function() { tree = new Tree(db, 'foo'); }).should.not.throwError();
    });
  });

  describe('bton', function() {
    it('should require b to be a buffer', function() {
      (function() { Tree.bton({}); }).should.throw('b must be a buffer');
    });

    it('should convert a 6 byte LE Buffer to a number', function() {
      should.strictEqual(Tree.bton(new Buffer([9, 0, 0, 0, 0, 0])), 9);
    });
  });

  describe('ntob', function() {
    it('should require n to be a number', function() {
      (function() { Tree.ntob({}); }).should.throw('n must be a number');
    });

    it('should require size to be a number', function() {
      (function() { Tree.ntob(0, {}); }).should.throw('size must be a number');
    });

    it('should convert to a 6 byte LE Buffer', function() {
      should.strictEqual(Tree.ntob(9, 6).toString(), new Buffer([9, 0, 0, 0, 0, 0]).toString());
    });

    it('should not err if size > 6', function() {
      should.strictEqual(Tree.ntob(9, 7).toString(), new Buffer([9, 0, 0, 0, 0, 0, 0]).toString());
    });
  });

  describe('invalidItem', function() {
    it('should require item to be an object', function() {
      Tree.invalidItem([]).should.equal('item must be an object');
    });

    it('should require item._h to be an object', function() {
      Tree.invalidItem({ _h: [] }).should.equal('item._h must be an object');
    });

    it('should require item._b to be an object', function() {
      Tree.invalidItem({ _h: {}, _b: [] }).should.equal('item._b must be an object');
    });

    it('should require item._h.id to be defined', function() {
      Tree.invalidItem({ _h: { }, _b: {} }).should.equal('item._h.id must be a buffer, a string or implement "toString"');
    });

    it('should require item._h.id not to be undefined', function() {
      Tree.invalidItem({ _h: { id: undefined }, _b: {} }).should.equal('item._h.id must be a buffer, a string or implement "toString"');
    });

    it('should require item._h.id not to be null', function() {
      Tree.invalidItem({ _h: { id: null }, _b: {} }).should.equal('item._h.id must be a buffer, a string or implement "toString"');
    });

    it('should require item._h.v to be a string', function() {
      Tree.invalidItem({ _h: { id: 'foo', v: [] }, _b: {} }).should.equal('item._h.v must be a string');
    });

    it('should require item._h.pa to be an array', function() {
      Tree.invalidItem({
        _h: { id: 'foo', v: 'A', pa: {} },
        _b: {}
      }).should.equal('item._h.pa must be an array');
    });

    it('should require that only item._h and item._b exist', function() {
      Tree.invalidItem({
        _h: {},
        _b: {},
        _c: {}
      }).should.equal('item should only contain _h and _b keys');
    });

    it('should be a valid item', function() {
      Tree.invalidItem({ _h: { id: 'foo', v: 'A', pa: [] }, _b: {} }).should.equal('');
    });

    it('should accept array type for id', function() {
      Tree.invalidItem({ _h: { id: [], v: 'A', pa: [] }, _b: {} }).should.equal('');
    });

    it('should accept empty string for id', function() {
      Tree.invalidItem({ _h: { id: '', v: 'A', pa: [] }, _b: {} }).should.equal('');
    });

    it('should accept positive number type for id', function() {
      Tree.invalidItem({ _h: { id: 10 , v: 'A', pa: [] }, _b: {} }).should.equal('');
    });

    it('should accept 0 for id', function() {
      Tree.invalidItem({ _h: { id: 0 , v: 'A', pa: [] }, _b: {} }).should.equal('');
    });
  });

  describe('getRange', function() {
    it('should require prefix to be a buffer', function() {
      (function() { Tree.getRange([]); }).should.throw('prefix must be a buffer');
    });

    it('should require start to be a buffer if provided', function() {
      (function() { Tree.getRange(new Buffer(0), []); }).should.throw('start must be a buffer if provided');
    });

    it('should require end to be a buffer if provided', function() {
      (function() { Tree.getRange(new Buffer(0), new Buffer(0), []); }).should.throw('end must be a buffer if provided');
    });

    describe('single byte prefix, start and end', function() {
      it('should end start-prefix with a null byte and end end-prefix with 0xff', function() {
        var prefix = new Buffer([65]);
        var p = Tree.getRange(prefix);
        should.strictEqual(p.s.toString('hex'), '4100');
        should.strictEqual(p.e.toString('hex'), '41ff');
      });

      it('should not end start-prefix with a null byte and end end-prefix with 0xff if start is given', function() {
        var prefix = new Buffer([65]);
        var start  = new Buffer([66]);
        var p = Tree.getRange(prefix, start);
        should.strictEqual(p.s.toString('hex'), '410042');
        should.strictEqual(p.e.toString('hex'), '410042ff');
      });

      it('should not end start-prefix with a null byte and end end-prefix with 0xff if start and end are given', function() {
        var prefix = new Buffer([65]);
        var start  = new Buffer([66]);
        var end    = new Buffer([67]);
        var p = Tree.getRange(prefix, start, end);
        should.strictEqual(p.s.toString('hex'), '410042');
        should.strictEqual(p.e.toString('hex'), '4100420043ff');
      });
    });

    describe('multi byte prefix, start and end', function() {
      it('should end start-prefix with a null byte and end end-prefix with 0xff', function() {
        var prefix = new Buffer([65, 66]);
        var p = Tree.getRange(prefix);
        should.strictEqual(p.s.toString('hex'), '414200');
        should.strictEqual(p.e.toString('hex'), '4142ff');
      });

      it('should not end start-prefix with a null byte and end end-prefix with 0xff if start is given', function() {
        var prefix = new Buffer([65, 66]);
        var start  = new Buffer([67, 68, 69]);
        var p = Tree.getRange(prefix, start);
        should.strictEqual(p.s.toString('hex'), '414200434445');
        should.strictEqual(p.e.toString('hex'), '414200434445ff');
      });

      it('should not end start-prefix with a null byte and end end-prefix with 0xff if start and end are given', function() {
        var prefix = new Buffer([65, 66]);
        var start  = new Buffer([67, 68, 69]);
        var end    = new Buffer([70, 71, 72]);
        var p = Tree.getRange(prefix, start, end);
        should.strictEqual(p.s.toString('hex'), '414200434445');
        should.strictEqual(p.e.toString('hex'), '41420043444500464748ff');
      });
    });
  });

  describe('_getIFromIKey', function() {
    var name = '_getIFromIKey';

    it('should require b to be a buffer', function() {
      var t = new Tree(db, name, { log: silence });
      (function() { t._getIFromIKey({}); }).should.throw('b must be a buffer');
    });

    it('should remove the prefix "f00ba4" and read the last 6 bytes by default', function() {
      var t = new Tree(db, name, { log: cons });
      // contains the number 8 in the last 6 bytes LE
      var b = new Buffer('f00ba400080000000000', 'hex');
      t._getIFromIKey(b).should.equal(8);
    });

    it('should remove the prefix "f00ba4" and read the last 2 bytes', function() {
      var t = new Tree(db, name, { iSize: 2, log: cons });
      // contains the number 8 in the last 2 bytes LE
      var b = new Buffer('f00ba4000800', 'hex');
      t._getIFromIKey(b).should.equal(8);
    });
  });

  describe('_getIKey', function() {
    var name = '_getIKey';

    it('should require i to be a number', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { log: silence });
      (function() { t._getIKey({}); }).should.throw('i must be a number');
    });

    it('should prepend "idx/i0x00_getIKey0x00" as a prefix and use 6 bytes for i by default', function() {
      var t = new Tree(db, name, { log: cons });
      t._getIKey(8).toString('hex').should.equal('6964782f69005f676574494b657900080000000000');
    });

    it('should prepend "idx/i0x00_getIKey0x00" as a prefix and use 2 bytes for i', function() {
      var t = new Tree(db, name, { iSize: 2, log: cons });
      t._getIKey(8).toString('hex').should.equal('6964782f69005f676574494b6579000800');
    });
  });

  describe('_getHeadKey', function() {
    var name = '_getHeadKey';

    it('should require that the header matches the vSize', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { vSize: 2, log: silence });
      (function() { t._getHeadKey({ id: 'foo', v: 'YWJj' }); }).should.throw('header.v is too short');
    });

    it('should save a buffer type id directly and read v as base64', function() {
      var t = new Tree(db, name, { log: cons });
      // YWJjWFla == abcovXYZ
      t._getHeadKey({ id: new Buffer([1, 2, 4]), v: 'YWJjWFla' }).toString().should.equal('idx/heads\u0000_getHeadKey\u0000\u0001\u0002\u0004\u0000abcXYZ');
    });

    it('should transform a string type id into a buffer and pad base64 v to 3 bytes', function() {
      var t = new Tree(db, name, { vSize: 3 });
      t._getHeadKey({ id: 'foo', v: 'YWJj' }).toString().should.equal('idx/heads\u0000_getHeadKey\u0000foo\u0000abc');
    });

    it('should transform a function type id into a buffer and pad base64 v to 3 bytes', function() {
      var t = new Tree(db, name, { vSize: 3 });
      t._getHeadKey({ id: function() { return true; }, v: 'YWJj' }).toString().should.equal('idx/heads\u0000_getHeadKey\u0000function () { return true; }\u0000abc');
    });

    it('should transform a boolean type id into a buffer and pad base64 v to 3 bytes', function() {
      var t = new Tree(db, name, { vSize: 3 });
      t._getHeadKey({ id: true, v: 'YWJj' }).toString().should.equal('idx/heads\u0000_getHeadKey\u0000true\u0000abc');
    });
  });

  describe('_getDataStoreKey', function() {
    var name = '_getDataStoreKey';

    it('should save a buffer type id directly and pad i to 48 bits LE', function() {
      var t = new Tree(db, name);
      // prefix is data\u0000_getDataStoreKey\u0000 which is 64617461005f6765744461746153746f72654b657900
      t._getDataStoreKey({ id: new Buffer([1, 2, 4]), i: 257 }).toString('hex').should.equal('64617461005f6765744461746153746f72654b65790001020400010100000000');
    });

    it('should transform a string type id into a buffer and pad i to 48 bits LE', function() {
      var t = new Tree(db, name);
      // prefix is data\u0000_getDataStoreKey\u0000 which is 64617461005f6765744461746153746f72654b657900
      t._getDataStoreKey({ id: 'foo', i: 257 }).toString('hex').should.equal('64617461005f6765744461746153746f72654b657900666f6f00010100000000');
    });

    it('should transform a function type id into a buffer and pad i to 48 bits LE', function() {
      var t = new Tree(db, name);
      // prefix is data\u0000_getDataStoreKey\u0000 which is 64617461005f6765744461746153746f72654b657900
      t._getDataStoreKey({ id: function() { return true; }, i: 257 }).toString('hex').should.equal('64617461005f6765744461746153746f72654b65790066756e6374696f6e202829207b2072657475726e20747275653b207d00010100000000');
    });

    it('should transform a boolean type id into a buffer and pad i to 48 bits LE', function() {
      var t = new Tree(db, name);
      // prefix is data\u0000_getDataStoreKey\u0000 which is 64617461005f6765744461746153746f72654b657900
      t._getDataStoreKey({ id: true, i: 257 }).toString('hex').should.equal('64617461005f6765744461746153746f72654b6579007472756500010100000000');
    });
  });

  describe('_nextI', function() {
    var name = '_nextI';

    it('should return 1 by default', function(done) {
      var t = new Tree(db, name, { log: silence });
      t._nextI(function(err, i) {
        if (err) { throw err; }
        should.equal(i, 1);
        done();
      });
    });

    it('needs an item with a number in the index', function(done) {
      var t = new Tree(db, name, { log: silence });
      /*
      store an object in the I index:
      {                 i  d  x  /  i     _  n  e  x  t  I     2
        key:   <Buffer 69 64 78 2f 69 00 5f 6e 65 78 74 49 00 02 00 00 00 00 00>,
        value: 'A'
      }
      */
      t._db.put(t._getIKey(2), 'A', done);
    });

    it('should give an increased i', function(done) {
      var t = new Tree(db, name, { log: silence });
      t._nextI(function(err, i) {
        if (err) { throw err; }
        should.equal(i, 3);
        done();
      });
    });

    it('needs a new higher i in the index', function(done) {
      var t = new Tree(db, name, { log: silence });
      /*
      store an object in the I index:
      {                 i  d  x  /  i     _  n  e  x  t  I     2
        key:   <Buffer 69 64 78 2f 69 00 5f 6e 65 78 74 49 00 02 00 00 00 00 00>,
        value: 'A'
      }
      */
      t._db.put(t._getIKey(20), 'A', done);
    });

    it('should return the highest increment in the snapshot collection', function(done) {
      var vc = new Tree(db, name, { log: silence });
      vc._nextI(function(err, i) {
        if (err) { throw err; }
        should.equal(i, 21);
        done();
      });
    });

    it('should return an increment after each subsequent call', function(done) {
      var vc = new Tree(db, name, { log: silence });
      vc._nextI(function(err, i) {
        if (err) { throw err; }
        vc._nextI(function(err, j) {
          if (err) { throw err; }
          should.equal(i, 21);
          should.equal(j, 22);
          done();
        });
      });
    });
  });

  describe('_validParents', function() {
    var name = '_validParents';

    var item1 = { _h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] } };
    var item2 = { _h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['A'] } };
    var item3 = { _h: { id: 'XI', v: 'Cccc', i: 3, pa: ['B'] } };

    it('should accept roots in an empty database', function(done) {
      var t = new Tree(db, name, { log: silence });
      t._validParents(item1, function(err, valid) {
        if (err) { throw err; }
        should.strictEqual(valid, true);
        done();
      });
    });

    it('should not accept non-roots in an empty database', function(done) {
      var t = new Tree(db, name, { log: silence });
      t._validParents(item2, function(err, valid) {
        if (err) { throw err; }
        should.strictEqual(valid, false);
        done();
      });
    });

    it('needs an item in the database and head and i index', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: cons });
      t._dbData.put(t._getDataStoreKey(item1._h), item1, function(err) {
        if (err) { throw err; }
        t._idxI.put(Tree.ntob(item1._h.i), t._getHeadKey(item1._h), function(err) {
          if (err) { throw err; }
          t._idxHeads.put(t._getHeadKey(item1._h), item1._h.i, done);
        });
      });
    });

    it('should not accept roots in a non-empty database', function(done) {
      var t = new Tree(db, name, { log: cons });
      t._validParents(item1, function(err, valid) {
        if (err) { throw err; }
        should.strictEqual(valid, false);
        done();
      });
    });

    it('should accept non-roots in a non-empty database', function(done) {
      var t = new Tree(db, name, { log: cons });
      t._validParents(item2, function(err, valid) {
        if (err) { throw err; }
        should.strictEqual(valid, true);
        done();
      });
    });
  });
});
