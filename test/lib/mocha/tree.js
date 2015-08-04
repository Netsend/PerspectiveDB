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
var BSON = require('bson').BSONPure.BSON;

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

describe('Tree', function() {
  describe('constructor', function() {
    var tree;
    it('should require db to be an object', function() {
      (function () { tree = new Tree(); }).should.throwError('db must be an object');
    });

    it('should require name to be a string', function() {
      (function() { tree = new Tree(db, {}); }).should.throw('name must be a string');
    });

    it('should require name to not exceed 255 bytes', function() {
      var name = '';
      for (var i = 0; i < 254; i++) {
        name += 'a';
      }
      // trick it with the last character taking two bytes making the total byte length 256
      name += '\u00bd';
      (function() { tree = new Tree(db, name); }).should.throw('name must not exceed 255 bytes');
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

  describe('getPrefix', function() {
    var p;

    it('should require name to be a string', function() {
      (function() { p = Tree.getPrefix({}); }).should.throw('name must be a string');
    });

    it('should require name to not exceed 255 bytes', function() {
      var name = '';
      for (var i = 0; i < 254; i++) {
        name += 'a';
      }
      // trick it with the last character taking two bytes making the total byte length 256
      name += '\u00bd';
      (function() { p = Tree.getPrefix(name); }).should.throw('name must not exceed 255 bytes');
    });

    it('should require type to be a numner', function() {
      (function() { p = Tree.getPrefix(''); }).should.throw('type must be a number');
    });

    it('should require type to be >= 1', function() {
      (function() { p = Tree.getPrefix('', 0x00); }).should.throw('type must be in the subkey range of 1 to 3');
    });

    it('should require type to be <= 3', function() {
      (function() { p = Tree.getPrefix('', 0x04); }).should.throw('type must be in the subkey range of 1 to 3');
    });

    it('should return the right prefix with an empty name', function() {
      p = Tree.getPrefix('', 0x03);
      should.strictEqual(p.toString('hex'), new Buffer([0,0,3]).toString('hex'));
    });

    it('should return the right prefix with a non-empty name', function() {
      p = Tree.getPrefix('abc', 0x02);
      should.strictEqual(p.toString('hex'), new Buffer([3,97,98,99,0,2]).toString('hex'));
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
      var t = new Tree(db, name, { log: silence });
      // contains the number 8 in the last 6 bytes LE
      var b = new Buffer('f00ba400080000000000', 'hex');
      t._getIFromIKey(b).should.equal(8);
    });

    it('should remove the prefix "f00ba4" and read the last 2 bytes', function() {
      var t = new Tree(db, name, { iSize: 2, log: silence });
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

    it('should prepend ikey prefix to i and use 6 bytes for i by default', function() {
      var t = new Tree(db, name, { log: silence });
      t._getIKey(8).toString('hex').should.equal('085f676574494b6579000206000000000008');
    });

    it('should prepend ikey prefix to i and use 2 bytes for i', function() {
      var t = new Tree(db, name, { iSize: 2, log: silence });
      t._getIKey(8).toString('hex').should.equal('085f676574494b65790002020008');
    });
  });

  describe('_getHeadKey', function() {
    var name = '_getHeadKey';

    it('should require id to be (convertiable to) a string', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { vSize: 2, log: silence });
      (function() { t._getHeadKey(null, 'YWJj'); }).should.throw('Cannot read property \'toString\' of null');
    });

    it('should require that v matches the vSize', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { vSize: 2, log: silence });
      (function() { t._getHeadKey('foo', 'YWJj'); }).should.throw('v is too short or too long');
    });

    it('should transform a string type id into a buffer and pad base64 v to 3 bytes', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._getHeadKey('foo', 'YWJj').toString('hex').should.equal('0b5f676574486561644b6579000303666f6f0003616263');
    });

    it('should transform a function type id into a buffer and pad base64 v to 3 bytes', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._getHeadKey(function() { return true; }, 'YWJj').toString('hex').should.equal('0b5f676574486561644b657900031c66756e6374696f6e202829207b2072657475726e20747275653b207d0003616263');
    });

    it('should transform a boolean type id into a buffer and pad base64 v to 3 bytes', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._getHeadKey(true, 'YWJj').toString('hex').should.equal('0b5f676574486561644b6579000304747275650003616263');
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
        value: <Buffer 0f 00 00 00 02 5f 62 00 02 00 00 00 41 00 00> // BSON { _b: 'A' }
      }
      */
      t._db.put(t._getIKey(2), BSON.serialize({ _b: 'A' }), done);
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
        value: <Buffer 0f 00 00 00 02 5f 62 00 02 00 00 00 41 00 00> // BSON { _b: 'A' }
      }
      */
      t._db.put(t._getIKey(20), BSON.serialize({ _b: 'A' }), done);
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

    // use 24-bit version numbers (base 64)
    var item1 = { _h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] } };
    var item2 = { _h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] } };

    it('should accept roots in an empty database', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._validParents(item1, function(err, valid) {
        if (err) { throw err; }
        should.strictEqual(valid, true);
        done();
      });
    });

    it('should not accept non-roots in an empty database', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._validParents(item2, function(err, valid) {
        if (err) { throw err; }
        should.strictEqual(valid, false);
        done();
      });
    });

    it('needs an item in the database and head- and i index', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._db.put(t._getDataStoreKey(item1._h), BSON.serialize(item1), function(err) {
        if (err) { throw err; }
        t._db.put(t._getIKey(item1._h.i), t._getHeadKey(item1._h), function(err) {
          if (err) { throw err; }
          t._db.put(t._getHeadKey(item1._h), t._getIKey(item1._h.i), done);
        });
      });
    });

    it('should not accept roots in a non-empty database', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._validParents(item1, function(err, valid) {
        if (err) { throw err; }
        should.strictEqual(valid, false);
        done();
      });
    });

    it('should accept connecting non-roots in a non-empty database', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._validParents(item2, function(err, valid) {
        if (err) { throw err; }
        should.strictEqual(valid, true);
        done();
      });
    });
  });

  describe('_write', function() {
    var name = '_write';

    // use 24-bit version numbers (base 64)
    var item1 = { _h: { id: 'XI', v: 'Aaaa', pa: [] }, _b: { some: 'body' } };
    var item2 = { _h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'] }, _b: { more: 'body' } };
    var item3 = { _h: { id: 'XI', v: 'Cccc', pa: ['Aaaa'] }, _b: { more2: 'body' } };

    it('should not accept a non-root in an empty database', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.on('error', function(err) {
        should.strictEqual(err.message, 'item is not connected to the DAG');
        done();
      });
      t.write(item2);
    });

    it('should accept new root and follow up in an empty database and increment i', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.once('data', function(obj) {
        should.strictEqual(obj._h.i, 1);
        t.once('data', function(obj) {
          should.strictEqual(obj._h.i, 2);
          done();
        });
      });
      t.write(item1);
      t.write(item2);
    });

    it('should have created an id + version index and an id + i index', function(done) {
      var s = db.createReadStream();
      s.on('data', function(obj) {
        console.log(obj);
        done();
      });
    });

    it('should accept an existing root item without incrementing i', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: cons });
      t.once('data', function(obj) {
        should.strictEqual(obj._h.i, 1);
        done();
      });
      t.write(item1);
    });

    it('should accept an existing item without incrementing i', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.once('data', function(obj) {
        should.strictEqual(obj._h.i, 2);
        done();
      });
      t.write(item2);
    });

    it('should accept new item that forks', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.once('data', function(obj) {
        should.strictEqual(obj._h.i, 3);
        done();
      });
      t.write(item3);
    });
  });
});
