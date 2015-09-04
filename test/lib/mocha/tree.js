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
var async = require('async');
var level = require('level');
var bson = require('bson');
var BSON = new bson.BSONPure.BSON();

var Tree = require('../../../lib/tree');
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

describe('Tree', function() {
  describe('verify ordering of used database', function() {
    var bytes = [
      [ 0x00 ],
      [ 0x00, 0x00 ],
      [ 0x00, 0x01 ],
      [ 0x00, 0xfe ],
      [ 0x00, 0xff ],
      [ 0x01 ],
      [ 0x01, 0x00 ],
      [ 0x01, 0x01 ],
      [ 0x01, 0xfe ],
      [ 0x01, 0xff ],
      [ 0xfe ],
      [ 0xfe, 0x00 ],
      [ 0xfe, 0x01 ],
      [ 0xfe, 0xfe ],
      [ 0xfe, 0xff ],
      [ 0xff ],
      [ 0xff, 0x00 ],
      [ 0xff, 0x01 ],
      [ 0xff, 0xfe ],
      [ 0xff, 0xff ]
    ];

    it('insert bytes in parallel', function(done) {
      // do a parallel insert
      bytes.reverse();
      async.each(bytes, function(b, cb) {
        db.put(new Buffer(b), null, cb);
      }, done);
      bytes.reverse();
    });

    it('should sort increasing, "0x00" after "", >= ""', function(done) {
      var i = 0;
      var it = db.createKeyStream({ gte: new Buffer([]) });
      it.on('data', function(key) {
        should.strictEqual(new Buffer(bytes[i]).toString('hex'), key.toString('hex'));
        i++;
      });

      it.on('end', function() {
        should.strictEqual(bytes.length, i);
        done();
      });
    });

    it('should sort increasing, "0x00" after "", > ""', function(done) {
      var i = 0;
      var it = db.createKeyStream({ gt: new Buffer([]) });
      it.on('data', function(key) {
        should.strictEqual(new Buffer(bytes[i]).toString('hex'), key.toString('hex'));
        i++;
      });

      it.on('end', function() {
        should.strictEqual(bytes.length, i);
        done();
      });
    });

    it('should sort increasing, "0x00" after "", >= 0x00', function(done) {
      var i = 0;
      var it = db.createKeyStream({ gte: new Buffer([0x00]) });
      it.on('data', function(key) {
        should.strictEqual(new Buffer(bytes[i]).toString('hex'), key.toString('hex'));
        i++;
      });

      it.on('end', function() {
        should.strictEqual(bytes.length, i);
        done();
      });
    });

    it('should sort increasing, "0x00" after "", > 0x00', function(done) {
      var i = 1;
      var it = db.createKeyStream({ gt: new Buffer([0x00]) });
      it.on('data', function(key) {
        should.strictEqual(new Buffer(bytes[i]).toString('hex'), key.toString('hex'));
        i++;
      });

      it.on('end', function() {
        should.strictEqual(bytes.length, i);
        done();
      });
    });

    it('should sort decreasing, "" before "0xff", <= 0xff 0xff 0xff', function(done) {
      var i = 0;
      var it = db.createKeyStream({ lte: new Buffer([0xff, 0xff, 0xff]) });
      it.on('data', function(key) {
        should.strictEqual(new Buffer(bytes[i]).toString('hex'), key.toString('hex'));
        i++;
      });

      it.on('end', function() {
        should.strictEqual(bytes.length, i);
        done();
      });
    });

    it('should sort decreasing, "" before "0xff", < 0xff 0xff 0xff', function(done) {
      var i = 0;
      var it = db.createKeyStream({ lt: new Buffer([0xff, 0xff, 0xff]) });
      it.on('data', function(key) {
        should.strictEqual(new Buffer(bytes[i]).toString('hex'), key.toString('hex'));
        i++;
      });

      it.on('end', function() {
        should.strictEqual(bytes.length, i);
        done();
      });
    });

    it('should sort decreasing, <= 0xff 0xff', function(done) {
      var i = 0;
      var it = db.createKeyStream({ lte: new Buffer([0xff, 0xff]) });
      it.on('data', function(key) {
        should.strictEqual(new Buffer(bytes[i]).toString('hex'), key.toString('hex'));
        i++;
      });

      it.on('end', function() {
        should.strictEqual(bytes.length, i);
        done();
      });
    });

    it('should sort decreasing, < 0xff 0xff', function(done) {
      var i = 0;
      var it = db.createKeyStream({ lt: new Buffer([0xff, 0xff]) });
      it.on('data', function(key) {
        should.strictEqual(new Buffer(bytes[i]).toString('hex'), key.toString('hex'));
        i++;
      });

      it.on('end', function() {
        should.strictEqual(bytes.length - 1, i);
        done();
      });
    });
  });

  describe('constructor', function() {
    var tree;
    it('should require db to be an object', function() {
      (function () { tree = new Tree(); }).should.throwError('db must be an object');
    });

    it('should require name to be a string', function() {
      (function() { tree = new Tree(db, {}); }).should.throw('name must be a string');
    });

    it('should require name to not exceed 254 bytes', function() {
      var name = '';
      for (var i = 0; i < 254; i++) {
        name += 'a';
      }
      // trick it with the last character taking two bytes making the total byte length 255
      name += '\u00bd';
      (function() { tree = new Tree(db, name); }).should.throw('name must not exceed 254 bytes');
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

    it('should construct and set name public', function() {
      tree = new Tree(db, 'foo');
      should.strictEqual(tree.name, 'foo');
    });
  });

  describe('getPrefix', function() {
    var p;

    it('should require name to be a string', function() {
      (function() { p = Tree.getPrefix({}); }).should.throw('name must be a string');
    });

    it('should require name to not exceed 254 bytes', function() {
      var name = '';
      for (var i = 0; i < 254; i++) {
        name += 'a';
      }
      // trick it with the last character taking two bytes making the total byte length 255
      name += '\u00bd';
      (function() { p = Tree.getPrefix(name); }).should.throw('name must not exceed 254 bytes');
    });

    it('should require type to be a numner', function() {
      (function() { p = Tree.getPrefix(''); }).should.throw('type must be a number');
    });

    it('should require type to be >= 0x01', function() {
      (function() { p = Tree.getPrefix('', 0x00); }).should.throw('type must be in the subkey range of 0x01 to 0x05');
    });

    it('should require type to be <= 0x05', function() {
      (function() { p = Tree.getPrefix('', 0x06); }).should.throw('type must be in the subkey range of 0x01 to 0x05');
    });

    it('should return the right prefix with an empty name', function() {
      p = Tree.getPrefix('', 0x04);
      should.strictEqual(p.toString('hex'), new Buffer([0,0,4]).toString('hex'));
    });

    it('should return the right prefix with a non-empty name', function() {
      p = Tree.getPrefix('abc', 0x02);
      should.strictEqual(p.toString('hex'), new Buffer([3,97,98,99,0,2]).toString('hex'));
    });
  });

  describe('parseKey', function() {
    it('should require key to be a buffer', function() {
      (function() { Tree.parseKey({}); }).should.throw('key must be a buffer');
    });

    it('should require subkey to be above 0x00', function() {
      var b = new Buffer('00000000', 'hex');
      (function() { Tree.parseKey(b); }).should.throw('key is of an unknown type');
    });

    it('should require subkey to be <= 0x05', function() {
      var b = new Buffer('00000600', 'hex');
      (function() { Tree.parseKey(b); }).should.throw('key is of an unknown type');
    });

    it('should err if name is smaller than specified length', function() {
      var b = new Buffer('026100010100', 'hex');
      (function() { Tree.parseKey(b); }).should.throw('expected a null byte after name');
    });

    it('should err if name is bigger than specified length', function() {
      var b = new Buffer('01618100010100', 'hex');
      (function() { Tree.parseKey(b); }).should.throw('expected a null byte after name');
    });

    describe('dskey', function() {
      it('should err if i length is zero', function() {
        var b = new Buffer('000001000000', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('i must be at least one byte');
      });

      it('should err if i is bigger than specified length', function() {
        var b = new Buffer('0000010000010000', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('expected no bytes after i');
      });

      it('should err if i is smaller than specified length (1)', function() {
        var b = new Buffer('000001000001', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('index out of range');
      });

      it('should err if i is smaller than specified length (2)', function() {
        var b = new Buffer('00000100000200', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('index out of range');
      });

      describe('i 1,', function() {
        it('name 0, id 0', function() {
          var b = new Buffer('00000100000100', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer(0),
            type: 0x01,
            id: new Buffer(0),
            i: 0,
          });
        });

        it('name 1, id 0', function() {
          var b = new Buffer('0161000100000100', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer([97]),
            type: 0x01,
            id: new Buffer(0),
            i: 0,
          });
        });

        it('name 0, id 1', function() {
          var b = new Buffer('0000010160000100', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer(0),
            type: 0x01,
            id: new Buffer([96]),
            i: 0,
          });
        });

        it('name 1, id 1', function() {
          var b = new Buffer('016000010159000100', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer([96]),
            type: 0x01,
            id: new Buffer([89]),
            i: 0,
          });
        });

      });

      describe('i 3,', function() {
        it('name 0, id 0', function() {
          var b = new Buffer('000001000003235761', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer(0),
            type: 0x01,
            id: new Buffer(0),
            i: 0x235761
          });
        });

        it('name 3, id 0', function() {
          var b = new Buffer('032357610001000003235761', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer([35, 87, 97]),
            type: 0x01,
            id: new Buffer(0),
            i: 0x235761
          });
        });

        it('name 0, id 3', function() {
          var b = new Buffer('000001032357600003235761', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer(0),
            type: 0x01,
            id: new Buffer([35, 87, 96]),
            i: 0x235761
          });
        });

        it('name 3, id 3', function() {
          var b = new Buffer('032357600001032357590003235761', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer([35, 87, 96]),
            type: 0x01,
            id: new Buffer([35, 87, 89]),
            i: 0x235761
          });
        });
      });

      it('should decode id to "hex" string', function() {
        var b = new Buffer('0000030144000100', 'hex');
        var obj = Tree.parseKey(b, { decodeId: 'hex' });
        should.deepEqual(obj, {
          name: new Buffer([]),
          type: 0x03,
          id: '44',
          v: 0
        });
      });

      it('should decode id to "base64" string', function() {
        var b = new Buffer('0000030144000100', 'hex');
        var obj = Tree.parseKey(b, { decodeId: 'base64' });
        should.deepEqual(obj, {
          name: new Buffer([]),
          type: 0x03,
          id: 'RA==',
          v: 0
        });
      });
    });

    describe('ikey', function() {
      it('should err if i length is zero', function() {
        var b = new Buffer('00000200', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('i must be at least one byte');
      });

      it('should err if i is bigger than specified length', function() {
        var b = new Buffer('000002010000', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('expected no bytes after i');
      });

      it('should err if i is smaller than specified length (1)', function() {
        var b = new Buffer('00000201', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('index out of range');
      });

      it('should err if i is smaller than specified length (2)', function() {
        var b = new Buffer('0000020200', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('index out of range');
      });

      describe('i 1,', function() {
        it('name 0', function() {
          var b = new Buffer('0000020100', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer(0),
            type: 0x02,
            i: 0,
          });
        });

        it('name 1', function() {
          var b = new Buffer('016100020100', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer([97]),
            type: 0x02,
            i: 0,
          });
        });
      });

      describe('i 3,', function() {
        it('name 0', function() {
          var b = new Buffer('00000203235761', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer(0),
            type: 0x02,
            i: 0x235761
          });
        });

        it('name 3', function() {
          var b = new Buffer('03235761000203235761', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer([35, 87, 97]),
            type: 0x02,
            i: 0x235761
          });
        });
      });
    });

    describe('headkey', function() {
      it('should err if v length is zero', function() {
        var b = new Buffer('000003000000', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('v must be at least one byte');
      });

      it('should err if v is bigger than specified length', function() {
        var b = new Buffer('0000030000010000', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('expected no bytes after v');
      });

      it('should err if v is smaller than specified length (1)', function() {
        var b = new Buffer('000003000001', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('index out of range');
      });

      it('should err if v is smaller than specified length (2)', function() {
        var b = new Buffer('00000300000200', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('index out of range');
      });

      describe('v 1,', function() {
        it('name 0, id 0', function() {
          var b = new Buffer('00000300000100', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer(0),
            type: 0x03,
            id: new Buffer(0),
            v: 0,
          });
        });

        it('name 1, id 0', function() {
          var b = new Buffer('0161000300000100', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer([97]),
            type: 0x03,
            id: new Buffer(0),
            v: 0,
          });
        });

        it('name 0, id 1', function() {
          var b = new Buffer('0000030160000100', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer(0),
            type: 0x03,
            id: new Buffer([96]),
            v: 0,
          });
        });

        it('name 1, id 1', function() {
          var b = new Buffer('016000030159000100', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer([96]),
            type: 0x03,
            id: new Buffer([89]),
            v: 0,
          });
        });
      });

      describe('v 3,', function() {
        it('name 0, id 0', function() {
          var b = new Buffer('000003000003235761', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer(0),
            type: 0x03,
            id: new Buffer(0),
            v: 0x235761
          });
        });

        it('name 3, id 0', function() {
          var b = new Buffer('032357610003000003235761', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer([35, 87, 97]),
            type: 0x03,
            id: new Buffer(0),
            v: 0x235761
          });
        });

        it('name 0, id 3', function() {
          var b = new Buffer('000003032357600003235761', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer(0),
            type: 0x03,
            id: new Buffer([35, 87, 96]),
            v: 0x235761
          });
        });

        it('name 3, id 3', function() {
          var b = new Buffer('032357600003032357590003235761', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer([35, 87, 96]),
            type: 0x03,
            id: new Buffer([35, 87, 89]),
            v: 0x235761
          });
        });
      });
    });

    describe('vkey', function() {
      it('should err if v length is zero', function() {
        var b = new Buffer('00000400', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('v must be at least one byte');
      });

      it('should err if v is bigger than specified length', function() {
        var b = new Buffer('000004010000', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('expected no bytes after v');
      });

      it('should err if v is smaller than specified length (1)', function() {
        var b = new Buffer('00000401', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('index out of range');
      });

      it('should err if v is smaller than specified length (2)', function() {
        var b = new Buffer('0000040200', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('index out of range');
      });

      describe('v 1,', function() {
        it('name 0', function() {
          var b = new Buffer('0000040100', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer(0),
            type: 0x04,
            v: 0,
          });
        });

        it('name 1', function() {
          var b = new Buffer('016100040100', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer([97]),
            type: 0x04,
            v: 0,
          });
        });
      });

      describe('v 3,', function() {
        it('name 0', function() {
          var b = new Buffer('00000403235761', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer(0),
            type: 0x04,
            v: 0x235761
          });
        });

        it('name 3', function() {
          var b = new Buffer('03235761000403235761', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer([35, 87, 97]),
            type: 0x04,
            v: 0x235761
          });
        });
      });
    });

    describe('pekey', function() {
      it('should err if pe length is zero', function() {
        var b = new Buffer('000005000000', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('pe must be at least one byte');
      });

      it('should err if i is bigger than specified length', function() {
        var b = new Buffer('000005010000010000', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('expected no bytes after i');
      });

      it('should err if i is smaller than specified length (1)', function() {
        var b = new Buffer('00000501000001', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('index out of range');
      });

      it('should err if i is smaller than specified length (2)', function() {
        var b = new Buffer('0000050100000200', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('index out of range');
      });

      describe('i 1,', function() {
        it('name 0, pe 1', function() {
          var b = new Buffer('0000050160000100', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer(0),
            type: 0x05,
            pe: new Buffer([96]),
            i: 0,
          });
        });

        it('name 1, pe 1', function() {
          var b = new Buffer('016000050159000100', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer([96]),
            type: 0x05,
            pe: new Buffer([89]),
            i: 0,
          });
        });

      });

      describe('i 3,', function() {
        it('name 0, pe 3', function() {
          var b = new Buffer('000005032357600003235761', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer(0),
            type: 0x05,
            pe: new Buffer([35, 87, 96]),
            i: 0x235761
          });
        });

        it('name 3, pe 3', function() {
          var b = new Buffer('032357600005032357590003235761', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer([35, 87, 96]),
            type: 0x05,
            pe: new Buffer([35, 87, 89]),
            i: 0x235761
          });
        });
      });

      it('should decode pe to "hex" string', function() {
        var b = new Buffer('0000050144000100', 'hex');
        var obj = Tree.parseKey(b, { decodePe: 'hex' });
        should.deepEqual(obj, {
          name: new Buffer([]),
          type: 0x05,
          pe: '44',
          i: 0
        });
      });

      it('should decode pe to "base64" string', function() {
        var b = new Buffer('0000050144000100', 'hex');
        var obj = Tree.parseKey(b, { decodePe: 'base64' });
        should.deepEqual(obj, {
          name: new Buffer([]),
          type: 0x05,
          pe: 'RA==',
          i: 0
        });
      });
    });

    it('should decode v to "hex" string', function() {
      var b = new Buffer('00000403235761', 'hex');
      var obj = Tree.parseKey(b, { decodeV: 'hex' });
      should.deepEqual(obj, {
        name: new Buffer([]),
        type: 0x04,
        v: '235761'
      });
    });

    it('should decode v to "base64" string', function() {
      var b = new Buffer('00000403235761', 'hex');
      var obj = Tree.parseKey(b, { decodeV: 'base64' });
      should.deepEqual(obj, {
        name: new Buffer([]),
        type: 0x04,
        v: 'I1dh'
      });
    });
  });

  describe('parseHeadVal', function() {
    it('should err if value is not a buffer', function() {
      (function() { Tree.parseHeadVal(''); }).should.throw('value must be a buffer');
    });

    it('should err if value does not contain i', function() {
      var b = new Buffer('00', 'hex');
      (function() { Tree.parseHeadVal(b); }).should.throw('i must be at least one byte');
    });

    it('should err if i length is zero', function() {
      var b = new Buffer('0000', 'hex');
      (function() { Tree.parseHeadVal(b); }).should.throw('i must be at least one byte');
    });

    it('should err if i is bigger than specified length', function() {
      var b = new Buffer('0002000000', 'hex');
      (function() { Tree.parseHeadVal(b); }).should.throw('unexpected length of value');
    });

    it('should err if i is smaller than specified length', function() {
      var b = new Buffer('000200', 'hex');
      (function() { Tree.parseHeadVal(b); }).should.throw('index out of range');
    });

    describe('i 1,', function() {
      it('conflict 0', function() {
        var b = new Buffer('000102', 'hex');
        var obj = Tree.parseHeadVal(b);
        should.deepEqual(obj, {
          i: 2
        });
      });

      it('conflict 1', function() {
        var b = new Buffer('010102', 'hex');
        var obj = Tree.parseHeadVal(b);
        should.deepEqual(obj, {
          i: 2,
          c: true
        });
      });
    });

    describe('i 3,', function() {
      it('conflict 0', function() {
        var b = new Buffer('0003235761', 'hex');
        var obj = Tree.parseHeadVal(b);
        should.deepEqual(obj, {
          i: 0x235761
        });
      });

      it('conflict 1', function() {
        var b = new Buffer('0103235761', 'hex');
        var obj = Tree.parseHeadVal(b);
        should.deepEqual(obj, {
          i: 0x235761,
          c: true
        });
      });
    });
  });

  describe('parsePeVal', function() {
    it('should err if value is not a buffer', function() {
      (function() { Tree.parsePeVal(''); }).should.throw('value must be a buffer');
    });

    it('should err if v length is zero', function() {
      var b = new Buffer('00', 'hex');
      (function() { Tree.parsePeVal(b); }).should.throw('v must be at least one byte');
    });

    it('should err if v is bigger than specified length', function() {
      var b = new Buffer('02000000', 'hex');
      (function() { Tree.parsePeVal(b); }).should.throw('unexpected length of value');
    });

    it('should err if v is smaller than specified length', function() {
      var b = new Buffer('0200', 'hex');
      (function() { Tree.parsePeVal(b); }).should.throw('index out of range');
    });

    it('v 1', function() {
      var b = new Buffer('0102', 'hex');
      var obj = Tree.parsePeVal(b);
      should.deepEqual(obj, {
        v: 2
      });
    });

    it('v 3', function() {
      var b = new Buffer('03235761', 'hex');
      var obj = Tree.parsePeVal(b);
      should.deepEqual(obj, {
        v: 0x235761
      });
    });

    it('v 3 decode hex', function() {
      var b = new Buffer('03235761', 'hex');
      var obj = Tree.parsePeVal(b, 'base64');
      should.deepEqual(obj, {
        v: 'I1dh'
      });
    });
  });

  describe('getIKeyRange', function() {
    it('should work with a zero byte name and default iSize = 6', function() {
      var t = new Tree(db, '');
      var p = t.getIKeyRange();
      should.strictEqual(p.s.toString('hex'), '000002');
      should.strictEqual(p.e.toString('hex'), '00000207ffffffffffffff');
    });

    it('should use iSize + 1 for 0xff end sequence', function() {
      var t = new Tree(db, '', { iSize: 1 });
      var p = t.getIKeyRange();
      should.strictEqual(p.s.toString('hex'), '000002');
      should.strictEqual(p.e.toString('hex'), '00000202ffff');
    });

    it('should work with a single byte name', function() {
      var t = new Tree(db, 'a', { iSize: 1 });
      var p = t.getIKeyRange();
      should.strictEqual(p.s.toString('hex'), '01610002');
      should.strictEqual(p.e.toString('hex'), '0161000202ffff');
    });

    it('should work with a multi byte name', function() {
      var t = new Tree(db, 'abc', { iSize: 1 });
      var p = t.getIKeyRange();
      should.strictEqual(p.s.toString('hex'), '036162630002');
      should.strictEqual(p.e.toString('hex'), '03616263000202ffff');
    });

    it('should require opts to be an object', function() {
      var t = new Tree(db, 'abc', { iSize: 1 });
      (function() { t.getIKeyRange(null); }).should.throw('opts must be an object');
    });

    it('should work with minI 3', function() {
      var t = new Tree(db, 'abc', { iSize: 1 });
      var p = t.getIKeyRange({ minI: 3 });
      should.strictEqual(p.s.toString('hex'), '0361626300020103');
      should.strictEqual(p.e.toString('hex'), '03616263000202ffff');
    });

    it('should work with maxI 4', function() {
      var t = new Tree(db, 'abc', { iSize: 1 });
      var p = t.getIKeyRange({ maxI: 4 });
      should.strictEqual(p.s.toString('hex'), '036162630002');
      should.strictEqual(p.e.toString('hex'), '0361626300020204ff');
    });

    it('should work with minI 5 and maxI 2', function() {
      var t = new Tree(db, 'abc', { iSize: 1 });
      var p = t.getIKeyRange({ minI: 5, maxI: 2 });
      should.strictEqual(p.s.toString('hex'), '0361626300020105');
      should.strictEqual(p.e.toString('hex'), '0361626300020202ff');
    });
  });

  describe('getVKeyRange', function() {
    it('should work with a zero byte name and default vSize = 6', function() {
      var t = new Tree(db, '');
      var p = t.getVKeyRange();
      should.strictEqual(p.s.toString('hex'), '000004');
      should.strictEqual(p.e.toString('hex'), '00000407ffffffffffffff');
    });

    it('should use vSize + 1 for 0xff end sequence', function() {
      var t = new Tree(db, '', { vSize: 1 });
      var p = t.getVKeyRange();
      should.strictEqual(p.s.toString('hex'), '000004');
      should.strictEqual(p.e.toString('hex'), '00000402ffff');
    });

    it('should work with a single byte name', function() {
      var t = new Tree(db, 'a', { vSize: 1 });
      var p = t.getVKeyRange();
      should.strictEqual(p.s.toString('hex'), '01610004');
      should.strictEqual(p.e.toString('hex'), '0161000402ffff');
    });

    it('should work with a multi byte name', function() {
      var t = new Tree(db, 'abc', { vSize: 1 });
      var p = t.getVKeyRange();
      should.strictEqual(p.s.toString('hex'), '036162630004');
      should.strictEqual(p.e.toString('hex'), '03616263000402ffff');
    });
  });

  describe('getHeadKeyRange', function() {
    describe('no id', function() {
      it('should work with a zero byte name', function() {
        var t = new Tree(db, '');
        var p = t.getHeadKeyRange();
        should.strictEqual(p.s.toString('hex'), '000003');
        should.strictEqual(p.e.toString('hex'), '000003ff');
      });

      it('should work with a single byte name', function() {
        var t = new Tree(db, 'a');
        var p = t.getHeadKeyRange();
        should.strictEqual(p.s.toString('hex'), '01610003');
        should.strictEqual(p.e.toString('hex'), '01610003ff');
      });

      it('should work with a multi byte name', function() {
        var t = new Tree(db, 'abc');
        var p = t.getHeadKeyRange();
        should.strictEqual(p.s.toString('hex'), '036162630003');
        should.strictEqual(p.e.toString('hex'), '036162630003ff');
      });
    });

    describe('with id', function() {
      it('should require id to be a buffer', function() {
        var t = new Tree(db, '');
        (function() { t.getHeadKeyRange([]); }).should.throw('id must be a buffer if provided');
      });

      it('should work with a zero byte name', function() {
        var t = new Tree(db, '');
        var p = t.getHeadKeyRange(new Buffer('cb', 'hex'));
        should.strictEqual(p.s.toString('hex'), '00000301cb');
        should.strictEqual(p.e.toString('hex'), '00000301cbff');
      });

      it('should work with a single byte name', function() {
        var t = new Tree(db, 'a');
        var p = t.getHeadKeyRange(new Buffer('cb', 'hex'));
        should.strictEqual(p.s.toString('hex'), '0161000301cb');
        should.strictEqual(p.e.toString('hex'), '0161000301cbff');
      });

      it('should work with a multi byte name', function() {
        var t = new Tree(db, 'abc');
        var p = t.getHeadKeyRange(new Buffer('cb', 'hex'));
        should.strictEqual(p.s.toString('hex'), '03616263000301cb');
        should.strictEqual(p.e.toString('hex'), '03616263000301cbff');
      });
    });
  });

  describe('getDsKeyRange', function() {
    describe('no id', function() {
      it('should work with a zero byte name', function() {
        var t = new Tree(db, '');
        var p = t.getDsKeyRange();
        should.strictEqual(p.s.toString('hex'), '000001');
        should.strictEqual(p.e.toString('hex'), '000001ff');
      });

      it('should work with a single byte name', function() {
        var t = new Tree(db, 'a');
        var p = t.getDsKeyRange();
        should.strictEqual(p.s.toString('hex'), '01610001');
        should.strictEqual(p.e.toString('hex'), '01610001ff');
      });

      it('should work with a multi byte name', function() {
        var t = new Tree(db, 'abc');
        var p = t.getDsKeyRange();
        should.strictEqual(p.s.toString('hex'), '036162630001');
        should.strictEqual(p.e.toString('hex'), '036162630001ff');
      });
    });

    describe('with id', function() {
      it('should require id to be a buffer', function() {
        var t = new Tree(db, '');
        (function() { t.getDsKeyRange([]); }).should.throw('id must be a buffer if provided');
      });

      it('should work with a zero byte name', function() {
        var t = new Tree(db, '');
        var p = t.getDsKeyRange(new Buffer('cb', 'hex'));
        should.strictEqual(p.s.toString('hex'), '00000101cb');
        should.strictEqual(p.e.toString('hex'), '00000101cbff');
      });

      it('should work with a single byte name', function() {
        var t = new Tree(db, 'a');
        var p = t.getDsKeyRange(new Buffer('cb', 'hex'));
        should.strictEqual(p.s.toString('hex'), '0161000101cb');
        should.strictEqual(p.e.toString('hex'), '0161000101cbff');
      });

      it('should work with a multi byte name', function() {
        var t = new Tree(db, 'abc');
        var p = t.getDsKeyRange(new Buffer('cb', 'hex'));
        should.strictEqual(p.s.toString('hex'), '03616263000101cb');
        should.strictEqual(p.e.toString('hex'), '03616263000101cbff');
      });
    });
  });

  describe('getHeadVersions', function() {
    // simple test, test further after write tests are done
    var name = 'getHeadVersions';

    var item1 = { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } };
    var item3 = { h: { id: 'XI', v: 'Dddd', i: 3, pa: ['Aaaa'] }, b: { some: 'other' } };
    var item4 = { h: { id: 'XI', v: 'Ffff', i: 4, pa: ['Bbbb'] }, b: { some: 'more2' } };

    it('should require id to be a buffer', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.getHeadVersions({}); }).should.throw('id must be a buffer');
    });

    it('should require cb to be a function', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.getHeadVersions(new Buffer(0)); }).should.throw('cb must be a function');
    });

    it('should call cb directly with an empty database', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.getHeadVersions(new Buffer(0), done);
    });

    it('needs item1', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.end(item1, done);
    });

    it('should return item1 version, the only head', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.getHeadVersions(new Buffer('XI'), function(err, versions) {
        should.deepEqual([item1.h.v], versions);
        done();
      });
    });

    it('needs item2, ff', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.end(item2, done);
    });

    it('should return item2 version, the only head', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.getHeadVersions(new Buffer('XI'), function(err, versions) {
        should.deepEqual([item2.h.v], versions);
        done();
      });
    });

    it('needs item3, fork', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.end(item3, done);
    });

    it('should return item2 and item3 versions, the only heads', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.getHeadVersions(new Buffer('XI'), function(err, versions) {
        should.deepEqual([item2.h.v, item3.h.v], versions);
        done();
      });
    });

    it('needs item4, ff', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.end(item4, done);
    });

    it('should iterate over item3 and item4, the only heads', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.getHeadVersions(new Buffer('XI'), function(err, versions) {
        should.deepEqual([item3.h.v, item4.h.v], versions);
        done();
      });
    });
  });

  describe('getHeads', function() {
    // simple test, test further after write tests are done
    var name = 'getHeads';

    var item1 = { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } };
    var item3 = { h: { id: 'XI', v: 'Dddd', i: 3, pa: ['Aaaa'] }, b: { some: 'other' } };
    var item4 = { h: { id: 'XI', v: 'Ffff', i: 4, pa: ['Bbbb'], c: true }, b: { some: 'more2' } };

    it('should require opts to be an object', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.getHeads(0); }).should.throw('opts must be an object');
    });

    it('should require opts.id to be a buffer', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.getHeads({ id: {} }); }).should.throw('opts.id must be a buffer');
    });

    it('should require opts.skipConflicts to be a boolean', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.getHeads({ skipConflicts: {} }); }).should.throw('opts.skipConflicts must be a boolean');
    });

    it('should require iterator to be a function', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.getHeads(); }).should.throw('iterator must be a function');
    });

    it('should require cb to be a function', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.getHeads(function() {}); }).should.throw('cb must be a function');
    });

    it('should call cb directly with an empty database', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.getHeads(function() {}, done);
    });

    it('needs item1', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.end(item1, done);
    });

    it('should iterate over item1, the only head', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      t.getHeads(function(item, next) {
        i++;
        if (i > 0) { should.deepEqual(item, item1); }
        next();
      }, function() {
        should.strictEqual(i, 1);
        done();
      });
    });

    it('needs item2, ff', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.end(item2, done);
    });

    it('should iterate over item2, the only head', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      t.getHeads(function(item, next) {
        i++;
        if (i > 0) { should.deepEqual(item, item2); }
        next();
      }, function() {
        should.strictEqual(i, 1);
        done();
      });
    });

    it('needs item3, fork', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.end(item3, done);
    });

    it('should iterate over item2 and item3, the only heads', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      t.getHeads(function(item, next) {
        i++;
        if (i === 1) { should.deepEqual(item, item2); }
        if (i > 1) { should.deepEqual(item, item3); }
        next();
      }, function() {
        should.strictEqual(i, 2);
        done();
      });
    });

    it('needs item4, ff', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.end(item4, done);
    });

    it('should iterate over item3 and item4, the only heads', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      t.getHeads(function(item, next) {
        i++;
        if (i === 1) { should.deepEqual(item, item3); }
        if (i > 1) { should.deepEqual(item, item4); }
        next();
      }, function() {
        should.strictEqual(i, 2);
        done();
      });
    });

    it('should stop iterating if next is called with false or an error', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      t.getHeads(function(item, next) {
        i++;
        if (i === 1) { should.deepEqual(item, item3); }
        if (i > 1) { should.deepEqual(item, item4); }
        next(false);
      }, function() {
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should not emit conflicting heads if skipConflicts is true', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      t.getHeads({ skipConflicts: true }, function(item, next) {
        i++;
        if (i > 0) { should.deepEqual(item, item3); }
        next();
      }, function() {
        should.strictEqual(i, 1);
        done();
      });
    });
  });

  describe('getDsKeyByVersion', function() {
    var name = 'getDsKeyByVersion';

    var item = { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] } };

    it('should require version to be a number or a base64 string', function() {
      var t = new Tree(db, name, { log: silence });
      (function() { t.getDsKeyByVersion({}); }).should.throw('version must be a number or a base64 string');
    });

    it('should return with null if the index is empty', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.getDsKeyByVersion('Aaaa', function(err, dsKey) {
        if (err) { throw err; }
        should.strictEqual(dsKey, null);
        done();
      });
    });

    it('needs an entry in vkey index', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var vKey = t._composeVKey(item.h.v);
      var dsKey = t._composeDsKey(item.h.id, item.h.i);

      t._db.put(vKey, dsKey, done);
    });

    it('should find the version', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.getDsKeyByVersion('Aaaa', function(err, dsKey) {
        if (err) { throw err; }
        should.strictEqual(dsKey.toString('hex'), t._composeDsKey(item.h.id, item.h.i).toString('hex'));
        done();
      });
    });

    it('should return null if version is not found', function(done) {
      var t = new Tree(db, name, { vSize: 2, log: silence });
      t.getDsKeyByVersion(8, function(err, dsKey) {
        if (err) { throw err; }
        should.strictEqual(dsKey, null);
        done();
      });
    });
  });

  describe('getByVersion', function() {
    var name = 'getByVersion';

    var item = { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] } };

    it('should require version to be a number or a base64 string', function() {
      var t = new Tree(db, name, { log: silence });
      (function() { t.getByVersion({}); }).should.throw('version must be a number or a base64 string');
    });

    it('should return with null if the index is empty', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.getByVersion('Aaaa', function(err, found) {
        if (err) { throw err; }
        should.strictEqual(found, null);
        done();
      });
    });

    it('needs an entry in vkey index', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var vKey = t._composeVKey(item.h.v);
      var dsKey = t._composeDsKey(item.h.id, item.h.i);

      t._db.put(vKey, dsKey, done);
    });

    it('should err if version in index but item not in dskey index', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.getByVersion('Aaaa', function(err) {
        console.log(err);
        err.message.match('NotFoundError: Key not found in database');
        done();
      });
    });

    it('needs an entry in dskey index', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var dsKey = t._composeDsKey(item.h.id, item.h.i);

      t._db.put(dsKey, BSON.serialize(item), done);
    });

    it('should find the item by version', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.getByVersion('Aaaa', function(err, found) {
        if (err) { throw err; }
        should.deepEqual(BSON.deserialize(found), item);
        done();
      });
    });

    it('should return null if version is not found', function(done) {
      var t = new Tree(db, name, { vSize: 2, log: silence });
      t.getByVersion(8, function(err, found) {
        if (err) { throw err; }
        should.strictEqual(found, null);
        done();
      });
    });
  });

  describe('iterateInsertionOrder', function() {
    var name = 'iterateInsertionOrder';

    var item1 = { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } };
    var item3 = { h: { id: 'XI', v: 'Dddd', i: 3, pa: ['Aaaa'] }, b: { some: 'other' } };

    it('should require iterator to be a function', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.iterateInsertionOrder(); }).should.throw('iterator must be a function');
    });

    it('should require cb to be a function', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.iterateInsertionOrder(function() {}); }).should.throw('cb must be a function');
    });

    it('should require opts.i to be a number', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.iterateInsertionOrder({ i: 'a' }, function() { }, function() { }); }).should.throw('opts.i must be a number');
    });

    it('should require opts.v to match vSize', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.iterateInsertionOrder({ v: 'a' }, function() { }, function() { }); }).should.throw('opts.v must be the same size as the configured vSize');
    });

    it('should require raw to be a buffer', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.iterateInsertionOrder({ raw: 'a' }, function() { }, function() { }); }).should.throw('opts.raw must be a boolean');
    });

    it('should call cb directly with an empty database', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.iterateInsertionOrder(function() {}, done);
    });

    it('needs item1', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.end(item1, done);
    });

    it('should emit raw buffers', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.iterateInsertionOrder({ raw: true }, function(obj, next) {
        i++;
        should.strictEqual(Buffer.isBuffer(obj), true);
        next();
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should iterate over item1', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.iterateInsertionOrder(function(obj, next) {
        i++;
        should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
        next();
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should emit the existing item but no new items that are added while the iterator is running', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.iterateInsertionOrder(function(obj, next) {
        i++;
        if (i > 0) {
          should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
        }
        t.write(item2, next);
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should wait with close before all items are iterated', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.iterateInsertionOrder(function(obj, next) {
        i++;
        setTimeout(next, 10);
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 2);
        done();
      });
    });

    it('should stop iterating and propagate the error that next is called with', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      var error = new Error('custom error');
      t.iterateInsertionOrder(function(obj, next) {
        i++;
        setTimeout(function() {
          next(error); 
        }, 10);
      }, function(err) {
        should.strictEqual(err.message, 'custom error');
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should stop iterating if next is called with a falsy value as second parameter', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.iterateInsertionOrder(function(obj, next) {
        i++;
        setTimeout(function() {
          next(null, false); 
        }, 10);
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should keep iterating if next is called with a truthy value as second parameter', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.iterateInsertionOrder(function(obj, next) {
        i++;
        setTimeout(function() {
          next(null, true); 
        }, 10);
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 2);
        done();
      });
    });

    it('should use opts.i and start emitting at offset 1', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.iterateInsertionOrder({ i: 1 }, function(obj, next) {
        i++;
        if (i === 1) {
          should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
        }

        if (i > 1) {
          should.deepEqual({ h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } }, obj);
        }
        next();
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 2);
        done();
      });
    });

    it('should use opts.i and start emitting at offset 2 (exludeOffset)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.iterateInsertionOrder({ i: 1, excludeOffset: true }, function(obj, next) {
        i++;
        if (i > 0) {
          should.deepEqual({ h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } }, obj);
        }
        next();
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should use opts.i and start emitting at offset 2', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.iterateInsertionOrder({ i: 2 }, function(obj, next) {
        i++;
        if (i > 0) {
          should.deepEqual({ h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } }, obj);
        }
        next();
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should use opts.v and start emitting at offset 2', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.iterateInsertionOrder({ v: 'Bbbb' }, function(obj, next) {
        i++;
        if (i > 0) {
          should.deepEqual({ h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } }, obj);
        }
        next();
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should emit the existing item but no new items that are added while the iterator is running the first time of many', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.iterateInsertionOrder(function(obj, next) {
        i++;
        if (i === 1) {
          should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
          t.write(item3, next);
        }
        if (i > 1) {
          should.deepEqual({ h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } }, obj);
          next();
        }
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 2);
        done();
      });
    });
  });

  describe('lastByPerspective', function() {
    var name = 'lastByPerspective';

    var item1 = { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [], pe: 'lbp' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'], pe: 'lbp' } };
    var item3 = { h: { id: 'XI', v: 'Cccc', i: 2, pa: ['Bbbb'], pe: 'lbp2' } };
    var item4 = { h: { id: 'XI', v: 'Dddd', i: 2, pa: ['Aaaa'], pe: 'lbp' } };

    it('should require pe to be a buffer or a string', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.lastByPerspective(function() {}); }).should.throw('pe must be a buffer or a string');
    });

    it('should return null with empty databases', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.lastByPerspective('lbp', function(err, v) {
        if (err) { throw err; }
        should.equal(v, null);
        done();
      });
    });

    it('needs item1', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.end(item1, done);
    });

    it('should return version of item1', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.lastByPerspective('lbp', function(err, v) {
        if (err) { throw err; }
        should.equal(v, 108186); // 108186 dec = Aaaa base64
        done();
      });
    });

    it('should return version of item1 base64', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.lastByPerspective('lbp', 'base64', function(err, v) {
        if (err) { throw err; }
        should.equal(v, 'Aaaa');
        done();
      });
    });

    it('should not return version of item1 if other pe is searched for', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.lastByPerspective('lbp2', function(err, v) {
        if (err) { throw err; }
        should.equal(v, null);
        done();
      });
    });

    it('needs item2 and item3 (lbp and lbp2)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.write(item2);
      t.end(item3, done);
    });

    it('should return version of item2 (lbp)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.lastByPerspective('lbp', 'base64', function(err, v) {
        if (err) { throw err; }
        should.equal(v, 'Bbbb');
        done();
      });
    });

    it('should return version of item3 (lbp2)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.lastByPerspective('lbp2', 'base64', function(err, v) {
        if (err) { throw err; }
        should.equal(v, 'Cccc');
        done();
      });
    });

    it('needs item4 (lbp)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.end(item4, done);
    });

    it('should return version of item4 (lbp)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.lastByPerspective('lbp', 'base64', function(err, v) {
        if (err) { throw err; }
        should.equal(v, 'Dddd');
        done();
      });
    });

    it('should return version of item3 (lbp2) unchanged', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.lastByPerspective('lbp2', 'base64', function(err, v) {
        if (err) { throw err; }
        should.equal(v, 'Cccc');
        done();
      });
    });
  });

  describe('maxI', function() {
    var name = 'maxI';

    it('should return null by default', function(done) {
      var t = new Tree(db, name, { log: silence });
      t.maxI(function(err, i) {
        if (err) { throw err; }
        should.equal(i, null);
        done();
      });
    });

    it('needs an item with a number in the index', function(done) {
      var t = new Tree(db, name, { log: silence });
      /*
      store an object in the I index:
      {                    _  n  e  x  t  I                          2
        key:   <Buffer 06 5f 6e 65 78 74 49 00 02 06 00 00 00 00 00 02>,
        value: <Buffer 0f 00 00 00 02 5f 62 00 02 00 00 00 41 00 00> // BSON { b: 'A' }
      }
      */
      t._db.put(t._composeIKey(2), BSON.serialize({ b: 'A' }), done);
    });

    it('should give the new i', function(done) {
      var t = new Tree(db, name, { log: silence });
      t.maxI(function(err, i) {
        if (err) { throw err; }
        should.equal(i, 2);
        done();
      });
    });

    it('needs a new higher i in the index', function(done) {
      var t = new Tree(db, name, { log: silence });
      /*
      store an object in the I index:
      {                    _  n  e  x  t  I                         20
        key:   <Buffer 06 5f 6e 65 78 74 49 00 02 06 00 00 00 00 00 14>,
        value: <Buffer 0f 00 00 00 02 5f 62 00 02 00 00 00 41 00 00> // BSON { b: 'A' }
      }
      */
      t._db.put(t._composeIKey(20), BSON.serialize({ b: 'A' }), done);
    });

    it('should return the new i which is 20', function(done) {
      var vc = new Tree(db, name, { log: silence });
      vc.maxI(function(err, i) {
        if (err) { throw err; }
        should.equal(i, 20);
        done();
      });
    });
  });

  describe('_ensureString', function() {
    it('should return the string itself if input type is already a string', function() {
      should.strictEqual(Tree._ensureString('a'), 'a');
    });

    it('should try to call toString if input type is not a string', function() {
      should.strictEqual(Tree._ensureString(9), '9');
    });

    it('should throw an error if not convertible to string', function() {
      (function() { Tree._ensureString(null); }).should.throw('Cannot read property \'toString\' of null');
    });
  });

  describe('_toBuffer', function() {
    it('should accept strings', function() {
      var b = Tree._toBuffer('a');
      should.strictEqual(Buffer.isBuffer(b), true);
      should.strictEqual(b.toString('hex'), '61');
    });

    it('should accept numbers', function() {
      var b = Tree._toBuffer(9);
      should.strictEqual(Buffer.isBuffer(b), true);
      should.strictEqual(b.toString('hex'), '09');
    });

    it('should accept an array', function() {
      var b = Tree._toBuffer([9, 8]);
      should.strictEqual(Buffer.isBuffer(b), true);
      should.strictEqual(b.toString('hex'), '0908');
    });

    it('should return the original buffer', function() {
      var a = new Buffer('ab');
      var b = Tree._toBuffer(a);
      should.strictEqual(Buffer.isBuffer(b), true);
      should.strictEqual(a, b);
    });

    it('should try to call toString if input type is something different', function() {
      var b = Tree._toBuffer(function() { return 1; });
      should.strictEqual(Buffer.isBuffer(b), true);
      should.strictEqual(b.toString('utf8'), 'function () { return 1; }');
    });

    it('should throw an error on "undefined" values', function() {
      (function() { Tree._ensureString(undefined); }).should.throw('Cannot read property \'toString\' of undefined');
    });

    it('should throw an error on "null" values', function() {
      (function() { Tree._ensureString(null); }).should.throw('Cannot read property \'toString\' of null');
    });
  });

  describe('_composeIKey', function() {
    var name = '_composeIKey';

    it('should require i to be a number', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { log: silence });
      (function() { t._composeIKey({}); }).should.throw('i must be a number');
    });

    it('should prepend ikey prefix to i and use 6 bytes for i by default', function() {
      var t = new Tree(db, name, { log: silence });
      t._composeIKey(8).toString('hex').should.equal('0c5f636f6d706f7365494b6579000206000000000008');
    });

    it('should prepend ikey prefix to i and use 2 bytes for i', function() {
      var t = new Tree(db, name, { iSize: 2, log: silence });
      t._composeIKey(8).toString('hex').should.equal('0c5f636f6d706f7365494b65790002020008');
    });
  });

  describe('_composeVKey', function() {
    var name = '_composeVKey';

    it('should require v to be a number or a base64 string', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { log: silence });
      (function() { t._composeVKey({}); }).should.throw('v must be a number or a base64 string');
    });

    it('should prepend vkey prefix to v and use 6 bytes for v by default', function() {
      var t = new Tree(db, name, { log: silence });
      t._composeVKey(8).toString('hex').should.equal('0c5f636f6d706f7365564b6579000406000000000008');
    });

    it('should prepend vkey prefix to v and use 2 bytes for v', function() {
      var t = new Tree(db, name, { vSize: 2, log: silence });
      t._composeVKey(8).toString('hex').should.equal('0c5f636f6d706f7365564b65790004020008');
    });

    it('should require provided base64 version to match vSize', function() {
      var t = new Tree(db, name, { vSize: 2, log: silence });
      (function() { t._composeVKey('YWJj'); }).should.throw('v must be the same size as the configured vSize');
    });

    it('should accept base64 version', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._composeVKey('YWJj').toString('hex').should.equal('0c5f636f6d706f7365564b6579000403616263');
    });
  });

  describe('_composeHeadKey', function() {
    var name = '_composeHeadKey';

    it('should require id to be (convertiable to) a string', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { vSize: 2, log: silence });
      (function() { t._composeHeadKey(null, 'YWJj'); }).should.throw('Cannot read property \'toString\' of null');
    });

    it('should require that v matches the vSize', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { vSize: 2, log: silence });
      (function() { t._composeHeadKey('foo', 'YWJj'); }).should.throw('v is too short or too long');
    });

    it('should transform a string type id into a buffer and pad base64 v to 3 bytes', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._composeHeadKey('foo', 'YWJj').toString('hex').should.equal('0f5f636f6d706f7365486561644b6579000303666f6f0003616263');
    });

    it('should transform a function type id into a buffer and pad base64 v to 3 bytes', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._composeHeadKey(function() { return true; }, 'YWJj').toString('hex').should.equal('0f5f636f6d706f7365486561644b657900031c66756e6374696f6e202829207b2072657475726e20747275653b207d0003616263');
    });

    it('should transform a boolean type id into a buffer and pad base64 v to 3 bytes', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._composeHeadKey(true, 'YWJj').toString('hex').should.equal('0f5f636f6d706f7365486561644b6579000304747275650003616263');
    });
  });

  describe('_composeHeadVal', function() {
    var name = '_composeHeadVal';

    it('should require item to be an object', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { iSize: 2, log: silence });
      (function() { t._composeHeadVal(null); }).should.throw('item must be an object');
    });

    it('should require item.h.i to exist', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { iSize: 2, log: silence });
      (function() { t._composeHeadVal({ foo: 'bar' }); }).should.throw('item.h.i must be a number');
    });

    it('should set option byte to 0 and i to 4', function() {
      var t = new Tree(db, name, { iSize: 3, log: silence });
      t._composeHeadVal({ h: { i: 4 } }).toString('hex').should.equal('0003000004');
    });

    it('should set conflict option byte and i to 4', function() {
      var t = new Tree(db, name, { iSize: 3, log: silence });
      t._composeHeadVal({ h: { i: 4, c: true } }).toString('hex').should.equal('0103000004');
    });
  });

  describe('_composePeKey', function() {
    var name = '_';

    it('should require pe to be a string', function() {
      var t = new Tree(db, name, { iSize: 2, log: silence });
      (function() { t._composePeKey(null, 1); }).should.throw('pe must be a string');
    });

    it('should require i to be a number', function() {
      var t = new Tree(db, name, { iSize: 2, log: silence });
      (function() { t._composePeKey('foo', {}); }).should.throw('i must be a number');
    });

    it('should transform a string type pe into a buffer and pad i to a lbeint', function() {
      var t = new Tree(db, name, { iSize: 2, log: silence });
      t._composePeKey('foo', 257).toString('hex').should.equal('015f000503666f6f00020101');
    });

    it('should accept buffer type pe', function() {
      var t = new Tree(db, name, { iSize: 2, log: silence });
      t._composePeKey(new Buffer('true'), 257).toString('hex').should.equal('015f0005047472756500020101');
    });
  });

  describe('_composePeVal', function() {
    var name = '_composePeVal';

    it('should require that v matches the vSize', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { vSize: 2, log: silence });
      (function() { t._composePeVal('YWJj'); }).should.throw('v is too short or too long');
    });

    it('should set version', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._composePeVal('YWJj').toString('hex').should.equal('03616263');
    });
  });

  describe('getPeKeyRange', function() {
    describe('no pe', function() {
      it('should work with a zero byte name', function() {
        var t = new Tree(db, '');
        var p = t.getPeKeyRange();
        should.strictEqual(p.s.toString('hex'), '000005');
        should.strictEqual(p.e.toString('hex'), '000005ff');
      });

      it('should work with a single byte name', function() {
        var t = new Tree(db, 'a');
        var p = t.getPeKeyRange();
        should.strictEqual(p.s.toString('hex'), '01610005');
        should.strictEqual(p.e.toString('hex'), '01610005ff');
      });

      it('should work with a multi byte name', function() {
        var t = new Tree(db, 'abc');
        var p = t.getPeKeyRange();
        should.strictEqual(p.s.toString('hex'), '036162630005');
        should.strictEqual(p.e.toString('hex'), '036162630005ff');
      });
    });

    describe('with pe', function() {
      it('should require pe to be a buffer', function() {
        var t = new Tree(db, '');
        (function() { t.getPeKeyRange([]); }).should.throw('pe must be a buffer if provided');
      });

      it('should work with a zero byte name', function() {
        var t = new Tree(db, '');
        var p = t.getPeKeyRange(new Buffer('cb', 'hex'));
        should.strictEqual(p.s.toString('hex'), '00000501cb');
        should.strictEqual(p.e.toString('hex'), '00000501cbff');
      });

      it('should work with a single byte name', function() {
        var t = new Tree(db, 'a');
        var p = t.getPeKeyRange(new Buffer('cb', 'hex'));
        should.strictEqual(p.s.toString('hex'), '0161000501cb');
        should.strictEqual(p.e.toString('hex'), '0161000501cbff');
      });

      it('should work with a multi byte name', function() {
        var t = new Tree(db, 'abc');
        var p = t.getPeKeyRange(new Buffer('cb', 'hex'));
        should.strictEqual(p.s.toString('hex'), '03616263000501cb');
        should.strictEqual(p.e.toString('hex'), '03616263000501cbff');
      });
    });
  });

  describe('_composeDsKey', function() {
    var name = '_composeDsKey';

    it('should require id to be (convertiable to) a string', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { vSize: 2, log: silence });
      (function() { t._composeDsKey(null, 1); }).should.throw('Cannot read property \'toString\' of null');
    });

    it('should require i to be a number', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { vSize: 2, log: silence });
      (function() { t._composeDsKey('foo', {}); }).should.throw('i must be a number');
    });

    it('should transform a string type id into a buffer and pad i to a lbeint', function() {
      var t = new Tree(db, name, { log: silence });
      t._composeDsKey('foo', 257).toString('hex').should.equal('0d5f636f6d706f736544734b6579000103666f6f0006000000000101');
    });

    it('should transform a function type id into a buffer and pad i to a lbeint', function() {
      var t = new Tree(db, name, { log: silence });
      t._composeDsKey(function() { return true; }, 257).toString('hex').should.equal('0d5f636f6d706f736544734b657900011c66756e6374696f6e202829207b2072657475726e20747275653b207d0006000000000101');
    });

    it('should transform a boolean type id into a buffer and pad i to a lbeint', function() {
      var t = new Tree(db, name);
      // prefix is data\u0000_composeDsKey\u0000 which is 64617461005f6765744461746153746f72654b657900
      t._composeDsKey(true, 257).toString('hex').should.equal('0d5f636f6d706f736544734b6579000104747275650006000000000101');
    });

    it('should accept buffer type id', function() {
      var t = new Tree(db, name);
      // prefix is data\u0000_composeDsKey\u0000 which is 64617461005f6765744461746153746f72654b657900
      t._composeDsKey(new Buffer('true'), 257).toString('hex').should.equal('0d5f636f6d706f736544734b6579000104747275650006000000000101');
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
      {                    _  n  e  x  t  I                          2
        key:   <Buffer 06 5f 6e 65 78 74 49 00 02 06 00 00 00 00 00 02>,
        value: <Buffer 0f 00 00 00 02 5f 62 00 02 00 00 00 41 00 00> // BSON { b: 'A' }
      }
      */
      t._db.put(t._composeIKey(2), BSON.serialize({ b: 'A' }), done);
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
      {                    _  n  e  x  t  I                         20
        key:   <Buffer 06 5f 6e 65 78 74 49 00 02 06 00 00 00 00 00 14>,
        value: <Buffer 0f 00 00 00 02 5f 62 00 02 00 00 00 41 00 00> // BSON { b: 'A' }
      }
      */
      t._db.put(t._composeIKey(20), BSON.serialize({ b: 'A' }), done);
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

  describe('_isConnected', function() {
    var name = '_isConnected';

    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] } };
    var item3 = { h: { id: 'XI', v: 'Cccc', i: 2, pa: ['Bbbb'] } };
    var item4 = { h: { id: 'XI', v: 'Dddd', i: 2, pa: ['Aaaa'] } };

    var itemA = { h: { id: 'XI', v: 'Xxxx', i: 1, pa: [] } };

    it('should accept roots in an empty database', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._isConnected(item1, function(err, isConnected, exists) {
        if (err) { throw err; }
        should.strictEqual(isConnected, true);
        should.strictEqual(exists, false);
        done();
      });
    });

    it('should not accept non-roots in an empty database', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._isConnected(item2, function(err, isConnected, exists) {
        if (err) { throw err; }
        should.strictEqual(isConnected, false);
        should.strictEqual(exists, false);
        done();
      });
    });

    it('needs item1 in dskey, ikey, headkey and vkey', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var dsKey = t._composeDsKey(item1.h.id, item1.h.i);
      var headKey = t._composeHeadKey(item1.h.id, item1.h.v);
      var headVal = t._composeHeadVal(item1);
      var iKey = t._composeIKey(item1.h.i);
      var vKey = t._composeVKey(item1.h.v);

      t._db.put(dsKey, BSON.serialize(item1), function(err) {
        if (err) { throw err; }
        t._db.put(iKey, headKey, function(err) {
          if (err) { throw err; }
          t._db.put(headKey, headVal, function(err) {
            if (err) { throw err; }
            t._db.put(vKey, dsKey, done);
          });
        });
      });
    });

    it('should not accept a new root in a non-empty database', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._isConnected(itemA, function(err, isConnected, exists) {
        if (err) { throw err; }
        should.strictEqual(isConnected, false);
        should.strictEqual(exists, false);
        done();
      });
    });

    it('should accept an existing root in a non-empty database', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._isConnected(item1, function(err, isConnected, exists) {
        if (err) { throw err; }
        should.strictEqual(isConnected, true);
        should.strictEqual(exists, true);
        done();
      });
    });

    it('should accept new connecting non-roots in a non-empty database (fast-forward)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._isConnected(item2, function(err, isConnected, exists) {
        if (err) { throw err; }
        should.strictEqual(isConnected, true);
        should.strictEqual(exists, false);
        done();
      });
    });

    it('should accept new connecting non-roots in a non-empty database (fork)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._isConnected(item4, function(err, isConnected, exists) {
        if (err) { throw err; }
        should.strictEqual(isConnected, true);
        should.strictEqual(exists, false);
        done();
      });
    });

    it('should not accept non-connecting non-roots in a non-empty database', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._isConnected(item3, function(err, isConnected, exists) {
        if (err) { throw err; }
        should.strictEqual(isConnected, false);
        should.strictEqual(exists, false);
        done();
      });
    });

    it('needs item2 in dskey, ikey, headkey and vkey', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var dsKey = t._composeDsKey(item2.h.id, item2.h.i);
      var headKey = t._composeHeadKey(item2.h.id, item2.h.v);
      var iKey = t._composeIKey(item2.h.i);
      var vKey = t._composeVKey(item2.h.v);

      t._db.put(dsKey, BSON.serialize(item2), function(err) {
        if (err) { throw err; }
        t._db.put(iKey, headKey, function(err) {
          if (err) { throw err; }
          t._db.put(headKey, iKey, function(err) {
            if (err) { throw err; }
            t._db.put(vKey, dsKey, done);
          });
        });
      });
    });

    it('should accept existing connecting non-roots in a non-empty database', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._isConnected(item2, function(err, isConnected, exists) {
        if (err) { throw err; }
        should.strictEqual(isConnected, true);
        should.strictEqual(exists, true);
        done();
      });
    });
  });

  describe('_write', function() {
    var name = '_write';

    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa', pa: [] }, b: { some: 'body' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'] }, b: { more: 'body' } };
    var item3 = { h: { id: 'XI', v: 'Cccc', pa: ['Aaaa'] }, b: { more2: 'body' } };
    var item4 = { h: { id: 'XI', v: 'Dddd', pa: ['Cccc'] }, b: { more3: 'b' } };

    it('should not accept a non-root in an empty database', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.on('error', function(err) {
        should.strictEqual(err.message, 'item is not a new child');
        done();
      });
      t.write(item2);
    });

    it('should accept a new root and a new child in an empty database and increment i twice', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      t.write(item1);
      t.write(item2);
      t.end(function(err) {
        if (err) { throw err; }
        t.iterateInsertionOrder(function(obj, next) {
          i++;
          should.strictEqual(obj.h.i, i);
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 2);
          done();
        });
      });
    });

    it('inspect keys: should have created two dskeys with incremented i values', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getDsKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = Tree.parseKey(obj.key, { decodeId: 'utf8' });
        var val = BSON.deserialize(obj.value);

        if (i === 1) {
          should.strictEqual(key.type, 0x01);
          should.strictEqual(key.id, 'XI');
          should.strictEqual(key.i, 1);
          should.deepEqual(val, { h: { id: 'XI', v: 'Aaaa', pa: [], i: 1 }, b: { some: 'body' } });
        }
        if (i === 2) {
          should.strictEqual(key.type, 0x01);
          should.strictEqual(key.id, 'XI');
          should.strictEqual(key.i, 2);
          should.deepEqual(val, { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], i: 2 }, b: { more: 'body' } });
        }
      });

      s.on('end', function() {
        should.strictEqual(i, 2);
        done();
      });
    });

    it('inspect keys: should have one head key with the latest version and corresponding head value', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getHeadKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = Tree.parseKey(obj.key, { decodeId: 'utf8', decodeV: 'base64' });
        var val = Tree.parseHeadVal(obj.value);

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
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getIKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = Tree.parseKey(obj.key, { decodeId: 'utf8' });
        var val = Tree.parseKey(obj.value, { decodeId: 'utf8', decodeV: 'base64' });

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
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getVKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = Tree.parseKey(obj.key, { decodeId: 'utf8', decodeV: 'base64' });
        var val = Tree.parseKey(obj.value, { decodeId: 'utf8' });

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

    it('inspect keys: should have created two pekeys with the tree name as perspective and version as value', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getPeKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = Tree.parseKey(obj.key, { decodePe: 'utf8' });
        var val = Tree.parsePeVal(obj.value, 'base64');

        if (i === 1) {
          should.strictEqual(key.type, 0x05);
          should.strictEqual(key.pe, name);
          should.strictEqual(key.i, 1);
          should.deepEqual(val.v, 'Aaaa');
        }
        if (i === 2) {
          should.strictEqual(key.type, 0x05);
          should.strictEqual(key.pe, name);
          should.strictEqual(key.i, 2);
          should.deepEqual(val.v, 'Bbbb');
        }
      });

      s.on('end', function() {
        should.strictEqual(i, 2);
        done();
      });
    });

    it('should not accept an existing root', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.on('error', function(err) {
        should.strictEqual(err.message, 'item is not a new child');
        done();
      });
      t.end(item1);
      // expect that streams do not emit a finish after an error occurred
      t.on('finish', done);
    });

    it('should not accept an existing non-root', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.on('error', function(err) {
        should.strictEqual(err.message, 'item is not a new child');
        done();
      });
      t.end(item2);
      // expect that streams do not emit a finish after an error occurred
      t.on('finish', done);
    });

    it('should accept new item (fork)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.write(item3);
      var i = 0;
      t.end(function(err) {
        if (err) { throw err; }
        t.iterateInsertionOrder(function(obj, next) {
          i++;
          should.strictEqual(obj.h.i, i);
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 3);
          done();
        });
      });
    });

    it('should accept new item (fast-forward)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.write(item4);
      var i = 0;
      t.end(function(err) {
        if (err) { throw err; }
        t.iterateInsertionOrder(function(obj, next) {
          i++;
          should.strictEqual(obj.h.i, i);
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 4);
          done();
        });
      });
    });

    it('inspect keys: should have created four dskeys with incremented i values', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getDsKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = Tree.parseKey(obj.key, { decodeId: 'utf8' });
        var val = BSON.deserialize(obj.value);

        if (i === 1) {
          should.strictEqual(key.type, 0x01);
          should.strictEqual(key.id, 'XI');
          should.strictEqual(key.i, 1);
          should.deepEqual(val, { h: { id: 'XI', v: 'Aaaa', pa: [], i: 1 }, b: { some: 'body' } });
        }
        if (i === 2) {
          should.strictEqual(key.type, 0x01);
          should.strictEqual(key.id, 'XI');
          should.strictEqual(key.i, 2);
          should.deepEqual(val, { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], i: 2 }, b: { more: 'body' } });
        }
        if (i === 3) {
          should.strictEqual(key.type, 0x01);
          should.strictEqual(key.id, 'XI');
          should.strictEqual(key.i, 3);
          should.deepEqual(val, { h: { id: 'XI', v: 'Cccc', pa: ['Aaaa'], i: 3 }, b: { more2: 'body' } });
        }
        if (i === 4) {
          should.strictEqual(key.type, 0x01);
          should.strictEqual(key.id, 'XI');
          should.strictEqual(key.i, 4);
          should.deepEqual(val, { h: { id: 'XI', v: 'Dddd', pa: ['Cccc'], i: 4 }, b: { more3: 'b' } });
        }
      });

      s.on('end', function() {
        should.strictEqual(i, 4);
        done();
      });
    });

    it('inspect keys: should have two headKeys with the latest versions and corresponding value', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getHeadKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = Tree.parseKey(obj.key, { decodeId: 'utf8', decodeV: 'base64' });
        var val = Tree.parseHeadVal(obj.value, 'utf8');

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
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getIKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = Tree.parseKey(obj.key, { decodeId: 'utf8' });
        var val = Tree.parseKey(obj.value, { decodeId: 'utf8', decodeV: 'base64' });

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
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getVKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = Tree.parseKey(obj.key, { decodeId: 'utf8', decodeV: 'base64' });
        var val = Tree.parseKey(obj.value, { decodeId: 'utf8' });

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

    it('inspect keys: should have four pekeys with the corresponding version as value', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      var r = t.getPeKeyRange();
      var s = db.createReadStream({ gt: r.s, lt: r.e });
      s.on('data', function(obj) {
        i++;

        var key = Tree.parseKey(obj.key, { decodePe: 'utf8', decodeV: 'base64' });
        var val = Tree.parsePeVal(obj.value, 'base64');

        should.strictEqual(key.type, 0x05);
        should.strictEqual(key.pe, name);

        if (i === 1) {
          should.strictEqual(key.i, 1);
          should.strictEqual(val.v, 'Aaaa');
        }
        if (i === 2) {
          should.strictEqual(key.i, 2);
          should.strictEqual(val.v, 'Bbbb');
        }
        if (i === 3) {
          should.strictEqual(key.i, 3);
          should.strictEqual(val.v, 'Cccc');
        }
        if (i === 4) {
          should.strictEqual(key.i, 4);
          should.strictEqual(val.v, 'Dddd');
        }
      });

      s.on('end', function() {
        should.strictEqual(i, 4);
        done();
      });
    });
  });
});
