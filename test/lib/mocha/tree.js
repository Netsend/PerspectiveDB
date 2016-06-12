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
var async = require('async');
var level = require('level-packager')(require('leveldown'));
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

    describe('uskey', function() {
      it('should err if us length is zero', function() {
        var b = new Buffer('000005000000', 'hex');
        (function() { Tree.parseKey(b); }).should.throw('us must be at least one byte');
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
        it('name 0, us 1', function() {
          var b = new Buffer('0000050160000100', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer(0),
            type: 0x05,
            us: new Buffer([96]),
            i: 0,
          });
        });

        it('name 1, us 1', function() {
          var b = new Buffer('016000050159000100', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer([96]),
            type: 0x05,
            us: new Buffer([89]),
            i: 0,
          });
        });

      });

      describe('i 3,', function() {
        it('name 0, us 3', function() {
          var b = new Buffer('000005032357600003235761', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer(0),
            type: 0x05,
            us: new Buffer([35, 87, 96]),
            i: 0x235761
          });
        });

        it('name 3, us 3', function() {
          var b = new Buffer('032357600005032357590003235761', 'hex');
          var obj = Tree.parseKey(b);
          should.deepEqual(obj, {
            name: new Buffer([35, 87, 96]),
            type: 0x05,
            us: new Buffer([35, 87, 89]),
            i: 0x235761
          });
        });
      });

      it('should decode pe to "hex" string', function() {
        var b = new Buffer('0000050144000100', 'hex');
        var obj = Tree.parseKey(b, { decodeUs: 'hex' });
        should.deepEqual(obj, {
          name: new Buffer([]),
          type: 0x05,
          us: '44',
          i: 0
        });
      });

      it('should decode pe to "base64" string', function() {
        var b = new Buffer('0000050144000100', 'hex');
        var obj = Tree.parseKey(b, { decodeUs: 'base64' });
        should.deepEqual(obj, {
          name: new Buffer([]),
          type: 0x05,
          us: 'RA==',
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
      it('opts 0', function() {
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

      it('delete 1', function() {
        var b = new Buffer('020102', 'hex');
        var obj = Tree.parseHeadVal(b);
        should.deepEqual(obj, {
          i: 2,
          d: true
        });
      });

      it('conflict 1, delete 1', function() {
        var b = new Buffer('030102', 'hex');
        var obj = Tree.parseHeadVal(b);
        should.deepEqual(obj, {
          i: 2,
          c: true,
          d: true
        });
      });
    });

    describe('i 3,', function() {
      it('opts 0', function() {
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

  describe('getIKeyRange', function() {
    it('should work with a zero byte name and default iSize = 6', function() {
      var t = new Tree(db, '');
      var p = t.getIKeyRange();
      should.strictEqual(p.s.toString('hex'), '000002');
      should.strictEqual(p.e.toString('hex'), '000002ff');
    });

    it('should use iSize + 1 for 0xff end sequence', function() {
      var t = new Tree(db, '', { iSize: 1 });
      var p = t.getIKeyRange();
      should.strictEqual(p.s.toString('hex'), '000002');
      should.strictEqual(p.e.toString('hex'), '000002ff');
    });

    it('should work with a single byte name', function() {
      var t = new Tree(db, 'a', { iSize: 1 });
      var p = t.getIKeyRange();
      should.strictEqual(p.s.toString('hex'), '01610002');
      should.strictEqual(p.e.toString('hex'), '01610002ff');
    });

    it('should work with a multi byte name', function() {
      var t = new Tree(db, 'abc', { iSize: 1 });
      var p = t.getIKeyRange();
      should.strictEqual(p.s.toString('hex'), '036162630002');
      should.strictEqual(p.e.toString('hex'), '036162630002ff');
    });

    it('should require opts to be an object', function() {
      var t = new Tree(db, 'abc', { iSize: 1 });
      (function() { t.getIKeyRange([]); }).should.throw('opts must be an object');
    });

    it('should work with minI 3', function() {
      var t = new Tree(db, 'abc', { iSize: 1 });
      var p = t.getIKeyRange({ minI: 3 });
      should.strictEqual(p.s.toString('hex'), '0361626300020103');
      should.strictEqual(p.e.toString('hex'), '036162630002ff');
    });

    it('should work with maxI 4', function() {
      var t = new Tree(db, 'abc', { iSize: 1 });
      var p = t.getIKeyRange({ maxI: 4 });
      should.strictEqual(p.s.toString('hex'), '036162630002');
      should.strictEqual(p.e.toString('hex'), '0361626300020104');
    });

    it('should work with minI 5 and maxI 2', function() {
      var t = new Tree(db, 'abc', { iSize: 1 });
      var p = t.getIKeyRange({ minI: 5, maxI: 2 });
      should.strictEqual(p.s.toString('hex'), '0361626300020105');
      should.strictEqual(p.e.toString('hex'), '0361626300020102');
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
        should.strictEqual(p.s.toString('hex'), '00000301cb00');
        should.strictEqual(p.e.toString('hex'), '00000301cb00ff');
      });

      it('should work with a single byte name', function() {
        var t = new Tree(db, 'a');
        var p = t.getHeadKeyRange(new Buffer('cb', 'hex'));
        should.strictEqual(p.s.toString('hex'), '0161000301cb00');
        should.strictEqual(p.e.toString('hex'), '0161000301cb00ff');
      });

      it('should work with a multi byte name', function() {
        var t = new Tree(db, 'abc');
        var p = t.getHeadKeyRange(new Buffer('cb', 'hex'));
        should.strictEqual(p.s.toString('hex'), '03616263000301cb00');
        should.strictEqual(p.e.toString('hex'), '03616263000301cb00ff');
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
      it('should work with a zero byte name', function() {
        var t = new Tree(db, '');
        var p = t.getDsKeyRange({ id: new Buffer('cb', 'hex') });
        should.strictEqual(p.s.toString('hex'), '00000101cb00');
        should.strictEqual(p.e.toString('hex'), '00000101cb00ff');
      });

      it('should work with a single byte name', function() {
        var t = new Tree(db, 'a');
        var p = t.getDsKeyRange({ id: new Buffer('cb', 'hex') });
        should.strictEqual(p.s.toString('hex'), '0161000101cb00');
        should.strictEqual(p.e.toString('hex'), '0161000101cb00ff');
      });

      it('should work with a multi byte name', function() {
        var t = new Tree(db, 'abc');
        var p = t.getDsKeyRange({ id: new Buffer('cb', 'hex') });
        should.strictEqual(p.s.toString('hex'), '03616263000101cb00');
        should.strictEqual(p.e.toString('hex'), '03616263000101cb00ff');
      });

      it('should work with a zero byte name and minI', function() {
        var t = new Tree(db, '', { iSize: 2 });
        var p = t.getDsKeyRange({ id: new Buffer('cb', 'hex'), minI: 8 });
        should.strictEqual(p.s.toString('hex'), '00000101cb00020008');
        should.strictEqual(p.e.toString('hex'), '00000101cb00ff');
      });

      it('should work with a zero byte name and maxI', function() {
        var t = new Tree(db, '', { iSize: 2 });
        var p = t.getDsKeyRange({ id: new Buffer('cb', 'hex'), maxI: 8 });
        should.strictEqual(p.s.toString('hex'), '00000101cb00');
        should.strictEqual(p.e.toString('hex'), '00000101cb00020008ff');
      });

      it('should work with a zero byte name, minI and maxI', function() {
        var t = new Tree(db, '', { iSize: 2 });
        var p = t.getDsKeyRange({ id: new Buffer('cb', 'hex'), minI: 7, maxI: 8 });
        should.strictEqual(p.s.toString('hex'), '00000101cb00020007');
        should.strictEqual(p.e.toString('hex'), '00000101cb00020008ff');
      });
      it('should work with a single byte name and minI', function() {
        var t = new Tree(db, 'a', { iSize: 2 });
        var p = t.getDsKeyRange({ id: new Buffer('cb', 'hex'), minI: 8 });
        should.strictEqual(p.s.toString('hex'), '0161000101cb00020008');
        should.strictEqual(p.e.toString('hex'), '0161000101cb00ff');
      });

      it('should work with a single byte name and maxI', function() {
        var t = new Tree(db, 'a', { iSize: 2 });
        var p = t.getDsKeyRange({ id: new Buffer('cb', 'hex'), maxI: 8 });
        should.strictEqual(p.s.toString('hex'), '0161000101cb00');
        should.strictEqual(p.e.toString('hex'), '0161000101cb00020008ff');
      });

      it('should work with a single byte name, minI and maxI', function() {
        var t = new Tree(db, 'a', { iSize: 2 });
        var p = t.getDsKeyRange({ id: new Buffer('cb', 'hex'), minI: 7, maxI: 8 });
        should.strictEqual(p.s.toString('hex'), '0161000101cb00020007');
        should.strictEqual(p.e.toString('hex'), '0161000101cb00020008ff');
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

  describe('createHeadReadStream', function() {
    var name = 'createHeadReadStream';

    it('should require opts to be an object', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.createHeadReadStream(0); }).should.throw('opts must be an object');
    });

    it('should require opts.skipConflicts to be a boolean', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.createHeadReadStream({ skipConflicts: {} }); }).should.throw('opts.skipConflicts must be a boolean');
    });

    it('should return a readable stream', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var s = t.createHeadReadStream();
      should.ok(s.readable);
    });
  });

  describe('getHeads', function() {
    // simple test, test further after write tests are done
    var name = 'getHeads';

    var item1 = { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } };
    var item3 = { h: { id: 'XI', v: 'Dddd', i: 3, pa: ['Aaaa'], d: true }, b: { some: 'other' } };
    var item4 = { h: { id: 'XI', v: 'Ffff', i: 4, pa: ['Bbbb'], c: true }, b: { some: 'more2' } };

    it('should require opts to be an object', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.getHeads(0); }).should.throw('opts must be an object');
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

    it('should iterate and wait with next item until next is called', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      var waiting;
      t.getHeads(function(item, next) {
        if (waiting) { throw new Error('next should wait'); }
        i++;
        if (i === 1) { should.deepEqual(item, item2); }
        if (i > 1) { should.deepEqual(item, item3); }
        waiting = true;
        setTimeout(function() {
          waiting = false;
          next();
        }, 100);
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
      }, function(err, lastContinue) {
        should.strictEqual(i, 2);
        should.strictEqual(lastContinue, undefined);
        done();
      });
    });

    it('should stop iterating if next is called with false', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      t.getHeads(function(item, next) {
        i++;
        if (i === 1) { should.deepEqual(item, item3); }
        if (i > 1) { should.deepEqual(item, item4); }
        next(null, false);
      }, function(err, lastContinue) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        should.strictEqual(lastContinue, false);
        done();
      });
    });

    it('should stop iterating if next is called with an error and propagate this error', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      t.getHeads(function(item, next) {
        i++;
        if (i === 1) { should.deepEqual(item, item3); }
        if (i > 1) { should.deepEqual(item, item4); }
        next(new Error('stop it'));
      }, function(err) {
        should.strictEqual(err.message, 'stop it');
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

    it('should not emit deleted heads if skipDeletes is true', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      t.getHeads({ skipDeletes: true }, function(item, next) {
        i++;
        if (i > 0) { should.deepEqual(item, item4); }
        next();
      }, function() {
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should not emit any heads if skipConflicts and skipDeletes are true', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      t.getHeads({ skipConflicts: true, skipDeletes: true }, function(item, next) {
        i++;
        next();
      }, function() {
        should.strictEqual(i, 0);
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
        should.deepEqual(found, item);
        done();
      });
    });

    it('should return the bson version', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.getByVersion('Aaaa', { bson: true }, function(err, found) {
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

  describe('insertionOrderStream', function() {
    var name = 'insertionOrderStream';

    var item1 = { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } };
    var item3 = { h: { id: 'XI', v: 'Dddd', i: 3, pa: ['Aaaa'] }, b: { some: 'other' } };
    var item4 = { h: { id: 'foo', v: 'Eeee', i: 4, pa: [] }, b: { som3: 'other' } };

    it('should require opts.first to match vSize', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.insertionOrderStream({ first: 'a' }, function() { }, function() { }); }).should.throw('opts.first must be the same size as the configured vSize');
    });

    it('should require opts.last to match vSize', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.insertionOrderStream({ last: 'a' }, function() { }, function() { }); }).should.throw('opts.last must be the same size as the configured vSize');
    });

    it('should require bson to be a boolean', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.insertionOrderStream({ bson: 'a' }, function() { }, function() { }); }).should.throw('opts.bson must be a boolean');
    });

    it('should end directly with an empty database', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var s = t.insertionOrderStream()
      s.on('end', done);
      s.resume(); // get the stream in flowing mode
    });

    it('needs item1', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.end(item1, done);
    });

    it('should emit bson buffers', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.insertionOrderStream({ bson: true }).on('data', function(obj) {
        i++;
        should.strictEqual(Buffer.isBuffer(obj), true);
      }).on('end', function() {
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should iterate over item1', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.insertionOrderStream().on('data', function(obj) {
        i++;
        should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
      }).on('end', function() {
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should emit the existing item but no new items that are added while the iterator is running', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.insertionOrderStream().on('data', function(obj) {
        i++;
        if (i > 0) {
          should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
        }
        t.write(item2);
      }).on('end', function() {
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should use opts.first and start emitting at offset 1', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.insertionOrderStream({ first: 'Aaaa' }).on('data', function(obj) {
        i++;
        if (i === 1) {
          should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
        }

        if (i > 1) {
          should.deepEqual({ h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } }, obj);
        }
      }).on('end', function() {
        should.strictEqual(i, 2);
        done();
      });
    });

    it('should use opts.first and start emitting at offset 2 (excludeFirst)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.insertionOrderStream({ first: 'Aaaa', excludeFirst: true }).on('data', function(obj) {
        i++;
        if (i > 0) {
          should.deepEqual({ h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } }, obj);
        }
      }).on('end', function() {
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should use opts.last and start emitting at offset 1', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.insertionOrderStream({ last: 'Aaaa' }).on('data', function(obj) {
        i++;
        if (i > 0) {
          should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
        }
      }).on('end', function() {
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should use opts.last and start emitting at offset 1 (excludeLast)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.insertionOrderStream({ last: 'Bbbb', excludeLast: true }).on('data', function(obj) {
        i++;
        if (i > 0) {
          should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
        }
      }).on('end', function() {
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should emit the existing item but no new items that are added while the iterator is running the first time of many', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.insertionOrderStream().on('data', function(obj) {
        i++;
        if (i === 1) {
          should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
          t.write(item3);
        }
        if (i > 1) {
          should.deepEqual({ h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } }, obj);
        }
      }).on('end', function() {
        should.strictEqual(i, 2);
        done();
      });
    });

    it('should use opts.first, opts.last, excludeFirst, excludeLast and emit offset 2 only', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      t.insertionOrderStream({ first: 'Aaaa', excludeFirst: true, last: 'Dddd', excludeLast: true }).on('data', function(obj) {
        i++;
        if (i > 0) {
          should.deepEqual({ h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } }, obj);
        }
      }).on('end', function() {
        should.strictEqual(i, 1);
        done();
      });
    });

    it('item4, different DAG', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.write(item4, done);
    });

    it('should iterate over DAG foo only (item4)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      t.insertionOrderStream({ id: 'foo' }).on('data', function(obj) {
        i++;
        if (i > 0) {
          should.deepEqual({ h: { id: 'foo', v: 'Eeee', i: 4, pa: [] }, b: { som3: 'other' } }, obj);
        }
      }).on('end', function() {
        should.strictEqual(i, 1);
        done();
      });
    });
  });

  describe('createReadStream', function() {
    var name = 'createReadStream';

    var item1 = { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } };
    var item3 = { h: { id: 'XI', v: 'Dddd', i: 3, pa: ['Aaaa'] }, b: { some: 'other' } };
    var item4 = { h: { id: 'foo', v: 'Eeee', i: 4, pa: [] }, b: { som3: 'other' } };
    var item5 = { h: { id: 'foo', v: 'Ffff', i: 5, pa: ['Eeee'] }, b: { som3: 'mooore' } };

    it('should require opts to be an object', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.createReadStream(function() {}); }).should.throw('opts must be an object');
    });

    it('should require opts.first to match vSize', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.createReadStream({ first: 'a' }, function() { }, function() { }); }).should.throw('opts.first must be the same size as the configured vSize');
    });

    it('should require opts.last to match vSize', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.createReadStream({ last: 'a' }, function() { }, function() { }); }).should.throw('opts.last must be the same size as the configured vSize');
    });

    it('should require opts.bson to be a boolean', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.createReadStream({ bson: 'a' }, function() { }, function() { }); }).should.throw('opts.bson must be a boolean');
    });

    it('should error on tail and reverse while not supported', function() {
      // configure 2 bytes and call with 3 bytes (base64)
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.createReadStream({ tail: true, reverse: true }, function() { }, function() { }); }).should.throw('opts.reverse is mutually exclusive with opts.tail');
    });

    it('should call end directly with an empty database (if data event is handled)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var s = t.createReadStream();
      s.on('end', done);
      s.on('data', done);
    });

    it('needs item1', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.end(item1, done);
    });

    it('should emit bson buffers', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      var s = t.createReadStream({ bson: true });

      s.on('data', function(obj) {
        i++;
        should.strictEqual(Buffer.isBuffer(obj), true);
      });

      s.on('end', function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should iterate over item1', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      var s = t.createReadStream();

      s.on('data', function(obj) {
        i++;
        should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
      });

      s.on('end', function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        done();
      });
    });

    it('item2', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.end(item2, done);
    });

    it('should not wait with end if stream is paused (node stream behavior)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      var s = t.createReadStream();

      var ended;
      s.on('data', function() {
        i++;
        s.pause();
        setTimeout(function() {
          s.resume();
          if (i > 1) {
            should.strictEqual(ended, true);
            done();
          }
        }, 10);
      });

      s.on('end', function() {
        ended = true;
        should.strictEqual(i, 2);
      });
    });

    it('should read in reverse', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      var s = t.createReadStream({ id: 'XI', reverse: true });

      s.on('data', function(obj) {
        i++;
        if (i === 1) {
          // expect item2
          should.deepEqual({ h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } }, obj);
        }
        if (i > 1) {
          // expect item1
          should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
        }
      });

      s.on('end', function() {
        should.strictEqual(i, 2);
        done();
      });
    });

    it('should use opts.first and start emitting at offset 1', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      var s = t.createReadStream({ first: 'Aaaa' });

      s.on('data', function(obj) {
        i++;
        if (i === 1) {
          should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
        }

        if (i > 1) {
          should.deepEqual({ h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } }, obj);
        }
      });

      s.on('end', function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 2);
        done();
      });
    });

    it('should use opts.first and start emitting at offset 2 (excludeFirst)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      var s = t.createReadStream({ first: 'Aaaa', excludeFirst: true });

      s.on('data', function(obj) {
        i++;
        if (i > 0) {
          should.deepEqual({ h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } }, obj);
        }
      });

      s.on('end', function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should use opts.last and start emitting at offset 1', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      var s = t.createReadStream({ last: 'Aaaa' });

      s.on('data', function(obj) {
        i++;
        if (i > 0) {
          should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
        }
      });

      s.on('end', function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should use opts.last and start emitting at offset 1 (excludeLast)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      var s = t.createReadStream({ last: 'Bbbb', excludeLast: true });

      s.on('data', function(obj) {
        i++;
        if (i > 0) {
          should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
        }
      });

      s.on('end', function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should not emit items that are added after the stream is openend and data handler is registered', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      var s = t.createReadStream();

      s.on('data', function(obj) {
        i++;
        if (i === 1) {
          should.deepEqual({ h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] }, b: { some: 'data' } }, obj);
        }
        if (i > 1) {
          should.deepEqual({ h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } }, obj);
        }
      });

      s.pause();
      t.write(item3, function(err) {
        if (err) { throw err; }
        s.resume();
      });

      s.on('end', function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 2);
        done();
      });
    });

    it('should use opts.first, opts.last, excludeFirst, excludeLast and emit offset 2 only', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var i = 0;
      var s = t.createReadStream({ first: 'Aaaa', excludeFirst: true, last: 'Dddd', excludeLast: true });

      s.on('data', function(obj) {
        i++;
        if (i > 0) {
          should.deepEqual({ h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] }, b: { some: 'more' } }, obj);
        }
      });

      s.on('end', function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        done();
      });
    });

    it('item4, different DAG', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.write(item4, done);
    });

    it('should iterate over DAG foo only (item4)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      var s = t.createReadStream({ id: 'foo' });

      s.on('data', function(obj) {
        i++;
        if (i > 0) {
          should.deepEqual({ h: { id: 'foo', v: 'Eeee', i: 4, pa: [] }, b: { som3: 'other' } }, obj);
        }
      });

      s.on('end', function(err) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should tail and read new item5 after it\'s written', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      var i = 0;
      var s = t.createReadStream({ id: 'foo', tail: true, tailRetry: 2 });

      s.on('data', function(obj) {
        i++;
        if (i === 1) {
          should.deepEqual({ h: { id: 'foo', v: 'Eeee', i: 4, pa: [] }, b: { som3: 'other' } }, obj);

          // write item5
          t.write(item5);
        }
        if (i > 1) {
          // expect item5
          should.deepEqual({ h: { id: 'foo', v: 'Ffff', i: 5, pa: ['Eeee'] }, b: { som3: 'mooore' } }, obj);
          // stop soon
          setTimeout(function() {
            s.close();
          }, 3);
        }
      });

      s.on('end', function() {
        should.strictEqual(i, 2);
        done();
      });
    });
  });

  describe('lastVersion', function() {
    var name = 'lastVersion';

    var item1 = { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] } };
    var item3 = { h: { id: 'XI', v: 'Cccc', i: 3, pa: ['Bbbb'] } };
    var item4 = { h: { id: 'XI', v: 'Dddd', i: 4, pa: ['Aaaa'] } };

    it('should return null with empty databases', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.lastVersion(function(err, v) {
        if (err) { throw err; }
        should.equal(v, null);
        done();
      });
    });

    it('needs item1', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.end(item1, done);
    });

    it('should return version of item1 as a number by default', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.lastVersion(function(err, v) {
        if (err) { throw err; }
        // var b = new Buffer(item1.h.v, 'base64');
        // var num = b.readUIntBE(0, b.length);
        // version number Aaaa base64 is 108186

        should.strictEqual(v, 108186);
        done();
      });
    });

    it('should return version of item1 base64', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.lastVersion('base64', function(err, v) {
        if (err) { throw err; }
        should.equal(v, 'Aaaa');
        done();
      });
    });

    it('needs item2 and item3', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.write(item2);
      t.end(item3, done);
    });

    it('should return version of item3', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.lastVersion('base64', function(err, v) {
        if (err) { throw err; }
        should.equal(v, 'Cccc');
        done();
      });
    });

    it('needs item4', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.end(item4, done);
    });

    it('should return version of item4 in hex', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.lastVersion('hex', function(err, v) {
        if (err) { throw err; }
        should.equal(v, '0dd75d');
        done();
      });
    });
  });

  describe('lastByPerspective', function() {
    var name = 'lastByPerspective';

    var item1 = { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [], pe: 'lbp' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'], pe: 'lbp' } };
    var item3 = { h: { id: 'XI', v: 'Cccc', i: 3, pa: ['Bbbb'], pe: 'lbp2' } };
    var item4 = { h: { id: 'XI', v: 'Dddd', i: 4, pa: ['Aaaa'], pe: 'lbp' } };

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

    it('should return version of item1 as a number by default', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.lastByPerspective('lbp', function(err, v) {
        if (err) { throw err; }
        // var b = new Buffer(item1.h.v, 'base64');
        // var num = b.readUIntBE(0, b.length);
        // version number Aaaa base64 is 108186

        should.strictEqual(v, 108186);
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

  describe('del', function() {
    var name = 'del';

    var item = { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [], pe: 'lbp' } };

    it('should require item to be an object', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.del(function() {}); }).should.throw('item must be an object');
    });

    it('should require cb to be a function', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.del({}); }).should.throw('cb must be a function');
    });

    it('needs item', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.write(item, done);
    });

    it('should err if skipValidation not set', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.del(item, function(err) {
        should.strictEqual(err.message, 'del is only available if skipValidation is set to true');
        done();
      });
    });

    it('should delete item', function(done) {
      var t = new Tree(db, name, { skipValidation: true, vSize: 3, log: silence });
      t.del(item, function(err) {
        if (err) { throw err; }
        done();
      });
    });

    it('should err if item not found', function(done) {
      var t = new Tree(db, name, { skipValidation: true, vSize: 3, log: silence });
      t.del(item, function(err) {
        should.strictEqual(err.message, 'version not found');
        done();
      });
    });
  });

  describe('stats', function() {
    var name = 'stats';

    var item1 = { h: { id: 'XI', v: 'Aaaa', pa: [] } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], d: true } };

    it('should require cb to be a function', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.stats({}); }).should.throw('cb must be a function');
    });

    it('should return head stats', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.stats(function(err, stats) {
        if (err) { throw err; }
        should.deepEqual(stats, { heads: { count: 0, conflict: 0, deleted: 0 } });
        done();
      });
    });

    it('write item1', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.write(item1, done);
    });

    it('should return head stats', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.stats(function(err, stats) {
        if (err) { throw err; }
        should.deepEqual(stats, { heads: { count: 1, conflict: 0, deleted: 0 } });
        done();
      });
    });

    it('write item2', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.write(item2, done);
    });

    it('should return head stats with deleted item', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.stats(function(err, stats) {
        if (err) { throw err; }
        should.deepEqual(stats, { heads: { count: 1, conflict: 0, deleted: 1 } });
        done();
      });
    });
  });

  describe('setConflictByVersion', function() {
    var name = 'setConflictByVersion';

    var item1 = { h: { id: 'XI', v: 'Aaaa', pa: [] } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'] } };

    it('should require version to be a number or a base64 string', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.setConflictByVersion({}); }).should.throw('version must be a number or a base64 string');
    });

    it('should require cb to be a function', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t.setConflictByVersion('', {}); }).should.throw('cb must be a function');
    });

    it('write item1 and item2', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.write(item1);
      t.write(item2);
      t.end(null, done);
    });

    it('should mark non-head', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.setConflictByVersion(item1.h.v, function(err) {
        if (err) { throw err; }
        var i = 0;
        t.insertionOrderStream().on('data', function(item) {
          i++;
          if (i === 1) {
            should.deepEqual(item, { h: { id: 'XI', v: 'Aaaa', pa: [], i: 1, c: true } });
          }
          if (i > 1) {
            should.deepEqual(item, { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], i: 2 } });
          }
        }).on('end', function() {
          should.strictEqual(i, 2);
          t.stats(function(err, stats) {
            if (err) { throw err; }
            should.deepEqual(stats, { heads: { count: 1, conflict: 0, deleted: 0 } });
            done();
          });
        });
      });
    });

    it('should be idempotent (redo, no changes)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.setConflictByVersion(item1.h.v, function(err) {
        if (err) { throw err; }
        var i = 0;
        t.insertionOrderStream().on('data', function(item) {
          i++;
          if (i === 1) {
            should.deepEqual(item, { h: { id: 'XI', v: 'Aaaa', pa: [], i: 1, c: true } });
          }
          if (i > 1) {
            should.deepEqual(item, { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], i: 2 } });
          }
        }).on('end', function() {
          should.strictEqual(i, 2);
          t.stats(function(err, stats) {
            if (err) { throw err; }
            should.deepEqual(stats, { heads: { count: 1, conflict: 0, deleted: 0 } });
            done();
          });
        });
      });
    });

    it('should mark head', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.setConflictByVersion(item2.h.v, function(err) {
        if (err) { throw err; }
        var i = 0;
        t.insertionOrderStream().on('data', function(item) {
          i++;
          if (i === 1) {
            should.deepEqual(item, { h: { id: 'XI', v: 'Aaaa', pa: [], i: 1, c: true } });
          }
          if (i > 1) {
            should.deepEqual(item, { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], i: 2, c: true } });
          }
        }).on('end', function() {
          should.strictEqual(i, 2);
          t.stats(function(err, stats) {
            if (err) { throw err; }
            should.deepEqual(stats, { heads: { count: 1, conflict: 1, deleted: 0 } });
            done();
          });
        });
      });
    });
  });

  describe('inBufferById', function() {
    var name = 'inBufferById';

    var items = [
      { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] } },
      { h: { id: 'XII', v: 'Cccc', i: 3, pa: [] } },
      { h: { id: 'XII', v: 'Dddd', i: 4, pa: ['Cccc'] } },
      { h: { id: 'XI', v: 'Eeee', i: 5, pa: ['Aaaa'] } },
    ];

    it('should return false on empty string', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      should.strictEqual(t.inBufferById(), false);
    });

    it('should return false on non-existing id', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      should.strictEqual(t.inBufferById('X'), false);
    });

    it('should return true if id is just written and still in buffer', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      t.write(items[0]);
      should.strictEqual(t.inBufferById('XI'), true);
    });

    it('should return false if id is already written and not in buffer', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      should.strictEqual(t.inBufferById('XI'), false);
    });

    it('should return true if id is somewhere in the buffer', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      t.write(items[1]);
      t.write(items[2]);
      t.write(items[3]);
      should.strictEqual(t.inBufferById('XI'), true);
      should.strictEqual(t.inBufferById('XII'), true);
      should.strictEqual(t.inBufferById('X'), false);
      should.strictEqual(t.inBufferById('XIII'), false);
    });
  });

  describe('inBufferByVersion', function() {
    var name = 'inBufferByVersion';

    var items = [
      { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] } },
      { h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] } },
      { h: { id: 'XI', v: 'Cccc', i: 3, pa: ['Bbbb'] } },
      { h: { id: 'XI', v: 'Dddd', i: 4, pa: ['Cccc'] } },
      { h: { id: 'XI', v: 'Eeee', i: 5, pa: ['Dddd'] } },
    ];

    it('should return false on empty string', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      should.strictEqual(t.inBufferByVersion(), false);
    });

    it('should return false on non-existing version', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      should.strictEqual(t.inBufferByVersion('Aaaa'), false);
    });

    it('should return true if version is just written and still in buffer', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      t.write(items[0]);
      should.strictEqual(t.inBufferByVersion('Aaaa'), true);
    });

    it('should return false if version is already written and not in buffer', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      should.strictEqual(t.inBufferByVersion('Aaaa'), false);
    });

    it('should return true if version is somewhere in the buffer', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      t.write(items[1]);
      t.write(items[2]);
      t.write(items[3]);
      should.strictEqual(t.inBufferByVersion('Aaaa'), false);
      should.strictEqual(t.inBufferByVersion('Bbbb'), true);
      should.strictEqual(t.inBufferByVersion('Cccc'), true);
      should.strictEqual(t.inBufferByVersion('Dddd'), true);
      should.strictEqual(t.inBufferByVersion('Eeee'), false);
      should.strictEqual(t.inBufferByVersion('Ffff'), false);
    });
  });

  describe('_getDsKeyByVersion', function() {
    var name = '_getDsKeyByVersion';

    var item = { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] } };

    it('should require version to be a number or a base64 string', function() {
      var t = new Tree(db, name, { log: silence });
      (function() { t._getDsKeyByVersion({}); }).should.throw('version must be a number or a base64 string');
    });

    it('should return with null if the index is empty', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._getDsKeyByVersion('Aaaa', function(err, dsKey) {
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
      t._getDsKeyByVersion('Aaaa', function(err, dsKey) {
        if (err) { throw err; }
        should.strictEqual(dsKey.toString('hex'), t._composeDsKey(item.h.id, item.h.i).toString('hex'));
        done();
      });
    });

    it('should return null if version is not found', function(done) {
      var t = new Tree(db, name, { vSize: 2, log: silence });
      t._getDsKeyByVersion(8, function(err, dsKey) {
        if (err) { throw err; }
        should.strictEqual(dsKey, null);
        done();
      });
    });
  });

  describe('_resolveVtoI', function() {
    var name = '_resolveVtoI';

    var item = { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] } };

    it('should require version to be a number or a base64 string', function() {
      var t = new Tree(db, name, { log: silence });
      (function() { t._resolveVtoI({}); }).should.throw('version must be a number or a base64 string');
    });

    it('should error if the index is empty', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._resolveVtoI('Aaaa', function(err) {
        should.strictEqual(err.message, 'version not found');
        done();
      });
    });

    it('needs an entry in vkey index', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.end(item, done);
    });

    it('should find the version', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._resolveVtoI('Aaaa', function(err, i) {
        if (err) { throw err; }
        should.strictEqual(i, 1);
        done();
      });
    });

    it('should error if version is not found', function(done) {
      var t = new Tree(db, name, { vSize: 2, log: silence });
      t._resolveVtoI(8, function(err) {
        should.strictEqual(err.message, 'version not found');
        done();
      });
    });
  });

  describe('_maxI', function() {
    var name = '_maxI';

    it('should return null by default', function(done) {
      var t = new Tree(db, name, { log: silence });
      t._maxI(function(err, i) {
        if (err) { throw err; }
        should.equal(i, null);
        done();
      });
    });

    it('needs an item with a number in the index', function(done) {
      var t = new Tree(db, name, { log: silence });
      t._db.put(t._composeIKey(2), BSON.serialize({ b: 'A' }), done);
    });

    it('should give the new i', function(done) {
      var t = new Tree(db, name, { log: silence });
      t._maxI(function(err, i) {
        if (err) { throw err; }
        should.equal(i, 2);
        done();
      });
    });

    it('needs a new higher i in the index', function(done) {
      var t = new Tree(db, name, { log: silence });
      t._db.put(t._composeIKey(20), BSON.serialize({ b: 'A' }), done);
    });

    it('should return the new i which is 20', function(done) {
      var vc = new Tree(db, name, { log: silence });
      vc._maxI(function(err, i) {
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

    it('should set delete option byte and i to 4', function() {
      var t = new Tree(db, name, { iSize: 3, log: silence });
      t._composeHeadVal({ h: { i: 4, d: true } }).toString('hex').should.equal('0203000004');
    });

    it('should set conflict and delete option byte and i to 4', function() {
      var t = new Tree(db, name, { iSize: 3, log: silence });
      t._composeHeadVal({ h: { i: 4, c: true, d: true } }).toString('hex').should.equal('0303000004');
    });
  });

  describe('_composeUsKey', function() {
    var name = '_';

    it('should require usKey to be a string', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      (function() { t._composeUsKey(null, 1); }).should.throw('Cannot read property \'toString\' of null');
    });

    it('should transform a string type usKey into a buffer', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._composeUsKey('foo', 257).toString('hex').should.equal('015f000503666f6f00');
    });

    it('should accept buffer type usKey', function() {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._composeUsKey(new Buffer('true'), 257).toString('hex').should.equal('015f0005047472756500');
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

  describe('_validNewItem', function() {
    var name = '_validNewItem';

    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa', i: 1, pa: [] } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', i: 2, pa: ['Aaaa'] } };
    var item3 = { h: { id: 'XI', v: 'Cccc', pa: ['Bbbb'] } };
    var item4 = { h: { id: 'XI', v: 'Dddd', pa: ['Aaaa'] } };

    var itemA = { h: { id: 'XI', v: 'Xxxx', pa: [] } };

    it('root in an empty database is valid', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._validNewItem(item1, function(err, valid, heads, exists) {
        if (err) { throw err; }
        should.strictEqual(valid, true);
        should.deepEqual(heads, []);
        should.strictEqual(exists, false);
        done();
      });
    });

    it('non-root in an empty database is invalid', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._validNewItem(item2, function(err, valid, problemParents, exists) {
        if (err) { throw err; }
        should.strictEqual(valid, false);
        should.deepEqual(problemParents, ['Aaaa']);
        should.strictEqual(exists, false);
        done();
      });
    });

    it('item1', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.write(item1, done);
    });

    it('root in a non-empty database is invalid', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._validNewItem(itemA, function(err, valid, heads, exists) {
        if (err) { throw err; }
        should.strictEqual(valid, false);
        should.deepEqual(heads, ['Aaaa']);
        should.strictEqual(exists, false);
        done();
      });
    });

    it('existing root in a non-empty database is invalid', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._validNewItem(item1, function(err, valid, heads, exists) {
        if (err) { throw err; }
        should.strictEqual(valid, false);
        should.deepEqual(heads, ['Aaaa']);
        should.strictEqual(exists, true);
        done();
      });
    });

    it('connecting non-root in a non-empty database (fast-forward) is valid', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._validNewItem(item2, function(err, valid, problemParents, exists) {
        if (err) { throw err; }
        should.strictEqual(valid, true);
        should.deepEqual(problemParents, []);
        should.strictEqual(exists, false);
        done();
      });
    });

    it('connecting non-root in a non-empty database (fork) is valid', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._validNewItem(item4, function(err, valid, problemParents, exists) {
        if (err) { throw err; }
        should.strictEqual(valid, true);
        should.deepEqual(problemParents, []);
        should.strictEqual(exists, false);
        done();
      });
    });

    it('non-connecting non-root in a non-empty database is invalid', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._validNewItem(item3, function(err, valid, problemParents, exists) {
        if (err) { throw err; }
        should.strictEqual(valid, false);
        should.deepEqual(problemParents, ['Bbbb']);
        should.strictEqual(exists, false);
        done();
      });
    });

    it('item2', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.write(item2, done);
    });

    it('existing connecting non-root in a non-empty database is invalid', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._validNewItem(item2, function(err, valid, problemParents, exists) {
        if (err) { throw err; }
        should.strictEqual(valid, false);
        should.deepEqual(problemParents, []);
        should.strictEqual(exists, true);
        done();
      });
    });

    //////////// DELETE ONE AND ONLY HEAD

    var item5 = { h: { id: 'XI', v: 'Eeee', pa: ['Bbbb'], d: true } };

    var item10 = { h: { id: 'XI', v: 'Ffff', i: 1, pa: [] } };

    var item20 = { h: { id: 'XI', v: 'Gggg', i: 2, pa: ['Eeee'] } }; // connect to deleted head
    var item30 = { h: { id: 'XI', v: 'Hhhh', pa: ['Gggg'] } };
    var item40 = { h: { id: 'XI', v: 'Iiii', pa: ['Eeee'] } };

    it('item5 (delete head)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.write(item5, done);
    });

    it('non-existing root in a non-empty database is invalid', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._validNewItem(item10, function(err, valid, heads, exists) {
        if (err) { throw err; }
        should.strictEqual(valid, false);
        should.deepEqual(heads, ['Eeee']);
        should.strictEqual(exists, false);
        done();
      });
    });

    it('existing root in a non-empty database is invalid (return heads)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._validNewItem(item1, function(err, valid, heads, exists) {
        if (err) { throw err; }
        should.strictEqual(valid, false);
        should.deepEqual(heads, ['Eeee']);
        should.strictEqual(exists, true);
        done();
      });
    });

    it('connecting non-root in a non-empty database (fast-forward) is valid', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._validNewItem(item20, function(err, valid, problemParents, exists) {
        if (err) { throw err; }
        should.strictEqual(valid, true);
        should.deepEqual(problemParents, []);
        should.strictEqual(exists, false);
        done();
      });
    });

    it('connecting non-root in a non-empty database (fork) is valid', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._validNewItem(item40, function(err, valid, problemParents, exists) {
        if (err) { throw err; }
        should.strictEqual(valid, true);
        should.deepEqual(problemParents, []);
        should.strictEqual(exists, false);
        done();
      });
    });

    it('non-connecting non-root in a non-empty database is invalid', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._validNewItem(item30, function(err, valid, problemParents, exists) {
        if (err) { throw err; }
        should.strictEqual(valid, false);
        should.deepEqual(problemParents, ['Gggg']);
        should.strictEqual(exists, false);
        done();
      });
    });

    it('item20', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.write(item20, done);
    });

    it('existing connecting non-root in a non-empty database is invalid', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t._validNewItem(item20, function(err, valid, problemParents, exists) {
        if (err) { throw err; }
        should.strictEqual(valid, false);
        should.deepEqual(problemParents, []);
        should.strictEqual(exists, true);
        done();
      });
    });
  });

  describe('_write', function() {
    var name = '_write';

    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa', pa: [] }, b: { some: 'body' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], pe: 'other' }, b: { more: 'body' } };
    var item3 = { h: { id: 'XI', v: 'Cccc', pa: ['Aaaa'] }, b: { more2: 'body' } };
    var item4 = { h: { id: 'XI', v: 'Dddd', pa: ['Cccc'], pe: 'other' }, b: { more3: 'b' } };

    it('should not accept a non-root in an empty database', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.on('error', function(err) {
        should.strictEqual(err.message, 'not a valid new item');
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
        t.insertionOrderStream().on('data', function(obj) {
          i++;
          should.strictEqual(obj.h.i, i);
        }).on('end', function() {
          should.strictEqual(i, 2);
          done();
        });
      });
    });

    it('inspect keys: should have created one usKey with the perspective and version of item2', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      db.get(t._composeUsKey(item2.h.pe), function(err, v) {
        should.strictEqual(Tree.parseKey(v, { decodeV: 'base64' }).v, 'Bbbb');
        done();
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
          should.deepEqual(val, { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], pe: 'other', i: 2 }, b: { more: 'body' } });
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

    it('should not accept an existing root', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.on('error', function(err) {
        should.strictEqual(err.message, 'not a valid new item');
        done();
      });
      t.end(item1);
      // expect that streams do not emit a finish after an error occurred
      t.on('finish', done);
    });

    it('should accept new item (fork)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.write(item3);
      var i = 0;
      t.end(function(err) {
        if (err) { throw err; }
        t.insertionOrderStream().on('data', function(obj) {
          i++;
          should.strictEqual(obj.h.i, i);
        }).on('end', function() {
          should.strictEqual(i, 3);
          done();
        });
      });
    });

    it('should not accept an existing non-root (of a local perspective)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.on('error', function(err) {
        should.strictEqual(err.message, 'not a valid new item');
        done();
      });
      t.end(item3);
      // expect that streams do not emit a finish after an error occurred
      t.on('finish', done);
    });

    it('inspect keys: should *not* have updated the usKey for the perspective "other" of item2', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      db.get(t._composeUsKey(item2.h.pe), function(err, v) {
        should.strictEqual(Tree.parseKey(v, { decodeV: 'base64' }).v, 'Bbbb');
        done();
      });
    });

    it('should accept new item (fast-forward)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.write(item4);
      var i = 0;
      t.end(function(err) {
        if (err) { throw err; }
        t.insertionOrderStream().on('data', function(obj) {
          i++;
          should.strictEqual(obj.h.i, i);
        }).on('end', function() {
          should.strictEqual(i, 4);
          done();
        });
      });
    });

    it('inspect keys: should have updated the usKey for the perspective "other" of item4', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      db.get(t._composeUsKey(item2.h.pe), function(err, v) {
        should.strictEqual(Tree.parseKey(v, { decodeV: 'base64' }).v, 'Dddd');
        done();
      });
    });

    it('should accept an existing non-root from a non-local perspective', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      t.on('error', function(err) {
        throw err;
      });
      t.end(item2);
      // expect that streams do not emit a finish after an error occurred
      t.on('finish', done);
    });

    it('inspect keys: should have updated the usKey for the perspective "other" to previously written key, item2', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });
      db.get(t._composeUsKey(item2.h.pe), function(err, v) {
        should.strictEqual(Tree.parseKey(v, { decodeV: 'base64' }).v, 'Bbbb');
        done();
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
          should.deepEqual(val, { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], pe: 'other', i: 2 }, b: { more: 'body' } });
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
          should.deepEqual(val, { h: { id: 'XI', v: 'Dddd', pa: ['Cccc'], pe: 'other', i: 4 }, b: { more3: 'b' } });
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

    it('should accept an existing non-root (of a local perspective) if skipValidation is true', function(done) {
      var t = new Tree(db, name, { skipValidation: true, vSize: 3, log: silence });
      t.write(item3, done);
    });

    it('should emit an error when invalid items are written with a callback error-handler (node behavior)', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var cbCalled;
      t.on('error', function(err) {
        should.strictEqual(err.message, 'item.h must be an object');
        should.strictEqual(cbCalled, true);
        done();
      });

      t.write({}, function(err) {
        cbCalled = true;
        should.strictEqual(err.message, 'item.h must be an object');
      });
    });

    it('should return false after writing 16 objects into the buffer', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: silence });

      var falses = 0;
      var truths = 0;

      for (var i = 1; i < 18; i++) {
        var v = new Buffer('Aaaa', 'base64');
        var num = v.readUIntBE(0, 3) + i;
        v.writeIntBE(num, 0, 3);
        var item = { h: { id: 'XI' + i, v: v.toString('base64'), i: i, pa: [] }, b: { some: 'data' } };
        var cont = t.write(item);
        if (cont) {
          truths++;
        } else {
          falses++;
        }
      }

      should.strictEqual(falses, 2);
      should.strictEqual(truths, 15);

      t.end(null, done);
    });

    describe('write with parents in batch', function() {
      var name = '_writevWithParentsInBatch';

      // use 24-bit version numbers (base 64)
      var item1 = { h: { id: 'XI', v: 'Aaaa', pa: [] }, b: { some: 'body' } };
      var item2 = { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], pe: 'other' }, b: { more: 'body' } };
      var item3 = { h: { id: 'XI', v: 'Cccc', pa: ['Aaaa', 'Bbbb'] }, b: { more2: 'body' } };

      it('should look in batch if not all parents are in the database yet', function(done) {
        var t = new Tree(db, name, { vSize: 3, log: silence });
        t.write(item1);
        t.write(item2);
        t.write(item3);
        t.end(null, done);
      });
    });

    describe('write with deletes', function() {
      var name = '_writevWithDeletes';

      // use 24-bit version numbers (base 64)
      var item1 = { h: { id: 'XI', v: 'Aaaa', pa: [] }, b: { some: 'body' } };
      var item2 = { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], d: true } };
      var item3 = { h: { id: 'XI', v: 'Cccc', pa: ['Bbbb'] }, b: { some: 'new' } };

      var item4 = { h: { id: 'XI', v: 'Dddd', pa: ['Cccc'], d: true } };
      var item5 = { h: { id: 'XI', v: 'Eeee', pa: ['Dddd'] } };
      var item6 = { h: { id: 'XI', v: 'Ffff', pa: ['Eeee'], d: true } };
      var item7 = { h: { id: 'XI', v: 'Gggg', pa: ['Ffff'] }, b: { som6: 'new' } };

      it('should look in batch for deleted items if new root does not connect', function(done) {
        var t = new Tree(db, name, { vSize: 3, log: silence });
        t.write(item1);
        t.write(item2);
        t.write(item3);
        t.end(null, done);
      });

      it('should look in batch for deleted items if new root does not connect (multiple root + delete in batch)', function(done) {
        var t = new Tree(db, name, { vSize: 3, log: silence });
        t.write(item4);
        t.write(item5);
        t.write(item6);
        t.write(item7);
        t.end(null, done);
      });

      it('should have updated the number of heads with every new root', function(done) {
        var t = new Tree(db, name, { vSize: 3, log: silence });
        var i = 0;
        t.getHeads(function(item, next) {
          i++;
          should.deepEqual(item, { h: { id: 'XI', v: 'Gggg', pa: ['Ffff'], i: 7 }, b: { som6: 'new' } });
          next();
        }, function(err) {
          if (err) { throw err; }
          should.strictEqual(i, 1);
          done();
        });
      });
    });
  });
});
