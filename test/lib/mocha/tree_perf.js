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

var crypto = require('crypto');
var cp = require('child_process');

var should = require('should');
var rimraf = require('rimraf');
var level = require('level-packager')(require('leveldown'));
var async = require('async');

var Tree = require('../../../lib/tree');
var logger = require('../../../lib/logger');

var db, cons, silence;
var dbPath = '/tmp/test_tree';

// open database
before(function(done) {
  logger({ console: true, mask: logger.INFO }, function(err, l) {
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
        cp.execFile('du', ['-sh', dbPath], function(err, stdout, stderr) {
          if (err) { throw err; }
          if (stdout) { console.log(stdout); }
          if (stderr) { console.error(stderr); }
          rimraf(dbPath, done);
        });
      });
    });
  });
});

// return array with n items
function genItems(n, gen) {
  var m = [];
  var i = 0;

  while (i++ < n) {
    m.push(gen());
  }

  return m;
}

// write items using an array, wait for each item to finish
function writeItemsSerial(t, items, cb) {
  async.eachSeries(items, function(item, cb2) {
    t.write(item, cb2);
  }, cb);
}

// write items using an array, write in bulk using drain
function writeItemsDrain(t, items, cb) {
  var i = 0;
  function test() {
    return i < items.length;
  }
  async.whilst(test, function(cb2) {
    while (test() && t.write(items[i++])) {
      //process.stdout.write('.');
    }
    if (test()) {
      t.once('drain', cb2);
    } else {
      cb2();
    }
  }, cb);
}

describe('Tree', function() {
  describe('ids with single heads but parents', function() {
    var name = 'siglehead';
    var heads = 0;

    var items;

    it('create 300 items where #heads <= 256', function() {
      var headIds = {};

      // return a random item, if id already exists, fast-forward head
      function nitem() {
        var buf = crypto.randomBytes(6);

        var id = buf[0].toString(16);
        var v  = buf.toString('base64');

        if (!headIds[id]) {
          heads++;
        }
        var pa  = headIds[id] || [];

        headIds[id] = [v];

        return { h: { id: id, v: v, pa: pa }, b: new Buffer(1) };
      }

      items = genItems(300, nitem);
    });

    it('insert these items', function(done) {
      this.timeout(4000);
      var t = new Tree(db, name, { log: silence });
      writeItemsDrain(t, items, function(err) {
        if (err) { throw err; }
        t.on('finish', done);
        t.end();
      });
    });

    it('should have created at least 10 heads for proper testing', function() {
      heads.should.greaterThan(9);
    });

    it('should return all heads in ascending order', function(done) {
      var t = new Tree(db, name, { log: silence });
      var i = 0;

      var prevId = -1;
      t.getHeads(function(item, next) {
        var id = Number('0x' + item.h.id);
        id.should.greaterThan(prevId);
        prevId = id;
        i++;
        next();
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, heads);
        done();
      });
    });
  });

  describe('ids with multiple heads', function() {
    var name = 'multihead';
    var roots = {};
    var heads = {};
    var items;

    it('create 10000 items where some ids have multple heads', function() {
      // return a random item, if id already exists, fork head
      function nitem() {
        var buf = crypto.randomBytes(6);

        var id  = buf[0].toString(16);
        var v   = buf.toString('base64');

        var pa;
        if (!roots[id]) {
          pa = [];
          roots[id] = v;
          heads[v] = true;
        } else {
          // take root as parent
          pa = [roots[id]];
          // and move root head
          delete heads[roots[id]];
          heads[v] = true;
        }

        return { h: { id: id, v: v, pa: pa }, b: new Buffer(1) };
      }

      items = genItems(10000, nitem);
    });

    it('insert these items', function(done) {
      this.timeout(10000);

      var t = new Tree(db, name, { log: silence });
      writeItemsDrain(t, items, function(err) {
        if (err) { throw err; }
        t.on('finish', done);
        t.end();
      });
    });

    it('should have created least 10 roots for proper testing', function() {
      Object.keys(roots).length.should.greaterThan(10);
    });

    it('should have created more heads than roots', function() {
      Object.keys(heads).length.should.greaterThan(Object.keys(roots).length);
    });

    it('should return all heads in non-decreasing order', function(done) {
      var t = new Tree(db, name, { log: silence });
      var i = 0;

      var prevId = 0;
      t.getHeads(function(item, next) {
        var id = Number('0x' + item.h.id);
        id.should.greaterThan(prevId - 1);
        prevId = id;
        i++;
        next();
      }, function(err) {
        if (err) { throw err; }
        should.strictEqual(i, Object.keys(heads).length);
        done();
      });
    });

    it('should find ids where number of heads >1', function(done) {
      var t = new Tree(db, name, { log: silence });

      var prevId;
      var j = 0;
      var k = 0;
      var high = 0;
      t.getHeads(function(item, next) {
        var id = Number('0x' + item.h.id);
        if (id === prevId) {
          k++;
          j++;
          if (j > high) { high = j; }
        } else {
          j = 0;
        }
        prevId = id;
        next();
      }, function(err) {
        if (err) { throw err; }
        console.log('heads with common ids: %s, highest number of heads per id: %s', k, high);
        k.should.greaterThan(10); // with only 256 random bits and 10000 items this should yield a lot more than 10
        done();
      });
    });
  });
});
