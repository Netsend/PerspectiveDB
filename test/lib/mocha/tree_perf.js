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
var level = require('level');

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

/**
 *
 */
function writeItems(t, items, nitem, cb) {
  var i = 0;
  var j = 0;

  function w() {
    while(i < items && t.write(nitem())) {
      i++;
    }

    // wait if write is paused
    if (i < items) {
      i++; // do skipped i++ from last write
      //console.log('write returned false, wait for drain...');
      t.once('drain', w);
    } else {
      t.end();
    }
  }

  t.on('data', function() {
    j++;
  });

  t.on('finish', function() {
    should.strictEqual(i, items);
    should.strictEqual(j, items);
    cb();
  });

  w();
}

describe('Tree', function() {
  describe('ids with single heads but parents', function() {
    var name = 'siglehead';
    var heads = 0;

    it('insert 300 items where #heads <= 256', function(done) {
      var t = new Tree(db, name, { log: silence });

      var ids = {};

      // return a random item, if id already exists, fast-forward head
      function nitem() {
        var buf = crypto.randomBytes(6);

        var id = buf[0].toString(16);
        var v  = buf.toString('base64');

        if (!ids[id]) {
          heads++;
        }
        var pa  = ids[id] || [];

        ids[id] = [v];

        var item = { h: { id: id, v: v, pa: pa }, b: { some: buf[1].toString(11) } };
        return item;
      }

      writeItems(t, 300, nitem, done);
    });

    it('should have created least 10 heads for proper testing', function() {
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

    it('insert 10000 items where some ids have multple heads', function(done) {
      this.timeout(20000);

      var t = new Tree(db, name, { log: silence });

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

        return { h: { id: id, v: v, pa: pa }, b: { some: buf[1].toString(11) } };
      }

      writeItems(t, 10000, nitem, done);
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
          //console.log('same id as previous', id, j + 1);
        } else {
          j = 0;
        }
        prevId = id;
        next();
      }, function(err) {
        if (err) { throw err; }
        console.log('heads with common ids: %s, highest number of heads per id: %s', k, high);
        k.should.greaterThan(10); // with only 256 random bits and 300 items this should yield a lot more than 10
        done();
      });
    });
  });
});
