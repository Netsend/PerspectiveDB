/**
 * Copyright 2015, 2016 Netsend.
 *
 * This file is part of PerspectiveDB.
 *
 * PerspectiveDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PerspectiveDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PerspectiveDB. If not, see <https://www.gnu.org/licenses/>.
 */

'use strict';

var tmpdir = require('os').tmpdir;

var should = require('should');
var rimraf = require('rimraf');
var level = require('level-packager')(require('leveldown'));

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
  describe('_writev', function() {
    var name = '_writev';

    // use 24-bit version numbers (base 64)
    var item1 = { h: { id: 'XI', v: 'Aaaa', pa: [] }, b: { some: 'body' } };
    var item2 = { h: { id: 'XI', v: 'Bbbb', pa: ['Aaaa'], pe: 'other' }, b: { more: 'body' } };
    var item3 = { h: { id: 'XI', v: 'Cccc', pa: ['Aaaa'] }, b: { more2: 'body' } };
    var item4 = { h: { id: 'XI', v: 'Dddd', pa: ['Cccc'], pe: 'other' }, b: { more3: 'b' } };

    it('should write atomically', function(done) {
      var t = new Tree(db, name, { vSize: 3, log: cons });
      //t.end([item1, item2, item3, item4], done); return;
      t.write(item1);
      t.write(item2);
      t.write(item3);
      t.write(item4);
      t.end(done);
    });
  });
});
