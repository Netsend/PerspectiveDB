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

var cp = require('child_process');
var tmpdir = require('os').tmpdir;

require('should');
var rimraf = require('rimraf');
var level = require('level-packager')(require('leveldown'));

var db;
var dbPath = tmpdir() + '/test_level_max';

// open database
before(function(done) {
  rimraf(dbPath, function(err) {
    if (err) { throw err; }
    db = level(dbPath, { keyEncoding: 'binary', valueEncoding: 'binary' });
    done();
  });
});

after(function(done) {
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

describe('levelmax', function() {
  var seconds = 2;
  var nritems = 50000;

  describe('single', function() {
    it('insert 1 byte keys for ' + seconds + ' seconds (timeout)', function(done) {
      this.timeout((seconds + 1) * 1000);

      var run = true;
      setTimeout(function() {
        run = false;
      }, seconds * 1000);

      var i = 0;
      function write() {
        i++;
        db.put(new Buffer(1), null, function(err) {
          if (err) { throw err; }
          if (run) {
            write();
          } else {
            console.log('%d keys inserted in %d seconds (%d items per second)', i, seconds, i / seconds);
            // expect at least 10.000 items per seconds
            i.should.greaterThan(seconds * 10000);
            done();
          }
        });
      }

      write();
    });

    it('insert 30 byte keys with 1 kilo-byte values for ' + seconds + ' seconds', function(done) {
      this.timeout((seconds + 1) * 1000);

      var run = true;
      setTimeout(function() {
        run = false;
      }, seconds * 1000);

      var i = 0;
      function write() {
        i++;
        db.put(new Buffer(30), new Buffer(1024), function(err) {
          if (err) { throw err; }
          if (run) {
            write();
          } else {
            console.log('%d keys inserted in %d seconds (%d items per second)', i, seconds, i / seconds);
            // expect at least 10.000 items per seconds
            i.should.greaterThan(seconds * 10000);
            done();
          }
        });
      }

      write();
    });

    it('insert 30 byte keys with 1 kilo-byte values for ' + seconds + ' seconds (fsync)', function(done) {
      this.timeout((seconds + 1) * 1000);

      var run = true;
      setTimeout(function() {
        run = false;
      }, seconds * 1000);

      var i = 0;
      function write() {
        i++;
        db.put(new Buffer(30), new Buffer(1024), { sync: true }, function(err) {
          if (err) { throw err; }
          if (run) {
            write();
          } else {
            console.log('%d keys inserted in %d seconds (%d items per second)', i, seconds, i / seconds);
            // expect at least 5.000 items per seconds
            i.should.greaterThan(seconds * 5000);
            done();
          }
        });
      }

      write();
    });
  });

  describe('bulk', function() {
    var items = [];

    it('create ' + nritems + ' items for bulk insert', function() {
      for (var i = 0; i < nritems; i++) {
        items.push({ type: 'put', key: new Buffer(30), value: new Buffer(1024) });
      }
    });

    it('items of 30 byte keys with 1 kilo-byte values for ' + seconds + ' seconds', function(done) {
      this.timeout((seconds + 1) * 1000);

      var run = true;
      setTimeout(function() {
        run = false;
      }, seconds * 1000);

      var i = 0;
      function write() {
        i++;
        db.batch(items, function(err) {
          if (err) { throw err; }
          if (run) {
            write();
          } else {
            console.log('%d keys inserted in %d seconds (%d items per second)', i * nritems, seconds, i * nritems / seconds);
            // expect at least 5.000 items per seconds
            (i * nritems).should.greaterThan(seconds * 5000);
            done();
          }
        });
      }

      write();
    });

    it('insert items of 30 byte keys with 1 kilo-byte values for ' + seconds + ' seconds (fsync)', function(done) {
      this.timeout((seconds + 1) * 1000);

      var run = true;
      setTimeout(function() {
        run = false;
      }, seconds * 1000);

      var i = 0;
      function write() {
        i++;
        db.batch(items, { sync: true }, function(err) {
          if (err) { throw err; }
          if (run) {
            write();
          } else {
            console.log('%d keys inserted in %d seconds (%d items per second)', i * nritems, seconds, i * nritems / seconds);
            // expect at least 5.000 items per seconds
            (i * nritems).should.greaterThan(seconds * 5000);
            done();
          }
        });
      }

      write();
    });
  });
});
