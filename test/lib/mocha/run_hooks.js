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

/*jshint -W068,-W116, nonew: false */

var should = require('should');

var runHooks = require('../../../lib/run_hooks');

var db;
var databaseName = 'test_versioned_collection';
var Database = require('../../_database');

// open database connection
var database = new Database(databaseName);
before(function(done) {
  database.connect(function(err, dbc) {
    db = dbc;
    done(err);
  });
});

after(database.disconnect.bind(database));

describe('runHooks', function() {
  it('should accept empty array', function(done) {
    var item = { foo: 'bar' };
    runHooks([], null, item, null, function(err, newItem) {
      if (err) { throw err; }
      should.deepEqual(newItem, { foo: 'bar' });
      done();
    });
  });

  it('should run both hooks', function(done) {
    var item = { foo: 'bar' };
    var hooks = [
      function(db, item, opts, cb) {
        item.hookPassed = true;
        cb(null, item);
      },
      function(db, item, opts, cb) {
        item.secondHook = true;
        cb(null, item);
      }
    ];

    runHooks(hooks, null, item, null, function(err, newItem) {
      if (err) { throw err; }
      should.deepEqual(newItem, {
        foo: 'bar',
        hookPassed: true,
        secondHook: true
      });
      done();
    });
  });

  it('should cancel executing hooks as soon as one filters the item', function(done) {
    var item = { foo: 'bar' };
    var hooks = [
      function(db, item, opts, cb) {
        cb(null, null);
      },
      function(db, item, opts, cb) {
        item.secondHook = true;
        cb(null, item);
      }
    ];

    runHooks(hooks, null, item, null, function(err, newItem) {
      if (err) { throw err; }
      should.deepEqual(newItem, null);
      done();
    });
  });

  it('should pass the options to each hook', function(done) {
    var hooks = [
      function(db, item, opts, cb) {
        should.strictEqual(opts.baz, true);
        cb();
      }
    ];

    var item = { foo: 'bar' };

    runHooks(hooks, null, item, { baz: true }, done);
  });
});
