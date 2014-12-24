/**
 * Copyright 2014 Netsend.
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

/*jshint -W068, -W030, nonew: false */

//var should = require('should');
require('should');

var Transform = require('../../../lib/transform');

var db;
var databaseName = 'test_transform';
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

describe('Transform', function() {
  describe('constructor', function() {
    it('should require db to be a mongodb.Db', function() {
      (function() { new Transform({}); }).should.throw('db must be an instance of mongodb.Db');
    });

    it('should require hooks to be an array', function() {
      (function() { new Transform(db); }).should.throw('hooks must be an array');
    });

    it('should construct', function() {
      new Transform(db, []);
    });
  });
});
