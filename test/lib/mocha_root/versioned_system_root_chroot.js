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

/*jshint -W068 */

if (process.getuid() !== 0) { 
  console.error('run tests as root');
  process.exit(1);
}

var fs = require('fs');

var should = require('should');

var VersionedSystem = require('../../../lib/versioned_system');

var db, oplogDb, oplogColl;
var databaseName = 'test_versioned_system';
var oplogDatabase = 'local';

var databaseNames = [databaseName, 'foo', 'bar'];
var Database = require('../../_database');

// open database connection
var database = new Database(databaseNames);
before(function(done) {
  database.connect(function(err, dbs) {
    if (err) { throw err; }
    db = dbs[0];
    oplogDb = db.db(oplogDatabase);
    oplogColl = oplogDb.collection('oplog.$main');
    done();
  });
});

after(database.disconnect.bind(database));

describe('VersionedSystem chroot', function() {
  it('should require user to be a string', function() {
    var vs = new VersionedSystem(oplogColl);
    (function() { vs.chroot(); }).should.throw('user must be a string');
  });

  it('should require opts to be an object', function() {
    var vs = new VersionedSystem(oplogColl);
    (function() { vs.chroot('foo', 1); }).should.throw('opts must be an object');
  });

  it('should chroot', function() {
    var vs = new VersionedSystem(oplogColl);

    var ls = fs.readdirSync('/');
    should.strictEqual(true, ls.length > 4);

    vs.chroot('nobody');

    ls = fs.readdirSync('/');
    should.strictEqual(0, ls.length);
    should.strictEqual(true, process.getuid() > 0);
  });
});
