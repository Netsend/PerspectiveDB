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
var net = require('net');

var should = require('should');
var BSONStream = require('bson-stream');

var VersionedSystem = require('../../../lib/versioned_system');

var db, db2, oplogDb, oplogColl;
var databaseName = 'test_versioned_system_root_listen';
var databaseName2 = 'test2_versioned_system_root_listen';
var oplogDatabase = 'local';

var databaseNames = [databaseName, databaseName2, 'foo', 'bar'];
var Database = require('../../_database');

// open database connection
var database = new Database(databaseNames);
before(function(done) {
  database.connect(function(err, dbs) {
    if (err) { throw err; }
    db = dbs[0];
    db2 = dbs[1];
    oplogDb = db.db(oplogDatabase);
    oplogColl = oplogDb.collection('oplog.$main');
    done();
  });
});

after(database.disconnect.bind(database));

describe('VersionedSystem listen', function() {
  it('should require user to be a string', function() {
    var vs = new VersionedSystem(oplogColl);
    (function() { vs.listen(); }).should.throw('user must be a string');
  });

  it('should require newRoot to be a string', function() {
    var vs = new VersionedSystem(oplogColl);
    (function() { vs.listen('foo', 1); }).should.throw('newRoot must be a string');
  });

  it('needs a valid user account for further testing', function(done) {
    // i.e. "username" : "foo", "password" : "$2a$10$g.TOamyToPM37K43CDL.tuaYsUc5AnYBOVBKbhV6eeO3/E0u6XN0W", "realm" : "test2"
    // password = 'secr3t';
    db.collection('users').insert({
      username: 'foo',
      password: '$2a$10$g.TOamyToPM37K43CDL.tuaYsUc5AnYBOVBKbhV6eeO3/E0u6XN0W',
      realm: 'test2_versioned_system_root_listen'
    }, done);
  });

  it('needs a valid replication config for further testing', function(done) {
    var cfg = {
      type: 'export',
      remote: 'foo',
      collections: {
        baz: {
          filter: { baz: 'A' }
        }
      }
    };
    db.collection('replication').insert(cfg, done);
  });

  it('needs an object in the vc test2_versioned_system_root_listen.baz for further testing', function(done) {
    var obj1 = { _id: 'X' };
    var obj2 = { _id: 'A', baz: 'A' };
    db.db('test2_versioned_system_root_listen').collection('baz').insert([obj1, obj2], done);
  });

  it('should require initVCs() first', function() {
    var vs = new VersionedSystem(oplogColl);
    (function() { vs.listen('nobody', '/var/run', { serverConfig: { port: 1234 } }, function(err) { if (err) { throw err; } }); }).should.throw('run initVCs first');
  });

  it('should chroot, disconnect invalid auth request and auth valid auth requests', function(done) {
    var ls = fs.readdirSync('/');
    should.strictEqual(true, ls.length > 4);

    // remove any previously created socket
    if (fs.existsSync('/var/run/ms-1234.sock')) {
      fs.unlink('/var/run/ms-1234.sock');
    }

    var cfg = {
      'test2_versioned_system_root_listen': {
        baz: {
          dbPort: 27019,
          debug: false,
          autoProcessInterval: 100,
          size: 1
        }
      }
    };

    var vs = new VersionedSystem(oplogColl, { usersDb: db.databaseName, replicationDb: db.databaseName, debug: false });
    vs.initVCs(cfg, true, function(err) {
      if (err) { throw err; }

      // should chroot
      vs.listen('nobody', '/var/run', { serverConfig: { port: 1234 } }, function(err) {
        if (err) { throw err; }

        should.strictEqual(true, fs.existsSync('/ms-1234.sock'));
        should.strictEqual(true, process.getuid() > 0);

        // should disconnect auth request because of invalid password
        var authReq = {
          username: 'foo',
          password: 'bar',
          database: 'test2_versioned_system_root_listen',
          collection: 'baz'
        };

        // write auth request
        var ms = net.createConnection('/ms-1234.sock');
        ms.write(JSON.stringify(authReq) + '\n');

        ms.on('data', function(data) {
          should.strictEqual(data.toString(), 'invalid auth request\n');

          // should not disconnect a valid auth request
          var authReq2 = {
            username: 'foo',
            password: 'secr3t',
            database: 'test2_versioned_system_root_listen',
            collection: 'baz'
          };

          // write a new auth request
          var ms2 = net.createConnection('/ms-1234.sock');
          ms2.write(JSON.stringify(authReq2) + '\n');

          ms2.pipe(new BSONStream()).on('data', function(obj) {
            delete obj._id._v;
            delete obj._m3._op;
            should.deepEqual(obj, {
              _id: {
                _co: 'baz',
                _id: 'A',
                _pa: []
              },
              baz: 'A',
              _m3: { }
            });
            done();
          });
        });
      });
    });
  });
});
