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
var mongodb = require('mongodb');
var BSONStream = require('bson-stream');

var Timestamp = mongodb.Timestamp;
var VersionedSystem = require('../../lib/versioned_system');

var db, db2, oplogDb, oplogColl;
var databaseName = 'test_versioned_system';
var databaseName2 = 'test2';
var oplogDatabase = 'local';

var databaseNames = [databaseName, databaseName2, 'foo', 'bar'];
var Database = require('../_database');

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

describe('VersionedSystem', function() {
  describe('initVCs root', function() {
    it('needs two documents in collection for further testing', function(done) {
      // insert two new documents in the collection and see if they're versioned using a
      // rebuild (one of which is already versioned). if a new snapshot collection is
      // created, rebuild should maintain original versions.
      var docs = [{ foo: 'a' }, { bar: 'b', _v: 'qux' }];
      db2.collection('someColl').insert(docs, done);
    });

    it('should rebuild a new snapshot collection', function(done) {
      var vcCfg = {
        test2: {
          someColl: {
            dbPort: 27019,
            debug: false,
            autoProcessInterval: 50,
            size: 1
          }
        }
      };

      var vs = new VersionedSystem(oplogColl, { debug: false });
      vs.initVCs(vcCfg, function(err, oplogReaders) {
        if (err) { throw err; }

        should.strictEqual(Object.keys(oplogReaders).length, 1);

        // oplog reader should never end
        var or = oplogReaders['test2.someColl'];
        or.on('end', function() { throw new Error('oplog reader closed'); });

        // should detect two updates in test2.someColl and set ackd
        var i = 0;
        or.on('data', function() {
          i++;
          if (i >= 2) {
            // check if items are ackd, but give vc some time to process oplog items first
            setTimeout(function() {
              db2.collection('m3.someColl').find().toArray(function(err, items) {
                if (err) { throw err; }
                should.strictEqual(items.length, 2);
                delete items[0]._id._id;
                delete items[0]._id._v;
                delete items[0]._m3._op;
                delete items[1]._id._id;
                delete items[1]._m3._op;

                should.deepEqual(items[0], {
                  foo: 'a',
                    _id: {
                      _co: 'someColl',
                      _pe: '_local',
                      _pa: [],
                      _lo: true,
                      _i: 1
                    },
                 _m3: { _ack: true }
                });
                should.deepEqual(items[1], {
                  bar: 'b',
                  _id: {
                    _co: 'someColl',
                    _v: 'qux',
                    _pe: '_local',
                    _pa: [],
                    _lo: true,
                    _i: 2
                  },
                _m3: { _ack: true }
                });
                done();
              });
            }, 60);
          }
        });
      });
    });
  });

  describe('info', function() {
    var collName  = 'info';
    var oplogCollName = '_infoOplog';
    var localOplogColl;

    it('needs a capped collection', function(done) {
      database.createCappedColl(oplogCollName, done);
    });

    it('needs an artificial oplog for testing', function(done) {
      var op1 = { ts: new Timestamp(2, 3) };
      var op2 = { ts: new Timestamp(40, 50) };
      var op3 = { ts: new Timestamp(600, 700) };

      localOplogColl = db.collection(oplogCollName);
      localOplogColl.insert([op1, op2, op3], done);
    });

    it('needs a collection with one object and a collection without objects', function(done) {
      var item = { foo: 'bar' };
      var snapshotItem = { foo: 'bar', _m3: { _op: new Timestamp(8000, 9000) } };
      var cfg = {};
      cfg[databaseName] = {};
      cfg[databaseName][collName] = { size: 1 };

      var vs = new VersionedSystem(localOplogColl);
      vs.initVCs(cfg, function(err) {
        if (err) { throw err; }
        db.collection('m3.' + collName).insert(snapshotItem, function(err) {
          if (err) { throw err; }
          db.collection(collName).insert(item, done);
        });
      });
    });

    it('should show info of the collection and the snapshot collection', function(done) {
      var ns = databaseName + '.' + collName;
      var cfg = {};
      cfg[databaseName] = {};
      cfg[databaseName][collName] = { size: 1 };

      var vs = new VersionedSystem(localOplogColl, { hide: true });
      vs.initVCs(cfg, function(err) {
        if (err) { throw err; }
        vs.info(function(err, result) {
          if (err) { throw err; }
          should.strictEqual(result[ns].collection.count, 1);
          should.strictEqual(result[ns].collection.capped, undefined);
          should.strictEqual(result[ns].snapshotCollection.count, 1);
          should.strictEqual(result[ns].snapshotCollection.capped, true);
          done();
        });
      });
    });

    it('should show extended info of the collection and the snapshot collection', function(done) {
      var ns = databaseName + '.' + collName;
      var cfg = {};
      cfg[databaseName] = {};
      cfg[databaseName][collName] = { size: 1 };

      var vs = new VersionedSystem(localOplogColl, { hide: true });
      vs.initVCs(cfg, function(err) {
        if (err) { throw err; }
        vs.info(true, function(err, result) {
          should.equal(err, null);
          should.strictEqual(result[ns].collection.count, 1);
          should.strictEqual(result[ns].collection.capped, undefined);
          should.strictEqual(result[ns].snapshotCollection.count, 1);
          should.strictEqual(result[ns].snapshotCollection.capped, true);
          should.strictEqual(result[ns].extended.ack, 0);
          done();
        });
      });
    });
  });

  // do these tests at last because chroot can be called only once
  describe('createServer', function() {
    it('should require user to be a string', function() {
      var vs = new VersionedSystem(oplogColl);
      (function() { vs.createServer(); }).should.throw('user must be a string');
    });

    it('should require newRoot to be a string', function() {
      var vs = new VersionedSystem(oplogColl);
      (function() { vs.createServer('foo', 1); }).should.throw('newRoot must be a string');
    });

    it('needs a valid user account for further testing', function(done) {
      // i.e. "username" : "foo", "password" : "$2a$10$g.TOamyToPM37K43CDL.tuaYsUc5AnYBOVBKbhV6eeO3/E0u6XN0W", "realm" : "test2"
      // password = 'secr3t';
      db.collection('users').insert({
        username: 'foo',
        password: '$2a$10$g.TOamyToPM37K43CDL.tuaYsUc5AnYBOVBKbhV6eeO3/E0u6XN0W',
        realm: 'test2'
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

    it('needs an object in the vc test2.baz for further testing', function(done) {
      var obj1 = { _id: 'X' };
      var obj2 = { _id: 'A', baz: 'A' };
      db.db('test2').collection('baz').insert([obj1, obj2], done);
    });

    it('should require initVCs() first', function() {
      var vs = new VersionedSystem(oplogColl);
      (function() { vs.createServer('nobody', '/var/run', { serverConfig: { port: 1234 } }, function(err) { if (err) { throw err; } }); }).should.throw('run initVCs first');
    });

    it('should chroot, disconnect invalid auth request and auth valid auth requests', function(done) {
      var ls = fs.readdirSync('/');
      should.strictEqual(true, ls.length > 4);

      // remove any previously created socket
      if (fs.existsSync('/var/run/ms-1234.sock')) {
        fs.unlink('/var/run/ms-1234.sock');
      }

      var cfg = {
        test2: {
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
        vs.createServer('nobody', '/var/run', { serverConfig: { port: 1234 } }, function(err) {
          if (err) { throw err; }

          should.strictEqual(true, fs.existsSync('/ms-1234.sock'));
          should.strictEqual(true, process.getuid() > 0);

          // should disconnect auth request because of invalid password
          var authReq = {
            username: 'foo',
            password: 'bar',
            database: 'test2',
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
              database: 'test2',
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

  // do these tests at last
  /*
  tested via createServer
  describe('chroot', function() {
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
  */
});
