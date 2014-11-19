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

/*jshint -W068, nonew: false */

// this test needs to use the real oplog, serviced by mongo. please run a separate mongod instance for this.
// the config below expects a mongod instance on localhost port 27019

var should = require('should');
var mongodb = require('mongodb');
var Timestamp = mongodb.Timestamp;
var BSONStream = require('bson-stream');

var OplogReader = require('../../lib/oplog_reader');
var oplogDb, oplogColl;

var db;
var databaseName = 'test_oplog_reader';
var collectionName = 'foo';
var Database = require('../_database');

// open database connection
var database = new Database(databaseName);
before(function(done) {
  database.connect(function(err, dbc) {
    if (err) { throw err; }
    db = dbc;
    oplogDb = db.db('local');
    oplogColl = oplogDb.collection('oplog.$main');
    done(err);
  });
});

after(database.disconnect.bind(database));

describe('OplogReader', function() {
  describe('constructor', function() {
    it('should require oplogColl to be an instance of mongdb.Collection', function() {
      (function() { new OplogReader(); }).should.throw('oplogColl must be an instance of mongodb.Collection');
    });

    it('should require ns to be a string', function() {
      (function() { new OplogReader(oplogColl); }).should.throw('ns must be a string');
    });

    it('should require ns to contain a dot', function() {
      (function() { new OplogReader(oplogColl, ''); }).should.throw('ns must contain at least two parts');
    });

    it('should require ns to contain a database name', function() {
      (function() { new OplogReader(oplogColl, '.'); }).should.throw('ns must contain a database name');
    });

    it('should require ns to contain a collection name', function() {
      (function() { new OplogReader(oplogColl, 'foo.'); }).should.throw('ns must contain a collection name');
    });

    it('should require opts to be an object', function() {
      (function() { new OplogReader(oplogColl, 'foo.bar', ''); }).should.throw('opts must be an object');
    });

    it('should require opts.filter to be an object', function() {
      (function() { new OplogReader(oplogColl, 'foo.bar', { filter: '' }); }).should.throw('opts.filter must be an object');
    });

    it('should require opts.offset to be an instance of mongodb.Timestamp', function() {
      (function() { new OplogReader(oplogColl, 'foo.bar', { offset: '' }); }).should.throw('opts.offset must be an instance of mongodb.Timestamp');
    });

    it('should require opts.includeOffset to be a boolean', function() {
      (function() { new OplogReader(oplogColl, 'foo.bar', { includeOffset: '' }); }).should.throw('opts.includeOffset must be a boolean');
    });

    it('should require opts.tailable to be a boolean', function() {
      (function() { new OplogReader(oplogColl, 'foo.bar', { tailable: '' }); }).should.throw('opts.tailable must be a boolean');
    });

    it('should require opts.tailableRetryInterval to be a number', function() {
      (function() { new OplogReader(oplogColl, 'foo.bar', { tailableRetryInterval: '' }); }).should.throw('opts.tailableRetryInterval must be a number');
    });

    it('should require opts.debug to be a boolean', function() {
      (function() { new OplogReader(oplogColl, 'foo.bar', { debug: '' }); }).should.throw('opts.debug must be a boolean');
    });

    it('should require opts.hide to be a boolean', function() {
      (function() { new OplogReader(oplogColl, 'foo.bar', { hide: '' }); }).should.throw('opts.hide must be a boolean');
    });

    var ns = databaseName + '.' + collectionName;
    it('should construct', function(done) {
      var or = new OplogReader(oplogColl, ns);
      // needs a data handler resume() to start flowing and needs to flow before an end will be emitted
      or.resume();
      or.on('end', done);
    });

    var offset;
    it('needs some data for further testing', function(done) {
      offset = new Timestamp(0, (new Date()).getTime() / 1000);
      var items = [{ foo: 'bar' }, { foo: 'baz' }];
      db.collection(collectionName).insert(items, done);
    });

    it('should emit previously inserted items from reading the oplog after offset', function(done) {
      var or = new OplogReader(oplogColl, ns, { offset: offset });
      var i = 0;
      or.pipe(new BSONStream()).on('data', function(obj) {
        should.strictEqual(obj.op, 'i');
        should.strictEqual(obj.ns, 'test_oplog_reader.foo');
        i++;
      });
      or.on('end', function() {
        should.strictEqual(i, 2);
        done();
      });
    });

    it('should tail and not close', function(done) {
      var or = new OplogReader(oplogColl, ns, { offset: offset, tailable: true });
      var i = 0;
      or.on('data', function() {
        i++;
        if (i > 1) {
          done();
        }
      });
      or.on('end', function() { throw new Error('should not close'); });
    });

    it('should exclude offset by default', function(done) {
      oplogColl.find({ ns: ns }, { ts: true }, { limit: 2, sort: { $natural: -1 } }).toArray(function(err, items) {
        if (err) { throw err; }

        var or = new OplogReader(oplogColl, ns, { offset: items[1].ts });
        var i = 0;
        or.on('data', function() { i++; });
        or.on('end', function() {
          should.strictEqual(i, 1);
          done();
        });
      });
    });

    it('should include offset', function(done) {
      oplogColl.find({ ns: ns }, { ts: true }, { limit: 2, sort: { $natural: -1 } }).toArray(function(err, items) {
        if (err) { throw err; }

        var or = new OplogReader(oplogColl, ns, { offset: items[1].ts, includeOffset: true });
        var i = 0;
        or.on('data', function() { i++; });
        or.on('end', function() {
          should.strictEqual(i, 2);
          done();
        });
      });
    });
  });
});
