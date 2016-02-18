/**
 * Copyright 2014-2016 Netsend.
 *
 * This file is part of PersDB.
 *
 * PersDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PersDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PersDB. If not, see <https://www.gnu.org/licenses/>.
 */

'use strict';

/*jshint -W068, nonew: false */

// this test needs to use the real oplog, serviced by mongo. please run a separate mongod instance for this.
// the config below expects a mongod instance on localhost port 27019

var should = require('should');
var mongodb = require('mongodb');
var Timestamp = mongodb.Timestamp;
var BSONStream = require('bson-stream');

var oplogReader = require('../../../adapter/mongo/oplog_reader');
var logger = require('../../../lib/logger');

var silence, cons;
var oplogDb, oplogColl;

var db;
var databaseName = 'test_oplog_reader';
var collectionName = 'foo';
var Database = require('./_database');

// open database connection
var database = new Database(databaseName);
before(function(done) {
  logger({ silence: true }, function(err, l) {
    if (err) { throw err; }
    silence = l;
    logger({ console: true, mask: logger.DEBUG2 }, function(err, l) {
      if (err) { throw err; }
      cons = l;
      database.connect(function(err, dbc) {
        if (err) { throw err; }
        db = dbc;
        oplogDb = db.db('local');
        oplogColl = oplogDb.collection('oplog.$main');
        done(err);
      });
    });
  });
});

after(function(done) {
  silence.close(function(err) {
    if (err) { throw err; }
    database.disconnect(done);
  });
});

describe('oplogReader', function() {
  var ns = databaseName + '.' + collectionName;

  describe('constructor', function() {
    it('should require oplogColl to be an object', function() {
      (function() { oplogReader(); }).should.throw('oplogColl must be an object');
    });

    it('should require ns to be a string', function() {
      (function() { oplogReader(oplogColl); }).should.throw('ns must be a string');
    });

    it('should require ns to contain a dot', function() {
      (function() { oplogReader(oplogColl, ''); }).should.throw('ns must contain at least two parts');
    });

    it('should require ns to contain a database name', function() {
      (function() { oplogReader(oplogColl, '.'); }).should.throw('ns must contain a database name');
    });

    it('should require ns to contain a collection name', function() {
      (function() { oplogReader(oplogColl, 'foo.'); }).should.throw('ns must contain a collection name');
    });

    it('should require opts to be an object', function() {
      (function() { oplogReader(oplogColl, 'foo.bar', 1); }).should.throw('opts must be an object');
    });

    it('should require opts.filter to be an object', function() {
      (function() { oplogReader(oplogColl, 'foo.bar', { filter: '' }); }).should.throw('opts.filter must be an object');
    });

    it('should require opts.offset to be an object', function() {
      (function() { oplogReader(oplogColl, 'foo.bar', { offset: '' }); }).should.throw('opts.offset must be an object');
    });

    it('should require opts.includeOffset to be a boolean', function() {
      (function() { oplogReader(oplogColl, 'foo.bar', { includeOffset: '' }); }).should.throw('opts.includeOffset must be a boolean');
    });

    it('should require opts.tailable to be a boolean', function() {
      (function() { oplogReader(oplogColl, 'foo.bar', { tailable: '' }); }).should.throw('opts.tailable must be a boolean');
    });

    it('should require opts.tailableRetryInterval to be a number', function() {
      (function() { oplogReader(oplogColl, 'foo.bar', { tailableRetryInterval: '' }); }).should.throw('opts.tailableRetryInterval must be a number');
    });

    it('should require opts.log to be an object', function() {
      (function() { oplogReader(oplogColl, 'foo.bar', { log: '' }); }).should.throw('opts.log must be an object');
    });

    it('should construct and start reading', function(done) {
      var or = oplogReader(oplogColl, ns, { log: silence });
      // move to the end by repeatedly calling read
      or.on('readable', function() {
        while (or.read()) {}
      });
      or.on('end', done);
    });

    var offset;
    it('needs some data for further testing', function(done) {
      offset = new Timestamp(0, (new Date()).getTime() / 1000);
      var items = [{ foo: 'bar' }, { foo: 'baz' }];
      db.collection(collectionName).insert(items, done);
    });

    it('should emit previously inserted items from reading the oplog after offset', function(done) {
      var or = new oplogReader(oplogColl, ns, { offset: offset, log: silence });
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

    it('should pause and resume', function(done) {
      var or = oplogReader(oplogColl, ns, { offset: offset, log: silence });
      var i = 0;
      function errHandler() {
        throw new Error('should not execute');
      }
      or.pause();
      or.on('data', function() {
        // pause and register error handler
        or.pause();
        or.on('data', errHandler);

        // resume after a while
        setTimeout(function() {
          or.removeListener('data', errHandler);
          or.resume();
        }, 100);

        i++;
      });

      or.on('end', function() {
        should.strictEqual(i, 2);
        done();
      });

      or.resume();
    });

    it('should tail and not close', function(done) {
      var or = oplogReader(oplogColl, ns, { offset: offset, tailable: true, log: silence });
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

        var or = oplogReader(oplogColl, ns, { offset: items[1].ts, log: silence });
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

        var or = oplogReader(oplogColl, ns, { offset: items[1].ts, includeOffset: true, log: silence });
        var i = 0;
        or.on('data', function() { i++; });
        or.on('end', function() {
          should.strictEqual(i, 2);
          done();
        });
      });
    });
  });

  describe('close', function() {
    it('should close', function(done) {
      var or = oplogReader(oplogColl, ns, { tailable: true, log: silence });

      or.on('end', done);
      or.resume();
      or.close();
    });
  });

});
