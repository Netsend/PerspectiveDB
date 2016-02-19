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

var OplogTransform = require('../../../adapter/mongo/oplog_transform');
var logger = require('../../../lib/logger');

var silence, cons;
var oplogDb, oplogColl;
var oplogCollName = 'oplog.$main';

var db;
var databaseName = 'test_oplog_transform';
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
        oplogColl = oplogDb.collection(oplogCollName);
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

describe('OplogTransform', function() {
  var ns = databaseName + '.' + collectionName;

  describe('constructor', function() {
    it('should require oplogDb to be an object', function() {
      (function() { new OplogTransform(); }).should.throw('oplogDb must be an object');
    });

    it('should require oplogCollName to be a non-empty string', function() {
      (function() { new OplogTransform(oplogDb); }).should.throw('oplogCollName must be a non-empty string');
    });

    it('should require ns to be a non-empty string', function() {
      (function() { new OplogTransform(oplogDb, oplogCollName, ''); }).should.throw('ns must be a non-empty string');
    });

    it('should require reqChan to be an object', function() {
      (function() { new OplogTransform(oplogDb, oplogCollName, ' '); }).should.throw('reqChan must be an object');
    });

    it('should require ns to contain a dot', function() {
      (function() { new OplogTransform(oplogDb, oplogCollName, ' ', {}); }).should.throw('ns must contain at least two parts');
    });

    it('should require ns to contain a database name', function() {
      (function() { new OplogTransform(oplogDb, oplogCollName, '.', {}); }).should.throw('ns must contain a database name');
    });

    it('should require ns to contain a collection name', function() {
      (function() { new OplogTransform(oplogDb, oplogCollName, 'foo.', {}); }).should.throw('ns must contain a collection name');
    });

    it('should require opts to be an object', function() {
      (function() { new OplogTransform(oplogDb, oplogCollName, 'foo.bar', {}, 1); }).should.throw('opts must be an object');
    });

    it('should construct', function() {
      var ot = new OplogTransform(oplogDb, oplogCollName, 'foo.bar', {});
    });
  });

  describe('_oplogReader', function() {
    it('should require opts.filter to be an object', function() {
      var ot = new OplogTransform(oplogDb, oplogCollName, 'foo.bar', {}, { log: silence });
      (function() { ot._oplogReader({ filter: '' }); }).should.throw('opts.filter must be an object');
    });

    it('should require opts.offset to be an object', function() {
      var ot = new OplogTransform(oplogDb, oplogCollName, 'foo.bar', {}, { log: silence });
      (function() { ot._oplogReader({ offset: '' }); }).should.throw('opts.offset must be an object');
    });

    it('should require opts.bson to be a boolean', function() {
      var ot = new OplogTransform(oplogDb, oplogCollName, 'foo.bar', {}, { log: silence });
      (function() { ot._oplogReader({ bson: '' }); }).should.throw('opts.bson must be a boolean');
    });

    it('should require opts.includeOffset to be a boolean', function() {
      var ot = new OplogTransform(oplogDb, oplogCollName, 'foo.bar', {}, { log: silence });
      (function() { ot._oplogReader({ includeOffset: '' }); }).should.throw('opts.includeOffset must be a boolean');
    });

    it('should require opts.tailable to be a boolean', function() {
      var ot = new OplogTransform(oplogDb, oplogCollName, 'foo.bar', {}, { log: silence });
      (function() { ot._oplogReader({ tailable: '' }); }).should.throw('opts.tailable must be a boolean');
    });

    it('should require opts.tailableRetryInterval to be a number', function() {
      var ot = new OplogTransform(oplogDb, oplogCollName, 'foo.bar', {}, { log: silence });
      (function() { ot._oplogReader({ tailableRetryInterval: '' }); }).should.throw('opts.tailableRetryInterval must be a number');
    });

    it('should construct and start reading', function(done) {
      var ot = new OplogTransform(oplogDb, oplogCollName, 'foo.bar', {}, { log: silence });
      var or = ot._oplogReader({ log: silence });
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
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, {}, { log: silence });
      var or = ot._oplogReader({ offset: offset });
      var i = 0;
      or.pipe(new BSONStream()).on('data', function(obj) {
        should.strictEqual(obj.op, 'i');
        should.strictEqual(obj.ns, 'test_oplog_transform.foo');
        i++;
      });
      or.on('end', function() {
        should.strictEqual(i, 2);
        done();
      });
    });

    it('should pause and resume', function(done) {
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, {}, { log: silence });
      var or = ot._oplogReader({ offset: offset });
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

    it('should tail and not manually close', function(done) {
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, {}, { log: silence });
      var or = ot._oplogReader({ offset: offset, tailable: true, tailableRetryInterval: 10 });

      var closeCalled = false;
      or.on('end', function() {
        should.strictEqual(closeCalled, true);
        done();
      });

      var i = 0;
      or.on('data', function() {
        i++;
        if (i >= 2) {
          setTimeout(function() {
            closeCalled = true;
            or.close();
          }, 20);
        }
      });
    });

    it('should exclude offset by default', function(done) {
      oplogColl.find({ ns: ns }, { ts: true }, { limit: 2, sort: { $natural: -1 } }).toArray(function(err, items) {
        if (err) { throw err; }

        var ot = new OplogTransform(oplogDb, oplogCollName, ns, {}, { log: silence });
        var or = ot._oplogReader({ offset: items[1].ts });
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

        var ot = new OplogTransform(oplogDb, oplogCollName, ns, {}, { log: silence });
        var or = ot._oplogReader({ offset: items[1].ts, includeOffset: true });
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
