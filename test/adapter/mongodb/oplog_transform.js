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
var MongoClient = mongodb.MongoClient;
var BSONStream = require('bson-stream');
var bson = require('bson');
var LDJSONStream = require('ld-jsonstream');
var through2 = require('through2');

var OplogTransform = require('../../../adapter/mongodb/oplog_transform');
var logger = require('../../../lib/logger');

var config = require('./config.json');

var BSON = new bson.BSONPure.BSON();

var silence, cons;
var oplogDb, oplogColl;
var oplogCollName = 'oplog.$main';

var db;
var databaseName = 'test_oplog_transform';

// open database connection
before(function(done) {
  logger({ silence: true }, function(err, l) {
    if (err) { throw err; }
    silence = l;
    logger({ console: true, mask: logger.DEBUG2 }, function(err, l) {
      if (err) { throw err; }
      cons = l;
      MongoClient.connect(config.url, function(err, dbc) {
        if (err) { throw err; }
        db = dbc.db(databaseName);
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
    //db.close(done); // not sure why the db already seems to be closed
    done();
  });
});

describe('OplogTransform', function() {
  describe('constructor', function() {
    var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
    var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });

    it('should require oplogDb to be an object', function() {
      (function() { new OplogTransform(); }).should.throw('oplogDb must be an object');
    });

    it('should require oplogCollName to be a non-empty string', function() {
      (function() { new OplogTransform(oplogDb); }).should.throw('oplogCollName must be a non-empty string');
    });

    it('should require ns to be a non-empty string', function() {
      (function() { new OplogTransform(oplogDb, oplogCollName, ''); }).should.throw('ns must be a non-empty string');
    });

    it('should require controlWrite to be an object', function() {
      (function() { new OplogTransform(oplogDb, oplogCollName, ' '); }).should.throw('controlWrite must be an object');
    });

    it('should require controlRead to be an object', function() {
      (function() { new OplogTransform(oplogDb, oplogCollName, ' ', controlWrite); }).should.throw('controlRead must be an object');
    });

    it('should require ns to contain a dot', function() {
      (function() { new OplogTransform(oplogDb, oplogCollName, ' ', controlWrite, controlRead); }).should.throw('ns must contain at least two parts');
    });

    it('should require ns to contain a database name', function() {
      (function() { new OplogTransform(oplogDb, oplogCollName, '.', controlWrite, controlRead); }).should.throw('ns must contain a database name');
    });

    it('should require ns to contain a collection name', function() {
      (function() { new OplogTransform(oplogDb, oplogCollName, 'foo.', controlWrite, controlRead); }).should.throw('ns must contain a collection name');
    });

    it('should require opts to be an object', function() {
      (function() { new OplogTransform(oplogDb, oplogCollName, 'foo.bar', controlWrite, controlRead, 1); }).should.throw('opts must be an object');
    });

    it('should construct', function() {
      new OplogTransform(oplogDb, oplogCollName, 'foo.bar', controlWrite, controlRead);
    });
  });

  describe('_oplogReader', function() {
    var collectionName = 'foo';
    var ns = databaseName + '.' + collectionName;

    it('should require opts.filter to be an object', function() {
      var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var ot = new OplogTransform(oplogDb, oplogCollName, 'foo.bar', controlWrite, controlRead, { log: silence });
      (function() { ot._oplogReader({ filter: '' }); }).should.throw('opts.filter must be an object');
    });

    it('should require opts.offset to be an object', function() {
      var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var ot = new OplogTransform(oplogDb, oplogCollName, 'foo.bar', controlWrite, controlRead, { log: silence });
      (function() { ot._oplogReader({ offset: '' }); }).should.throw('opts.offset must be an object');
    });

    it('should require opts.bson to be a boolean', function() {
      var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var ot = new OplogTransform(oplogDb, oplogCollName, 'foo.bar', controlWrite, controlRead, { log: silence });
      (function() { ot._oplogReader({ bson: '' }); }).should.throw('opts.bson must be a boolean');
    });

    it('should require opts.includeOffset to be a boolean', function() {
      var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var ot = new OplogTransform(oplogDb, oplogCollName, 'foo.bar', controlWrite, controlRead, { log: silence });
      (function() { ot._oplogReader({ includeOffset: '' }); }).should.throw('opts.includeOffset must be a boolean');
    });

    it('should require opts.tailable to be a boolean', function() {
      var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var ot = new OplogTransform(oplogDb, oplogCollName, 'foo.bar', controlWrite, controlRead, { log: silence });
      (function() { ot._oplogReader({ tailable: '' }); }).should.throw('opts.tailable must be a boolean');
    });

    it('should require opts.tailableRetryInterval to be a number', function() {
      var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var ot = new OplogTransform(oplogDb, oplogCollName, 'foo.bar', controlWrite, controlRead, { log: silence });
      (function() { ot._oplogReader({ tailableRetryInterval: '' }); }).should.throw('opts.tailableRetryInterval must be a number');
    });

    it('should construct and start reading', function(done) {
      var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var ot = new OplogTransform(oplogDb, oplogCollName, 'foo.bar', controlWrite, controlRead, { log: silence });
      var or = ot._oplogReader({ log: silence });
      // move to the end by repeatedly calling read
      or.on('readable', function() {
        while (or.read());
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
      var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
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
      var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
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
      var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
      var or = ot._oplogReader({ offset: offset, tailable: true, awaitData: false });

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

        var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
        var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
        var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
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

        var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
        var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
        var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
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

  describe('_transform', function() {
    var collectionName = '_transform';
    var ns = databaseName + '.' + collectionName;

    it('should require a valid oplog item', function(done) {
      var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
      ot.write({ o: { } }, function(err) {
        should.equal('invalid oplogItem', err.message);
        done();
      });
    });

    describe('insert', function() {
      var oplogItem = {
        ts: new Timestamp(9, 1),
        ns: ns,
        op: 'i',
        o: {
          _id : 'bar',
          baz: 'foobar'
        }
      };

      it('should handle an oplog insert item', function(done) {
        var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
        var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
        var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
        ot.write(oplogItem);
        ot.on('readable', function() {
          var newVersion = ot.read();

          should.deepEqual(newVersion, {
            h: { id: 'bar' },
            m: { _op: new Timestamp(9, 1) },
            b: {
              _id: 'bar',
              baz: 'foobar'
            }
          });
          done();
        });
      });

      it('should not have altered the original doc and have side-effects', function() {
        should.deepEqual(oplogItem, {
          ts: new Timestamp(9, 1),
          ns: ns,
          op: 'i',
          o: {
            _id : 'bar',
            baz: 'foobar'
          }
        });
      });
    });

    describe('updateFullDoc', function() {
      var oplogItem = {
        ts: new Timestamp(1414516132, 1),
        ns: ns,
        op: 'u',
        o: { _id: 'foo', qux: 'quux' },
        o2: { _id: 'foo' }
      };

      it('should handle an oplog update by full doc item', function(done) {
        var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
        var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
        var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
        ot.write(oplogItem);
        ot.on('readable', function() {
          var newVersion = ot.read();

          should.deepEqual(newVersion, {
            h: { id: 'foo' },
            m: { _op: new Timestamp(1414516132, 1)  },
            b: {
              _id: 'foo',
              qux: 'quux'
            }
          });
          done();
        });
      });

      it('should not have altered the original doc and have side-effects', function() {
        should.deepEqual(oplogItem, {
          ts: new Timestamp(1414516132, 1),
          ns: ns,
          op: 'u',
          o: { _id: 'foo', qux: 'quux' },
          o2: { _id: 'foo' }
        });
      });
    });

    describe('updateModifier', function() {
      var oplogItem = {
        ts: new Timestamp(1414516132, 1),
        ns: ns,
        o: { $set: { bar: 'baz' } },
        op: 'u',
        o2: { _id: 'foo' }
      };

      var dagItem = {
        h: { id: 'foo', v: 'A', pe: '_local', pa: [] },
        m: { _op: new Timestamp(1414516132, 1) },
        b: {
          _id: 'foo',
          qux: 'quux',
        }
      };

      it('should handle an oplog update by modifier item (request last version)', function(done) {
        var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
        var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });

        var ls = new LDJSONStream();
        controlWrite.pipe(ls).on('readable', function() {
          var obj = ls.read();
          if (!obj) { throw new Error('expected version request'); } // end reached

          // expect a request for id "foo" in ld-json
          should.strictEqual(obj.id, 'foo');

          // send back the DAG item
          controlRead.write(BSON.serialize(dagItem));
        });

        var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
        ot.write(oplogItem);
        ot.on('readable', function() {
          var newVersion = ot.read();
          should.deepEqual(newVersion, {
            h: { id: 'foo' },
            m: { _op: new Timestamp(1414516132, 1) },
            b: {
              _id: 'foo',
              bar: 'baz',
              qux: 'quux'
            }
          });
          done();
        });
      });

      it('should not have altered the original doc and have side-effects', function() {
        should.deepEqual(oplogItem, {
          ts: new Timestamp(1414516132, 1),
          ns: ns,
          o: { $set: { bar: 'baz' } },
          op: 'u',
          o2: { _id: 'foo' }
        });
      });
    });

    describe('delete', function() {
      var oplogItem = {
        ts: new Timestamp(9, 1),
        ns: ns,
        op: 'd',
        o: { _id: 'foo' }
      };

      it('should handle an oplog delete item', function(done) {
        var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
        var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
        var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
        ot.write(oplogItem);
        ot.on('readable', function() {
          var newVersion = ot.read();

          should.deepEqual(newVersion, {
            h: { id: 'foo', d: true },
            m: { _op: new Timestamp(9, 1) },
          });
          done();
        });
      });

      it('should not have altered the original doc and have side-effects', function() {
        should.deepEqual(oplogItem, {
          ts: new Timestamp(9, 1),
          ns: ns,
          op: 'd',
          o: { _id: 'foo' }
        });
      });
    });
  });

  describe('startStream', function() {
    var collectionName = 'foo';
    var ns = databaseName + '.' + collectionName;
    var coll;

    // ensure empty collection
    before(function(done) {
      coll = db.collection(collectionName);
      coll.deleteMany({}, done);
    });

    it('should ask for the last version of any id', function(done) {
      var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
      ot.startStream();

      var ls = new LDJSONStream();
      controlWrite.pipe(ls).on('readable', function() {
        var obj = ls.read();
        if (!obj) { throw new Error('expected version request'); } // end reached

        // expect a request for the last version of any id
        should.deepEqual(obj, { id: null });
        done();
      });
    });

    it('should require to have m._op on the last version', function(done) {
      var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
      ot.startStream();

      ot.on('error', function(err) {
        should.strictEqual(err.message, 'unable to determine offset');
        done();
      });

      var ls = new LDJSONStream();
      controlWrite.pipe(ls).on('readable', function() {
        var obj = ls.read();
        should.deepEqual(obj, { id: null });

        // send back a fake DAG item without a timestamp
        var dagItem = {
          h: { id: 'foo', v: 'Aaaaaa', pa: [] },
          b: { foo: 'bar' }
        };
        controlRead.write(BSON.serialize(dagItem));
      });
    });

    var lastOplogTs;
    it('should last oplog item for next tests', function(done) {
      oplogColl.find({ ns: ns }).limit(1).sort({ $natural: -1 }).next(function(err, obj){
        if (err) { throw err; }
        lastOplogTs = obj.ts;
        done();
      });
    });

    it('should process new oplog items on collection insert', function(done) {
      var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence, awaitData: false });
      ot.startStream();

      // expect a request for the last item in the DAG
      // and later a request for oplog insert item
      var i = 0;
      var ls = new LDJSONStream();
      controlWrite.pipe(ls).on('readable', function() {
        i++;
        var obj = ls.read();

        if (i === 1) {
          should.deepEqual(obj, { id: null });

          // send back a fake DAG item with the timestamp of the last oplog item
          var dagItem = {
            h: { id: 'some', v: 'Aaaaaa', pa: [] },
            m: { _op: lastOplogTs },
            b: { foo: 'bar' }
          };
          controlRead.write(BSON.serialize(dagItem));
        }

        if (i === 2) {
          should.deepEqual(obj, { id: 'foo' });

          // send back a fake DAG item as if there is non-existing yet
          controlRead.write(BSON.serialize({}));
        }
      });

      ot.on('readable', function() {
        var obj = ot.read();
        if (!obj) { return; }

        var ts = obj.m._op;
        should.strictEqual(lastOplogTs.lessThan(ts), true);
        should.deepEqual(obj, {
          h: { id: 'foo' },
          m: { _op: ts },
          b: {
            _id: 'foo',
            foo: 'buz'
          }
        });
        done();
      });

      // write something to the collection that is monitored so that the oplog gets a new entry
      coll.insertOne({
        _id: 'foo',
        foo: 'buz'
      });
    });
  });

  describe('_oplogUpdateContainsModifier', function() {
    it('should return false on objects where o is an array', function() {
      should.equal(OplogTransform._oplogUpdateContainsModifier({ o: ['$set'] }), false);
    });

    it('should return true if first key is a string starting with "$"', function() {
      should.equal(OplogTransform._oplogUpdateContainsModifier({ o: { '$set': 'foo' } }), true);
    });

    it('should return false if first key does not start with "$"', function() {
      should.equal(OplogTransform._oplogUpdateContainsModifier({ o: { 'set': 'foo', '$set': 'bar' } }), false);
    });

    it('should return true if only first key does starts with "$"', function() {
      should.equal(OplogTransform._oplogUpdateContainsModifier({ o: { '$set': 'foo', 'set': 'bar' } }), true);
    });

    it('should return true if all keys start with "$"', function() {
      should.equal(OplogTransform._oplogUpdateContainsModifier({ o: { '$set': 'foo', '$in': 'bar' } }), true);
    });
  });

  describe('_createNewVersionByUpdateDoc', function() {
    var collectionName = '_createNewVersionByUpdateDoc';
    var ns = databaseName + '.' + collectionName;
    var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
    var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });

    var dagItem = {
      h: { id: 'foo', v: 'A', pe: '_local', pa: [] },
      b: {
        _id: 'foo',
        bar: 'qux'
      }
    };
    var mod = { $set: { bar: 'baz' } };
    var oplogItem = { ts: new Timestamp(1414516132, 1), o: mod, op: 'u', o2: { _id: 'foo' } };

    it('should require op to be "u"', function(done) {
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
      var updateItem = {
        ts: 123,
        op: 'i',
        ns : 'qux.raboof',
        o2: { _id: 'baz' },
        o: { $set: { qux: 'quux' } }
      };
      ot._createNewVersionByUpdateDoc(dagItem, updateItem, function(err) {
        should.equal(err.message, 'oplogItem op must be "u"');
        done();
      });
    });

    it('should require an o2 object', function(done) {
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
      ot._createNewVersionByUpdateDoc(dagItem, { o: mod, op: 'u' }, function(err) {
        should.equal(err.message, 'Cannot read property \'_id\' of undefined');
        done();
      });
    });

    it('should require oplogItem.o2._id', function(done) {
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
      ot._createNewVersionByUpdateDoc(dagItem, { o: mod, op: 'u', o2: { } }, function(err) {
        should.equal(err.message, 'missing oplogItem.o2._id');
        done();
      });
    });

    it('should require o._id to not exist', function(done) {
      var item = {
        o: { $foo: 'bar', _id: 'applyOplogItemTest' },
        op: 'u',
        o2: { _id: 'applyOplogItemTest' }
      };
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
      ot._createNewVersionByUpdateDoc(dagItem, item, function(err) {
        should.equal(err.message, 'oplogItem contains o._id');
        done();
      });
    });

    it('should create a new version', function(done) {
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
      ot._createNewVersionByUpdateDoc(dagItem, oplogItem, function(err, item) {
        if (err) { throw err; }

        should.deepEqual(item, {
          h: { id: 'foo' },
          m: { _op: new Timestamp(1414516132, 1) },
          b: {
            _id: 'foo',
            bar: 'baz'
          }
        });
        done();
      });
    });

    it('should not have altered the original doc and have side-effects', function() {
      should.deepEqual(oplogItem, {
        ts: new Timestamp(5709483428, 1),
        o: { $set: { bar: 'baz' } },
        op: 'u',
        o2: { _id: 'foo' }
      });
    });

    it('should create a new version even when the update modifier leads to the same result', function(done) {
      var item = {
        h: { id: 'foo', v: 'A', pe: '_local', pa: [] },
        b: {
          _id: 'foo',
          bar: 'qux'
        }
      };
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
      ot._createNewVersionByUpdateDoc(item, oplogItem, function(err, newVersion) {
        if (err) { throw err; }

        should.deepEqual(newVersion, {
          h: { id: 'foo' },
          m: { _op: new Timestamp(1414516132, 1) },
          b: {
            _id: 'foo',
            bar: 'baz'
          }
        });
        done();
      });
    });
  });

  describe('_applyOplogFullDoc', function() {
    var collectionName = '_applyOplogFullDoc';
    var ns = databaseName + '.' + collectionName;
    var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
    var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });

    var time = new Date();

    var oplogItemInsert = {
      ts: new Timestamp(1414516124, 1),
      op: 'i',
      o: { _id : 'foo', baz: 'raboof' }
    };

    var oplogItemUpdate = {
      ts: new Timestamp(1414516190, 1),
      op: 'u',
      o: { _id: 'foo', qux: 'quux', foo: time }
    };

    it('should require op to be "u" or "i"', function(done) {
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
      var updateItem = {
        op: 'd'
      };
      ot._applyOplogFullDoc(updateItem, function(err) {
        should.equal(err.message, 'oplogItem.op must be "u" or "i"');
        done();
      });
    });

    it('should require oplogItem.o._id', function(done) {
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
      ot._applyOplogFullDoc({ o: {}, op: 'u' }, function(err) {
        should.equal(err.message, 'missing oplogItem.o._id');
        done();
      });
    });

    it('should create a new version of an insert oplog item', function(done) {
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
      ot._applyOplogFullDoc(oplogItemInsert, function(err, item) {
        if (err) { throw err; }

        should.deepEqual(item, {
          h: { id: 'foo' },
          m: { _op: new Timestamp(1414516124, 1) },
          b: {
            _id : 'foo',
            baz: 'raboof'
          }
        });

        done();
      });
    });
    it('should create a new version of an update by full doc oplog item', function(done) {
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
      ot._applyOplogFullDoc(oplogItemUpdate, function(err, item) {
        if (err) { throw err; }

        should.deepEqual(item, {
          h: { id: 'foo' },
          m: { _op: new Timestamp(1414516190, 1) },
          b: {
            _id: 'foo',
            qux: 'quux',
            foo: time
          }
        });

        done();
      });
    });

    it('should not have side-effects on the insert oplog item', function() {
      should.deepEqual(oplogItemInsert, {
        ts: new Timestamp(1414516124, 1),
        op: 'i',
        o: { _id : 'foo', baz: 'raboof' }
      });
    });

    it('should not have side-effects on the update oplog item', function() {
      should.deepEqual(oplogItemUpdate, {
        ts: new Timestamp(1414516190, 1),
        op: 'u',
        o: { _id: 'foo', qux: 'quux', foo: time }
      });
    });
  });

  describe('_applyOplogUpdateModifier', function() {
    var collectionName = '_applyOplogUpdateModifier';
    var ns = databaseName + '.' + collectionName;

    var mod = { $set: { bar: 'baz' } };
    var oplogItem = { ts: new Timestamp(999, 1), o: mod, op: 'u', o2: { _id: 'foo' } };

    it('should complain about missing parent and don\'t add to the DAG', function(done) {
      var versionRequested = false;
      var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });

      // expect ld-json on the request stream
      var ls = new LDJSONStream();
      controlWrite.pipe(ls).on('readable', function() {
        var obj = ls.read();
        if (!obj) { throw new Error('expected version request'); } // end reached

        // expect a request for id "foo" in ld-json
        should.strictEqual(obj.id, 'foo');
        versionRequested = true;

        // send back an empty response as if this version does not exist yet
        controlRead.write(BSON.serialize({}));
      });

      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
      ot._applyOplogUpdateModifier(oplogItem, function(err, newVersion) {
        should.equal(err.message, 'previous version of doc not found');
        should.strictEqual(versionRequested, true);
        should.equal(newVersion, null);
        done();
      });
    });

    it('should make new version based on the passed head and oplog item', function(done) {
      var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });

      var head = {
        h: { id: 'foo', v: 'A', pe: '_local', pa: [] },
        b: {
          _id: 'foo',
          bar: 'qux'
        }
      };

      // expect ld-json on the request stream and send back the head
      var ls = new LDJSONStream();
      controlWrite.pipe(ls).on('readable', function() {
        var obj = ls.read();
        should.strictEqual(obj.id, 'foo');
        controlRead.write(BSON.serialize(head));
      });

      ot._applyOplogUpdateModifier(oplogItem, function(err, newVersion) {
        if (err) { throw err; }
        should.deepEqual(newVersion, {
          h: { id: 'foo' },
          m: { _op: oplogItem.ts },
          b: {
            _id: 'foo',
            bar: 'baz'
          }
        });
        done();
      });
    });

    it('should not have altered the original doc and have side-effects', function() {
      should.deepEqual(oplogItem, {
        ts: new Timestamp(999, 1),
        o: { $set: { bar: 'baz' } },
        op: 'u',
        o2: { _id: 'foo' }
      });
    });
  });

  describe('_applyOplogDeleteItem', function() {
    var collectionName = '_applyOplogDeleteItem';
    var ns = databaseName + '.' + collectionName;
    var controlWrite = through2(function(chunk, enc, cb) { cb(null, chunk); });
    var controlRead = through2(function(chunk, enc, cb) { cb(null, chunk); });

    var oplogItem = {
      ts: new Timestamp(1234, 1),
      op: 'd',
      o: { _id: 'foo' }
    };

    it('should require op to be "d"', function(done) {
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
      var updateItem = {
        op: 'i',
        o: { qux: 'quux' }
      };
      ot._applyOplogDeleteItem(updateItem, function(err) {
        should.equal(err.message, 'oplogItem.op must be "d"');
        done();
      });
    });

    it('should require oplogItem.o._id', function(done) {
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
      ot._applyOplogDeleteItem({ o: {}, op: 'd' }, function(err) {
        should.equal(err.message, 'missing oplogItem.o._id');
        done();
      });
    });

    it('should create a new version with the right id, no body and the oplog timestamp', function(done) {
      var ot = new OplogTransform(oplogDb, oplogCollName, ns, controlWrite, controlRead, { log: silence });
      ot._applyOplogDeleteItem(oplogItem, function(err, newVersion) {
        if (err) { throw err; }

        should.deepEqual(newVersion, {
          h: { id: 'foo', d: true },
          m: { _op: new Timestamp(1234, 1) }
        });

        done();
      });
    });
  });

  describe('invalidOplogItem', function() {
    it('should not be valid if no parameter given', function() {
      should.strictEqual(OplogTransform._invalidOplogItem(), 'missing item');
    });
    it('should not be valid if parameter is empty object', function() {
      should.strictEqual(OplogTransform._invalidOplogItem({}), 'missing item.o');
    });
    it('should not be valid if object has no o attribute', function() {
      should.strictEqual(OplogTransform._invalidOplogItem({ ts: 'a', op: 'b', ns: 'c' }), 'missing item.o');
    });
    it('should not be valid if object has no ts attribute', function() {
      should.strictEqual(OplogTransform._invalidOplogItem({ o: 'a', op: 'b', ns: 'c' }), 'missing item.ts');
    });
    it('should not be valid if object has no op attribute', function() {
      should.strictEqual(OplogTransform._invalidOplogItem({ ts: 'a', o: 'b', ns: 'c' }), 'missing item.op');
    });
    it('should not be valid if object has no ns attribute', function() {
      should.strictEqual(OplogTransform._invalidOplogItem({ ts: 'a', o: 'b', op: 'c' }), 'missing item.ns');
    });
    it('should be valid if op is u', function() {
      should.strictEqual(OplogTransform._invalidOplogItem({ op: 'u', ts: 'a', o: 'b', ns: 'c' }), '');
    });
    it('should be valid if op is i', function() {
      should.strictEqual(OplogTransform._invalidOplogItem({ op: 'i', ts: 'a', o: 'b', ns: 'c' }), '');
    });
    it('should be valid if op is d', function() {
      should.strictEqual(OplogTransform._invalidOplogItem({ op: 'd', ts: 'a', o: 'b', ns: 'c' }), '');
    });
    it('should not be valid if op is not u, i or d', function() {
      should.strictEqual(OplogTransform._invalidOplogItem({ op: 'a', ts: 'a', o: 'b', ns: 'c' }), 'invalid item.op');
    });
  });
});
