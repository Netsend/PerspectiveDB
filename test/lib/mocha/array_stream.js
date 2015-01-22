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

var should = require('should');

var ArrayStream = require('../../../lib/array_stream');

var db;
var databaseName = 'test_array_stream';
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

describe('ArrayStream', function() {
  var vColl;

  describe('constructor', function() {
    it('should require items to be an array', function() {
      (function() { new ArrayStream(); }).should.throw('items must be an array');
    });

    it('should construct', function() {
      (function() { vColl = new ArrayStream([]); }).should.not.throwError();
    });
  });

  describe('stream', function() {
    var C = { _id: { _id: 'foo', _v: 'C', _pe: 'bar', _pa: ['B'], _i: 4 }, _m3: { _ack: true }, foo: 'qux' };
    var D = { _id: { _id: 'foo', _v: 'D', _pe: 'bar', _pa: ['B'], _i: 3 }, _m3: { _ack: false }, foo: 'quux' };

    it('should stream asc', function(done) {
      var items = [C, D];
      var vc = new ArrayStream(items);
      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);
      });

      stream.on('close', function() {
        should.deepEqual(received, [C, D]);
        done();
      });
    });

    it('should stream only once', function(done) {
      var items = [C, D];
      var vc = new ArrayStream(items, { debug: false });

      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);
        if (received.length > items.length) { throw new Error('streamed twice'); }
      });

      stream.on('close', function() {
        should.deepEqual(received, [C, D]);

        vc.stream();
        process.nextTick(done);
      });
    });

    it('should filter', function(done) {
      var items = [C, D];
      var selector = { foo: { $in: [ 'bar', 'quux' ] } };
      var vc = new ArrayStream(items, { filter: selector, debug: false });
      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);
      });

      stream.on('close', function() {
        should.equal(received.length, 1);
        should.deepEqual(received, [D]);
        done();
      });
    });

    it('should filter nested namespaces', function(done) {
      var items = [C, D];
      var selector = { '_id._v': { $in: [ 'B', 'D' ] } };
      var vc = new ArrayStream(items, { filter: selector, debug: false });
      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);
      });

      stream.on('close', function() {
        should.equal(received.length, 1);
        should.deepEqual(received, [D]);
        done();
      });
    });
  });

  describe('pause', function() {
    var collectionName = 'array_stream_pause';

    var C = { _id: { _id: 'foo', _v: 'C', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: true } };
    var D = { _id: { _id: 'foo', _v: 'D', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: false } };

    it('should pause and resume', function(done) {
      var items = [C, D];
      var vc = new ArrayStream(items);

      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'C') {
          stream.pause();
          process.nextTick(function() {
            should.equal(received.length, 1);
            should.deepEqual(received[0], C);
            stream.resume();
          });
        }
      });

      stream.on('close', function() {
        should.equal(received.length, 2);
        should.deepEqual(received[0], C);
        should.deepEqual(received[1], D);
        done();
      });
    });

    it('should pause and destroy', function(done) {
      var vc = new ArrayStream([C, D], { debug: false });
      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'C') {
          stream.pause();
          process.nextTick(function() {
            stream.destroy();
          });
        }
      });

      stream.on('close', function() {
        should.deepEqual(received, [C]);
        done();
      });
    });

    it('needs some items in a real mongo collection to test behavior', function(done) {
      db.collection(collectionName).insert([C, D], done);
    });

    it('should pause and stop calling back', function(done) {
      var vc = new ArrayStream([C, D], { debug: false });
      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'C') {
          stream.pause();
          stream.on('data', done);
          should.deepEqual(received, [C]);
          done();
        }
      });

      stream.on('close', done);
    });

    it('ensure mongo driver does not emit close after pause on last item', function(done) {
      var stream = db.collection(collectionName).find().stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'D') {
          stream.pause();
          done();
        }
      });

      stream.on('close', function() {
        throw new Error('unexpected stream closed while paused');
      });
    });

    it('should not emit close after pause on last item (iff previous test succeeded)', function(done) {
      var vc = new ArrayStream([C, D], { debug: false });
      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'D') {
          stream.pause();
          done();
        }
      });

      stream.on('close', function() {
        throw new Error('unexpected stream closed while paused');
      });
    });

    it('ensure mongo driver does emit close after resume after pause on last item', function(done) {
      var stream = db.collection(collectionName).find().stream();

      function shouldNotBeCalled() {
        throw new Error('unexpected stream closed while paused');
      }

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'D') {
          stream.pause();

          setTimeout(function() {
            stream.removeListener('close', shouldNotBeCalled);
            stream.on('close', done);
            stream.resume();
          }, 10);
        }
      });

      stream.on('close', shouldNotBeCalled);
    });

    it('should emit close after resume after pause on last item (iff previous test succeeded)', function(done) {
      var vc = new ArrayStream([C, D], { debug: false });
      var stream = vc.stream();

      function shouldNotBeCalled() {
        throw new Error('unexpected stream closed while paused');
      }

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'D') {
          stream.pause();

          setTimeout(function() {
            stream.removeListener('close', shouldNotBeCalled);
            stream.on('close', done);
            stream.resume();
          }, 10);
        }
      });

      stream.on('close', shouldNotBeCalled);
    });
  });

  describe('destroy', function() {
    var C = { _id: { _id: 'foo', _v: 'C', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: true } };
    var D = { _id: { _id: 'foo', _v: 'D', _pe: 'bar', _pa: ['B'] }, _m3: { _ack: false } };

    it('should destroy', function(done) {
      var items = [C, D];
      var vc = new ArrayStream(items);

      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'C') {
          stream.destroy();
        }
      });

      stream.on('close', function() {
        should.equal(received.length, 1);
        should.deepEqual(received[0], C);
        stream.resume();
        done();
      });
    });

    it('should destroy and don\'t resume', function(done) {
      var items = [C, D];
      var vc = new ArrayStream(items);

      var stream = vc.stream();

      var received = [];
      stream.on('data', function(item) {
        received.push(item);

        if (item._id._v === 'C') {
          stream.destroy();
          stream.resume();
        }
      });

      stream.on('close', function() {
        should.equal(received.length, 1);
        should.deepEqual(received[0], C);
        stream.resume();
        done();
      });
    });

    it('should emit close only once', function(done) {
      var items = [C, D];
      var vc = new ArrayStream(items);

      var stream = vc.stream();
      stream.destroy();
      stream.destroy();

      stream.on('close', done);
    });

    it('should emit close only once, async', function(done) {
      var items = [C, D];
      var vc = new ArrayStream(items);

      var stream = vc.stream();
      process.nextTick(function() {
        stream.destroy();
        process.nextTick(function() {
          stream.destroy();
        });
      });

      stream.on('close', done);
    });
  });
});
