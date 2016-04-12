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

var should = require('should');

var ConcatReadStream = require('../../../lib/concat_read_stream');
var streamify = require('../../../lib/streamify');

describe('ConcatReadStream', function() {
  it('should emit stream data in serial', function(done) {
    var s1 = streamify([1,2,3,4]);
    var s2 = streamify(['a','b','c','d']);
    var s3 = streamify(['I','II','III','IIII']);

    var s = new ConcatReadStream([s1, s2, s3]);

    var i = 0;
    s.on('data', function(item) {
      switch (i) {
      case 0: should.strictEqual(item, 1); break;
      case 1: should.strictEqual(item, 2); break;
      case 2: should.strictEqual(item, 3); break;
      case 3: should.strictEqual(item, 4); break;
      case 4: should.strictEqual(item, 'a'); break;
      case 5: should.strictEqual(item, 'b'); break;
      case 6: should.strictEqual(item, 'c'); break;
      case 7: should.strictEqual(item, 'd'); break;
      case 8: should.strictEqual(item, 'I'); break;
      case 9: should.strictEqual(item, 'II'); break;
      case 10: should.strictEqual(item, 'III'); break;
      case 11: should.strictEqual(item, 'IIII'); break;
      }
      i++;
    });
    s.on('end', function() {
      should.strictEqual(i, 12);
      done();
    });
  });

  it('should stop when paused', function(done) {
    var s1 = streamify([1,2,3,4]);
    var s2 = streamify(['a','b','c','d']);

    var s = new ConcatReadStream([s1, s2]);

    var i = 0;
    s.on('data', function(item) {
      switch (i) {
      case 0: should.strictEqual(item, 1); break;
      case 1: should.strictEqual(item, 2); break;
      case 2: should.strictEqual(item, 3); break;
      case 3: should.strictEqual(item, 4); break;
      case 4: should.strictEqual(item, 'a'); break;
      case 5:
        should.strictEqual(item, 'b');
        s.pause();
          // test after 10ms
        setTimeout(function() {
          should.strictEqual(i, 6);
          done();
        }, 10);
        break;
      default:
        throw new Error('should not happen');
      }
      i++;
    });
  });

  it('should pause and resume', function(done) {
    var s1 = streamify([1,2,3,4]);
    var s2 = streamify(['a','b','c','d']);

    var s = new ConcatReadStream([s1, s2]);

    var i = 0;
    s.on('data', function(item) {
      switch (i) {
      case 0: should.strictEqual(item, 1); break;
      case 1: should.strictEqual(item, 2); break;
      case 2: should.strictEqual(item, 3); break;
      case 3: should.strictEqual(item, 4); break;
      case 4: should.strictEqual(item, 'a'); break;
      case 5:
        should.strictEqual(item, 'b');
        s.pause();
          // test after 10ms
        setTimeout(function() {
          should.strictEqual(i, 6);
          s.resume();
        }, 10);
        break;
      case 6: should.strictEqual(item, 'c'); break;
      case 7: should.strictEqual(item, 'd'); break;
      }
      i++;
    });
    s.on('end', function() {
      should.strictEqual(i, 8);
      done();
    });
  });

  it('should not continue when paused at last element of first stream', function(done) {
    var s1 = streamify([1,2,3,4]);
    var s2 = streamify(['a','b','c','d']);

    var s = new ConcatReadStream([s1, s2]);

    var i = 0;
    s.on('data', function(item) {
      switch (i) {
      case 0: should.strictEqual(item, 1); break;
      case 1: should.strictEqual(item, 2); break;
      case 2: should.strictEqual(item, 3); break;
      case 3:
        should.strictEqual(item, 4);
        s.pause();
          // test after 10ms
        setTimeout(function() {
          should.strictEqual(i, 4);
          done();
        }, 10);
        break;
      default:
        throw new Error('should not happen');
      }
      i++;
    });
  });

  it('should not end when paused at last element of last stream', function(done) {
    var s1 = streamify([1,2,3,4]);
    var s2 = streamify(['a','b','c','d']);

    var s = new ConcatReadStream([s1, s2]);

    var i = 0;
    s.on('data', function(item) {
      switch (i) {
      case 0: should.strictEqual(item, 1); break;
      case 1: should.strictEqual(item, 2); break;
      case 2: should.strictEqual(item, 3); break;
      case 3: should.strictEqual(item, 4); break;
      case 4: should.strictEqual(item, 'a'); break;
      case 5: should.strictEqual(item, 'b'); break;
      case 6: should.strictEqual(item, 'c'); break;
      case 7:
        should.strictEqual(item, 'd');
        s.pause();
          // test after 10ms
        setTimeout(function() {
          should.strictEqual(i, 8);
          done();
        }, 10);
        break;
      default:
        throw new Error('should not happen');
      }
      i++;
    });
    s.on('end', function() {
      throw new Error('should not "end"');
    });
  });

  it('should reopen a new stream', function(done) {
    var arr1 = [1,2,3,4];
    var arr2 = ['a','b','c','d'];
    var s1 = streamify(arr1);
    var s2 = streamify(arr2);

    // make sure the streams in the concat stream support reopen
    s1.reopen = function() {
      return streamify(arr1);
    };
    s2.reopen = function() {
      return streamify(arr2);
    };

    var i = 0;
    var s = new ConcatReadStream([s1, s2]);

    s.on('data', function() {
      i++;
    });

    s.on('end', function() {
      should.strictEqual(i, 8);

      var u = s.reopen();

      u.on('data', function(item) {
        switch (i) {
        case 8: should.strictEqual(item, 1); break;
        case 9: should.strictEqual(item, 2); break;
        case 10: should.strictEqual(item, 3); break;
        case 11: should.strictEqual(item, 4); break;
        case 12: should.strictEqual(item, 'a'); break;
        case 13: should.strictEqual(item, 'b'); break;
        case 14: should.strictEqual(item, 'c'); break;
        case 15: should.strictEqual(item, 'd'); break;
        default:
          throw new Error('should not happen');
        }
        i++;
      });

      u.on('end', function() {
        should.strictEqual(i, 16);
        done();
      });
    });
  });
});
