/**
 * Copyright 2016 Netsend.
 *
 * This file is part of PerspectiveDB.
 *
 * PerspectiveDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PerspectiveDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PerspectiveDB. If not, see <https://www.gnu.org/licenses/>.
 */

'use strict';

var assert = require('assert');

var streamify = require('../../../lib/streamify');

describe('streamify', function() {
  it('should iterate over three items', function(done) {
    var arr = ['a', 'b', 'c'];
    var i = 0;
    streamify(arr).on('data', function(item) {
      assert.equal(item, arr[i++]);
    }).on('end', function() {
      assert.equal(i, 3);
      done();
    });
  });

  it('should iterate over three items in reverse', function(done) {
    var arr = ['a', 'b', 'c'];
    var i = 0;
    streamify(arr, { reverse: true }).on('data', function(item) {
      assert.equal(item, arr[arr.length - ++i]);
    }).on('end', function() {
      assert.equal(i, 3);
      done();
    });
  });

  it('should filter an item', function(done) {
    var arr = ['a', 'b', 'c'];
    var i = 0;
    function filter(item) {
      return item !== 'b';
    }
    streamify(arr, { filter }).on('data', function(item) {
      switch (i++) {
      case 0:
        assert.equal(item, arr[0]);
        break;
      case 1:
        assert.equal(item, arr[2]);
        break;
      }
    }).on('end', function() {
      assert.equal(i, 2);
      done();
    });
  });

  it('should map items', function(done) {
    var arr = ['a', 'b', 'c'];
    var i = 0;
    function map(item) {
      return { mapped: item };
    }
    streamify(arr, { map }).on('data', function(item) {
      assert.deepEqual(item, { mapped: arr[i++] });
    }).on('end', function() {
      assert.equal(i, 3);
      done();
    });
  });
});
