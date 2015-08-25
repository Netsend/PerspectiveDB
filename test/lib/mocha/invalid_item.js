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

require('should');

var invalidItem = require('../../../lib/invalid_item');

describe('invalidItem', function() {
  var header = { id: '', v: 'A', pa: [] };

  it('should require item to be an object', function() {
    invalidItem([]).should.equal('item must be an object');
  });

  it('should require a valid header', function() {
    invalidItem({ h: {} }).should.equal('item.h.id must be a buffer, a string or implement "toString"');
  });

  it('should be a valid item', function() {
    invalidItem({ h: header }).should.equal('');
  });

  it('should be a valid item with "m"', function() {
    invalidItem({ h: header, m: '' }).should.equal('');
  });

  it('should be a valid item with "b"', function() {
    invalidItem({ h: header, b: '' }).should.equal('');
  });

  it('should be a valid item with "m" and "b"', function() {
    invalidItem({ h: header, m: '', b: '' }).should.equal('');
  });

  it('should not accept other keys than h, m or b', function() {
    invalidItem({ h: header, foo: 'bar' }).should.equal('item should only contain "h" and optionally "b" and "m" keys');
  });
});
