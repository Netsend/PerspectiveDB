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

var invalidHeader = require('../../../lib/invalid_header');

describe('invalidHeader', function() {
  it('should require h to be an object', function() {
    invalidHeader([]).should.equal('h must be an object');
  });

  it('should require h.id to be a valid id', function() {
    invalidHeader({ id: undefined }).should.equal('h.id must be a buffer, a string or implement "toString"');
  });

  it('should require h.pe to be a string', function() {
    invalidHeader({ id: 'foo', pe: [] }).should.equal('h.pe must be a string');
  });

  it('should require h.v to be a string', function() {
    invalidHeader({ id: 'foo', pe: 'some', v: [] }).should.equal('h.v must be a string');
  });

  it('should require h.pa to be an array', function() {
    invalidHeader({ id: 'foo', pe: 'some', v: 'A', pa: {} }).should.equal('h.pa must be an array');
  });

  it('should be a valid item', function() {
    invalidHeader({ id: 'foo', pe: 'some', v: 'A', pa: [] }).should.equal('');
  });

  it('should require h.d to be a boolean', function() {
    invalidHeader({ id: 'foo', pe: 'some', v: 'A', pa: [], d: 0 }).should.equal('h.d must be a boolean');
  });

  it('should be a valid item with h.d', function() {
    invalidHeader({ id: 'foo', pe: 'some', v: 'A', pa: [], d: false }).should.equal('');
  });
});
