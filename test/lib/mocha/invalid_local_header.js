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

var invalidLocalHeader = require('../../../lib/invalid_local_header');

describe('invalidLocalHeader', function() {
  it('should require h to be an object', function() {
    invalidLocalHeader(1).should.equal('h must be an object');
  });

  it('should require h.id to be a valid id', function() {
    invalidLocalHeader({ id: undefined }).should.equal('h.id must be a buffer, a string or implement "toString"');
  });

  it('should be a valid item', function() {
    invalidLocalHeader({ id: 'foo' }).should.equal('');
  });

  it('should require h.v to be a string', function() {
    invalidLocalHeader({ id: 'foo', v: 1 }).should.equal('h.v must be a string');
  });

  it('should be a valid item with h.v', function() {
    invalidLocalHeader({ id: 'foo', v: 'A' }).should.equal('');
  });

  it('should require h.d to be a boolean', function() {
    invalidLocalHeader({ id: 'foo', d: 0 }).should.equal('h.d must be a boolean');
  });

  it('should be a valid item with h.d', function() {
    invalidLocalHeader({ id: 'foo', d: false }).should.equal('');
  });
});
