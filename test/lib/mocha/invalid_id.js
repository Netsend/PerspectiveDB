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

var invalidId = require('../../../lib/invalid_id');

describe('invalidId', function() {
  describe('invalidItem', function() {
    it('should require id to be defined', function() {
      invalidId().should.equal('id must be a buffer, a string or implement "toString"');
    });

    it('should require id not to be undefined', function() {
      invalidId(undefined).should.equal('id must be a buffer, a string or implement "toString"');
    });

    it('should require id not to be null', function() {
      invalidId(null).should.equal('id must be a buffer, a string or implement "toString"');
    });

    it('should require id not to exceed 254 bytes (type string)', function() {
      invalidId((new Buffer(128)).toString('hex')).should.equal('id must not exceed 254 bytes');
    });

    it('should require id not to exceed 254 bytes (type buffer)', function() {
      invalidId(new Buffer(255)).should.equal('id must not exceed 254 bytes');
    });

    it('should accept array type for id', function() {
      invalidId({ _h: { id: [], v: 'A', pa: [] }, _b: {} }).should.equal('');
    });

    it('should accept empty string for id', function() {
      invalidId({ _h: { id: '', v: 'A', pa: [] }, _b: {} }).should.equal('');
    });

    it('should accept positive number type for id', function() {
      invalidId({ _h: { id: 10 , v: 'A', pa: [] }, _b: {} }).should.equal('');
    });

    it('should accept 0 for id', function() {
      invalidId({ _h: { id: 0 , v: 'A', pa: [] }, _b: {} }).should.equal('');
    });

    it('should accept non-empty string for id', function() {
      invalidId('foo').should.equal('');
    });

    it('should accept 0 length buffer for id', function() {
      invalidId(new Buffer(0)).should.equal('');
    });

    it('should accept 254 byte string', function() {
      invalidId((new Buffer(127)).toString('hex')).should.equal('');
    });

    it('should accept 254 byte buffer', function() {
      invalidId(new Buffer(254)).should.equal('');
    });

  });
});
