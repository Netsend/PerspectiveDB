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

/*jshint -W068 */

var should = require('should');

var pushRequest = require('../../../lib/push_request');

describe('pushRequest', function () {
  describe('valid', function () {
    it('should return true without parameters', function() {
      var result = pushRequest.valid();
      should.strictEqual(result, true);
    });

    it('should return true when parameter is null', function() {
      var result = pushRequest.valid(null);
      should.strictEqual(result, true);
    });

    it('should return false when parameter is not an object', function() {
      var result = pushRequest.valid('');
      should.strictEqual(result, false);
    });

    it('should return true when all fields are missing', function() {
      var result = pushRequest.valid({});
      should.strictEqual(result, true);
    });

    it('should return false when all fields are missing but others are present', function() {
      var result = pushRequest.valid({ foo: 'bar' });
      should.strictEqual(result, false);
    });

    it('should return false when some fields are missing and non-valid fields are present', function() {
      var result = pushRequest.valid({
        offset: 'baz',
        foo: 'bar'
      });
      should.strictEqual(result, false);
    });

    it('should return true when all fields are present', function() {
      var result = pushRequest.valid({
        filter: {},
        hooks: [],
        hooksOpts: {},
        offset: ''
      });
      should.strictEqual(result, true);
    });

    it('should return false when all fields are present and others are present', function() {
      var result = pushRequest.valid({
        filter: {},
        hooks: [],
        hooksOpts: {},
        offset: '',
        foo: 'bar'
      });
      should.strictEqual(result, false);
    });

    it('should return false if offset is not a string', function() {
      var result = pushRequest.valid({ offset: 1 });
      should.strictEqual(result, false);
    });

    it('should pass if offset is a string', function() {
      var result = pushRequest.valid({ offset: 'foo' });
      should.strictEqual(result, true);
    });

    it('should work with undefined values', function() {
      var result = pushRequest.valid({
        filter: { baz: 'A' },
        hooks: [ 'some' ],
        hooksOpts: {},
        offset: undefined
      });
      should.strictEqual(result, true);
    });

    describe('field length checks', function() {
      var tooLong = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' +
                    'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' +
                    'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';

      it('should return false when offset is too long', function() {
        var result = pushRequest.valid({ offset: tooLong });
        should.strictEqual(result, false);
      });
    });
  });
});
