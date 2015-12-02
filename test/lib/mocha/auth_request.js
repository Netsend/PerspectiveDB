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

var authRequest = require('../../../lib/auth_request');

describe('authRequest', function () {
  describe('valid', function () {
    it('should return false without parameters', function() {
      var result = authRequest.valid();
      should.strictEqual(result, false);
    });

    it('should return false when parameter is null', function() {
      var result = authRequest.valid(null);
      should.strictEqual(result, false);
    });

    it('should return false when all fields are missing', function() {
      var result = authRequest.valid({});
      should.strictEqual(result, false);
    });

    it('should return false when all fields are missing but others are present', function() {
      var result = authRequest.valid({ foo: 'bar' });
      should.strictEqual(result, false);
    });

    it('should pass without offset', function() {
      var result = authRequest.valid({
        username: 'foo',
        password: 'bar'
      });
      should.strictEqual(result, true);
    });

    it('should pass with offset', function() {
      var result = authRequest.valid({
        username: 'foo',
        password: 'bar',
        offset: '10'
      });
      should.strictEqual(result, true);
    });

    it('should pass with "undefined" offset', function() {
      var result = authRequest.valid({
        username: 'foo',
        password: 'bar',
        offset: undefined
      });
      should.strictEqual(result, true);
    });

    it('should return false when extra fields are present', function() {
      var result = authRequest.valid({
        username: 'foo',
        password: 'bar',
        offset: 10,
        foo: 'bar'
      });
      should.strictEqual(result, false);
    });

    describe('field type checks', function() {
      it('should return false when username is not a string', function() {
        var result = authRequest.valid({
          username: 1,
          password: 'bar'
        });
        should.strictEqual(result, false);
      });

      it('should return false when password is not a string', function() {
        var result = authRequest.valid({
          username: 'foo',
          password: 1
        });
        should.strictEqual(result, false);
      });

      it('should return false when offset is not a string', function() {
        var result = authRequest.valid({
          username: 'foo',
          password: 'bar',
          offset: 10
        });
        should.strictEqual(result, false);
      });
    });

    describe('field min length checks', function() {
      it('should return false when username is too short', function() {
        var result = authRequest.valid({
          username: '',
          password: 'bar'
        });
        should.strictEqual(result, false);
      });

      it('should return false when password is too short', function() {
        var result = authRequest.valid({
          username: 'foo',
          password: ''
        });
        should.strictEqual(result, false);
      });
    });

    describe('field max length checks', function() {
      var tooLong = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' +
                    'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' +
                    'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';

      it('should return false when username is too long', function() {
        var result = authRequest.valid({
          username: tooLong,
          password: 'bar'
        });
        should.strictEqual(result, false);
      });

      it('should return false when password is too long', function() {
        var result = authRequest.valid({
          username: 'foo',
          password: tooLong
        });
        should.strictEqual(result, false);
      });
    });
  });
});
