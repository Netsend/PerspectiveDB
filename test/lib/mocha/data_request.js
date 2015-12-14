/**
 * Copyright 2015 Netsend.
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

var should = require('should');

var dataRequest = require('../../../lib/data_request');

describe('dataRequest', function () {
  describe('valid', function () {
    it('should return false without parameters', function() {
      var result = dataRequest.valid();
      should.strictEqual(result, false);
    });

    it('should return false when parameter is null', function() {
      var result = dataRequest.valid(null);
      should.strictEqual(result, false);
    });

    it('should return false when all fields are missing', function() {
      var result = dataRequest.valid({});
      should.strictEqual(result, false);
    });

    it('should return false when all fields are missing but others are present', function() {
      var result = dataRequest.valid({ foo: 'bar' });
      should.strictEqual(result, false);
    });

    it('should return false when extra fields are present', function() {
      var result = dataRequest.valid({
        start: 'foo',
        foo: 'bar'
      });
      should.strictEqual(result, false);
    });

    describe('field type checks string or boolean', function() {
      it('should return false when start is a number', function() {
        var result = dataRequest.valid({
          start: 1
        });
        should.strictEqual(result, false);
      });

      it('should return true when start is a string', function() {
        var result = dataRequest.valid({
          start: 'foo'
        });
        should.strictEqual(result, true);
      });

      it('should return true when start is true', function() {
        var result = dataRequest.valid({
          start: true
        });
        should.strictEqual(result, true);
      });

      it('should return true when start is false', function() {
        var result = dataRequest.valid({
          start: false
        });
        should.strictEqual(result, true);
      });
    });

    describe('field min length checks', function() {
      it('should return false when start is too short', function() {
        var result = dataRequest.valid({
          start: ''
        });
        should.strictEqual(result, false);
      });
    });

    describe('field max length checks', function() {
      var tooLong = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' +
                    'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' +
                    'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';

      it('should return false when start is too long', function() {
        var result = dataRequest.valid({
          start: tooLong
        });
        should.strictEqual(result, false);
      });
    });
  });
});
