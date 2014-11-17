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

var pullRequest = require('../../lib/pull_request');

describe('pullRequest', function () {
  describe('valid', function () {
    it('should return false without parameters', function() {
      var result = pullRequest.valid();
      should.strictEqual(result, false);
    });

    it('should return false when parameter is null', function() {
      var result = pullRequest.valid(null);
      should.strictEqual(result, false);
    });

    it('should return false when username is missing', function() {
      var result = pullRequest.valid({ password: 'bar' });
      should.strictEqual(result, false);
    });

    it('should return false password is missing', function() {
      var result = pullRequest.valid({ foo: 'bar' });
      should.strictEqual(result, false);
    });

    it('should return true when username and password are present', function() {
      var result = pullRequest.valid({
        username: 'foo',
        password: 'bar'
      });
      should.strictEqual(result, true);
    });

    it('should return true when collection is "undefined"', function() {
      var result = pullRequest.valid({
        username: 'foo',
        password: 'bar',
        collection: undefined
      });
      should.strictEqual(result, true);
    });

    it('should return false when path and port are set', function() {
      var result = pullRequest.valid({
        username: 'foo',
        password: 'bar',
        database: 'baz',
        collection: 'zab',
        path: 'foo',
        port: 1
      });
      should.strictEqual(result, false);
    });

    it('should return false when path and host are set', function() {
      var result = pullRequest.valid({
        username: 'foo',
        password: 'bar',
        database: 'baz',
        collection: 'zab',
        path: 'foo',
        host: 'bar'
      });
      should.strictEqual(result, false);
    });

    it('should return false when all fields are set', function() {
      var result = pullRequest.valid({
        username: 'foo',
        password: 'bar',
        database: 'baz',
        collection: 'zab',
        path: 'foo',
        host: 'bar',
        port: 0
      });
      should.strictEqual(result, false);
    });

    it('should return true when all fields but host and port are set', function() {
      var result = pullRequest.valid({
        username: 'foo',
        password: 'bar',
        database: 'baz',
        collection: 'zab',
        path: 'foo'
      });
      should.strictEqual(result, true);
    });

    it('should return true when all fields but path are set', function() {
      var result = pullRequest.valid({
        username: 'foo',
        password: 'bar',
        database: 'baz',
        collection: 'zab',
        host: 'bar',
        port: 0
      });
      should.strictEqual(result, true);
    });

    it('should return true when all fields but path and host are set', function() {
      var result = pullRequest.valid({
        username: 'foo',
        password: 'bar',
        database: 'baz',
        collection: 'zab',
        port: 0
      });
      should.strictEqual(result, true);
    });

    it('should return true when all fields but path and port are set', function() {
      var result = pullRequest.valid({
        username: 'foo',
        password: 'bar',
        database: 'baz',
        collection: 'zab',
        host: 'foo'
      });
      should.strictEqual(result, true);
    });

    it('should return false when port is a string', function() {
      var result = pullRequest.valid({
        username: 'foo',
        password: 'bar',
        port: '1'
      });
      should.strictEqual(result, false);
    });
  });
});
