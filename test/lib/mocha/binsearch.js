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

var should = require('should');

var inArray = require('../../../lib/binsearch').inArray;

describe('binsearch', function() {
  describe('inArray', function() {
    var ar = ['ab', 'cd', 'ef'];

    it('should find if el in array', function() {
      var result = inArray(ar, 'ab');
      should.equal(result, true);
    });

    it('should not find if el not in array', function() {
      var result = inArray(ar, 'c');
      should.equal(result, false);
    });
  });
});
