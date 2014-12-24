'use strict';

/* jshint -W068 */

var should = require('should');

var Netsend = require('../../../lib/netsend');

describe('Netsend', function () {
  describe('pad', function () {
    it('should require val to be a string', function () {
      (function () { Netsend.pad(0); }).should.throwError('val must be a string');
    });

    it('should require len to be a number', function () {
      (function () { Netsend.pad('', ' '); }).should.throwError('len must be a number');
    });

    it('should require character to be a string', function () {
      (function () { Netsend.pad('', 0, []); }).should.throwError('character must be a string');
    });

    it('should require rpad to be a boolean', function () {
      (function () { Netsend.pad('', 0, '', []); }).should.throwError('rpad must be a boolean');
    });

    it('should pad "0" to "0"', function () {
      var result = Netsend.pad('0');
      should.strictEqual(result, '0');
    });

    it('should pad with custom characters and length', function () {
      var result = Netsend.pad('1', 3, '0');
      should.strictEqual(result, '001');
    });

    it('should pad to the right with custom characters and length', function () {
      var result = Netsend.pad('1', 3, '0', true);
      should.strictEqual(result, '100');
    });
  });
});
