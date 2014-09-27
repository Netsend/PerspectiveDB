/*jshint -W068, -W030 */

'use strict';

var should = require('should');

var hook = require('../../hooks/if_not_older_than.js');

describe('IfNotOlderThan', function() {
  it('should not filter item if field is not older than max age', function(done) {
    var quxDate = new Date();
    var item = {
      foo: 'bar',
      qux: quxDate
    };
    var opts = {
      field: 'qux',
      maxAge: 3600
    };
    hook(null, item, opts, function(err, newItem) {
      if (err) { throw err; }
      should.deepEqual(newItem, { foo: 'bar', qux: quxDate });
      done();
    });
  });

  it('should filter item if field is older than max age', function(done) {
    var quxDate = (new Date()).getTime();
    quxDate = quxDate - (3601 * 1000);
    quxDate = new Date(quxDate);
    var item = {
      foo: 'bar',
      qux: quxDate
    };
    var opts = {
      field: 'qux',
      maxAge: 3600
    };
    hook(null, item, opts, function(err, newItem) {
      if (err) { throw err; }
      should.deepEqual(newItem, null);
      done();
    });
  });
});
