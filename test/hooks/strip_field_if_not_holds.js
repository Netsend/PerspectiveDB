'use strict';

var should = require('should');

var hook = require('../../hooks/strip_field_if_not_holds.js');

describe('StripFieldIfNotHolds', function() {
  it('should not strip given field if criteria holds on item', function(done) {
    var item = { foo: 'bar', qux: 'raboof' };
    var opts = {
      field: 'qux',
      fieldFilter: {
        foo: 'bar'
      }
    };
    hook(null, item, opts, function(err) {
      if (err) { throw err; }
      should.deepEqual(item, { foo: 'bar', qux: 'raboof' });
      done();
    });
  });

  it('should strip field if criteria does not hold on item', function(done) {
    var item = { foo: 'bar', qux: 'raboof' };
    var opts = {
      field: 'qux',
      fieldFilter: {
        foo: 'baz'
      }
    };
    hook(null, item, opts, function(err) {
      if (err) { throw err; }
      should.deepEqual(item, { foo: 'bar' });
      done();
    });
  });
});
