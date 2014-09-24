/*jshint -W068, -W030 */

var should = require('should');

var hook = require('../../hooks/strip_field_if_holds.js');

describe('StripFieldIfHolds', function() {
  it('should strip given field if criteria holds on item', function(done) {
    var item = { foo: 'bar', qux: 'raboof' };
    var opts = {
      field: 'qux',
      fieldFilter: {
        foo: 'bar'
      }
    };
    hook(null, item, opts, function(err) {
      if (err) { throw err; }
      should.deepEqual(item, { foo: 'bar' });
      done();
    });
  });

  it('should not strip field if criteria does not hold on item', function(done) {
    var item = { foo: 'bar', qux: 'raboof' };
    var opts = {
      field: 'qux',
      fieldFilter: {
        foo: 'baz'
      }
    };
    hook(null, item, opts, function(err) {
      if (err) { throw err; }
      should.deepEqual(item, { foo: 'bar', qux: 'raboof' });
      done();
    });
  });
});
