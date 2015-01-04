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

var RemoteTransform = require('../../../lib/remote_transform');

describe('RemoteTransform', function() {
  it('should require remote to be a string', function() {
    (function() { var rt = new RemoteTransform([]); return rt; }).should.throw('remote must be a string');
  });

  it('should require opts to be an object', function() {
    (function() { var rt = new RemoteTransform('', ''); return rt; }).should.throw('opts must be an object');
  });

  it('should require opts.debug to be a boolean', function() {
    (function() { var rt = new RemoteTransform('', { debug: '' }); return rt; }).should.throw('opts.debug must be a boolean');
  });

  it('should require opts.hide to be a boolean', function() {
    (function() { var rt = new RemoteTransform('', { hide: '' }); return rt; }).should.throw('opts.hide must be a boolean');
  });

  it('should construct', function() {
    var rt = new RemoteTransform('');
    return rt;
  });

  it('should be a writable stream', function(done) {
    var rt = new RemoteTransform('');
    rt.end(done);
  });

  it('should be a readable stream', function(done) {
    var rt = new RemoteTransform('');
    rt.resume();
    rt.on('end', done);
    rt.end();
  });

  it('should reset _id._pe from baz to foo', function(done) {
    var rt = new RemoteTransform('foo');
    rt.on('data', function(obj) {
      should.deepEqual(obj, { _id: { _pe: 'foo' } });
      done();
    });
    rt.end({ _id: { _pe: 'baz' } });
  });

  it('should emit an error if _id._pe can not be set', function(done) {
    var rt = new RemoteTransform('foo', { hide: true });
    rt.on('error', function(err) {
      should.strictEqual(err.message, 'Cannot set property \'_pe\' of undefined');
      done();
    });

    rt.on('data', function() { throw new Error('should not emit'); });
    rt.end({});
  });

  it('should emit an error if error is set as the only key', function(done) {
    var rt = new RemoteTransform('foo', { hide: true });
    rt.on('error', function(err) {
      should.strictEqual(err.message, 'some error');
      done();
    });

    rt.on('data', function() { throw new Error('should not emit'); });
    rt.end({ error: 'some error' });
  });

  it('should emit data when error is not the only key', function(done) {
    var rt = new RemoteTransform('foo', { hide: true });
    rt.on('data', function(data) {
      should.deepEqual(data, { error: 'some error', _id: { some: 'not only error', _pe: 'foo' } });
      done();
    });

    rt.on('err', function(err) { throw err; });
    rt.end({ error: 'some error', _id: { some: 'not only error' } });
  });

  it('should support simple hooks and hook options', function(done) {
    var hooksOpts = { some: 'options' };

    function hook1(db, item, opts, cb) {
      if (opts.some !== 'options') { throw new Error('missing opts'); }

      if (item.foo === 'bar') {
        item.hook1 = true;
      }
      cb(null, item);
    }

    function hook2(db, item, opts, cb) {
      if (opts.some !== 'options') { throw new Error('missing opts'); }

      if (item.foo === 'baz') {
        cb(null, null);
      }
      cb(null, item);
    }

    var hooks = [hook1, hook2];

    var opts = {
      hooks: hooks,
      hooksOpts: hooksOpts
    };
    var rt = new RemoteTransform('fu', opts);

    rt.once('data', function(data) {
      should.deepEqual(data, { foo: 'bar', hook1: true, _id: { _id: 'foo', _pe: 'fu' } });

      rt.on('data', function(data) {
        should.deepEqual(data, { foo: 'quz', _id: { _id: 'foo', _pe: 'fu' } });
        done();
      });
      rt.write({ _id: { _id: 'foo' }, foo: 'baz' }); // this one should be filtered by hook2
      rt.write({ _id: { _id: 'foo' }, foo: 'quz' }); // nothing should happen to this one and it should be emitted
    });

    rt.write({ _id: { _id: 'foo' }, foo: 'bar' });
  });
});
