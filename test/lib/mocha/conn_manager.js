/**
 * Copyright 2014, 2015 Netsend.
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

var net = require('net');
var fs = require('fs');

var should = require('should');

var connManager = require('../../../lib/conn_manager');

describe('connManager', function () {
  describe('createId', function () {
    it('should require conn to be an object', function() {
      (function() { connManager.createId(); }).should.throw('conn must be an object');
    });

    it('should default to "unknown socket"', function() {
      var result = connManager.createId({});
      should.strictEqual(result, 'unknown socket');
    });

    it('should use remoteAddress', function() {
      var result = connManager.createId({ remoteAddress: 'foo' });
      should.strictEqual(result, 'foo-undefined-undefined-undefined');
    });

    it('should use remote and local address and port', function() {
      var result = connManager.createId({ remoteAddress: 'foo', remotePort: 20, localAddress: 'bar', localPort: 40 });
      should.strictEqual(result, 'foo-20-bar-40');
    });
  });

  describe('register', function () {
    it('should require conn to be an object', function() {
      (function() { connManager.create().register(); }).should.throw('conn must be an object');
    });

    it('should add the given object to the connections', function() {
      var conn = new net.Socket();
      var cm = connManager.create();
      cm.register(conn);
      should.strictEqual(cm._conns.length, 1);
      should.strictEqual(cm._conns[0], conn);
    });
  });

  describe('close', function () {
    it('should require cb to be a function', function() {
      (function() { connManager.create().close(); }).should.throw('cb must be a function');
    });

    it('should call callback (without connections)', function(done) {
      connManager.create().close(done);
    });

    var conn = new net.Socket();
    var cm = connManager.create({ hide: true });

    it('needs a connection for further testing', function() {
      cm.register(conn);
    });

    it('should remove all connections and call back', function(done) {
      should.strictEqual(cm._conns.length, 1);
      should.strictEqual(cm._conns[0], conn);

      cm.close(function(err) {
        if (err) { throw err; }
        should.strictEqual(cm._conns.length, 0);
        done();
      });
    });
  });

  describe('_remove', function () {
    it('should require conn to be an object', function() {
      (function() { connManager.create()._remove(); }).should.throw('conn must be an object');
    });

    it('should return false if connection is not found', function() {
      var result = connManager.create({ hide: true })._remove({});
      should.strictEqual(result, false);
    });

    var conn = new net.Socket();
    var cm = connManager.create();

    it('needs a connection for further testing', function() {
      cm.register(conn);
    });

    it('should return true if connection is removed', function() {
      should.strictEqual(cm._conns.length, 1);
      should.strictEqual(cm._conns[0], conn);

      cm._remove(conn);
      should.strictEqual(cm._conns.length, 0);
    });
  });

  describe('error', function () {
    describe('unix socket', function () {
      var path = '/tmp/conn_manager_test.sock';

      var server, conn;
      var cm = connManager.create({ hide: true });

      var incoming = 0;
      function handler() {
        incoming++;
      }

      it('needs a server and connection for further testing', function(done) {
        try {
          fs.unlinkSync(path);
        } catch(err) {
        }
        server = net.createServer(handler);
        server.listen(path, function() {
          conn = net.createConnection(path, done);
          conn.path = path;
          cm.register(conn, { reconnectOnError: true });
        });
      });

      it('should try to reconnect on connection error in two seconds', function(done) {
        this.timeout(10000);

        should.strictEqual(incoming, 1);

        conn.on('close', function(err) {
          should.strictEqual(err, true);

          // disable the server for 2 seconds to trigger next interval
          server.close(function() {
            should.strictEqual(incoming, 1);

            setTimeout(function() {
              should.strictEqual(incoming, 1);

              server = net.createServer(handler);
              server.listen(path);

              // wait some time and see if it is reconnected
              setTimeout(function() {
                should.strictEqual(incoming, 2);
                done();
              }, 4100);
            }, 2100);
          });
        });

        conn.destroy(new Error('dummy error'));
      });
    });

    describe('inet socket', function () {
      var host = '127.0.0.1';
      var port = 54367;

      var server, conn;
      var cm = connManager.create({ hide: true });

      var incoming = 0;
      function handler() {
        incoming++;
      }

      it('needs a server and connection for further testing', function(done) {
        server = net.createServer(handler);
        server.listen(port, host, function() {
          conn = net.createConnection(port, host, done);
          cm.register(conn, { reconnectOnError: true });
        });
      });

      it('should try to reconnect on connection error in two seconds', function(done) {
        this.timeout(10000);

        should.strictEqual(incoming, 1);

        conn.on('close', function(err) {
          should.strictEqual(err, true);

          // disable the server for 2 seconds to trigger next interval
          server.close(function() {
            should.strictEqual(incoming, 1);

            setTimeout(function() {
              should.strictEqual(incoming, 1);

              server = net.createServer(handler);
              server.listen(port, host);

              // wait some time and see if it is reconnected
              setTimeout(function() {
                should.strictEqual(incoming, 2);
                done();
              }, 4100);
            }, 2100);
          });
        });

        conn.destroy(new Error('dummy error'));
      });
    });
  });
});
