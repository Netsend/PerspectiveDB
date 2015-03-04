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
var logger = require('../../../lib/logger');

var silence;

before(function(done) {
  logger({ silence: true }, function(err, l) {
    if (err) { throw err; }
    silence = l;
    done();
  });
});

after(function(done) {
  silence.close(done);
});

describe('connManager', function () {
  describe('destination', function () {
    it('should require socket to be an object', function() {
      (function() { connManager.destination(); }).should.throw('socket must be an object');
    });

    it('should default to undefined', function() {
      var result = connManager.destination({});
      should.strictEqual(result, undefined);
    });

    it('should use remoteAddress', function() {
      var result = connManager.destination({ remoteAddress: 'foo' });
      should.strictEqual(result, 'foo-undefined-undefined-undefined');
    });

    it('should use remote and local address and port', function() {
      var result = connManager.destination({ remoteAddress: 'foo', remotePort: 20, localAddress: 'bar', localPort: 40 });
      should.strictEqual(result, 'foo-20-bar-40');
    });
  });

  describe('open', function () {
    it('should require address to be a string', function() {
      (function() { connManager.create().open(); }).should.throw('address must be a string');
    });

    it('should callback with error if not reconnecting', function(done) {
      var cm = connManager.create({ log: silence });
      cm.open('/var/run/nonexisting.sock', {}, function(err, socket) {
        should.strictEqual(err.message, 'connect ENOENT');
        should.strictEqual(socket, null);
        should.strictEqual(Object.keys(cm._conns).length, 1);
        done();
      });
    });
  });

  describe('error', function () {
    describe('unix socket', function () {
      var path = '/tmp/conn_manager_test.sock';

      var cm;
      it('needs a manager and connection for further testing', function() {
        cm = connManager.create({ log: silence });
      });

      it('should not retry', function(done) {
        // setup a server
        try {
          fs.unlinkSync(path);
        } catch(err) {
        }

        var incoming = 0;
        function handler() { incoming++; }
        var server = net.createServer(handler);
        server.listen(path);

        var called = 0;
        cm.open(path, { reconnectOnError: true, maxRetries: 0 }, function(err, conn) {
          called++;

          if (err) {
            should.strictEqual(err.message, 'maxRetries reached');
            should.strictEqual(incoming, 1);
            should.strictEqual(called, 2);
            done();
            return;
          }

          server.close();
          conn.destroy(new Error('dummy error'));
        });
      });

      it('should try to reconnect on connection error in two seconds', function(done) {
        this.timeout(10000);

        // setup a server
        try {
          fs.unlinkSync(path);
        } catch(err) {
        }

        var incoming = 0;
        function handler() { incoming++; }
        var server = net.createServer(handler);
        server.listen(path);

        var called = 0;
        cm.open(path, { reconnectOnError: true }, function(err, conn) {
          if (err) { throw err; }

          called++;

          if (called === 1) {
            should.strictEqual(incoming, 1);

            conn.on('close', function(err) {
              should.strictEqual(err, true);

              // disable the server for 3 seconds to trigger next interval
              server.close(function() {
                should.strictEqual(incoming, 1);

                setTimeout(function() {
                  should.strictEqual(incoming, 1);

                  // start a new server
                  server = net.createServer(handler);
                  server.listen(path);

                  // wait some time and see if it is reconnected
                  setTimeout(function() {
                    should.strictEqual(incoming, 2);
                    should.strictEqual(called, 2);
                    done();
                  }, 4100);
                }, 3100);
              });
            });

            conn.destroy(new Error('dummy error'));
          }
        });
      });
    });

    describe('inet socket', function () {
      var host = '127.0.0.1';
      var port = 54367;

      var server;

      var incoming = 0;
      function handler() {
        incoming++;
      }

      var cm;
      it('needs a manager and connection for further testing', function() {
        cm = connManager.create({ log: silence });
      });

      it('needs a server for further testing', function(done) {
        server = net.createServer(handler);
        server.listen(port, host, done);
      });

      it('should try to reconnect on connection error in two seconds', function(done) {
        this.timeout(10000);

        var called = 0;
        cm.open(host, port, { reconnectOnError: true }, function(err, conn) {
          if (err) { throw err; }

          called++;

          if (called === 1) {
            should.strictEqual(incoming, 1);

            conn.on('close', function(err) {
              should.strictEqual(err, true);

              // disable the server for 2 seconds to trigger next interval
              server.close(function() {
                should.strictEqual(incoming, 1);

                setTimeout(function() {
                  should.strictEqual(incoming, 1);

                  // start a new server
                  server = net.createServer(handler);
                  server.listen(port, host);

                  // wait some time and see if it is reconnected
                  setTimeout(function() {
                    should.strictEqual(incoming, 2);
                    should.strictEqual(called, 2);
                    done();
                  }, 4100);
                }, 3100);
              });
            });

            conn.destroy(new Error('dummy error'));
          }
        });
      });
    });
  });

  describe('registerIncoming', function () {
    it('should require socket to be an object', function() {
      (function() { connManager.create().registerIncoming(); }).should.throw('socket must be an object');
    });

    it('should add the given object to the connections', function() {
      var conn = new net.Socket();
      var cm = connManager.create({ log: silence });
      cm.registerIncoming(conn);
      should.strictEqual(Object.keys(cm._conns).length, 1);
      var connId = Object.keys(cm._conns)[0];
      should.strictEqual(cm._conns[connId].s, conn);
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
    var cm;
    it('needs a manager and connection for further testing', function() {
      cm = connManager.create({ log: silence });
    });

    it('needs a connection for further testing', function() {
      cm.registerIncoming(conn);
    });

    it('should remove all connections and call back', function(done) {
      should.strictEqual(Object.keys(cm._conns).length, 1);
      var connId = Object.keys(cm._conns)[0];
      should.strictEqual(cm._conns[connId].s, conn);

      cm.close(function(err) {
        if (err) { throw err; }
        should.strictEqual(Object.keys(cm._conns).length, 0);
        done();
      });
    });

    it('should close pending reconnects', function(done) {
      // needs a new connection for further testing
      cm.open('/tmp/some.sock', { reconnectOnError: true });

      should.strictEqual(Object.keys(cm._conns).length, 1);

      cm.close(function(err) {
        if (err) { throw err; }
        should.strictEqual(Object.keys(cm._conns).length, 0);
        done();
      });
    });
  });

  describe('_remove', function () {
    it('should require conn to be an object', function() {
      (function() { connManager.create()._remove(); }).should.throw('connId must be a string');
    });

    it('should return false if connection is not found', function() {
      var result = connManager.create({ log: silence })._remove('');
      should.strictEqual(result, false);
    });

    var conn = new net.Socket();
    var cm;
    it('needs a manager and connection for further testing', function() {
      cm = connManager.create({ log: silence });
      cm.registerIncoming(conn);
    });

    it('should return true if connection is removed', function() {
      should.strictEqual(Object.keys(cm._conns).length, 1);
      var connId = Object.keys(cm._conns)[0];
      should.strictEqual(cm._conns[connId].s, conn);

      cm._remove(connId);
      should.strictEqual(Object.keys(cm._conns).length, 0);
    });
  });
});
