/**
 * Copyright 2014, 2015 Netsend.
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

/*jshint -W068 */

var fs = require('fs');

var should = require('should');
var through = require('through2');

var logger = require('../../../lib/logger');

describe('logger', function () {
  it('should require opts to be an object', function() {
    (function() { logger(''); }).should.throw('opts must be an object');
  });

  it('should require cb to be a function', function() {
    (function() { logger({}); }).should.throw('cb must be a function');
  });

  it('should require at least one logging method', function() {
    (function() { logger({}, function() {}); }).should.throw('configure at least one logging method');
  });

  it('should return an object with all prios as methods', function(done) {
    logger({ silence: true }, function(err, log) {
      if (err) { throw err; }
      should.strictEqual(typeof log, 'object');
      should.strictEqual(typeof log.emerg, 'function');
      should.strictEqual(typeof log.alert, 'function');
      should.strictEqual(typeof log.crit, 'function');
      should.strictEqual(typeof log.err, 'function');
      should.strictEqual(typeof log.warning, 'function');
      should.strictEqual(typeof log.notice, 'function');
      should.strictEqual(typeof log.info, 'function');
      should.strictEqual(typeof log.debug, 'function');
      should.strictEqual(typeof log.debug2, 'function');
      done();
    });
  });

  it('should close after opening', function(done) {
    var ws = through();

    logger({ file: ws }, function(err, log) {
      if (err) { throw err; }
      log.close(done);
    });
  });

  it('should write on emerg level', function(done) {
    var log, ws = through();
    ws.setEncoding('utf8');

    logger({ file: ws }, function(err, l) {
      if (err) { throw err; }
      log = l;
      log.emerg('foo');
    });

    ws.on('data', function(data) {
      should.strictEqual(/ logger\[[0-9]+] 0: foo\n$/.test(data), true);
      log.close(done);
    });
  });

  it('should write on alert level', function(done) {
    var log, ws = through();
    ws.setEncoding('utf8');

    logger({ file: ws }, function(err, l) {
      if (err) { throw err; }
      log = l;
      log.alert('foo');
    });

    ws.on('data', function(data) {
      should.strictEqual(/ logger\[[0-9]+] 1: foo\n$/.test(data), true);
      log.close(done);
    });
  });

  it('should write on crit level', function(done) {
    var log, ws = through();
    ws.setEncoding('utf8');

    logger({ file: ws }, function(err, l) {
      if (err) { throw err; }
      log = l;
      log.crit('foo');
    });

    ws.on('data', function(data) {
      should.strictEqual(/ logger\[[0-9]+] 2: foo\n$/.test(data), true);
      log.close(done);
    });
  });

  it('should write on err level', function(done) {
    var log, ws = through();
    ws.setEncoding('utf8');

    logger({ file: ws }, function(err, l) {
      if (err) { throw err; }
      log = l;
      log.err('foo');
    });

    ws.on('data', function(data) {
      should.strictEqual(/ logger\[[0-9]+] 3: foo\n$/.test(data), true);
      log.close(done);
    });
  });

  it('should write on warning level', function(done) {
    var log, ws = through();
    ws.setEncoding('utf8');

    logger({ file: ws }, function(err, l) {
      if (err) { throw err; }
      log = l;
      log.warning('foo');
    });

    ws.on('data', function(data) {
      should.strictEqual(/ logger\[[0-9]+] 4: foo\n$/.test(data), true);
      log.close(done);
    });
  });

  it('should write on notice level', function(done) {
    var log, ws = through();
    ws.setEncoding('utf8');

    logger({ file: ws }, function(err, l) {
      if (err) { throw err; }
      log = l;
      log.notice('foo');
    });

    ws.on('data', function(data) {
      should.strictEqual(/ logger\[[0-9]+] 5: foo\n$/.test(data), true);
      log.close(done);
    });
  });

  it('should write on info level', function(done) {
    var log, ws = through();
    ws.setEncoding('utf8');

    logger({ file: ws, mask: logger.INFO }, function(err, l) {
      if (err) { throw err; }
      log = l;
      log.info('foo');
    });

    ws.on('data', function(data) {
      should.strictEqual(/ logger\[[0-9]+] 6: foo\n$/.test(data), true);
      log.close(done);
    });
  });

  it('should write on debug level', function(done) {
    var log, ws = through();
    ws.setEncoding('utf8');

    logger({ file: ws, mask: logger.DEBUG }, function(err, l) {
      if (err) { throw err; }
      log = l;
      log.debug('foo');
    });

    ws.once('data', function(data) {
      should.strictEqual(/ logger\[[0-9]+] 7: foo\n$/.test(data), true);
      log.close(done);
    });
  });

  it('should write on debug2 level', function(done) {
    var log, ws = through();
    ws.setEncoding('utf8');

    logger({ file: ws, mask: logger.DEBUG2 }, function(err, l) {
      if (err) { throw err; }
      log = l;
      log.debug2('foo');
    });

    ws.once('data', function(data) {
      should.strictEqual(/ logger\[[0-9]+] 8: foo\n$/.test(data), true);
      log.close(done);
    });
  });

  it('should use given ident', function(done) {
    var log, ws = through();
    ws.setEncoding('utf8');

    logger({ file: ws, ident: 'fubar' }, function(err, l) {
      if (err) { throw err; }
      log = l;
      log.notice('foo');
    });

    ws.once('data', function(data) {
      should.strictEqual(/ fubar\[[0-9]+] 5: foo\n$/.test(data), true);
      log.close(done);
    });
  });

  it('should be silent', function(done) {
    var log, ws = through();
    ws.setEncoding('utf8');

    logger({ file: ws, mask: logger.DEBUG2, silence: true }, function(err, l) {
      if (err) { throw err; }
      log = l;
      log.notice('foo');
      log.close(done);
    });

    ws.on('data', function() {
      throw new Error('not silent');
    });
  });

  it('should return file stream', function(done) {
    var ws = through();

    logger({ file: ws }, function(err, log) {
      if (err) { throw err; }
      should.strictEqual(log.getFileStream(), ws);
      should.strictEqual(log.getErrorStream(), undefined);
      log.close(done);
    });
  });

  it('should return opts', function(done) {
    var ws = through();

    logger({ file: ws, some: 'a' }, function(err, log) {
      if (err) { throw err; }
      should.deepEqual(log.getOpts(), { file: ws, some: 'a' });
      log.close(done);
    });
  });

  it('should return error stream', function(done) {
    var ws = through();

    logger({ error: ws }, function(err, log) {
      if (err) { throw err; }
      should.strictEqual(log.getFileStream(), undefined);
      should.strictEqual(log.getErrorStream(), ws);
      log.close(done);
    });
  });

  it('should map all levels to priority', function() {
    should.strictEqual(logger.levelToPrio(),          null);
    should.strictEqual(logger.levelToPrio('foo'),     null);

    should.strictEqual(logger.levelToPrio('emerg'),   logger.EMERG);
    should.strictEqual(logger.levelToPrio('alert'),   logger.ALERT);
    should.strictEqual(logger.levelToPrio('crit'),    logger.CRIT);
    should.strictEqual(logger.levelToPrio('err'),     logger.ERR);
    should.strictEqual(logger.levelToPrio('warning'), logger.WARNING);
    should.strictEqual(logger.levelToPrio('notice'),  logger.NOTICE);
    should.strictEqual(logger.levelToPrio('info'),    logger.INFO);
    should.strictEqual(logger.levelToPrio('debug'),   logger.DEBUG);
    should.strictEqual(logger.levelToPrio('debug2'),  logger.DEBUG2);
  });

  it('should open and read from a file', function(done) {
    var log, path = '/tmp/sometest';

    logger({ file: path }, function(err, l) {
      if (err) { throw err; }
      log = l;
      log.notice('foo');

      var ws = fs.createReadStream(path);

      ws.on('data', function(data) {
        should.strictEqual(/ logger\[[0-9]+] 5: foo\n$/.test(data), true);
        log.close(done);
      });
    });
  });

  it('should open a file by fd', function(done) {
    var path = '/tmp/sometest2';

    var ws = fs.createWriteStream(path);

    ws.on('open', function() {
      logger({ file: ws.fd }, function(err, l) {
        if (err) { throw err; }
        l.close(done);
      });
    });
  });
});
