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

var fs = require('fs');
var util = require('util');

var async = require('async');
var posix = require('posix');

// ordered priorities (see POSIX)
var EMERG     = 0;      /* system is unusable */
var ALERT     = 1;      /* action must be taken immediately */
var CRIT      = 2;      /* critical conditions */
var ERR       = 3;      /* error conditions */
var WARNING   = 4;      /* warning conditions */
var NOTICE    = 5;      /* normal but significant condition */
var INFO      = 6;      /* informational */
var DEBUG     = 7;      /* debug-level messages */
var DEBUG2    = 8;      /* more verbose debug-level messages */

var PRIOS = ['EMERG', 'ALERT', 'CRIT', 'ERR', 'WARNING', 'NOTICE', 'INFO', 'DEBUG', 'DEBUG2'];

var ERR_THRESHOLD = 3;      /* priority error or worse */

// ensure a log file is opened in append only mode
function openFile(file, cb) {
  var options = { flags: 'a' };

  switch (typeof file) {
    case 'object':
      process.nextTick(function() {
        if (typeof file.write !== 'function') {
          cb(new TypeError('file must support write'));
          return;
        }
        cb(null, file);
      });
      break;
    case 'number':
      options.fd = file;
      file = '';
      /* falls through */
    case 'string':
      var logFile = fs.createWriteStream(file, options);
      logFile.once('error', cb);
      logFile.once('open', function() {
        cb(null, logFile);
      });
      break;
    default:
      process.nextTick(function() {
        cb(new Error('file not opened'));
      });
  }
}

/**
 * Create a new logger.
 *
 * Note: specify at least one of console, syslog, file, error or silence.
 *
 * @param {Object} [opts]  options, see below
 * @param {Function} cb  first parameter will be an error or null. Second parameter
 *                       will be an boject containing the following functions:
 *     emerg:   log emerg
 *     alert:   log alert
 *     crit:    log crit
 *     err:     log err
 *     warning: log warning
 *     notice:  log notice
 *     info:    log info
 *     debug:   log debug
 *     debug2:  log debug2
 *     getFileStream:  return writable stream for the normal logging file
 *     getErrorStream: return writable stream for the error logging file
 *     close: stop logging and close file handles
 *
 * Options
 *   ident {String, default 'logger'}  name to prepend after the date
 *   console {Boolean, default: false}  whether to log to the console
 *   syslog {Boolean, default false}  whether to log to the system logger
 *   file {String|Number|Object}  log all messages to this file, either a filename,
 *                                file descriptor or writable stream.
 *   error {String|Number|Object}  extra file to log errors only, either a
 *                                 filename, file descriptor or writable stream.
 *   mask {Number, default NOTICE}  set a minimum priority for "file"
 *   silence {Boolean, default false}  whether to suppress logging or not
 */
function logger(opts, cb) {
  if (typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  if (opts.hasOwnProperty('ident') && typeof opts.ident !== 'string') { throw new TypeError('opts.ident must be a string'); }
  if (opts.hasOwnProperty('file')) {
    if (typeof opts.file === 'object') {
      if (typeof opts.file.write !== 'function') { throw new TypeError('opts.file writable stream must have a write function'); }
    } else if (typeof opts.file !== 'string' && typeof opts.file !== 'number') {
      throw new TypeError('opts.file must be a string or a number');
    }
  }
  if (opts.hasOwnProperty('error')) {
    if (typeof opts.error === 'object') {
      if (typeof opts.error.write !== 'function') { throw new TypeError('opts.error writable stream must have a write function'); }
    } else if (typeof opts.error !== 'string' && typeof opts.error !== 'number') {
      throw new TypeError('opts.error must be a string or a number');
    }
  }

  var ident = opts.ident || 'logger';
  var file = opts.file;
  var error = opts.error;

  var cons = false;
  if (opts.hasOwnProperty('console')) {
    if (typeof opts.console !== 'boolean') { throw new TypeError('opts.console must be a boolean'); }
    cons = opts.console;
  }

  var syslog = false;
  if (opts.hasOwnProperty('syslog')) {
    if (typeof opts.syslog !== 'boolean') { throw new TypeError('opts.syslog must be a boolean'); }
    syslog = opts.syslog;
  }

  var mask = NOTICE;
  if (opts.hasOwnProperty('mask')) {
    if (typeof opts.mask !== 'number') { throw new TypeError('opts.mask must be a priority number'); }
    mask = opts.mask;
  }

  var silence = false;
  if (opts.hasOwnProperty('silence')) {
    if (typeof opts.silence !== 'boolean') { throw new TypeError('opts.silence must be a boolean'); }
    silence = opts.silence;
  }

  if (!cons && !syslog && !file && !error && !silence) { throw new Error('configure at least one logging method'); }

  var logFile, errFile;

  if (syslog) {
    posix.openlog(ident, { ndelay: true, pid: true }, 'daemon');
  }

  var tasks = [function(cb2) {
    // ensure async even without any other tasks
    process.nextTick(cb2);
  }];

  if (file) {
    tasks.push(function(cb2) {
      openFile(file, function(err, f) {
        if (err) { cb2(err); return; }
        logFile = f;
        cb2();
      });
    });
  }

  if (error) {
    tasks.push(function(cb2) {
      openFile(error, function(err, f) {
        if (err) { cb2(err); return; }
        errFile = f;
        cb2();
      });
    });
  }

  // format date
  var start = new Date();
  var startYear   = start.getFullYear();
  var startMonth  = start.getMonth() + 1;
  var startDay    = start.getDate();
  var startHour   = start.getHours();
  var startMinute = start.getMinutes();
  var startSecond = start.getSeconds();

  var YEAR   = 0x01;
  var MONTH  = 0x02;
  var DAY    = 0x04;
  var HOUR   = 0x08;
  var MINUTE = 0x10;
  var SECOND = 0x20;

  var disabled = YEAR + MONTH + DAY + HOUR + MINUTE + SECOND;

  function padTwoDigits(val) {
    return ('00' + val).slice(-2);
  }

  function strFmtTime() {
    var now = new Date();

    var currYear   = now.getFullYear()
      , currMonth  = now.getMonth() + 1
      , currDay    = now.getDate()
      , currHour   = now.getHours()
      , currMinute = now.getMinutes()
      , currSecond = now.getSeconds();

    var result = '';

    if (YEAR & disabled) {
      if (currYear > startYear) {
        disabled = 0x00;
        result += currYear + '-';
      }
    } else {
      result += currYear + '-';
    }

    if (MONTH & disabled) {
      if (currMonth > startMonth) {
        disabled = YEAR;
        result += padTwoDigits(currMonth) + '-';
      }
    } else {
      result += padTwoDigits(currMonth) + '-';
    }

    if (DAY & disabled) {
      if (currDay > startDay) {
        disabled = YEAR + MONTH;
        result += padTwoDigits(currDay) + ' ';
      }
    } else {
      result += padTwoDigits(currDay) + ' ';
    }

    if (HOUR & disabled) {
      if (currHour > startHour) {
        disabled = YEAR + MONTH + DAY;
        result += padTwoDigits(currHour) + ':';
      }
    } else {
      result += padTwoDigits(currHour) + ':';
    }

    if (MINUTE & disabled) {
      if (currMinute > startMinute) {
        disabled = YEAR + MONTH + DAY + HOUR;
        result += padTwoDigits(currMinute) + ':';
      }
    } else {
      result += padTwoDigits(currMinute) + ':';
    }

    if (SECOND & disabled) {
      if (currSecond > startSecond) {
        disabled = YEAR + MONTH + DAY + HOUR + MINUTE;
        result += padTwoDigits(currSecond);
      }
    } else {
      result += padTwoDigits(currSecond);
    }

    return result;
  }

  /**
   * Log to syslog, and/or file and/or err and/or console
   *
   * @param {Number} prio  priority or severity of this message
   * @param {String} msg  message to log
   *   all remaining arguments will be concatenated to msg with a space
   */
  function log() {
    if (arguments[0] > mask || silence) { return; }

    var prio = Array.prototype.slice.call(arguments, 0, 1);
    var msgs = Array.prototype.slice.call(arguments, 1);

    var msg = msgs.join(' ');

    if (syslog) {
      posix.syslog(prio, msg);
    }

    var fmtMsg = strFmtTime() + ' ' + ident + '[' + process.pid + '] ' + prio + ': ' + util.format.apply(this, msgs) + '\n';

    if (logFile) {
      logFile.write(fmtMsg);
    }

    if (errFile && (prio <= ERR_THRESHOLD)) {
      errFile.write(fmtMsg);
    }

    if (cons) {
      if (prio <= ERR_THRESHOLD) {
        process.stderr.write(fmtMsg);
      } else {
        process.stdout.write(fmtMsg);
      }
    }
  }

  function logEmerg()   { Array.prototype.unshift.call(arguments, EMERG);   log.apply(null, arguments); }
  function logAlert()   { Array.prototype.unshift.call(arguments, ALERT);   log.apply(null, arguments); }
  function logCrit()    { Array.prototype.unshift.call(arguments, CRIT);    log.apply(null, arguments); }
  function logErr()     { Array.prototype.unshift.call(arguments, ERR);     log.apply(null, arguments); }
  function logWarning() { Array.prototype.unshift.call(arguments, WARNING); log.apply(null, arguments); }
  function logNotice()  { Array.prototype.unshift.call(arguments, NOTICE);  log.apply(null, arguments); }
  function logInfo()    { Array.prototype.unshift.call(arguments, INFO);    log.apply(null, arguments); }
  function logDebug()   { Array.prototype.unshift.call(arguments, DEBUG);   log.apply(null, arguments); }
  function logDebug2()  { Array.prototype.unshift.call(arguments, DEBUG2);  log.apply(null, arguments); }

  function close(cb2) {
    var tasks2 = [function(cb3) {
      // ensure async even without any other tasks
      process.nextTick(cb3);
    }];

    logDebug('closing all logs');

    if (syslog) { posix.closelog(); }

    if (logFile)  {
      tasks2.push(function(cb3) {
        logFile.end(cb3);
      });
    }
    if (errFile) {
      tasks2.push(function(cb3) {
        errFile.end(cb3);
      });
    }

    async.parallel(tasks2, cb2);
  }

  async.parallel(tasks, function(err) {
    if (err) { cb(err); return; }

    cb(null, {
      emerg:   logEmerg,
      alert:   logAlert,
      crit:    logCrit,
      err:     logErr,
      warning: logWarning,
      notice:  logNotice,
      info:    logInfo,
      debug:   logDebug,
      debug2:  logDebug2,
      getFileStream: function() { return logFile; },
      getErrorStream: function() { return errFile; },
      close: close
    });
  });
}

module.exports = logger;

module.exports.EMERG     =  EMERG;
module.exports.ALERT     =  ALERT;
module.exports.CRIT      =  CRIT;
module.exports.ERR       =  ERR;
module.exports.WARNING   =  WARNING;
module.exports.NOTICE    =  NOTICE;
module.exports.INFO      =  INFO;
module.exports.DEBUG     =  DEBUG;
module.exports.DEBUG2    =  DEBUG2;

module.exports.levelToPrio = function(name) {
  if (!name) { return null; }

  var idx = PRIOS.indexOf(name.toUpperCase());
  if (~idx) {
    return idx;
  }
  return null;
};

// ensure a log file is opened in append only mode
module.exports.openFile = openFile;
