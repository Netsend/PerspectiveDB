#!/usr/bin/env node

/**
 * Copyright 2016 Netsend.
 *
 * This file is part of PersDB.
 *
 * PersDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PersDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PersDB. If not, see <https://www.gnu.org/licenses/>.
 */

'use strict';

if (process.getuid() !== 0) {
  console.error('run as root');
  process.exit(1);
}

var url = require('url');

var chroot = require('chroot');
var posix = require('posix');
var program = require('commander');

var spawn = require('../test/lib/spawn');
var logger = require('../lib/logger');

program
  .version('0.0.2')
  .usage('-f fingerprint -L [bind_address:]port:host:hostport [-i identity_file] ssh://foo@host')
  .description('setup ssh tunnel')
  .option('-f, --fingerprint <print>', 'hex or base64 fingerprint of remote server')
  .option('-L, --listen [bind_address:]port:host:hostport]', 'listen directive')
  .option('-i, --identity <file>', 'default to ~/.ssh/id_rsa')
  .parse(process.argv);

if (!program.fingerprint) { program.help(); }
if (!program.listen) { program.help(); }
if (!program.args) { program.help(); }

var parsedUrl = url.parse(program.args[0]);

var opts = {
  onMessage: function(msg, child) {
    switch (msg) {
    case 'init':
      var options = {
        log: { console: true, mask: logger.DEBUG },
        key: program.identity || '~/.ssh/id_rsa',
        forward: program.listen,
        sshUser: parsedUrl.auth || process.env.USER,
        host: parsedUrl.hostname,
        port: parsedUrl.port,
        fingerprint: program.fingerprint
      };
      child.send(options);
      break;
    case 'listen':
      // chroot or exit
      var uid, gid;
      uid = posix.getpwnam('nobody').uid;
      gid = posix.getgrnam('nogroup').gid;
      chroot('/var/empty', uid, gid);
      break;
    }
  },
  echoOut: true
};
spawn([__dirname + '/../lib/tunnel_exec'], opts);
