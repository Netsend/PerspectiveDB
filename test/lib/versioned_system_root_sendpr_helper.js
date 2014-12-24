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

// spawn two servers

'use strict';

if (process.getuid() !== 0) { 
  console.error('run helper as root');
  process.exit(1);
}

var spawn = require('child_process').spawn;

var args = [__dirname + '/../../server2.js', '-d'];
var child_client = spawn('/usr/local/bin/node', args, {
  env: {
    NODE_ENV: 'test_client'
  }
});

child_client.on('error', function(err) {
  console.error(err);
});

child_client.on('data', function(data) {
  console.log(data);
});

child_client.stdout.setEncoding('utf8');
child_client.stdout.on('data', function(data) {
  console.log(data);
});

child_client.stderr.setEncoding('utf8');
child_client.stderr.on('data', function(data) {
  console.error(data);
});
