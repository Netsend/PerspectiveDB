#!/usr/bin/env node

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

var program = require('commander');
var read = require('read');
var User = require('mongo-bcrypt-user');

var _db = require('./_db');

program
  .version('0.0.1')
  .usage('-d <database> -u <username> [-c <collection> -r <realm>] [config.json]')
  .option('-d, --database <database>', 'name of the database to use')
  .option('-c, --collection <collection>', 'optional name of the collection to use, defaults to users')
  .option('-u, --username <username>', 'name of the user to add')
  .option('-r, --realm <realm>', 'optional realm this user belongs to, defaults to ms')
  .description('add a user to the database, password is read from stdin')
  .parse(process.argv);

if (!program.database || !program.username) {
  program.help();
}

var config = program.args[0] || '';

// if relative, prepend current working dir
if (config[0] !== '/') {
  config = process.cwd() + '/' + config;
}

if (fs.existsSync(config)) {
  var stat = fs.statSync(config);
  if (stat.isFile()) {
    config = require(config);
  } else {
    config = { dbName: program.database };
  }
} else {
  config = { dbName: program.database };
}

var collectionName = program.collection || 'users';
var realm = program.realm || 'ms';

function start(db) {
  read({ prompt: 'password:', silent: true }, function(err, password) {
    if (err) { console.error(err.message); process.exit(1); }

    read({ prompt: 'repeat:', silent: true }, function(err, password2) {
      if (err) { console.error(err.message); process.exit(1); }

      if (password !== password2) {
        console.log('passwords are not equal');
        process.exit(2);
      }

      var coll = db.db(program.database).collection(collectionName);
      try {
        User.register(coll, program.username, password, realm, function(err) {
          if (err) { console.error(err.message); process.exit(3); }

          process.exit();
        });
      } catch(err) {
        console.error(err.message);
        process.exit(4);
      }
    });
  });
}

// open database
_db(config, function(err, db) {
  if (err) { throw err; }
  start(db);
});
