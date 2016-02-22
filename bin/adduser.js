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

var program = require('commander');
var read = require('read');
var User = require('array-bcrypt-user');

program
  .version('0.0.3')
  .usage('adduser.js')
  .description('create a passwd entry containing a username and a bcrypt password')
  .parse(process.argv);

read({ prompt: 'username:' }, function(err, username) {
  if (err) { console.error(err.message); process.exit(1); }

  read({ prompt: 'password:', silent: true }, function(err, password) {
    if (err) { console.error(err.message); process.exit(1); }

    read({ prompt: 'repeat password:', silent: true }, function(err, password2) {
      if (err) { console.error(err.message); process.exit(1); }

      if (password !== password2) {
        console.log('passwords are not equal');
        process.exit(2);
      }

      var db = [];
      User.register(db, username, password, function(err) {
        if (err) { console.error(err.message); process.exit(1); }

        console.log('%s:%s', db[0].username, db[0].password);
        process.exit();
      });
    });
  });
});
