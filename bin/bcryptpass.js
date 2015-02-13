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

var program = require('commander');
var read = require('read');
var User = require('array-bcrypt-user');

program
  .version('0.0.2')
  .usage('bcryptpass.js')
  .description('echo a minimal user object containing a username, password and optionally a realm')
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

      read({ prompt: 'realm [_default]:' }, function(err, realm) {
        if (err) { console.error(err.message); process.exit(1); }
        if (realm === '') {
          realm = undefined;
        }

        var db = [];
        User.register(db, username, password, realm, function(err) {
          if (err) { console.error(err.message); process.exit(1); }

          console.log(db[0]);
          process.exit();
        });
      });
    });
  });
});
