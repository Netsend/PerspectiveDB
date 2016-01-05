#!/usr/bin/env node

/**
 * Copyright 2015 Netsend.
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

var fs = require('fs');

var hjson = require('hjson');
var program = require('commander');
var level = require('level');

program
  .version(require('../package.json').version)
  .description('show database info')
  .usage('[-e] [config.hjson]')
  .option('-e, --extended', 'show extended information')
  .parse(process.argv);

if (program.args[1]) {
  program.help();
}

var config = {};
var configFile = program.args[0];

if (configFile) {
  config = hjson.parse(fs.readFileSync(configFile, 'utf8'));
}

function start(db) {
  console.log('leveldb.stats:\n%s', db.db.getProperty('leveldb.stats'));
  console.log('leveldb.sstables:\n%s', db.db.getProperty('leveldb.sstables'));
  console.log('leveldb.num-files-at-level0: %s', db.db.getProperty('leveldb.num-files-at-level0'));
  console.log('leveldb.num-files-at-level1: %s', db.db.getProperty('leveldb.num-files-at-level1'));
  console.log('leveldb.num-files-at-level2: %s', db.db.getProperty('leveldb.num-files-at-level2'));
  console.log('leveldb.num-files-at-level3: %s', db.db.getProperty('leveldb.num-files-at-level3'));

  if (!!program.extended) {
    console.log('EXTENDED');
  }
}

var newRoot = config.chroot || '/var/persdb';

var path = config.path || '/data';
// ensure leading slash
if (path[0] !== '/') {
  path = '/' + path;
}

level(newRoot + path, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, db) {
  if (err) {
    console.error(err);
    process.exit(9);
  }

  start(db);
});
