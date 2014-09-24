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

var mongodb = require('mongodb');

program
  .version('0.0.1')
  .usage('timestamp')
  .description('translate a Timestamp to a time')
  .parse(process.argv);

if (!program.args[0]) { program.help(); }

var input = program.args[0];
var date;

if (input.length === 10) {
  date = new Date(Number(input) * 1000);
} else if (input.length === 13) {
  date = new Date(Number(input));
} else {
  var ts = mongodb.Timestamp.fromString(program.args[0]);
  date = new Date(ts.getHighBits() * 1000);
  date = '[' + ts.getLowBits() + ', ' + ts.getHighBits() + '] ' + date;
}

console.log(date);
