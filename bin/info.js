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

var crc32 = require('crc-32');
var properties = require('properties');
var fs = require('fs');

var _db = require('./_db');

var program = require('commander');
var VersionedSystem = require('../lib/versioned_system');
var Netsend = require('../lib/netsend');

program
  .version(require('../package.json').version)
  .description('show snapshot collection status')
  .usage('[-ef] config\n         info.js [-e] -f config [dbname][.collname]')
  .option('-f, --config <config>', 'an ini config file')
  .option('-e, --extended', 'show extended information')
  .parse(process.argv);

if (!program.args[0] && !program.config) {
  program.help();
}

var config = program.config || program.args[0];

// if relative, prepend current working dir
if (config[0] !== '/') {
  config = process.cwd() + '/' + config;
}

var ns = [];

// check if certain database and collection name is given
// add them to ns array
if (program.config && program.args[0]) {
  var splitted = program.args[0].split('.');
  if (!splitted[0]) {
    ns.push('.' + splitted[1]);
  } else {
    ns.push(program.args[0]);
  }
}

config = properties.parse(fs.readFileSync(config, { encoding: 'utf8' }), { sections: true, namespaces: true });

// create namespaces from all vcs
Object.keys(config.vc).forEach(function(db) {
  if (!config.vc[db]) {
    ns.push(db);
  } else {
    Object.keys(config.vc[db]).forEach(function(collection) {
      ns.push(db + '.' + collection);
    });
  }
});

function start(db) {
  var oplogColl = db.db(config.database.oplogDb || 'local').collection(config.database.oplogCollection || 'oplog.$main');

  var opts = { debug: program.debug };
  var vs = new VersionedSystem(oplogColl, opts);
  vs.info({ extended: !!program.extended, nsList: ns }, function(err, stats) {
    if (err) {
      console.error(err);
      process.exit(1);
    }

    // determine max database + collection name length
    var dbColls = Object.keys(stats).sort();
    var maxLength = 1 + dbColls.reduce(function(prev, curr) {
      return Math.max(prev, curr.length);
    }, 0);

    var params = [
      Netsend.pad('', maxLength, ' ', true),
      Netsend.pad('size (KB)', 13),
      Netsend.pad('storageSize (KB)', 17),
      Netsend.pad('%', 7),
      Netsend.pad('docs', 10)
    ];
    if (!!program.extended) {
      params.push(Netsend.pad('ack', 10));
    }
    params.push(Netsend.pad('coll', 10));
    if (!!program.extended) {
      params.push(Netsend.pad('crc32 docs/ack/coll count', 25));
    }

    // print header
    console.log('Snapshot and oplog collection stats');
    console.log.apply(console, params);

    // find info per versioned collection
    var size, storageSize, sizeKb, storageSizeKb, percentage, docs, vers, ack, coll, sum;
    dbColls.forEach(function(dbColl) {
      size = stats[dbColl].snapshotCollection.size;
      storageSize = stats[dbColl].snapshotCollection.storageSize;
      sizeKb = Math.round(size / 1024);
      storageSizeKb = Math.round(storageSize / 1024);
      vers = stats[dbColl].snapshotCollection.count;
      coll = stats[dbColl].collection.count;
      if (!!program.extended) {
        ack = stats[dbColl].extended.ack;
        sum = crc32.str('' + vers + '-' + ack + '-' + coll);
        sum = sum < 0 ? 4294967296 + sum : sum;
        sum = sum.toString(36);
      }
      percentage = Math.round((10000 / storageSize) * size) / 100;

      params = [
        Netsend.pad(dbColl, maxLength, ' ', true),
        Netsend.pad('' + sizeKb, 13),
        Netsend.pad('' + storageSizeKb, 17),
        Netsend.pad(percentage+'%', 7),
        Netsend.pad('' + vers, 10),
      ];
      if (!!program.extended) {
        params.push(Netsend.pad('' + ack, 10));
      }
      params.push(Netsend.pad('' + coll, 10));
      if (!!program.extended) {
        params.push(Netsend.pad(sum, 25));
      }

      console.log.apply(console, params);
    });

    var oplog = db.db('local').collection('oplog.$main');
    oplog.stats(function(err, stats) {
      if (err) { throw err; }

      size = stats.size;
      storageSize = stats.storageSize;
      sizeKb = Math.round(size / 1024);
      storageSizeKb = Math.round(storageSize / 1024);
      docs = stats.count;
      percentage = Math.round((10000 / storageSize) * size) / 100;

      // find the first and oldest oplog document to determine the retention time
      oplog.findOne({}, function(err, first) {
        if (err) { throw err; }

        var todaySeconds = Date.now() / 1000;
        var oplogSeconds = first.ts.getHighBits();

        var todayDays = todaySeconds / 86400;
        var oplogDays = oplogSeconds / 86400;

        var todayHours = todaySeconds / 3600;
        var oplogHours = oplogSeconds / 3600;

        var todayMinutes = todaySeconds / 60;
        var oplogMinutes = oplogSeconds / 60;

        var days = Math.floor(todayDays - oplogDays);
        var hours = Math.floor(todayHours - oplogHours - (days * 24));
        var minutes = Math.floor(todayMinutes - oplogMinutes - (days * 24 * 60 + hours * 60));
        var seconds = Math.floor(todaySeconds - oplogSeconds - (days * 24 * 60 * 60 + hours * 60 * 60 + minutes * 60));

        console.log(
          Netsend.pad('oplog.$main', maxLength, ' ', true),
          Netsend.pad('' + sizeKb, 13),
          Netsend.pad('' + storageSizeKb, 17),
          Netsend.pad(percentage+'%', 7),
          Netsend.pad('' + docs, 10),
          Netsend.pad('(' + days + 'd ' + hours + 'h ' + minutes + 'm ' + seconds + 's)')
          //Netsend.pad(new Date(oplogSeconds * 1000) + ' [' + first.ts.getLowBits() + ']')
        );
        db.close();
      });
    });
  });
}

var database = config.database;
var dbCfg = {
  dbName: database.name || 'local',
  dbHost: database.path || database.host,
  dbPort: database.port,
  dbUser: database.username,
  dbPass: database.password,
  adminDb: database.adminDb
};

// open database
_db(dbCfg, function(err, db) {
  if (err) { throw err; }
  start(db);
});
