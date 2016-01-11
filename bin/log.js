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

var fs = require('fs');

var async = require('async');
var hjson = require('hjson');
var level = require('level');
var program = require('commander');

var doDiff = require('../lib/diff');
var MergeTree = require('../lib/merge_tree');

program
  .version('0.0.1')
  .usage('[-f config] [-n]')
  .description('print graph of given database')
  .option('    --id <id>', 'show the log of one string based id')
  .option('-f, --config <config>', 'hjson config file with database path')
  .option('-s, --show', 'show complete objects')
  .option('-p, --patch', 'show patch compared to previous version')
  .option('    --pe <perspective>', 'perspective')
  .option('-n, --number <number>', 'number of latest versions to show, defaults to 10, 0 means unlimited')
  .parse(process.argv);

var config = {};
var configFile = program.config;

if (configFile) {
  config = hjson.parse(fs.readFileSync(configFile, 'utf8'));
}

if (program.number === '0') {
  program.number = Infinity;
} else {
  program.number = Number(program.number) || 10;
}

var id;
if (program.id) {
  id = program.id;
}
// format merge tree stats
function fmtStats(stats) {
  var result = '';
  Object.keys(stats).forEach(function(pe) {
    result += pe + ' heads:\t' + stats[pe].heads.count + '\n';

    if (stats[pe].heads.conflict) {
      result += '  conflict:\t' + stats[pe].heads.conflict + '\n';
    }

    if (stats[pe].heads.deleted) {
      result += '  deleted:\t' + stats[pe].heads.deleted + '\n';
    }
  });

  return result;
}

// shows old values of deleted and changed attributes
function fmtPatch(patch, oldItem) {
  var result = {};
  Object.keys(patch).forEach(function(key) {
    if (patch[key] !== '+') {
      result[key] = oldItem[key];
    }
  });
  return JSON.stringify(result);
}

function fmtItem(item, parents) {
  parents = parents || [];

  var out = '';
  if (program.show) {
    // print tree
    if (parents.length) {
      out += '\u250f ';
    } else {
      out += '\u257a ';
    }
    out += ' ' + JSON.stringify(item);
    if (parents.length) {
      out += '\n';
    }

    var i = 0;
    parents.forEach(function(p) {
      i++;
      if (i === parents.length) {
        out += '\u2517\u2501 ';
      } else {
        out += '\u2523\u2501 ';
      }
      var diff = doDiff(item.b, p.b);
      out += ' ' + p.h.id + ' ' + p.h.v + ' diff: ' + JSON.stringify(diff) + ' ' + fmtPatch(diff, p);
      if (i !== parents.length) {
        out += '\n';
      }
    });
  } else {
    out += item.h.id;
    out += ' ' + item.h.v;
    if (item.h.hasOwnProperty('d') && item.h.d) {
      out += ' d';
    } else {
      out += ' .';
    }
    out += ' ' + (item.h.pe || '_local');
    if (item.h.hasOwnProperty('i')) {
      out += ' (' + item.h.i + ')';
    }
    out += ' ' + JSON.stringify(item.h.pa);
    if (program.patch && parents.length) {
      parents.forEach(function(p) {
        var diff = doDiff(item.b, p.b);
        if (parents.length > 1) {
          out += ' ' + p.h.v;
        }
        out += ' diff: ' + JSON.stringify(diff) + ' ' + fmtPatch(diff, p);
      });
    }
  }
  return out;
}

var noop = function() {};

function run(db, cfg, cb) {
  cfg = cfg || {};
  cfg.log = {
    err: noop,
    notice: noop,
    info: noop,
    debug: noop,
    debug2: noop
  };
  var mt = new MergeTree(db, cfg);

  var counter = 0;

  mt.stats(function(err, stats) {
    if (err) { cb(err); return; }
    console.log(fmtStats(stats));

    mt.getLocalTree().iterateInsertionOrder({ reverse: true }, function(item, cb2) {
      counter++;

      if (!program.patch || !item.h.pa.length) {
        console.log(fmtItem(item));

        cb2(null, counter < program.number);
        return;
      }

      var parents = [];
      async.eachSeries(item.h.pa, function(pa, cb3) {
        mt.getByVersion(pa, function(err, p) {
          if (err) { cb3(err); return; }
          if (!p) {
            var error = new Error('parent not found');
            cb3(error);
            return;
          }

          parents.push(p);
          cb3();
        });
      }, function(err) {
        if (err) { cb2(err); return; }

        console.log(fmtItem(item, parents));

        cb2(null, counter < program.number);
      });
    }, cb);
  });
}

if (config.dbs) {
  async.eachSeries(config.dbs, function(dbCfg, cb) {
    var chroot = dbCfg.chroot || '/var/persdb';
    var data = dbCfg.data || 'data';

    if (dbCfg.perspectives) {
      dbCfg.perspectives = dbCfg.perspectives.map(function(peCfg) {
        return peCfg.name;
      });
    }
    // open database
    level(chroot + '/' + data, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, db) {
      run(db, dbCfg, function(err) {
        //db.close();
        if (err) { cb(err); return; }
      });
    });
  }, function(err) {
    if (err) { console.error(err); process.exit(2); }
  });
} else {
  // open database
  level('/var/persdb/data', { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, db) {
    if (err) { throw err; }
    run(db, {}, function(err) {
      db.close();
      if (err) { console.error(err); process.exit(2); }
    });
  });
}
