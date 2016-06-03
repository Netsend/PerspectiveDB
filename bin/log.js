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
var program = require('commander');

var doDiff = require('../lib/diff');
var MergeTree = require('../lib/merge_tree');
var noop = require('../lib/noop');

var openDbs = require('./_open_dbs');

program
  .version('0.1.0')
  .usage('[-span] <config>')
  .description('print graph of the given database')
  .option('config defaults to ../local/config/pdb.hjson')
  .option('-s, --show', 'show complete objects')
  .option('-p, --patch', 'show patch compared to previous version')
  .option('    --pe <perspective>', 'print only this perspective')
  .option('-a  --all', 'print all perspectives')
  .option('-n, --number <number>', 'number of heads to show, defaults to 10, 0 means unlimited')
  .parse(process.argv);

var configFile = program.args[0] || __dirname + '/../local/config/pdb.hjson';
var config = hjson.parse(fs.readFileSync(configFile, 'utf8'));

if (program.number === '0') {
  program.number = Infinity;
} else {
  program.number = Number(program.number) || 10;
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

// shows new values of added and changed attributes
function fmtPatch(patch, newItem) {
  var result = {};
  Object.keys(patch).forEach(function(key) {
    if (patch[key] !== '-') {
      result[key] = newItem[key];
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
      var diff = doDiff(item.b || {}, p.b || {});
      out += ' ' + p.h.id + ' ' + p.h.v + ' diff: ' + JSON.stringify(diff) + ' ' + fmtPatch(diff, item.b);
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
    if (item.h.hasOwnProperty('c') && item.h.c) {
      out += ' c';
    } else {
      out += ' .';
    }
    out += ' ' + (item.h.pe || 'local');
    if (item.h.hasOwnProperty('i')) {
      out += ' (' + item.h.i + ')';
    }
    out += ' ' + JSON.stringify(item.h.pa);
    if (program.patch && parents.length) {
      parents.forEach(function(p) {
        var diff = doDiff(item.b || {}, p.b || {});
        if (parents.length > 1) {
          out += ' ' + p.h.v;
        }
        out += ' diff: ' + JSON.stringify(diff) + ' ' + fmtPatch(diff, item.b);
      });
    }
  }
  return out;
}

function printTree(mt, tree, cb) {
  var counter = 0;
  console.log('%s:', tree.name);
  tree.iterateInsertionOrder({ reverse: true }, function(item, cb2) {
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
}

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

  mt.stats(function(err, stats) {
    if (err) { cb(err); return; }
    console.log('#######################');
    console.log(cfg.name);
    console.log('#######################');
    console.log(fmtStats(stats));

    var remoteTrees = mt.getRemoteTrees();

    if (program.all) {
      printTree(mt, mt.getLocalTree(), function(err) {
        if (err) { cb(err); return; }
        console.log(); // newline
        printTree(mt, mt.getStageTree(), function(err) {
          if (err) { cb(err); return; }
          console.log(); // newline
          async.eachSeries(Object.keys(remoteTrees), function(name, cb2) {
            printTree(mt, remoteTrees[name], cb2);
          }, function(err) {
            if (err) { cb(err); return; }
            cb();
          });
        });
      });
    } else {
      // print one tree, default to the local tree
      var tree, pe = program.pe || 'local';

      if (remoteTrees[pe]) {
        tree = remoteTrees[pe];
      } else if (pe === 'stage') {
        tree = mt.getStageTree();
      } else if (pe === 'local' || pe === mt._local.name) {
        tree = mt.getLocalTree();
      } else {
        cb(new Error('unknown perspective'));
        return;
      }

      printTree(mt, tree, cb);
    }
  });
}

openDbs(config, run, function(err) {
  if (err) { console.error(err); process.exit(2); }
});
