#!/usr/bin/env node

/**
 * Copyright 2014, 2016 Netsend.
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
var util = require('util');

var hjson = require('hjson');
var program = require('commander');
var xtend = require('xtend');

var compare = require('../lib/compare');
var doDiff = require('../lib/diff');
var MergeTree = require('../lib/merge_tree');
var noop = require('../lib/noop');

var openDbs = require('./_open_dbs');

program
  .version('0.2.0')
  .usage('[-v] -a database [-b database] [-c tree [-d tree]] <config>')
  .description('compare each item in tree1 with tree2')
  .option('config defaults to ../local/config/pdb.hjson')
  .option('-a, --database1 <database>', 'name of the database for tree1')
  .option('-b, --database2 <database>', 'name of the database for tree2 if different from database1')
  .option('-c, --tree1 <tree>', 'name of the tree to report about, defaults to _local')
  .option('-d, --tree2 <tree>', 'name of the tree to compare against if different from tree1')
  .option('-f, --config <config>', 'ini config file with database access credentials')
  .option('-i, --include <attrs>', 'comma separated list of attributes to include in comparison', function(val) { return val.split(','); })
  .option('-e, --exclude <attrs>', 'comma separated list of attributes to exclude in comparison', function(val) { return val.split(','); })
  .option('-s  --showid', 'print id of missing and unequal items')
  .option('-p, --patch', 'show a patch with differences between unequal items (contains output of -s)')
  .option('               diff: "-" is not in a, "+" is not in b, "~" both differ')
  .option('-v, --verbose', 'verbose (contains output of -p)')
  .option('    --debug', 'show debug output')
  .option('    --attrstats', 'add per attribute statistics')
  .parse(process.argv);

var programName = require('path').basename(__filename, '.js');

if (process.getuid() !== 0) {
  console.error('%s: execute as root', programName);
  process.exit(1);
}

if (!program.database1) { program.help(); }
if (!program.database2) { program.database2 = program.database1; }
if (!program.tree1) { program.tree1 = '_local' }
if (!program.tree2) { program.tree2 = program.tree1; }

if (program.database1 === program.database2 && program.tree1 === program.tree2) {
  console.error('either use different databases or different trees or both');
  program.help();
}

var configFile = program.args[0] || __dirname + '/../local/config/pdb.hjson';
var config = hjson.parse(fs.readFileSync(configFile, 'utf8'));

var excludeAttrs = {};
(program.exclude || []).forEach(function(attr) {
  excludeAttrs[attr] = true;
});

var includeAttrs = {};
(program.include || []).forEach(function(attr) {
  includeAttrs[attr] = true;
});

if (program.verbose && Object.keys(includeAttrs).length) { console.log('include:', program.include); }
if (program.verbose && Object.keys(excludeAttrs).length) { console.log('exclude:', program.exclude); }

// group patch by type
function groupPatch(patch) {
  var result = { '+': [], '-': [], '~': [] };
  Object.keys(patch).forEach(function(key) {
    result[patch[key]].push(key);
  });
  return result;
}

// count attrs
function diffColumnCount(item1, item2, diffColumns) {
  var diff = doDiff(item1.b, item2.b);
  Object.keys(diff).forEach(function(key) {
    diffColumns[key] = diffColumns[key] || [];
    if (diff[key] === '~') {
      diffColumns[key].push([item1.h.id, diff[key], item1.b[key], item2.b[key]]);
    } else if (diff[key] === '-') {
      diffColumns[key].push([item1.h.id, diff[key], item2.b[key]]);
    } else if (diff[key] === '+') {
      diffColumns[key].push([item1.h.id, diff[key], item1.b[key]]);
    } else {
      console.error(diff, key);
      throw new Error('unknown diff format');
    }
  });
  return diff;
}

function loadTree(db, name) {
  if (!name || name === 'local' || name === '_local') {
    return db.getLocalTree()
  } else if (name === 'stage' || name === '_stage') {
    return db.getStageTree()
  } else {
    return db.getRemoteTrees()[name];
  }
}

var db1, db2, dbCfg1, dbCfg2;
function run(db, dbCfg, cb) {
  if (dbCfg.name !== program.database1 && dbCfg.name !== program.database2) {
    db.close();
    cb();
    return;
  }
  dbCfg = xtend(dbCfg, {
    log: {
      err: noop,
      notice: noop,
      info: noop,
      debug: noop,
      debug2: noop
    }
  });

  if (dbCfg.name === program.database1) {
    db1 = db;
    dbCfg1 = dbCfg;
  }

  if (dbCfg.name === program.database2) {
    db2 = db;
    dbCfg2 = dbCfg;
  }

  if (!db1 || !db2) {
    cb();
    return;
  }

  // all needed dbs are open

  var mt1, mt2;
  if (program.database1 === program.database2) {
    mt1 = new MergeTree(db1, dbCfg1);
    mt2 = mt1; // reuse same merge tree
  } else {
    mt1 = new MergeTree(db1, dbCfg1);
    mt2 = new MergeTree(db2, dbCfg2);
  }

  var tree1, tree2;
  tree1 = loadTree(mt1, program.tree1);
  tree2 = loadTree(mt2, program.tree2);

  if (!tree1) { throw new Error('tree1 not found'); }
  if (!tree2) { throw new Error('tree2 not found'); }

  var opts = {
    includeAttrs: includeAttrs,
    excludeAttrs: excludeAttrs,
    debug: program.debug
  };

  compare(tree1, tree2, opts, function(err, stats) {
    if (err) { throw err; }

    var diffColumns = {};

    if (program.showid) {
      console.log('missing');
      stats.missing.forEach(function(item) {
        console.log(item.h.id);
      });
      console.log('end of missing');

      console.log('unequal');
      stats.inequal.forEach(function(item) {
        console.log(item.item1.h.id);
      });
      console.log('end of unequal');

      console.log('multiple');
      stats.multiple.forEach(function(item) {
        console.log(item.item1.h.id);
      });
      console.log('end of multiple');
    } else if (program.patch) {
      console.log('missing');
      stats.missing.forEach(function(item) {
        console.log(item.h.id);
      });
      console.log('end of missing');

      console.log('unequal');
      stats.inequal.forEach(function(item) {
        var diff = diffColumnCount(item.item1, item.item2, diffColumns);
        console.log(item.item1.h.id, JSON.stringify(groupPatch(diff)));
      });
      console.log('end of unequal');

      console.log('multiple');
      stats.multiple.forEach(function(item) {
        item.items2.forEach(function(subItem) {
          var diff = diffColumnCount(item.item1, subItem, diffColumns);
          console.log(item.item1.h.id, JSON.stringify(groupPatch(diff)));
        });
      });
      console.log('end of multiple');
    } else if (program.verbose) {
      console.log('missing');
      stats.missing.forEach(function(item) {
        console.log(item);
      });
      console.log('end of missing');

      console.log('unequal');
      stats.inequal.forEach(function(item) {
        var diff = diffColumnCount(item.item1, item.item2, diffColumns);
        console.log(item.item1.h.id, JSON.stringify(groupPatch(diff)));
        console.log(1, item.item1);
        console.log(2, item.item2);
      });
      console.log('end of unequal');

      console.log('multiple');
      stats.multiple.forEach(function(item) {
        item.items2.forEach(function(subItem) {
          var diff = diffColumnCount(item.item1, subItem, diffColumns);
          console.log(item.item1.h.id, JSON.stringify(groupPatch(diff)));
        });
        console.log(1, item.item1);
        console.log(2, JSON.stringify(item.items2));
      });
      console.log('end of multiple');
    }

    if (program.attrstats) {
      // ensure counts
      if (!program.patch && !program.verbose) {
        stats.inequal.forEach(function(item) {
          diffColumnCount(item.item1, item.item2, diffColumns);
        });

        stats.multiple.forEach(function(item) {
          item.items2.forEach(function(subItem) {
            diffColumnCount(item.item1, subItem, diffColumns);
          });
        });
      }

      // show diffColumns sorted
      var sorted = Object.keys(diffColumns).map(function(key) {
        return {
          name: key,
          len: diffColumns[key].length
        };
      }).sort(function(a, b) {
        if (a.len < b.len) {
          return -1;
        } else if (a.len === b.len) {
          return 0;
        } else {
          return 1;
        }
      }).map(function(key) {
        var obj = {};
        if (program.verbose && !excludeAttrs[key.name]) {
          obj[key.name + ' ' + key.len] = diffColumns[key.name];
        } else {
          obj[key.name] = key.len;
        }
        return obj;
      });
      console.log('attrstats', util.inspect(sorted, { depth: null }));
    }

    console.log('missing',  stats.missing.length);
    console.log('unequal',  stats.inequal.length);
    console.log('equal',    stats.equal.length);
    console.log('multiple', stats.multiple.length);

    db.close();
  });
}

openDbs(config, { keepOpen: true }, run, function(err) {
  if (err) { console.error(err); process.exit(2); }
});
