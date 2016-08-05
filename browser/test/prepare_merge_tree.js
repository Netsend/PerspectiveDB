'use strict';

var async = require('async');
var level = require('level-packager')(require('level-js'));

var MergeTree = require('../../lib/merge_tree');

function prepare(idb, structure, cb) {
  var trees = Object.keys(structure);

  var db = level(idb.name, {
    storeName: '_pdb',
    idb: idb, // pass the opened database instance
    keyEncoding: 'binary',
    valueEncoding: 'none',
    asBuffer: false,
    reopenOnTimeout: true
  });

  var mt = new MergeTree(db, { perspectives: trees });
  async.eachSeries(trees, function(tree, cb2) {
    mt.getRemoteTree(tree).write(structure[tree], cb2);
  }, cb);
}

module.exports = prepare;
