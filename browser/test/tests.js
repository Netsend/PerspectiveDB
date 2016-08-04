'use strict';

var test = require('tape');

var recreateDb = require('./idb.js').recreateDb;

var PersDB = require('../lib/persdb.js');

// create some stores
var stores = {
  customers: { keyPath: 'email' },
  employees: {},
  _pdb: {},
  _conflicts: { autoIncrement: true }
}

// ensure an empty database exists
recreateDb('PersDB', { stores: stores }, function(err, db) {
  if (err) throw err;

  test('PersDB.createNode', function(t) {
    var opts = {
      snapshotStore: '_pdb',
      conflictStore: '_conflicts'
    }
    PersDB.createNode(db, opts, (err, pdb) => {
      t.plan(2);

      t.error(err);
      t.ok(pdb);
    });
  });
});
