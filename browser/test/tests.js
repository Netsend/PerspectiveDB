'use strict';

var test = require('tape');

var idb = require('./idb.js');
var dropDb = idb.dropDb;
var recreateDb = idb.recreateDb;

var PersDB = require('../lib/persdb.js');

// create some stores
var opts = {
  stores: {
    customers: { keyPath: 'email' },
    employees: {},
    _pdb: {},
    _conflicts: { autoIncrement: true }
  },
  fixtures: {
    customers: [
      { email: 'test@example.com' },
    ]
  }
}

// ensure an empty database exists
recreateDb('PersDB', opts, function(err, db) {
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

  test('pdb.resolveConflict', function(t) {
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

  // cleanup db
  test.onFinish(function() {
    db.close();
    dropDb('PersDB', function() {})
  });
});
