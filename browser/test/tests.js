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
    t.plan(2);

    var opts = {
      snapshotStore: '_pdb',
      conflictStore: '_conflicts'
    }
    PersDB.createNode(db, opts, (err, pdb) => {

      t.error(err);
      t.ok(pdb);
    });
  });

  test('pdb.resolveConflict', function(t) {
    // first create a pdb node to test with
    t.plan(3);

    // use default opts
    var pdb;
    PersDB.createNode(db, (err, p) => {
      t.error(err);
      t.ok(p);
      pdb = p;
    });

    t.test('pdb.resolveConflict', function(st) {
      st.end();
    });
  });

  // drop db
  test.onFinish(function() {
    db.close();
    dropDb('PersDB', function() {})
  });
});
