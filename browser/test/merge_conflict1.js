'use strict';

// Test a genuine merge conflict on object keys:
// 1. test if it yields a conflict event
// 2. test if the conflict object is saved in the conflict object store
// 3. do some tests with resolving if 1 and 2 succeed

function runTests(idbc, PersDB, cb) {
  var test = require('tape');

  var idb = require('../lib/idb.js');
  var prepareMT = require('./prepare_merge_tree.js');

  var dropDb = idbc.dropDb;
  var recreateDb = idbc.recreateDb;

  var conflictStore = 'conflict';

  var pdbOpts = {
    snapshotStore: '_pdb',
    conflictStore: conflictStore,
    mergeTree: {
      perspectives: ['aRemote']
    }
  };

  // create a local tree:
  // A <-- B

  // and a remote tree with conflicting changes:
  // A <-- C

  var versionA = { same: 'something' }
  var versionB = { same: 'something else' }
  var versionC = { same: 'not something else' }

  // these are versions that should be saved within the merge tree
  var mtFixtures = {
    _local: [{
      h: { id: 'customers\x01foo', v: 'Aaaaaaaa', pa: [] },
      b: versionA
    }, {
      h: { id: 'customers\x01foo', v: 'Bbbbbbbb', pa: ['Aaaaaaaa'] },
      b: versionB
    }],
    aRemote: [{
      h: { id: 'customers\x01foo', v: 'Aaaaaaaa', pa: [], pe: 'aRemote' },
      b: versionA
    }, {
      h: { id: 'customers\x01foo', v: 'Cccccccc', pa: ['Aaaaaaaa'], pe: 'aRemote' },
      b: versionC
    }]
  };

  var opts = {
    stores: {
      customers: {},
      _pdb: {},
      conflict: { autoIncrement: true }
    },
    data: {
      customers: {
        foo: versionB
      }
    }
  };

  // ensure an empty database exists
  recreateDb('PersDB', opts, function(err, db) {
    if (err) throw err;

    // prepare merge tree
    prepareMT(db, mtFixtures, function(err) {
      if (err) throw err;

      test('pdb with startMerge default', function(t) {
        PersDB.createNode(db, pdbOpts, (err, pdb) => {
          t.error(err);

          pdb.on('conflict', function(conflict) {
            t.deepEqual(conflict, {
              id: 1,
              store: 'customers',
              key: 'foo',
              new: versionC,
              prev: versionB,
              conflict: ['same'],
              remote: 'aRemote'
            });

            pdb.close(function(err) {
              t.error(err);
              t.end();
            });
          });
        });
      });

      test('pdb should have saved previous conflict in conflict store', function(t) {
        t.plan(1);
        idb.get(db, conflictStore, 1, function(err, conflict) {
          if (err) throw err;
          t.deepEqual(conflict, {
            n: {
              h: { id: 'customers\x01foo', v: 'Cccccccc', pa: ['Aaaaaaaa'], pe: 'aRemote' },
              b: versionC
            },
            l: {
              h: { id: 'customers\x01foo', v: 'Bbbbbbbb', pa: ['Aaaaaaaa'], i: 2 },
              b: versionB
            },
            c: ['same'],
            lcas: ['Aaaaaaaa'],
            pe: 'aRemote'
          });
        });
      });

      test('pdb.getConflict to get this conflict', function(t) {
        PersDB.createNode(db, pdbOpts, (err, pdb) => {
          t.error(err);

          pdb.getConflict(1, function(err, conflict, current) {
            t.error(err);
            t.deepEqual(conflict, {
              id: 1,
              store: 'customers',
              key: 'foo',
              new: versionC,
              prev: versionB,
              conflict: ['same'],
              remote: 'aRemote'
            });
            t.deepEqual(current, versionB);

            pdb.close(function(err) {
              t.error(err);
              t.end();
            });
          });
        });
      });

      test('use getConflicts to get this conflict', function(t) {
        PersDB.createNode(db, pdbOpts, (err, pdb) => {
          t.error(err);

          var i = 0;
          pdb.getConflicts(function(conflict, next) {
            i++;
            t.deepEqual(conflict, {
              id: 1,
              store: 'customers',
              key: 'foo',
              new: versionC,
              prev: versionB,
              conflict: ['same'],
              remote: 'aRemote'
            });
            next();
          }, function(err) {
            t.error(err);
            t.equal(i, 1);
            pdb.close(function(err) {
              t.error(err);
              t.end();
            });
          });
        });
      });

      test('pdb.resolveConflict', function(t) {
        PersDB.createNode(db, pdbOpts, (err, pdb) => {
          t.error(err);

          t.test('resolve err if conflict can\'t be found', function(st) {
            pdb.resolveConflict(0, undefined, { some: true }, function(err) {
              st.equal(err.message, 'conflict not found');
              st.end()
            });
          });

          t.test('resolve err if toBeResolved does not match current local head', function(st) {
            pdb.resolveConflict(1, undefined, { some: true }, function(err) {
              st.equal(err.message, 'local head and toBeResolved don\'t match');
              st.end()
            });
          });

          t.test('resolve', function(st) {
            pdb.resolveConflict(1, versionB, { g: true }, function(err) {
              st.error(err);

              // check if the conflict is removed
              idb.get(db, conflictStore, 1, function(err, conflict) {
                st.error(err);
                st.equal(conflict, undefined);

                // check if the new version is saved in the object store
                idb.get(db, 'customers', 'foo', function(err, item) {
                  st.error(err);
                  st.deepEqual(item, { g: true });
                  st.end()
                });
              });
            });
          });
        });
      });

      // drop db
      test.onFinish(function() {
        db.close();
        dropDb('PersDB', cb);
      });
    });
  });
}

module.exports = runTests;
