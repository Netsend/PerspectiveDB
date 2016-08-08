'use strict';

// Test a merge conflict because the object store is not consistent with the DAG:
// 1. test if it yields a conflict event
// 2. test if the conflict object is saved in the conflict object store
// 3. do some tests with resolving if 1 and 2 succeed

var idb, pdbOpts, conflictStore, versionA, versionB, versionC, versionD;

function setup(test, idbTools) {
  var prepareMT = require('./prepare_merge_tree.js');

  conflictStore = 'conflict';

  pdbOpts = {
    startMerge: false,
    snapshotStore: '_pdb',
    conflictStore: conflictStore,
    mergeTree: {
      perspectives: ['aRemote']
    }
  };

  // create a local tree:
  // A <-- B

  // let the object store contain not B but another version D (inconsistent with the local tree)

  // and a remote tree with conflicting changes:
  // A <-- C


  versionA = { same: 'something' }
  versionB = { same: 'something else' }
  versionC = { same: 'not something else' }
  versionD = { a: 'missed update' }

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
        foo: versionD
      }
    }
  };

  test('recreate db', function(t) {
    idbTools.recreateDb('PersDB', opts, function(err, db) {
      t.error(err);
      idb = db;
      // prepare merge tree
      prepareMT(db, mtFixtures, function(err) {
        t.error(err);
        t.end();
      });
    });
  });
}

function all(test, idbTools, PersDB) {
  test('pdb emit merge conflict', function(t) {
    PersDB.createNode(idb, pdbOpts, (err, pdb) => {
      t.error(err);

      pdb.startMerge();

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
    idbTools.get(idb, conflictStore, 1, function(err, conflict) {
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

  test('use getConflicts to get this conflict', function(t) {
    PersDB.createNode(idb, pdbOpts, (err, pdb) => {
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
    PersDB.createNode(idb, pdbOpts, (err, pdb) => {
      t.error(err);

      t.test('resolve err if conflict can\'t be found', function(st) {
        pdb.resolveConflict(0, undefined, { some: true }, function(err) {
          st.equal(err.message, 'conflict not found');
          st.end()
        });
      });

      t.test('resolve err if toBeResolved does not match current local head', function(st) {
        pdb.resolveConflict(1, versionD, { g: true }, function(err) {
          st.equal(err.message, 'local head and toBeResolved don\'t match');
          st.end()
        });
      });

      t.test('resolve with new conflict because of object store mismatch', function(st) {
        pdb.resolveConflict(1, versionB, { g: true }, function(err) {
          st.error(err);

          // check if the version in the object store is not blindly overwritten
          idbTools.get(idb, 'customers', 'foo', function(err, item) {
            st.error(err);
            st.deepEqual(item, versionD);

            // check if the old conflict is removed, and a new one saved because of mismatch with the local head
            var i = 0;
            pdb.getConflicts(function(conflict, next) {
              i++;
              st.deepEqual(conflict, {
                id: 2,
                store: 'customers',
                key: 'foo',
                new: { g: true },
                prev: versionB,
                conflict: [],
                remote: 'aRemote',
                error: 'unexpected local version'
              });
              next();
            }, function(err) {
              st.error(err);
              st.equal(i, 1);
              pdb.close(function(err) {
                st.error(err);
                st.end();
              });
            });
          });
        });
      });
    });
  });
}

function teardown(test, idbTools) {
  test('close and drop idb', function(t) {
    idb.close();
    idbTools.dropDb('PersDB', function(err) {
      t.error(err);
      t.end();
    });
  });
}

function run(test, idbTools, PersDB) {
  setup(test, idbTools);
  all(test, idbTools, PersDB);
  teardown(test, idbTools);
}

module.exports = { run };
