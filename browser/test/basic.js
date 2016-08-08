'use strict';

var idb, pdbOpts, conflict1, conflict2, conflictStore;

function setup(test, idbTools) {
  // create two conflict fixtures
  conflict1 = {
    n: {
      h: {
        id: 'customers\x01john',
        v: 'Aaaaaaaa',
        pa: []
      },
      b: {
        email: 'john@example.com'
      }
    },
    l: null,
    c: null,
    lcas: [],
    pe: 'remote1',
    err: 'some rando'
  };
  conflict2 = {
    n: {
      h: {
        id: 'customers\x01jane',
        v: 'Aaaaaaaa',
        pa: []
      },
      b: {
        email: 'jane@example.com'
      }
    },
    l: null,
    c: null,
    lcas: [],
    pe: 'remote2',
    err: 'unexpected local head'
  };

  // create some stores
  var opts = {
    stores: {
      customers: {},
      employees: {},
      _pdb: {},
      _conflicts: { autoIncrement: true }
    },
    data: {
      customers: {
        jane: { email: 'test@example.com' }
      },
      _conflicts: [conflict1, conflict2]
    }
  };

  conflictStore = '_conflicts';

  pdbOpts = {
    snapshotStore: '_pdb',
    conflictStore: conflictStore,
    startMerge: false,
    mergeTree: {
      perspectives: ['remote1', 'remote2']
    }
  };

  test('recreate db', function(t) {
    idbTools.recreateDb('PerspectiveDB', opts, function(err, db) {
      t.error(err);
      idb = db;
      t.end();
    });
  });
}

function all(test, idbTools, PerspectiveDB) {
  test('PerspectiveDB.createNode', function(t) {
    t.plan(2);

    PerspectiveDB.createNode(idb, pdbOpts, (err, pdb) => {
      t.error(err);
      t.ok(pdb);
    });
  });

  test('pdb.getConflicts', function(t) {
    // first create a pdb node to test with
    t.plan(2);

    PerspectiveDB.createNode(idb, pdbOpts, (err, pdb) => {
      if (err) throw err;

      t.test('list all conflicts with the public properties', function(st) {
        var i = 0;
        pdb.getConflicts(function next(conflictObject, proceed) {
          i++;
          switch (i) {
          case 1:
            st.deepEqual(conflictObject, {
              id: 1,
              store: 'customers',
              key: 'john',
              new: { email: 'john@example.com' },
              prev: null,
              conflict: [],
              remote: 'remote1',
              error: 'some rando'
            });
            break;
          case 2:
            st.deepEqual(conflictObject, {
              id: 2,
              store: 'customers',
              key: 'jane',
              new: { email: 'jane@example.com' },
              prev: null,
              conflict: [],
              remote: 'remote2',
              error: 'unexpected local head'
            });
            break;
          default:
            throw new Error('unexpected');
          }
          proceed();
        }, function(err) {
          st.error(err);
          st.equal(i, 2);
          st.end();
        });
      });

      t.test('don\'t proceed', function(st) {
        var i = 0;
        pdb.getConflicts(function next(conflictObject, proceed) {
          i++;
          switch (i) {
          case 1:
            st.deepEqual(conflictObject, {
              id: 1,
              store: 'customers',
              key: 'john',
              new: { email: 'john@example.com' },
              prev: null,
              conflict: [],
              remote: 'remote1',
              error: 'some rando'
            });
            break;
          default:
            throw new Error('unexpected');
          }
          proceed(false);
        }, function(err) {
          st.error(err);
          st.equal(i, 1);
          st.end();
        });
      });
    });
  });

  test('pdb.getConflict', function(t) {
    // first create a pdb node to test with
    t.plan(2);

    PerspectiveDB.createNode(idb, pdbOpts, (err, pdb) => {
      if (err) throw err;

      t.test('get existing conflict', function(st) {
        pdb.getConflict(1, function(err, conflict, current) {
          st.error(err);
          st.deepEqual(conflict, {
            id: 1,
            store: 'customers',
            key: 'john',
            new: { email: 'john@example.com' },
            prev: null,
            conflict: [],
            remote: 'remote1',
            error: 'some rando'
          });
          st.deepEqual(current, undefined);
          st.end();
        });
      });

      t.test('get non-existing conflict', function(st) {
        pdb.getConflict(0, function(err, conflict, current) {
          st.equal(err.message, 'conflict not found');
          st.deepEqual(conflict, undefined);
          st.deepEqual(current, undefined);
          st.end();
        });
      });
    });
  });
}

function teardown(test, idbTools) {
  test('close and drop idb', function(t) {
    idb.close();
    idbTools.dropDb('PerspectiveDB', function(err) {
      t.error(err);
      t.end();
    });
  });
}

function run(test, idbTools, PerspectiveDB) {
  setup(test, idbTools);
  all(test, idbTools, PerspectiveDB);
  teardown(test, idbTools);
}

module.exports = { run };
