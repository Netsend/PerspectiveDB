/**
 * Copyright 2014 Netsend.
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

/*jshint -W068, -W030 */

var os = require('os');
var fs = require('fs');

var async = require('async');
var should = require('should');

var VersionedCollection = require('../../lib/versioned_collection');
var Replicator = require('../../lib/replicator');

var dbImport, dbExport, dbFoo, dbBar, dbQux, dbRaboof;

var databaseNames = ['dbImport', 'dbExport', 'dbFoo', 'dbBar', 'dbQux', 'dbRaboof'];
var Database = require('../_database');

// open database connection
var database = new Database(databaseNames);
before(function(done) {
  database.connect(function(err, dbs) {
    dbImport = dbs[0];
    dbExport = dbs[1];
    dbFoo    = dbs[2];
    dbBar    = dbs[3];
    dbQux    = dbs[4];
    dbRaboof = dbs[5];
    done(err);
  });
});

after(database.disconnect.bind(database));

describe('Replicator', function() {
  describe('bidirFrom', function() {
    it('should require config to be an object', function() {
      (function() { Replicator.bidirFrom([]); }).should.throw('config must be an object');
    });

    it('should return empty array', function() {
      should.deepEqual(Replicator.bidirFrom({}), []);
    });

    it('should only return bidirectional froms', function() {
      var result = Replicator.bidirFrom({
        'foo': {
          'from': {
            'baz': { 'collZ': true }
          },
          'to': {
            'baz': { 'collZ': true },
            'bar': {
              'collX': true,
              'collY': true
            }
          },
        },

        'baz': {
          'from': {
            'foo': { 'collZ': true }
          }
        },

        'bar': {
          'from': {
            'foo': { 'collX': true }
          },
          'to': {
            'foo': {
              'collY': { 'filter': { } }
            },
            'baz': {
              'collZ': { 'filter': true }
            }
          }
        }
      });

      should.deepEqual(result, [
        { 'baz.collZ': true, 'foo.collZ': true }
      ]);
    });
  });

  describe('replicateTo', function() {
    var config = {
      'foo': {
        'to': {
          'bar': {
            'collX': {
              'hooks': ['foo.js', 'bar.js'],
              'filter': { 'foobar': 'Bar' },
              'hide': ['qux']
            },
            'collY': true
          },
          'baz': {
            'collZ': { 'filter': true }
          }
        },
        'from': {
          'foo': { 'collX': true },
          'bar': {
            'collW': true,
            'collX': true,
            'collY': false
          },
          'baz': {
            'collX': true,
            'collY': true
          }
        }
      },
      'bar': {
        'to': {
          'foo': {
            'collW': { a: 'b' },
            'collX': true,
            'collY': true,
            'collZ': true
          },
          'baz': { 'collY': true },
          'raboof': { 'collZ': true }
        },
        'from': {
          'foo': {
            'collX': true,
            'collY': true
          },
          'qux': { 'collZ': true }
        }
      },
      'baz': {
        'to': {
          'foo': { 'collX': true },
          'baz': { 'collY': true },
          'raboof': { 'collZ': true }
        },
        'from': {
          'foo': { 'collX': true },
          'qux': { 'collZ': true }
        }
      }
    };

    it('should require config to be an object', function() {
      (function() { Replicator.replicateTo([]); }).should.throw('config must be an object');
    });

    it('should require target to be a string', function() {
      (function() { Replicator.replicateTo({}, {}); }).should.throw('target must be a string');
    });

    it('should require a non-empty string', function() {
      (function() { Replicator.replicateTo({}, ''); }).should.throw('could not determine target database name');
    });

    it('should require a string preceeding the dot', function() {
      (function() { Replicator.replicateTo({}, '.'); }).should.throw('could not determine target database name');
    });

    it('should require a string after the dot', function() {
      (function() { Replicator.replicateTo({}, 'foo.'); }).should.throw('could not determine target collection name');
    });

    it('should return an empty config', function() {
      var result = Replicator.replicateTo({}, 'foo.bar');
      should.deepEqual(result, {});
    });

    it('should replicate to foo.bar only', function() {
      var result = Replicator.replicateTo(config, 'foo.collX');
      should.deepEqual(result, {
        'foo': {
          'from': {
            'bar': {
              'collX': true
            },
            'baz': {
              'collX': true
            }
          }
        },
        'bar': {
          'to': {
            'foo': { 'collX': true }
          }
        },
        'baz': {
          'to': {
            'foo': { 'collX': true }
          }
        }
      });
    });

    it('should replicate to bar.collX only', function() {
      var result = Replicator.replicateTo(config, 'bar.collX');
      should.deepEqual(result, {
        'foo': {
          'to': {
            'bar': {
              'collX': {
                'hooks': ['foo.js', 'bar.js'],
                'filter': { 'foobar': 'Bar' },
                'hide': ['qux']
              }
            }
          }
        },
        'bar': {
          'from': {
            'foo': { 'collX': true }
          }
        }
      });
    });

    it('should replicate to baz.foo only', function() {
      var result = Replicator.replicateTo(config, 'baz.foo');
      should.deepEqual(result, {});
    });
  });

  describe('splitImportExport', function() {
    it('should require config to be an object', function() {
      (function() { Replicator.splitImportExport([]); }).should.throw('config must be an object');
    });

    it('should return empty import export objects', function() {
      should.deepEqual(Replicator.splitImportExport({}), { import: {}, export: {} });
    });

    it('should split config into import and export rules and concatenate db and collection names', function() {
      var result = Replicator.splitImportExport({
        'foo': {
          'to': {
            'bar': {
              'collX': {
                'hooks': ['foo.js', 'bar.js'],
                'filter': { 'foobar': 'Bar' },
                'hide': ['qux']
              }
            },
            'baz': {
              'collZ': { 'filter': true }
            }
          }
        },
        'bar': {
          'from': {
            'foo': { 'collX': true }
          }
        }
      });

      should.deepEqual(result.import, {
        'bar': {
          'foo.collX': true
        }
      });
      should.deepEqual(result.export, {
        'foo.collX': {
          'bar': {
            'hooks': ['foo.js', 'bar.js'],
            'filter': { 'foobar': 'Bar' },
            'hide': ['qux']
          }
        },
        'foo.collZ': { baz: { filter: true  }}
      });
    });

    it('should work with multiple import and exports', function() {
      var result = Replicator.splitImportExport({
        'foo': {
          'to': {
            'qux': {
              'baz': {
                'hooks': ['export_baz'],
                'filter': true
              },
              'quux': {
                'hooks': ['export_quux'],
                'filter': true
              }
            }
          }
        },
        'bar': {
          'to': {
            'raboof': {
              'baz': {
                'hooks': ['export_baz'],
                'filter': true
              },
              'quux': {
                'hooks': ['export_quux'],
                'filter': true
              }
            }
          }
        },
        'qux': {
          'from': {
            'foo': {
              'baz': {
                'hooks': ['import_baz'],
                'filter': true
              },
              'quux': {
                'hooks': ['import_quux']
              }
            }
          }
        },
        'raboof': {
          'from': {
            'bar': {
              'baz': {
                'hooks': ['import_baz'],
                'filter': true
              },
              'quux': {
                'hooks': ['import_quux']
              }
            }
          }
        }
      });

      should.deepEqual(result.import, {
        qux: {
          'foo.baz':  { hooks: ['import_baz'], filter: true },
          'foo.quux': { hooks: ['import_quux']}
        },
        raboof: {
          'bar.baz':  { hooks: ['import_baz'], filter: true },
          'bar.quux': { hooks: ['import_quux']}
        }
      });
      should.deepEqual(result.export, {
        'foo.baz':  { qux:    { hooks: ['export_baz'],  filter: true } },
        'foo.quux': { qux:    { hooks: ['export_quux'], filter: true } },
        'bar.baz':  { raboof: { hooks: ['export_baz'],  filter: true } },
        'bar.quux': { raboof: { hooks: ['export_quux'], filter: true } }
      });
    });
  });

  describe('verifyImportExport', function() {
    it('should require config to be an object', function() {
      (function() { Replicator.verifyImportExport([]); }).should.throw('config must be an object');
    });

    it('should require config.import to be an object', function() {
      (function() { Replicator.verifyImportExport({}); }).should.throw('config.import must be an object');
    });

    it('should require config.export to be an object', function() {
      (function() { Replicator.verifyImportExport({ import: {} }); }).should.throw('config.export must be an object');
    });

    it('should return empty object on empty import', function() {
      should.deepEqual(Replicator.verifyImportExport({ import: {}, export: {} }), {});
    });

    it('should return empty object if all imports match an export', function() {
      var config = {
        import: {
          'raboof': { 'test.coll': true }
        },
        export: {}
      };

      var result = Replicator.verifyImportExport(config);
      should.deepEqual(result, { raboof: ['test.coll'] });
    });

    it('should return empty object if all imports match an export', function() {
      should.deepEqual(Replicator.verifyImportExport({
        import: {
          'bar': {
            'foo.collX': true
          }
        },
        export: {
          'foo.collX': {
            'bar': {
              'filter': { 'foobar': 'Bar' },
              'hide': ['qux']
            }
          }
        }
      }), {});
    });

    it('should return keys and values of missing exports', function() {
      should.deepEqual(Replicator.verifyImportExport({
        import: {
          'raboof': {
            'foo.collX': true
          },
          'bar': {
            'foo.collX': true
          },
          'tip': {
            'foo.collX': true,
            'foo.foo': true,
            'foo.bar': false
          }
        },
        export: {
          'foo.collX': {
            'bar': {
              'filter': { 'foobar': 'Bar' },
              'hide': ['qux']
            }
          }
        }
      }), {
        'raboof': ['foo.collX'],
        'tip': ['foo.collX', 'foo.foo']
      });
    });
  });

  describe('getTailOptions', function() {
    var collectionName = 'getTailOptions';

    var A =  { _id: { _co: collectionName, _id: 'foo', _v: 'A', _pe: 'dbExport', _pa: [] } };
    var Ap = { _id: { _co: collectionName, _id: 'foo', _v: 'A', _pe: 'dbImport', _pa: [] } };
    var B =  { _id: { _co: collectionName, _id: 'foo', _v: 'B', _pe: 'dbExport', _pa: ['A'] } };
    var Bp = { _id: { _co: collectionName, _id: 'foo', _v: 'B', _pe: 'dbImport', _pa: ['A'] } };
    var C =  { _id: { _co: collectionName, _id: 'foo', _v: 'C', _pe: 'dbExport', _pa: ['B'] } };

    it('should save DAG', function(done) {
      var vc = new VersionedCollection(dbImport, collectionName);
      vc._snapshotCollection.insert([A, Ap, B, Bp, C], {w: 1}, done);
    });

    it('should require config to be an object', function() {
      (function() { Replicator.getTailOptions([]); }).should.throw('config must be an object');
    });

    it('should require vcs to be an object', function() {
      (function() { Replicator.getTailOptions({}, []); }).should.throw('vcs must be an object');
    });

    it('should return empty object on empty import', function(done) {
      Replicator.getTailOptions({ import: {}, export: {} }, {}, function(err, result) {
        should.equal(err, null);
        should.deepEqual(result, {});
        done();
      });
    });

    it('should complain if export not in vcs', function(done) {
      Replicator.getTailOptions({
        import: { 'dbx': { 'dby.collX': true } },
        export: { 'dby.collX': { 'dbx': true } }
      }, {}, function(err) {
        should.equal(err.message, 'export not in vcs: dby.collX');
        done();
      });
    });

    it('should complain if import not in vcs', function(done) {
      Replicator.getTailOptions({
        import: { 'dbx': { 'dby.collX': true } },
        export: { 'dby.collX': { 'dbx': true } }
      }, { 'dby.collX': {} }, function(err) {
        should.equal(err.message, 'import not in vcs: dbx.collX');
        done();
      });
    });

    it('should set last to false if last saved item not found from that perspective', function(done) {
      var vc = new VersionedCollection(dbImport, collectionName);
      Replicator.getTailOptions({
        import: { 'dbx': { 'dby.collX': true, 'dby.collZ': true } },
        export: {
          'dby.collX': {
            'dbx': {
              'filter': { 'foobar': 'Bar' },
              'hide': ['qux']
            }
          },
          'dby.collZ': {
            'dbx': {
              'filter': true
            }
          }
        }
      }, { 'dby.collZ': vc, 'dbx.collZ': vc, 'dby.collX': vc, 'dbx.collX': vc }, function(err, result) {
        if (err) { throw err; }
        should.strictEqual(result['dby.collX+dbx.collX'].last, false);
        should.strictEqual(result['dby.collZ+dbx.collZ'].last, false);
        done();
      });
    });

    var hookPath = os.tmpdir();
    var files = ['baz.js', 'foo.js','bar.js', 'import_baz.js', 'import_quux.js', 'export_baz.js', 'export_quux.js'];

    it('needs some hooks on the disk to exist for further testing', function(done) {
      async.each(files, function(filename, cb) {
        fs.writeFile(hookPath + '/' + filename, '', cb);
      }, done);
    });

    it('should return all options for one pair of import and export vcs', function(done) {
      var vcImport = new VersionedCollection(dbImport, 'collX');
      var vcExport = new VersionedCollection(dbExport, 'collX');

      // insert some filter in dbImport from dbExport
      vcImport._snapshotCollection.insert([A, Ap], {w: 1}, function(err, inserts) {
        if (err) { throw err; }
        should.equal(inserts.length, 2);
        vcExport._snapshotCollection.insert([A, B, C], {w: 1}, function(err, inserts) {
          if (err) { throw err; }
          should.equal(inserts.length, 3);

          Replicator.getTailOptions({
            hookPath: hookPath,
            import: {
              'dbImport': {
                'dbExport.collX': { 'hooks': ['baz.js'] }
              }
            },
            export: {
              'dbExport.collX': {
                'dbImport': {
                  'hooks': ['foo.js', 'bar.js'],
                  'filter': { 'foobar': 'Bar' },
                  'hide': ['qux']
                }
              }
            }
          }, { 'dbExport.collX': vcExport, 'dbImport.collX': vcImport }, function(err, result) {
            should.equal(err, null);
            var key = 'dbExport.collX+dbImport.collX';
            should.exist(result[key]);
            should.strictEqual(result[key].last, 'A');
            should.deepEqual(result[key].filter, { 'foobar': 'Bar' });
            //should.deepEqual(result[key].importHooks, ['baz.js']);
            should.strictEqual(result[key].importHooks.length, 1);
            //should.deepEqual(result[key].exportHooks, ['foo.js', 'bar.js']);
            should.strictEqual(result[key].exportHooks.length, 2);
            result[key].transform.should.be.a.function;
            done();
          });
        });
      });
    });

    it('should call back once with the correct key config of 2 imports and 2 exports', function(done) {
      this.timeout(3000);
      var config = {
        hookPath: hookPath,
        import: {
          qux: {
            'foo.baz':  { hooks: ['import_baz'], filter: true },
            'foo.quux': { hooks: ['import_quux']}
          },
          raboof: {
            'bar.baz':  { hooks: ['import_baz'], filter: true },
            'bar.quux': { hooks: ['import_quux']}
          }
        },
        export: {
          'foo.baz':  { qux:    { hooks: ['export_baz'],  filter: true } },
          'foo.quux': { qux:    { hooks: ['export_quux'], filter: true } },
          'bar.baz':  { raboof: { hooks: ['export_baz'],  filter: true } },
          'bar.quux': { raboof: { hooks: ['export_quux'], filter: true } }
        }
      };

      // create versioned collections
      var vcFooBaz = new VersionedCollection(dbFoo, 'baz');
      var vcFooQuux = new VersionedCollection(dbFoo, 'quux');

      var vcBarBaz = new VersionedCollection(dbBar, 'baz');
      var vcBarQuux = new VersionedCollection(dbBar, 'quux');

      var vcQuxBaz = new VersionedCollection(dbQux, 'baz');
      var vcQuxQuux = new VersionedCollection(dbQux, 'quux');

      var vcRaboofBaz = new VersionedCollection(dbRaboof, 'baz');
      var vcRaboofQuux = new VersionedCollection(dbRaboof, 'quux');

      Replicator.getTailOptions(config, {
        'foo.baz':     vcFooBaz,
        'foo.quux':    vcFooQuux,
        'bar.baz':     vcBarBaz,
        'bar.quux':    vcBarQuux,
        'qux.baz':     vcQuxBaz,
        'qux.quux':    vcQuxQuux,
        'raboof.baz':  vcRaboofBaz,
        'raboof.quux': vcRaboofQuux
      }, function(err, result) {
        if (err) { throw err; }

        var keys = Object.keys(result);
        should.deepEqual(keys, ['foo.baz+qux.baz', 'foo.quux+qux.quux', 'bar.baz+raboof.baz', 'bar.quux+raboof.quux']);

        done();
      });
    });
  });

  describe('loadHooks', function() {
    var path = os.tmpdir();

    it('should require files to be an array', function() {
      (function() { Replicator.loadHooks({}); }).should.throw('files must be an array');
    });

    it('should require path to be a string', function() {
      (function() { Replicator.loadHooks(['foo'], 1); }).should.throw('path must be a string');
    });

    it('should raise an error when file does not exist', function() {
      (function() { Replicator.loadHooks(['foo']); }).should.throw(/^Cannot find module /);
    });

    it('needs a file on the disk to exist for further testing', function(done) {
      fs.writeFile(path + '/hook_foo.js', 'module.exports = function beep() { return "boop"; }', done);
    });

    it('should return an array with one function', function() {
      var hooks = Replicator.loadHooks(['hook_foo'], path);
      should.deepEqual(hooks.length, 1);
      should.deepEqual(typeof hooks[0], 'function');
      // test execution
      should.deepEqual(hooks[0](), 'boop');
    });
  });

  describe('loadConfig', function() {
    var hookPath = os.tmpdir();

    it('should require config to be an object', function() {
      (function() { Replicator.loadConfig(1); }).should.throw('config must be an object');
    });

    it('should require options to be an object', function() {
      (function() { Replicator.loadConfig({}, 1); }).should.throw('options must be an object');
    });

    it('should require options.hookPath to be a string', function() {
      (function() { Replicator.loadConfig({}, { hookPath: 1 }); }).should.throw('options.hookPath must be a string');
    });

    var exportConfig = {
      'collFoo': {
        'hooks': ['export_hook_foo'],
        'filter': { 'someAttr': { '$in': ['foo', 'bar'] } },
        'hide': ['hideFoo']
      },
      'collBar': {
        'hooks': ['export_hook_bar']
      }
    };

    it('should require hooks to exist', function() {
      (function() { Replicator.loadConfig(exportConfig); }).should.throw(/Cannot find module .*export_hook_foo/);
    });

    it('needs some hooks for further testing', function(done) {
      var files = ['export_hook_foo.js', 'export_hook_bar.js'];
      async.each(files, function(filename, cb) {
        fs.writeFile(hookPath + '/' + filename, '', cb);
      }, done);
    });

    it('should return loaded export config', function() {
      var result = Replicator.loadConfig(exportConfig, { hookPath: hookPath });

      should.deepEqual(Object.keys(result), [ 'collFoo', 'collBar' ]);
      should.deepEqual(Object.keys(result.collFoo), [ 'filter', 'hooks', 'orig' ]);
      should.deepEqual(Object.keys(result.collBar), [ 'filter', 'hooks', 'orig' ]);
      // collFoo
      should.deepEqual(result.collFoo.filter, { 'someAttr': { '$in': ['foo', 'bar'] } });
      should.strictEqual(result.collFoo.hooks.length, 2); // hook to hide keys added
      should.deepEqual(result.collFoo.orig, {
        'hooks': ['export_hook_foo'],
        'filter': { 'someAttr': { '$in': ['foo', 'bar'] } },
        'hide': ['hideFoo']
      });
      // collBar
      should.deepEqual(result.collBar.filter, undefined);
      should.strictEqual(result.collBar.hooks.length, 1); // hook to hide keys added
      should.deepEqual(result.collBar.orig, {
        'hooks': ['export_hook_bar']
      });
    });
  });

  describe('fetchFromDb', function() {
    var coll;

    it('should require coll to be an object', function() {
      (function() { Replicator.fetchFromDb(1); }).should.throw('coll must be an object');
    });

    it('should require type to be either "import" or "export"', function() {
      coll = dbImport.collection('replication');
      (function() { Replicator.fetchFromDb(coll, 1); }).should.throw('type must be either "import" or "export"');
    });

    it('should require remote to be a string', function() {
      (function() { Replicator.fetchFromDb(coll, 'import'); }).should.throw('remote must be a string');
    });

    it('should require cb to be a function', function() {
      (function() { Replicator.fetchFromDb(coll, 'import', 'foo', {}); }).should.throw('cb must be a function');
    });

    it('needs an import config for further testing', function(done) {
      var importConfig = {
        _id: 'some',
        type: 'import',
        remote: 'bar',
        collections: {
          fooColl: {
            hooks: ['import_bar']
          },
          barColl: {
            hooks: ['import_bar']
          }
        }
      };
      coll.insert(importConfig, done);
    });

    it('should callback with an error if config is not found', function(done) {
      Replicator.fetchFromDb(coll, 'import', 'foo', function(err) {
        should.strictEqual(err.message, 'replication config not found');
        done();
      });
    });

    it('should return import config', function(done) {
      Replicator.fetchFromDb(coll, 'import', 'bar', function(err, cfg) {
        if (err) { throw err; }
        should.deepEqual(cfg, {
          _id: 'some',
          type: 'import',
          remote: 'bar',
          collections: {
            fooColl: {
              hooks: ['import_bar']
            },
            barColl: {
              hooks: ['import_bar']
            }
          }
        });
        done();
      });
    });
  });
});
