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

/*jshint -W068 */

if (process.getuid() !== 0) { 
  console.error('run tests as root');
  process.exit(1);
}

var VersionedSystem = require('../../lib/versioned_system');

var db, db2, oplogDb, oplogColl;
var databaseName = 'test_versioned_system';
var databaseName2 = 'test2';
var oplogDatabase = 'local';

var databaseNames = [databaseName, databaseName2, 'foo', 'bar'];
var Database = require('../_database');

// open database connection
var database = new Database(databaseNames);
before(function(done) {
  database.connect(function(err, dbs) {
    if (err) { throw err; }
    db = dbs[0];
    db2 = dbs[1];
    oplogDb = db.db(oplogDatabase);
    oplogColl = oplogDb.collection('oplog.$main');
    done();
  });
});

after(database.disconnect.bind(database));

describe('VersionedSystem', function() {
  describe('initVCs root', function() {
    it('needs a document in collection for further testing', function(done) {
      // insert two new documents in the collection and see if they're versioned using a
      // rebuild (one of which is already versioned). if a new snapshot collection is
      // created, rebuild should maintain original versions.
      var docs = [{ foo: 'a' }];
      db2.collection('someColl').insert(docs, done);
    });

    it('should start and stop', function(done) {
      var vcCfg = {
        test2: {
          someColl: {
            dbPort: 27019,
            debug: false,
            autoProcessInterval: 50,
            size: 1
          }
        }
      };

      var vs = new VersionedSystem(oplogColl, { debug: false });
      vs.initVCs(vcCfg, function(err) {
        if (err) { throw err; }

        setTimeout(function() {
          vs.stop(done);
        }, 100);
      });
    });
  });
});
