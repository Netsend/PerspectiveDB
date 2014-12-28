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

var should = require('should');
var ObjectID = require('mongodb').ObjectID;
var Timestamp = require('mongodb').Timestamp;

var walkBranch = require('../../../lib/walk_branch');
var VersionedCollection = require('../../../lib/versioned_collection');

var db;
var databaseName = 'test_versioned_collection';
var Database = require('../../_database');

// open database connection
var database = new Database(databaseName);
before(function(done) {
  database.connect(function(err, dbc) {
    db = dbc;
    done(err);
  });
});

after(database.disconnect.bind(database));


describe('walkBranch', function() {
  var collectionName = 'walkBranch';

  var A = { _id : { _id: 'foo', _v: 'A', _pa: [] } };
  var B = { _id : { _id: 'foo', _v: 'B', _pa: ['A'] } };
  var C = { _id : { _id: 'foo', _v: 'C', _pa: ['B'] } };
  var D = { _id : { _id: 'foo', _v: 'D', _pa: ['C'] } };
  var E = { _id : { _id: 'foo', _v: 'E', _pa: ['B'] } };
  var F = { _id : { _id: 'foo', _v: 'F', _pa: ['E', 'C'] } };
  var G = { _id : { _id: 'foo', _v: 'G', _pa: ['F'] } };
  var H = { _id : { _id: 'foo', _v: 'H', _pa: ['F'] } };
  var J = { _id : { _id: 'foo', _v: 'J', _pa: ['H'] } };
  var K = { _id : { _id: 'foo', _v: 'K', _pa: ['J'] } };
  var I = { _id : { _id: 'foo', _v: 'I', _pa: ['H', 'G', 'D'] } };

  // create the following structure:
  // A <-- B <-- C <----- D
  //        \     \        \
  //         E <-- F <-- G  \
  //                \     \  \      
  //                 H <------- I
  //                  \
  //                   J <-- K
  it('should save DAG', function(done) {
    var vc = new VersionedCollection(db, collectionName);
    vc._snapshotCollection.insert([A, B, C, D, E, F, G, H, J, K, I], {w: 1}, done);
  });

  it('should find A after B', function(done) {
    var vc = new VersionedCollection(db, collectionName);
    var i = 0;
    walkBranch({ '_id._id': 'foo' }, B._id._v, vc.localPerspective, vc._snapshotCollection, function(err, item, stream) {
      if (!item) { return done(); }
      should.equal(err, null);
      if (i === 0) {
        should.deepEqual(item, B);
      } else {
        should.deepEqual(item, A);
        stream.destroy();
      }
      i++;
    });
  });

  it('should find F after G', function(done) {
    var vc = new VersionedCollection(db, collectionName);
    var i = 0;
    walkBranch({ '_id._id': 'foo' }, G._id._v, vc.localPerspective, vc._snapshotCollection, function(err, item, stream) {
      if (!item) { return done(); }
      should.equal(err, null);
      if (i === 0) {
        should.deepEqual(item, G);
      } else {
        should.deepEqual(item, F);
        stream.destroy();
      }
      i++;
    });
  });

  it('should find H, G, F, E, D after I', function(done) {
    var vc = new VersionedCollection(db, collectionName);
    var i = 0;
    walkBranch({ '_id._id': 'foo' }, 'I', vc.localPerspective, vc._snapshotCollection, function(err, item, stream) {
      if (!item) {
        // stream closed 
        should.equal(i, 6);
        return done();
      }
      should.equal(err, null);
      switch (i) {
      case 0:
        should.deepEqual(item, I);
        break;
      case 1:
        should.deepEqual(item, H);
        break;
      case 2:
        should.deepEqual(item, G);
        break;
      case 3:
        should.deepEqual(item, F);
        break;
      case 4:
        should.deepEqual(item, E);
        break;
      case 5:
        should.deepEqual(item, D);
        break;
      case 6:
        stream.destroy();
      }
      i++;
    });
  });
});
