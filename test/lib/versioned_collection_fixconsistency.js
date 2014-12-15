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
var Timestamp = require('mongodb').Timestamp;
var async = require('async');

var VersionedCollection = require('../../lib/versioned_collection');

var db;
var databaseName = 'test_versioned_collection_fixconsistency';
var Database = require('../_database');

// open database connection
var database = new Database(databaseName);
before(function(done) {
  database.connect(function(err, dbc) {
    db = dbc;
    done(err);
  });
});

after(database.disconnect.bind(database));

describe('VersionedCollection.fixConsistency', function() {
  describe('processBatchSize', function() {
    it('should default processBatchSize to 500', function () {
      var vc = new VersionedCollection(db, 'processBatchSize', { });
      should.equal(vc._processBatchSize, 500);
    });

    it('should set processBatchSize', function () {
      var vc = new VersionedCollection(db, 'processBatchSize', { processBatchSize: 3 });
      should.equal(vc._processBatchSize, 3);
    });
  });

  describe('_ensureAllInDAG', function() {
    var A = { _id: { _id: 'foo', _v: 'A', _pe: 'I', _pa: [] } };
    var B = { _id: { _id: 'bar', _v: 'B', _pe: 'I', _pa: []} };
    var C = { _id: { _id: 'qux', _v: 'C', _pe: 'I', _pa: [] } };

    it('should not add the same version twice with _ensureAllInDAG', function(done) {
      var collectionName = 'fixConsistency2';
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      var vc2 = new VersionedCollection(db, collectionName, { hide: true });
      vc._ensureAllInDAG([{ item: A }, { item: B }, { item:C }], function(err) {
        if (err) { throw err; }
        vc2._ensureAllInDAG([{ item: A }, { item: B }, { item:C }], function(err) {
          if (err) { throw err; }
          vc2._snapshotCollection.find().toArray(function(err, items) {
            if (err) { throw err; }
            should.equal(items.length, 6);
          });
          done();
        });
      });
    });
  });

  describe('fixConsistency', function() {
    var A = { _id: { _id: 'foo', _v: 'A', _pe: 'I', _pa: [] } };
    var B = { _id: { _id: 'bar', _v: 'B', _pe: 'I', _pa: []} };
    var C = { _id: { _id: 'qux', _v: 'C', _pe: 'I', _pa: [] } };
    var allItems = [{ item: A }, { item: B }, { item:C }];
    var firstItem = [{ item: A }];

    it('should run fixConsistency without errors', function(done) {
      var collectionName = 'fixConsistency';
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      var vc2 = new VersionedCollection(db, collectionName, { hide: true });
      vc._ensureAllInDAG(allItems, function(err) {
        if (err) { throw err; }
        vc2.fixConsistency('I', function(err) {
          if (err) { throw err; }
          vc2._snapshotCollection.find().toArray(function(err, items) {
            if (err) { throw err; }
            should.equal(items.length, 6);
            done();
          });
        });
      });
    });

    it('should fix consistency when crashed after 1 item _ensureSamePerspective', function(done) {
      var collectionName = 'fixConsistency_ensureSamePerspective';
      var vc = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
      vc._ensureSamePerspective(firstItem, function(err){
        if (err) { throw err; }
        var vc2 = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
        vc2.fixConsistency('I', function(err) {
          if (err) { throw err; }
          vc2._snapshotCollection.find().toArray(function(err, items) {
            // nothing is inserted yet, ensureSamePerspective is merely a check
            should.equal(items.length, 0);
            done();
          });
        });
      });
    });

    it('should fix consistency when crashed after 1 item _ensureM3', function(done) {
      var collectionName = 'fixConsistency_ensureM3';
      var vc = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
      vc._ensureSamePerspective(allItems, function(err){
        if (err) { throw err; }
        vc._ensureM3(firstItem, function(err) {
          if (err) { throw err; }
          var vc2 = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
          vc2.fixConsistency('I', function(err) {
            if (err) { throw err; }
            vc2._snapshotCollection.find().toArray(function(err, items) {
              // nothing is inserted yet, _ensureM3 is merely a check
              should.equal(items.length, 0);
              done();
            });
          });
        });
      });
    });

    it('should fix consistency when crashed after 1 item _checkAncestry', function(done) {
      var collectionName = 'fixConsistency_checkAncestry';
      var vc = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
      vc._ensureSamePerspective(allItems, function(err){
        if (err) { throw err; }
        vc._ensureM3(allItems, function(err) {
          if (err) { throw err; }
          vc._checkAncestry(firstItem, function(err) {
            if (err) { throw err; }
            var vc2 = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
            vc2.fixConsistency('I', function(err) {
              if (err) { throw err; }
              vc2._snapshotCollection.find().toArray(function(err, items) {
                if (err) { throw err; }
                // nothing is inserted yet, _checkAncestry is merely a check
                should.equal(items.length, 0);
                done();
              });
            });
          });
        });
      });
    });

    it('should fix consistency when crashed after 1 item _ensureVirtualCollection', function(done) {
      var collectionName = 'fixConsistency_ensureVirtualCollection';
      var localItems, remoteItems;
      var vc = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
      vc._ensureSamePerspective(allItems, function(err, perspective){
        vc._ensureM3(allItems, function(err) {
          if (err) { throw err; }
          vc._checkAncestry(allItems, function(err, allItms, DAGs) {
            if (err) { throw err; }
            if (perspective === vc.localPerspective) {
              localItems = allItms;
            } else {
              remoteItems = allItms;
            }

            async.eachSeries(Object.keys(DAGs), function(id, cb) {
              var items = DAGs[id];

              vc._ensureVirtualCollection([items[0]], function(err) {
                if (err) { throw err; }
                cb(new Error('crash after first item'));
              });
            }, function(err) {
              if (err && err.message!=='crash after first item') { throw err; }
              var vc2 = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
              vc2.fixConsistency('I', function(err) {
                if (err) { throw err; }
                vc2._snapshotCollection.find().toArray(function(err, items) {
                  // nothing is inserted yet, _ensureVirtualCollection is merely a check
                  should.equal(items.length, 0);
                  done();
                });
              });
            });
          });
        });
      });
    });

    it('should fix consistency when crashed after 1 item _checkParentsInVirtualCollection', function(done) {
      var collectionName = 'fixConsistency_checkParentsInVirtualCollection';
      var localItems, remoteItems;
      var vc = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
      vc._ensureSamePerspective(allItems, function(err, perspective){
        vc._ensureM3(allItems, function(err) {
          if (err) { throw err; }
          vc._checkAncestry(allItems, function(err, allItms, DAGs) {
            if (err) { throw err; }
            if (perspective === vc.localPerspective) {
              localItems = allItms;
            } else {
              remoteItems = allItms;
            }

            async.eachSeries(Object.keys(DAGs), function(id, cb) {
              var items = DAGs[id];

              vc._ensureVirtualCollection(items, function(err) {
                if (err) { throw err; }
                vc._checkParentsInVirtualCollection([items[0]], function(err){
                  if (err) { throw err; }
                  cb(new Error('crash after first item'));
                });
              });
            }, function(err) {
              if (err && err.message!=='crash after first item') { throw err; }
              var vc2 = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
              vc2.fixConsistency('I', function(err) {
                if (err) { throw err; }
                vc2._snapshotCollection.find().toArray(function(err, items) {
                  // nothing is inserted yet, _checkParentsInVirtualCollection is merely a check
                  should.equal(items.length, 0);
                  done();
                });
              });
            });
          });
        });
      });
    });

    it('should fix consistency when crashed after 1 item _ensureLocalPerspective', function(done) {
      var collectionName = 'fixConsistency_ensureLocalPerspective';
      var localItems, remoteItems;
      var vc = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
      vc._ensureSamePerspective(allItems, function(err, perspective){
        if (err) { throw err; }
        vc._ensureM3(allItems, function(err) {
          if (err) { throw err; }
          vc._checkAncestry(allItems, function(err, allItms, DAGs) {
            if (err) { throw err; }
            if (perspective === vc.localPerspective) {
              localItems = allItms;
            } else {
              remoteItems = allItms;
            }

            async.eachSeries(Object.keys(DAGs), function(id, cb) {
              var items = DAGs[id];

              vc._ensureVirtualCollection(items, function(err) {
                if (err) { throw err; }
                vc._checkParentsInVirtualCollection(items, function(err) {
                  if (err) { throw err; }
                  vc._ensureLocalPerspective([items[0]], function(err){
                    if (err) { throw err; }
                    cb(new Error('crash after first item'));
                  });
                  return;
                });
              });
            }, function(err) {
              if (err && err.message!=='crash after first item') { throw err; }
              var vc2 = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
              vc2.fixConsistency('I', function(err) {
                if (err) { throw err; }
                vc2._snapshotCollection.find().toArray(function(err, items) {
                  // nothing is inserted yet, _ensureLocalPerspective is merely a check
                  should.equal(items.length, 0);
                  vc._virtualCollection.find().toArray(function(err, items) {
                    should.deepEqual(items, [{
                      _id: { _id: 'foo', _v:'A', _pe:'I', _pa:[] },
                      _m3: { _ack: false, _op: new Timestamp(0, 0)}
                    },{
                      _id:{_id:'foo',_v:'A',_pe:'_local',_pa:[]},
                      _m3:{_ack:false,_op:new Timestamp(0, 0)}
                    }]);
                    done();
                  });
                });
              });
            });
          });
        });
      });
    });

    it('should fix consistency when crashed after 1 item _ensureOneHead', function(done) {
      var collectionName = 'fixConsistency_ensureOneHead';
      var localItems = [];
      var remoteItems = [];

      var vc = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
      vc._ensureSamePerspective(allItems, function(err, perspective){
        vc._ensureM3(allItems, function(err) {
          if (err) { throw err; }
          vc._checkAncestry(allItems, function(err, allItms, DAGs) {
            if (err) { throw err; }

            if (perspective === vc.localPerspective) {
              localItems = allItms;
            } else {
              remoteItems = allItms;
            }

            async.eachSeries(Object.keys(DAGs), function(id, cb) {
              var items = DAGs[id];

              vc._ensureVirtualCollection(items, function(err) {
                if (err) { throw err; }
                vc._checkParentsInVirtualCollection(items, function(err) {
                  if (err) { throw err; }
                  vc._ensureLocalPerspective(items, function(err, newOrExistingLocalItems){
                    if (newOrExistingLocalItems.length) {
                      if (perspective === vc.localPerspective) {
                        throw(new Error('local duplicates created'));
                      }

                      // make sure items that are used in virtual collection are updated
                      Array.prototype.push.apply(items, newOrExistingLocalItems);

                      // make sure items that are used at the end on insertion are updated
                      Array.prototype.push.apply(localItems, newOrExistingLocalItems);
                    }

                    var toProcess = items;
                    if (newOrExistingLocalItems.length) {
                      toProcess = newOrExistingLocalItems;
                    }

                    vc._ensureOneHead(toProcess, function(){
                      cb(new Error('crash after ensureOneHead of first DAG item'));
                    });
                  });
                });
              });
            }, function(err) {
              if (err && err.message!=='crash after ensureOneHead of first DAG item') { throw err; }
              var vc2 = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
              vc2.fixConsistency('I', function(err) {
                if (err) { throw err; }
                vc2._snapshotCollection.find().toArray(function(err, items) {
                  // nothing is inserted yet, _ensureLocalPerspective is merely a check
                  should.equal(items.length, 0);
                  vc._virtualCollection.find().toArray(function(err, items) {
                    should.deepEqual(items, [{
                      _id: { _id: 'foo', _v:'A', _pe:'I', _pa:[] },
                      _m3: { _ack: false, _op: new Timestamp(0, 0)}
                    },{
                      _id:{_id:'foo',_v:'A',_pe:'_local',_pa:[]},
                      _m3:{_ack:false,_op:new Timestamp(0, 0)}
                    }]);
                    done();
                  });
                });
              });
            });
          });
        });
      });
    });

    var A0 = { _id: { _id: 'foo', _v: 'A0', _pe: 'I', _pa: [] } };
    var A1 = { _id: { _id: 'foo', _v: 'A', _pe: 'I', _pa: ['A0'] } };
    var B0 = { _id: { _id: 'bar', _v: 'B', _pe: 'I', _pa: []} };
    var C0 = { _id: { _id: 'qux', _v: 'C', _pe: 'I', _pa: [] } };
    var allItems0 = [{ item: A1},{ item: B0}, {item: C0}];

    it('should insert item into snapshot to merge with in next test', function(done) {
      var collectionName = 'fixConsistency_mergeNewHeads';
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc._ensureAllInDAG([{ item: A0}], function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.find().toArray(function(err, items) {
          should.deepEqual(items,[
            {'_id':{'_id':'foo','_v':'A0','_pe':'I','_pa':[]},'_m3':{'_ack':false,'_op':new Timestamp(0, 0)}},
            {'_id':{'_id':'foo','_v':'A0','_pe':'_local','_pa':[],'_i':1},'_m3':{'_ack':false,'_op':new Timestamp(0, 0)}}]
          );
          done();
        });
      });
    });

    it('should fix consistency when crashed after 1 item _mergeNewHeads', function(done) {
      var collectionName = 'fixConsistency_mergeNewHeads';
      var localItems = [];
      var remoteItems = [];

      var vc = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
      vc._ensureSamePerspective(allItems0, function(err, perspective){
        if (err) { throw err; }
        vc._ensureM3(allItems0, function(err) {
          if (err) { throw err; }
          vc._checkAncestry(allItems0, function(err, allItms, DAGs, newRoots) {
            if (err) { throw err; }

            if (perspective === vc.localPerspective) {
              localItems = allItms;
            } else {
              remoteItems = allItms;
            }

            async.eachSeries(Object.keys(DAGs), function(id, cb) {
              var items = DAGs[id];

              vc._ensureVirtualCollection(items, function(err) {
                if (err) { throw err; }
                vc._checkParentsInVirtualCollection(items, function(err) {
                  if (err) { throw err; }
                  vc._ensureLocalPerspective(items, function(err, newOrExistingLocalItems){
                    if (newOrExistingLocalItems.length) {
                      if (perspective === vc.localPerspective) {
                        throw(new Error('local duplicates created'));
                      }

                      // make sure items that are used in virtual collection are updated
                      Array.prototype.push.apply(items, newOrExistingLocalItems);

                      // make sure items that are used at the end on insertion are updated
                      Array.prototype.push.apply(localItems, newOrExistingLocalItems);
                    }

                    var toProcess = items;
                    if (newOrExistingLocalItems.length) {
                      toProcess = newOrExistingLocalItems;
                    }

                    vc._ensureOneHead(toProcess, function(err, newHeads){
                      if (err) { return cb(err); }
                      vc._snapshotCollection.find().toArray(function(err) {
                        if (err) { throw err; }
                        vc._mergeNewHeads([newHeads[0]], newRoots, function(){
                          cb(new Error('crash after mergeNewHeads of first newHead'));
                        });
                      });
                    });
                  });
                });
              });
            }, function(err) {
              if (err && err.message!=='crash after mergeNewHeads of first newHead') { throw err; }
              var vc2 = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
              vc2.fixConsistency('I', function(err) {
                if (err) { throw err; }
                vc2._snapshotCollection.find().toArray(function(err, items) {
                  if (err) { throw err; }
                  should.deepEqual(items, [
                    {'_id':{'_id':'foo','_v':'A0','_pe':'I','_pa':[]},'_m3':{'_ack':false,'_op':new Timestamp(0,0)}},
                    {'_id':{'_id':'foo','_v':'A0','_pe':'_local','_pa':[],'_i':1},'_m3':{'_ack':false,'_op':new Timestamp(0,0)}}]
                  );
                  done();
                });
              });
            });
          });
        });
      });
    });

    it('should insert item into snapshot to merge with in next test', function(done) {
      var collectionName = 'fixConsistency_ensureIntoSnapshot';
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc._ensureAllInDAG([{ item: A0}], function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.find().toArray(function(err, items0) {
          if (err) { throw err; }
          should.deepEqual(items0,[
            {'_id':{'_id':'foo','_v':'A0','_pe':'I','_pa':[]},'_m3':{'_ack':false,'_op':new Timestamp(0, 0)}},
            {'_id':{'_id':'foo','_v':'A0','_pe':'_local','_pa':[],'_i':1},'_m3':{'_ack':false,'_op':new Timestamp(0, 0)}}]
          );
          done();
        });
      });
    });

    it('should fix consistency when crashed after 1 item _ensureIntoSnapshot', function(done) {
      var collectionName = 'fixConsistency_ensureIntoSnapshot';
      var localItems2 = [];
      var remoteItems2 = [];

      var vc = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
      vc._ensureSamePerspective(allItems0, function(err, perspective){
        if (err) { throw err; }
        vc._ensureM3(allItems0, function(err) {
          if (err) { throw err; }
          vc._checkAncestry(allItems0, function(err, allItms, DAGs, newRoots) {

            if (perspective === vc.localPerspective) {
              localItems2 = allItms;
            } else {
              remoteItems2 = allItms;
            }

            async.eachSeries(Object.keys(DAGs), function(id, cb) {
              var items = DAGs[id];

              vc._ensureVirtualCollection(items, function(err) {
                if (err) { throw err; }
                vc._checkParentsInVirtualCollection(items, function(err) {
                  if (err) { throw err; }
                  vc._ensureLocalPerspective(items, function(err, newOrExistingLocalItems){
                    if (err) { throw err; }
                    if (newOrExistingLocalItems.length) {
                      if (perspective === vc.localPerspective) {
                        throw(new Error('local duplicates created'));
                      }

                      // make sure items that are used in virtual collection are updated
                      Array.prototype.push.apply(items, newOrExistingLocalItems);

                      // make sure items that are used at the end on insertion are updated
                      Array.prototype.push.apply(localItems2, newOrExistingLocalItems);
                    }

                    var toProcess = items;
                    if (newOrExistingLocalItems.length) {
                      toProcess = newOrExistingLocalItems;
                    }

                    vc._ensureOneHead(toProcess, function(err, newHeads){
                      if (err) { return cb(err); }
                      vc._mergeNewHeads([newHeads[0]], newRoots, function(err, newItems){
                        if (err) { return cb(err); }

                        // make sure items that are used at the end on insertion are updated
                        Array.prototype.push.apply(localItems2, newItems);
                        cb();
                      });
                    });
                  });
                });
              });
            }, function(err) {
              if (err) { throw(err); }
              // 'crash' after 1 remote item
              vc._snapshotCollection.insert(remoteItems2[0], {w: 1, comment: '_insertIntoSnapshot_crashafter' }, function(err) {
                if (err) { throw err; }
                var vc2 = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
                vc2.fixConsistency('I', function(err) {
                  if (err) { throw(err); }
                  vc2._snapshotCollection.find().toArray(function(err, items) {
                    if (err) { throw(err); }
                    should.deepEqual(items,[{'_id':{'_id':'foo','_v':'A0','_pe':'I','_pa':[]},'_m3':{'_ack':false,'_op':new Timestamp(0,0)}},
                       {'_id':{'_id':'foo','_v':'A0','_pe':'_local','_pa':[],'_i':1},'_m3':{'_ack':false,'_op':new Timestamp(0,0)}},
                       {'_id':{'_id':'foo','_v':'A','_pe':'I','_pa':['A0']},'_m3':{'_ack':false,'_op':new Timestamp(0,0)}},
                       {'_id':{'_id':'foo','_v':'A','_pe':'_local','_pa':['A0'],'_i':2},'_m3':{'_ack':false,'_op':new Timestamp(0,0)}}]
                    );
                    done();
                  });
                });
              });
            });
          });
        });
      });
    });

    var A2 = { _id: { _id: 'foo', _v: 'A2', _pe: 'I', _pa: [] } };
    var A3 = { _id: { _id: 'foo', _v: 'A', _pe: 'I', _pa: ['A2'] } };
    var B2 = { _id: { _id: 'bar', _v: 'B', _pe: 'I', _pa: []} };
    var C2 = { _id: { _id: 'qux', _v: 'C', _pe: 'I', _pa: [] } };
    var allItems2 = [{ item: A3},{ item: B2}, {item: C2}];

    it('should insert item into snapshot to merge with in next test', function(done) {
      var collectionName = 'fixConsistency_syncLocalHeadsWithCollection';
      var vc = new VersionedCollection(db, collectionName, { hide: true });
      vc._ensureAllInDAG([{ item: A2}], function(err) {
        if (err) { throw err; }
        vc._snapshotCollection.find().toArray(function(err, items0) {
          if (err) { throw err; }
          should.deepEqual(items0,[
            {'_id':{'_id':'foo','_v':'A2','_pe':'I','_pa':[]},'_m3':{'_ack':false,'_op':new Timestamp(0, 0)}},
            {'_id':{'_id':'foo','_v':'A2','_pe':'_local','_pa':[],'_i':1},'_m3':{'_ack':false,'_op':new Timestamp(0, 0)}}]
          );
          done();
        });
      });
    });

    it('should fix consistency when crashed after 1 item _syncLocalHeadsWithCollection', function(done) {
      var collectionName = 'fixConsistency_syncLocalHeadsWithCollection';
      var localItems = [];
      var remoteItems = [];

      var vc = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
      vc._ensureSamePerspective(allItems2, function(err, perspective){
        vc._ensureM3(allItems2, function(err) {
          if (err) { throw err; }
          vc._checkAncestry(allItems2, function(err, allItms, DAGs, newRoots) {

            if (perspective === vc.localPerspective) {
              localItems = allItms;
            } else {
              remoteItems = allItms;
            }

            async.eachSeries(Object.keys(DAGs), function(id, cb) {
              var items = DAGs[id];

              vc._ensureVirtualCollection(items, function(err) {
                if (err) { throw err; }
                vc._checkParentsInVirtualCollection(items, function(err) {
                  if (err) { throw err; }
                  vc._ensureLocalPerspective(items, function(err, newOrExistingLocalItems){
                    if (newOrExistingLocalItems.length) {
                      if (perspective === vc.localPerspective) {
                        throw(new Error('local duplicates created'));
                      }

                      // make sure items that are used in virtual collection are updated
                      Array.prototype.push.apply(items, newOrExistingLocalItems);

                      // make sure items that are used at the end on insertion are updated
                      Array.prototype.push.apply(localItems, newOrExistingLocalItems);
                    }

                    var toProcess = items;
                    if (newOrExistingLocalItems.length) {
                      toProcess = newOrExistingLocalItems;
                    }

                    vc._ensureOneHead(toProcess, function(err, newHeads){
                      if (err) { return cb(err); }
                      vc._mergeNewHeads([newHeads[0]], newRoots, function(err, newItems){
                        if (err) { return cb(err); }

                        // make sure items that are used at the end on insertion are updated
                        Array.prototype.push.apply(localItems, newItems);
                        cb();
                      });
                    });
                  });
                });
              });
            }, function(err) {
              if (err) { throw(err); }
              vc._ensureIntoSnapshot(perspective, localItems, remoteItems, function(err, newLocalHeads) {
                if (err) { throw(err); }
                // 'crash' after 1 new local head
                vc._syncLocalHeadsWithCollection([newLocalHeads[0]], function(err){
                  if (err) { throw err; }
                  var vc2 = new VersionedCollection(db, collectionName, { processBatchSize: 3 });
                  vc2._collection.find().toArray(function(err, items) {
                    should.deepEqual(items, [{'_id':'foo','_v':'A'}]);
                    vc2.fixConsistency('I', function(err) {
                      if (err) { throw(err); }
                      vc2._collection.find().toArray(function(err, items) {
                        if (err) { throw(err); }
                        should.deepEqual(items,[{'_id':'foo','_v':'A'},{'_id':'bar','_v':'B'},{'_id':'qux','_v':'C'}]);
                        done();
                      });
                    });
                  });
                });
              });
            });
          });
        });
      });
    });
  });
});
