'use strict';

var cp = require('child_process');
var tmpdir = require('os').tmpdir;

var rimraf = require('rimraf');
var level = require('level');

var db;
var dbPath = tmpdir() + '/test_iterator';

// open database
function start(done) {
  rimraf(dbPath, function(err) {
    if (err) { throw err; }
    level(dbPath, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, _db) {
      if (err) { throw err; }
      db = _db;
      done();
    });
  });
}

function after(done) {
  done = done || function() {};
  db.close(function(err) {
    if (err) { throw err; }
    rimraf(dbPath, done);
  });
}

// write a one byte key, then iterate, do this 20 times
function readAfterWriteTest(done) {
  var i = 0;

  function createBatch(n) {
    var trx = [];
    var b;

    for (var j = 0; j < n; j++) {
      b = new Buffer(6);
      b.writeUIntBE(i, 0, 6);
      trx.push({ type: 'put', key: b, value: new Buffer(1000) });
      i++;
    }

    // delete last value
    b = new Buffer(6);
    b.writeUIntBE(--i, 0, 6);
    trx.push({ type: 'del', key: b });

    return trx;
  }

  function writeRead() {
    var b = createBatch(5);
    db.batch(b, { sync: false }, function(err) {
      if (err) { throw err; }
      var gt = new Buffer(6);
      var lt = new Buffer(13);
      gt.fill(0x00);
      lt.fill(0xff);
      var s = db.createKeyStream({ gte: gt, lte: lt });
      var j = 0;
      s.on('data', function() { j++; });
      s.on('close', function(err) {
        if (err) { throw err; }
        if (i - j != 0) {
          console.error(b);
          console.log('missed %d, i: %d, j: %d', (i - j).toString(), i, j);
          throw new Error();
        } else if (i % 100 == 0) {
          console.log(i);
        }
        if (i < 1000000) {
          writeRead();
        } else {
          done();
        }
      });
    });
  }

  writeRead();
}

start(function() {
  readAfterWriteTest(after);
});
