/**
 * Copyright 2015, 2016 Netsend.
 *
 * This file is part of PerspectiveDB.
 *
 * PerspectiveDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PerspectiveDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PerspectiveDB. If not, see <https://www.gnu.org/licenses/>.
 */

'use strict';

if (process.getuid() !== 0) { 
  console.error('run tests as root');
  process.exit(1);
}

var c = require('constants');
var assert = require('assert');
var fs = require('fs');
var net = require('net');

var async = require('async');
var bson = require('bson');
var BSON = new bson.BSONPure.BSON();
var level = require('level-packager')(require('leveldown'));
var rimraf = require('rimraf');
var ws = require('nodejs-websocket');

var logger = require('../../../lib/logger');
var MergeTree = require('../../../lib/merge_tree');
var spawn = require('../../lib/spawn');

var cert;

cert = __dirname + '/cert.pem';

var wsClientOpts = {
  ca: fs.readFileSync(cert),
  secureProtocol: 'SSLv23_client_method',
  secureOptions: c.SSL_OP_NO_SSLv2|c.SSL_OP_NO_SSLv3|c.SSL_OP_NO_TLSv1|c.SSL_OP_NO_TLSv1_1,
  ciphers: 'ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-CHACHA20-POLY1305:ECDHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES128-GCM-SHA256'
};

var tasks = [];
var tasks2 = [];

var cons, silence;
var chroot = '/var/pdb';
var dbPath = '/test_pdb_root';

// open loggers
tasks.push(function(done) {
  logger({ console: true, mask: logger.DEBUG2 }, function(err, l) {
    if (err) { throw err; }
    cons = l;
    logger({ silence: true }, function(err, l) {
      if (err) { throw err; }
      silence = l;
      // ensure chroot
      fs.mkdir(chroot, 0o755, function(err) {
        if (err && err.code !== 'EEXIST') { throw err; }

        // remove any pre-existing dbPath
        rimraf(chroot + dbPath, done);
      });
    });
  });
});

// should start a TCP server and listen on port 1234, test by connecting only
tasks.push(function(done) {
  function onSpawn(child) {
    setTimeout(function() {
      net.createConnection(1234, function() {
        child.kill();
      });
    }, 1000);
  }

  var opts = {
    onSpawn: onSpawn,
    onExit: done,
    testStdout: function(stdout) {
      assert(/TCP server bound 127.0.0.1:1234/.test(stdout));
      assert(/client connected 127.0.0.1-/.test(stdout));
    }
  };
  spawn([__dirname + '/../../../bin/pdb', __dirname + '/test_pdb.hjson'], opts);
});

// should try to start a WSS server and if this fails because of missing attributes (key, cert, dhparam) automatically shutdown
tasks.push(function(done) {
  var opts = {
    echoErr: false,
    onExit: done,
    testStderr: function(stderr) {
      assert(/msg.key must be a non-empty string/.test(stderr));
    }
  };
  spawn([__dirname + '/../../../bin/pdb', __dirname + '/test_pdb_wss_wrong_config.hjson'], opts);
});

// should start a WSS server, test by connecting with wss client
tasks.push(function(done) {
  function onSpawn(child) {
    setTimeout(function() {
      ws.connect('wss://localhost:1235', wsClientOpts, function() {
        child.kill();
      });
    }, 1000);
  }

  var opts = {
    onSpawn: onSpawn,
    onExit: done,
    testStdout: function(stdout) {
      assert(/-127.0.0.1-1235: end \(no binary data received\)/.test(stdout));
    }
  };
  spawn([__dirname + '/../../../bin/pdb', __dirname + '/test_pdb_wss_export.hjson'], opts);
});

// should start a WSS server, and disconnect because of empty data request
tasks.push(function(done) {
  function onSpawn(child) {
    setTimeout(function() {
      var authReq = {};

      var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
        client.on('close', function() {
          child.kill();
        });
        client.sendText(JSON.stringify(authReq) + '\n');
      });
    }, 1000);
  }

  var opts = {
    onSpawn: onSpawn,
    echoErr: false,
    onExit: done,
    testStderr: function(stderr) {
      assert(/-127.0.0.1-1234 invalid auth request/.test(stderr));
    }
  };
  spawn([__dirname + '/../../../bin/pdb', __dirname + '/test_pdb_wss_export.hjson'], opts);
});

// should start a WSS server, and disconnect because db name does not exist in config
tasks.push(function(done) {
  function onSpawn(child) {
    setTimeout(function() {
      var authReq = {
        db: 'baz',
        username: 'foo',
        password: 'bar'
      };

      var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
        client.on('close', function() {
          child.kill();
        });
        client.sendText(JSON.stringify(authReq) + '\n');
      });
    }, 1000);
  }

  var opts = {
    onSpawn: onSpawn,
    echoErr: false,
    onExit: done,
    testStderr: function(stderr) {
      assert(/_verifyAuthRequest no config found for db: baz invalid credentials/.test(stderr));
    }
  };
  spawn([__dirname + '/../../../bin/pdb', __dirname + '/test_pdb_wss_export.hjson'], opts);
});

// should start a WSS server, and disconnect because username does not exist in config
tasks.push(function(done) {
  function onSpawn(child) {
    setTimeout(function() {
      var authReq = {
        username: 'foo',
        password: 'bar',
        db: 'someDb'
      };

      var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
        client.on('close', function() {
          child.kill();
        });
        client.sendText(JSON.stringify(authReq) + '\n');
      });
    }, 1000);
  }

  var opts = {
    onSpawn: onSpawn,
    echoErr: false,
    onExit: done,
    testStderr: function(stderr) {
      assert(/_verifyAuthRequest no users or import \/ export config found someDb invalid credentials/.test(stderr));
    }
  };
  spawn([__dirname + '/../../../bin/pdb', __dirname + '/test_pdb_wss_export.hjson'], opts);
});

// should start a WSS server, and disconnect because password is incorrect
tasks.push(function(done) {
  function onSpawn(child) {
    setTimeout(function() {
      var authReq = {
        username: 'someClient',
        password: 'bar',
        db: 'someDb'
      };

      var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
        client.on('close', function() {
          child.kill();
        });
        client.sendText(JSON.stringify(authReq) + '\n');
      });
    }, 1000);
  }

  var opts = {
    onSpawn: onSpawn,
    echoErr: false,
    onExit: done,
    testStderr: function(stderr) {
      assert(/_verifyAuthRequest someDb someClient invalid credentials/.test(stderr));
    }
  };
  spawn([__dirname + '/../../../bin/pdb', __dirname + '/test_pdb_wss_export.hjson'], opts);
});

// should start a WSS server, and send a data request that signals no data is expected (no import key in config)
tasks.push(function(done) {
  function onSpawn(child) {
    setTimeout(function() {
      var authReq = {
        username: 'someClient',
        password: 'somepass',
        db: 'someDb'
      };

      var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
        client.sendText(JSON.stringify(authReq) + '\n');
        // expect data request
        client.on('text', function(data) {
          assert.deepEqual(data, JSON.stringify({ start: false }) + '\n');
          client.on('close', function(code, reason) {
            assert(code, 9823);
            assert(reason, 'test');
            child.kill();
          });
          client.close(9823, 'test');
        });
      });
    }, 1000);
  }

  var opts = {
    onSpawn: onSpawn,
    onExit: done,
    testStdout: function(stdout) {
      assert(/successfully authenticated "someClient"/.test(stdout));
    }
  };
  spawn([__dirname + '/../../../bin/pdb', __dirname + '/test_pdb_wss_export.hjson'], opts);
});

// should start a WSS server, and send a data request that signals data is expected (import true)
tasks.push(function(done) {
  function onSpawn(child) {
    setTimeout(function() {
      var authReq = {
        username: 'someClient',
        password: 'somepass',
        db: 'someDb'
      };

      var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
        client.sendText(JSON.stringify(authReq) + '\n');
        // expect data request
        client.on('text', function(data) {
          assert.deepEqual(data, JSON.stringify({ start: true }) + '\n');
          client.on('close', function(code, reason) {
            assert(code, 9823);
            assert(reason, 'test');
            child.kill();
          });
          client.close(9823, 'test');
        });
      });
    }, 1000);
  }

  var opts = {
    config: 'test_pdb_wss_import.hjson',
    onSpawn: onSpawn,
    onExit: done,
    testStdout: function(stdout) {
      assert(/successfully authenticated "someClient"/.test(stdout));
    }
  };
  spawn([__dirname + '/../../../bin/pdb', __dirname + '/test_pdb_wss_import.hjson'], opts);
});

// should start a WSS server, and accept a data request + but disconnect because no export is configured and data is requested
tasks.push(function(done) {
  var pe = 'someClient';

  function onSpawn(child) {
    setTimeout(function() {
      var authReq = {
        username: pe,
        password: 'somepass',
        db: 'someDb'
      };
      var dataReq = {
        start: true
      };

      var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
        client.sendText(JSON.stringify(authReq) + '\n');

        // expect data request
        client.on('text', function(data) {
          assert.deepEqual(data, JSON.stringify({ start: true }) + '\n');

          client.sendText(JSON.stringify(dataReq) + '\n');

          client.on('close', function() {
            child.kill();
          });
        });
      });
    }, 1000);
  }

  var opts = {
    onSpawn: onSpawn,
    onExit: done,
    testStdout: function(stdout) {
      assert(/data requested but no export configured/.test(stdout));
    }
  };
  spawn([__dirname + '/../../../bin/pdb', __dirname + '/test_pdb_wss_import.hjson'], opts);
});

// should start a WSS server, and accept a data request + two BSON items
tasks.push(function(done) {
  var pe = 'someClient';
  var vSize = 3;

  function onSpawn(child) {
    setTimeout(function() {
      var authReq = {
        username: pe,
        password: 'somepass',
        db: 'someDb'
      };
      var dataReq = {
        start: true
      };

      var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
        client.sendText(JSON.stringify(authReq) + '\n');

        // expect data request
        client.on('text', function(data) {
          assert.deepEqual(data, JSON.stringify({ start: true }) + '\n');

          client.sendText(JSON.stringify(dataReq) + '\n');

          // now send a root node and a child
          var obj1 = { h: { id: 'abc', v: 'Aaaa', pa: [] }, b: { some: true } };
          var obj2 = { h: { id: 'abc', v: 'Bbbb', pa: ['Aaaa'] }, b: { some: 'other' } };

          client.sendBinary(BSON.serialize(obj1));
          client.sendBinary(BSON.serialize(obj2));

          client.on('close', function(code, reason) {
            assert(code, 9823);
            assert(reason, 'test');
            child.kill();
          });
          setTimeout(function() {
            client.close(9823, 'test');
          }, 100);
        });
      });
    }, 1000);
  }

  function onExit() {
    // open and search in database if item is written
    level('/var/pdb' + dbPath, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, db) {
      if (err) { throw err; }

      var mt = new MergeTree(db, {
        perspectives: [pe],
        vSize: vSize,
        log: silence
      });
      var rs = mt._pe[pe].createReadStream();
      var i = 0;
      rs.on('data', function(item) {
        i++;
        if (i === 1) {
          assert.deepEqual(item, { h: { id: 'abc', v: 'Aaaa', pe: pe, i: 1, pa: [] }, b: { some: true } });
        }
        if (i === 2) {
          assert.deepEqual(item, { h: { id: 'abc', v: 'Bbbb', pe: pe, i: 2, pa: ['Aaaa'] }, b: { some: 'other' } });
        }
      });
      rs.on('end', function() {
        assert.strictEqual(i, 2);
        mt.startMerge({ tail: false }).on('finish', function() {
          db.close(done);
        });
      });
    });
  }

  var opts = {
    onSpawn: onSpawn,
    onExit: onExit,
  };
  spawn([__dirname + '/../../../bin/pdb', __dirname + '/test_pdb_wss_import_export.hjson'], opts);
});

// should start a WSS server, and accept a data request + one BSON item, and send previously saved items (depends on previous test)
tasks.push(function(done) {
  var pe = 'someClient';
  var vSize = 3;

  function onSpawn(child) {
    setTimeout(function() {
      var authReq = {
        username: pe,
        password: 'somepass',
        db: 'someDb'
      };
      var dataReq = {
        start: true
      };

      var client = ws.connect('wss://localhost:1235', wsClientOpts, function() {
        client.sendText(JSON.stringify(authReq) + '\n');

        // expect data request
        client.on('text', function(data) {
          assert.deepEqual(data, JSON.stringify({ start: 'Bbbb' }) + '\n');

          client.sendText(JSON.stringify(dataReq) + '\n');

          // now send a new child
          var obj = { h: { id: 'abc', v: 'Cccc', pa: ['Bbbb'] }, b: { some: 'new' } };

          client.sendBinary(BSON.serialize(obj));

          var i = 0;
          client.on('binary', function(rs) {
            rs.on('readable', function() {
              i++;
              var item = BSON.deserialize(rs.read());
              if (i === 1) {
                assert.deepEqual(item, { h: { id: 'abc', v: 'Aaaa', pa: [] }, b: { some: true } });
              }
              if (i === 2) {
                assert.deepEqual(item, { h: { id: 'abc', v: 'Bbbb', pa: ['Aaaa'] }, b: { some: 'other' } });
              }
              if (i > 2) {
                assert.deepEqual(item, { h: { id: 'abc', v: 'Cccc', pa: ['Bbbb'] }, b: { some: 'new' } });
                client.close(9823, 'test');
              }
            });
          });

          client.on('close', function(code, reason) {
            assert(code, 9823);
            assert(reason, 'test');
            assert.strictEqual(i, 3);
            child.kill();
          });
        });
      });
    }, 1000);
  }

  function onExit() {
    // open and search in database if item is written
    level('/var/pdb' + dbPath, { keyEncoding: 'binary', valueEncoding: 'binary' }, function(err, db) {
      if (err) { throw err; }

      var mt = new MergeTree(db, {
        perspectives: [pe],
        vSize: vSize,
        log: silence
      });
      var rs = mt._pe[pe].createReadStream();
      var i = 0;
      rs.on('data', function(item) {
        i++;
        if (i === 1) {
          assert.deepEqual(item, { h: { id: 'abc', v: 'Aaaa', pe: pe, i: 1, pa: [] }, b: { some: true } });
        }
        if (i === 2) {
          assert.deepEqual(item, { h: { id: 'abc', v: 'Bbbb', pe: pe, i: 2, pa: ['Aaaa'] }, b: { some: 'other' } });
        }
        if (i > 2) {
          assert.deepEqual(item, { h: { id: 'abc', v: 'Cccc', pe: pe, i: 3, pa: ['Bbbb'] }, b: { some: 'new' } });
        }
      });

      rs.on('end', function() {
        assert.strictEqual(i, 3);
        mt.startMerge({ tail: false }).on('finish', function() {
          db.close(done);
        });
      });
    });
  }

  var opts = {
    onSpawn: onSpawn,
    onExit: onExit,
  };
  spawn([__dirname + '/../../../bin/pdb', __dirname + '/test_pdb_wss_import_export.hjson'], opts);
});

// test native client via TCP server, loop perspectives and see if merge procedures fire off
tasks.push(function(done) {
  // give the children some time
  function onSpawn(child) {
    setTimeout(function() {
      child.kill();
    }, 5000);
  }

  var opts = {
    onSpawn: onSpawn,
    onExit: done,
    testStdout: function(stdout) {
      assert(/t:otherClient createReadStream open reader/.test(stdout));
    }
  };
  spawn([__dirname + '/../../../bin/pdb', __dirname + '/test_pdb_with_client.hjson'], opts);
});

async.series(tasks, function(err) {
  if (err) {
    console.error(err);
  } else {
    console.log('ok');
  }

  // cleanup after
  cons.close(function(err) {
    if (err) { throw err; }
    silence.close(function(err) {
      if (err) { throw err; }
      rimraf(chroot + dbPath, function(err) {
        if (err) { console.error(err); }
      });
    });
  });
});
