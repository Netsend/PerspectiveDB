/**
 * Copyright 2016 Netsend.
 *
 * This file is part of PersDB.
 *
 * PersDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PersDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PersDB. If not, see <https://www.gnu.org/licenses/>.
 */

'use strict';

var BSONStream = require('bson-stream');
var LDJSONStream = require('ld-jsonstream');
var once = require('once');

var dataRequest = require('./data_request');

function remoteConnHandler(conn, mt, remote, receiveBeforeSend, db, cb) {
  doHandshake(conn, mt, remote, receiveBeforeSend, function(err, sendOffset) {
    if (err) { cb(err); return; }
    setupReaderWriter(conn, mt, remote, sendOffset, db);
    cb();
  });
}

/**
 * Receive and send a data request.
 *
 * @param {Object} conn  tcp connection
 * @param {MergeTree} mt  MergeTree instance
 * @param {Object} remote  configuration of the remote, including hooks
 * @param {Boolean} receiveBeforeSend  whether to initiate sending a data request
 *                                     or wait for the other party to initiate
 * @param {Function} cb  first parameter will be an error object or null
 */
function doHandshake(conn, mt, remote, receiveBeforeSend, cb) {
  cb = once(cb);

  // expect one data request
  // create line delimited json stream
  var ls = new LDJSONStream({ flush: false, maxDocs: 1, maxBytes: 512 });

  ls.once('error', cb);

  var dataReqReceived, dataReqSent;

  var sendOffset; // remember requested offset
  var dataReq; // must be accessible in finalize if receiveBeforeSend

  var i = 0;
  function finalize() {
    i++;
    // handle receiveBeforeSend
    if (!dataReqSent && receiveBeforeSend && dataReqReceived && dataReq) {
      conn.write(JSON.stringify(dataReq) + '\n');
      dataReqSent = true;
    }
    if (dataReqReceived && dataReqSent) {
      cb(null, sendOffset);
    } else if (i >= 2) {
      throw new Error('unexpected state in handshake');
    }
    // wait
  }

  conn.pipe(ls).once('readable', function() {
    var req = ls.read();
    // callback in case the connection is ended before any data is received, rely on once(cb)
    if (req == null) { cb(new Error('no data request received')); return; }

    conn.unpipe(ls);

    // push back any data in ls
    if (ls.buffer.length) {
      conn.unshift(ls.buffer);
    }

    if (!dataRequest.valid(req)) {
      cb(new Error('invalid data request'));
      return;
    }

    if (req.start && !remote.export) {
      cb(new Error('data requested but no export configured'));
      return;
    }

    sendOffset = req.start;

    dataReqReceived = true;

    finalize();
  });

  // send data request with last offset if there are any import rules
  if (remote.import) {
    // find last received item of this remote
    mt.lastReceivedFromRemote(remote.name, 'base64', function(err, last) {
      if (err) { cb(err); return; }

      // send data request with last known version
      dataReq = { start: last || true };

      if (!receiveBeforeSend || dataReqReceived) {
        conn.write(JSON.stringify(dataReq) + '\n');
        dataReqSent = true;
      }
      finalize();
    });
  } else {
    // signal that no data is expected
    conn.write(JSON.stringify({ start: false }) + '\n');
    dataReqSent = true;
    finalize();
  }
}

/**
 * Setup mt reader and writer and connect to the connection.
 *
 * - maybe export data
 * - maybe import data
 */
function setupReaderWriter(conn, mt, remote, sendOffset, db) {
  var mtr, mtw;

  // send data to remote if requested and allowed
  if (remote.export && sendOffset) {
    var readerOpts = {
      tail: true,
      tailRetry: remote.tailRetry || 5000,
      bson: true
    };
    if (typeof sendOffset === 'string') {
      readerOpts.first = sendOffset;
      readerOpts.excludeFirst = true;
    }
    if (typeof remote.export === 'object') {
      readerOpts.filter    = remote.export.filter;
      readerOpts.hooks     = remote.export.hooks;
      readerOpts.hooksOpts = remote.export.hooksOpts;
    }

    mtr = mt.createReadStream(readerOpts);

    conn.on('error', function() {
      mtr.end();
    });
    conn.on('close', function() {
      mtr.end();
    });

    mtr.pipe(conn);
  }

  if (remote.import) {
    // create remote transform to ensure h.pe is set to this remote and run all hooks
    var wsOpts = {
      db:         db,
      filter:     remote.import.filter || {},
      hooks:      remote.import.hooks || [],
      hooksOpts:  remote.import.hooksOpts || {},
    };
    // some export hooks need the name of the database
    wsOpts.hooksOpts.to = db;

    // set hooksOpts with all keys but the pre-configured ones
    Object.keys(remote.import).forEach(function(key) {
      if (!~['filter', 'hooks', 'hooksOpts'].indexOf(key)) {
        wsOpts.hooksOpts[key] = remote.import[key];
      }
    });

    mtw = mt.createRemoteWriteStream(remote.name, wsOpts);

    mtw.on('error', function() {
      conn.end();
    });

    // expect bson from remote
    var bs = new BSONStream();

    bs.on('error', function() {
      conn.end();
    });

    conn.pipe(bs).pipe(mtw);
  }
}

module.exports = remoteConnHandler;
