// open an IndexedDB instance as usual
var req = indexedDB.open('MyDB')

req.onsuccess = function(ev) {
  var db = ev.target.result

  // pass the IndexedDB instance to PersDB, PersDB is a global set by including
  // build.js.
  var pdb = new PersDB(db)

  // connect to a remote peer at wss://example.com (use the default port, 3344)
  var aRemote = {
    name: 'aRemote',        // local reference for the remote
    host: 'example.com',    // address of a secure websocket server
    db: 'foo',              // name of the database on the remote
    username: 'joe',
    password: 'secret'
  }

  pdb.connect(aRemote);

  // listen for merge conflicts.
  pdb.on('conflict', function(obj) {
    console.error('merge conflict:', obj)
  })

  // a data event is emitted for every new version
  pdb.on('data', function(item) {
    console.log('new version:', item)
  })
}
