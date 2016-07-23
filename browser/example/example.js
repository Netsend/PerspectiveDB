// open an IndexedDB instance as usual
var req = indexedDB.open('MyDB')

req.onsuccess = (ev) => {
  var db = ev.target.result

  // Initiate PersDB with this database. Use watch mode to automatically track
  // changes to the object stores. This requires support for ES6 Proxy, e.g.
  // Firefox 18+ or Chrome 49+. If not using watch mode, then all updates to any
  // object store should be written via pdb.put and pdb.del.
  var pdb = new PersDB(db, { // PersDB is a global set by including build.js.
    watch: true
  })

  // connect to a remote peer at wss://example.com (use the default port, 3344)
  pdb.connect({
    name: 'aRemote',         // local reference for the remote
    host: 'example.com',     // address of a secure websocket server
    db: 'foo',               // name of the database on the remote
    username: 'joe',
    password: 'secret'
  }).then(() => {
    console.log('connected');
  }).catch(err => console.error(err));

  // a data event is emitted for every new version
  pdb.on('data', (item) => {
    console.log('new version:', item)
  })
}
