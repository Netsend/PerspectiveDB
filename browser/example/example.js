// open an IndexedDB instance as usual
var req = indexedDB.open('MyDB')

// create some stores to work with and create a snapshot and conflict store
req.onupgradeneeded = (ev) => {
  var db = ev.target.result
  db.createObjectStore('customers', { keyPath: 'email' })
  db.createObjectStore('employees')

  // needed for PersDB
  db.createObjectStore('_pdb')
  db.createObjectStore('conflict', { autoIncrement: true })
}

req.onsuccess = (ev) => {
  var db = ev.target.result

  // Initiate PersDB with this database. Use watch mode to automatically track
  // changes. This requires support for ES6 Proxy (Firefox 18+ or Chrome 49+).
  var opts = {
    watch: true,
    snapshotStore: '_pdb',
    conflictStore: 'conflict'
  }
  PersDB.createNode(db, opts, (err, pdb) => {
    if (err) throw err

    // connect to a remote peer at wss://example.com
    pdb.connect({
      name: 'aRemote',         // local reference for the remote
      host: 'example.com',     // address of a secure websocket server
      db: 'foo',               // name of the database on the remote
      username: 'joe',
      password: 'secret'
    }).then(() => {
      console.log('connected')
    }).catch(err => console.error(err))

    // a data event is emitted for every new version
    pdb.on('data', (item) => {
      console.log('new version:', item)
    })
  })
}
