# PersDB-browser

Track changes to IndexedDB object stores and sync with peers over a WebSocket.


## Example

Create the stores *customers* and *employees* and sync with a peer at example.com.

Include a [build](https://netsend.nl/persdb/build.js) in your html so that the global PersDB is set:
```html
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>PersDB example</title>
</head>
<body>
<script type="text/javascript" src="https://netsend.nl/persdb/build.js"></script>
<script type="text/javascript" src="example.js"></script>
</body>
</html>
```

[example.js](https://github.com/Netsend/persdb/blob/master/browser/example/example.js) looks as follows:
```js
// open an IndexedDB instance as usual
var req = indexedDB.open('MyDB')

// create some stores to work with
req.onupgradeneeded = (ev) => {
  var db = ev.target.result
  db.createObjectStore('customers', { keyPath: 'email' })
  db.createObjectStore('employees')
}

req.onsuccess = (ev) => {
  var db = ev.target.result

  // Initiate PersDB with this database. Use watch mode to automatically track
  // changes. This requires support for ES6 Proxy (Firefox 18+ or Chrome 49+).
  PersDB.createNode(db, { watch: true }).then(pdb => {
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
  }).catch(err => console.error(err))
}
```

If not using watch mode, then all updates to any object store should be written
via `pdb.put` and `pdb.del`.

Note that a build can be created by using browserify, see *Development* for instructions.


## API

The API documentation can be found under [docs](https://github.com/Netsend/persdb/tree/master/browser/docs).


## Development

This module is built with [browserify](http://browserify.org). It is advised to use [watchify](https://www.npmjs.com/package/watchify) to
automatically create a build after each source change.

```shell
$ npm i
$ watchify browser/lib/persdb.js -o browser/build.js
```

Quickly wipe all IndexedDBs in all your Firefox profiles on OS X:
```shell
$ rm -rf ~/Library/Application\ Support/Firefox/profiles/*/storage/default/*
```

For instructions on how to start a websocket server, see the main readme.


## License

Copyright 2015, 2016 Netsend.

This file is part of PersDB.

PersDB is free software: you can redistribute it and/or modify it under the
terms of the GNU Affero General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

PersDB is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along
with PersDB. If not, see <https://www.gnu.org/licenses/>.
