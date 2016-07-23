# PersDB-browser

Features:
* transparent versioning by using ES6 Proxy (Firefox only for now)

# Usage example

Track changes to all object stores in "MyDB" and sync with the WebSocket server
at example.com.

Include [browser/build/persdb.js](https://raw.githubusercontent.com/Netsend/persdb/master/browser/build/persdb.js) in your html file:
```html
<!DOCTYPE html>
<html>
<head>
<title>PersDB example</title>
</head>
<body>
<script type="text/javascript" src="../build/persdb.js"></script>
<script type="text/javascript" src="example.js"></script>
</body>
</html>
```

example.js looks as follows:
```js
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
  pdb.on('conflict', function(item) {
    console.error('merge conflict:', item.c, item.n, item.l)
  })

  // merges happen when updates are coming in from a remote
  pdb.on('merge', function(item) {
    console.log('new merge saved:', item)
  })
}
```

# Building with browserify

Clone this repo and use browserify to create a bundle that can be loaded in the
browser.

```
$ git clone https://github.com/Netsend/persdb.git
$ cd persdb
$ npm install
$ browserify browser/example/example.js > browser/build/build.js
```

See config.json and example.html for a working example.

For instructions on how to start a websocket server, see the main readme.

# License

Copyright 2015 Netsend.

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
