# MongoDB adapter

Features:
* sync from an existing collection to a new PersDB
* PersDB uses mongodb as a master

Alpha status.

# Configuration

The oplog in MongoDB must be enabled. This can be done either by enabling a
replica set or enabling master/slave mode. If you don't have this already set,
add the following settings to your `/etc/mongodb.conf`:
```
master = true
oplogSize = 2000
```

Then in your PersDB config file, add the `source` key to the database you want
to connect with a mongodb collection. I.e. connect persdb foo with pdb.test3 in
mongodb:
```
dbs: [
  {
    name: foo
    ...
    source: {
      url: mongodb://127.0.0.1:27017/pdb
      coll: test3
      #dbUser: bar
      #oplogDbUser: bar
      #passdb: "secrets.hjson"
    }
  }
]
```

A full example is given in config/example-with-mongo.hjson.

# License

Copyright 2016 Netsend.

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
