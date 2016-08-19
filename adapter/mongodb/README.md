# MongoDB adapter

Synchronize PerspectiveDB with mongodb databases.

Features:
* sync with an existing mongodb database
* use mongodb as the local perspective

Status: beta

# Configuration

The oplog in MongoDB must be enabled. This can be done either by enabling a
replica set or enabling master/slave mode. If you don't have these already,
add the following settings to your `/etc/mongodb.conf`:
```
master = true
oplogSize = 2000
```

In your PerspectiveDB config file, add the `source` key for the database you
want to track. I.e. database "pdb" in mongodb:
```
dbs: [
  {
    name: foo
    ...
    source: {
      url: mongodb://127.0.0.1:27017/pdb
    }
  }
]
```

A full example is given in config/examples/example.hjson. Make sure to specify
mongo auth credentials if they are needed.
