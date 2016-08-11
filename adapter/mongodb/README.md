# MongoDB adapter

Features:
* sync with an existing mongodb collection
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

In your PerspectiveDB config file, add the `source` key to the database you want to
connect with a mongodb collection. I.e. connect db foo with mongodb pdb.test3:
```
dbs: [
  {
    name: foo
    ...
    source: {
      url: mongodb://127.0.0.1:27017/pdb
      collections: [test3]
      #dbUser: bar
      #oplogDbUser: bar
      #passdb: "secrets.hjson"
    }
  }
]
```

A full example is given in config/examples/example.hjson. Make sure to specify
mongo auth credentials if they are needed.

Note: it is currently not supported to add collections after the database is
initialized. If you want to add extra collections after the database is
bootstrapped, you have to delete the existing pdb database and start over.
