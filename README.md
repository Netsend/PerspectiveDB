# PersDB

*multi-master replication with transformations*

Features:
* built on the principle of owning your own data:
  * you decide what you want to share, with who and how
  * don't rely on a third party (i.e. a cloud service)
  * privacy and [security](https://github.com/Netsend/PerspectiveDB/wiki/privilege-separation) by design
* multi-master replication with transformations
* support for browsers via IndexedDB and WebSockets (see [browser](https://github.com/Netsend/PerspectiveDB/tree/master/browser))
* support for MongoDB

Status: beta

Currently deploying the software into a controlled production environment.


## Installation

Note: Node.js 4.x is required.

Clone this repo and change your working dir to it:
```
$ git clone https://github.com/Netsend/PerspectiveDB.git
$ cd PerspectiveDB
```

Install npm modules:
```
$ npm install
```

Ensure /var/empty exists for the chrooted processes:
```
$ sudo mkdir /var/empty
```

Ensure database directory exists, i.e. /srv/pdb/mydb:
```
$ sudo mkdir -p /srv/pdb/mydb
```

Create a dedicated system user for the main process:
```
$ sudo useradd -d /var/empty -r -s /bin/false -U _pdbnull
```

Create a dedicated system user for the database process:
```
$ sudo useradd -d /var/empty -r -s /bin/false -U pdblevel
```

Create a user account database:
```
$ omask=$(umask); umask 077 && touch config/local/passwd.hjson; umask $omask
```

Grant access to a user for remote login, i.e. setup a password for user "john":
```
$ ./bin/adduser john >> config/local/passwd.hjson
```

Create a config file. I.e. copy and edit example.hjson:
```
$ cp config/examples/example.hjson config/local/pdb.hjson
```

Start the server:
```
$ sudo bin/pdb config/local/pdb.hjson
```

By default a tcp server is started on 127.0.0.1 port 2344. Communication between
different servers should be done over an ssh tunnel.


## Communicate with browsers
### Add a WebSocket server

Communication with browsers can be done using a WebSocket. It is required to
start the WebSocket server over a modern TLS connection with forward secrecy.

Generate the following files:
* DH parameters
* private key
* temporary CSR
* self signed certificate (or use a certificate signed by one of the major CAs)
```
$ umask 077
$ openssl dhparam -outform PEM -out config/local/dhparam.pem 2048
$ openssl genrsa -out config/local/key.pem 2048
$ umask 022
$ openssl req -new -sha256 -key config/local/key.pem -out config/local/csr.pem
$ openssl x509 -req -in config/local/csr.pem -signkey config/local/key.pem -out config/local/cert.pem
$ rm config/local/csr.pem
```

Edit your config to enable the WebSocket server and include these three files.
Relative paths are relative to the directory of the config file.

### Run the web client
A web client is included in the [browser](https://github.com/Netsend/PerspectiveDB/tree/master/browser)
directory. Use a local webserver like [http-server](https://www.npmjs.com/package/http-server)
to run the example in [browser/example](https://github.com/Netsend/PerspectiveDB/tree/master/browser/example).


## SSH tunnels

In order to communicate with external hosts an SSH tunnel must be used.
Different tunnels can be setup by PersDB. For those familiar, this works just
like the `ssh -L` command. Say PerspectiveDB runs on pdb.example.com. Then the
following is an example of a tunnel to the machine at mongodb.example.com. Every
connection on pdb.example.com to 127.0.0.1:1337 will be tunneled to
mongodb.example.com:27017.
```
tunnels: [
  {
    connect: ssh://mongodb.example.com:22
    fingerprint: DR5pI2uaxhQDyem9Cf0tQQ3RivlbC7jmJn5kkWpiY2g
    key: /home/john/.ssh/myidentity.key
    sshUser: john
    forward: 1337:127.0.0.1:27017
  }
]
```

Two things are important: the identity of the remote host, and the idenity of
PersDB. The identity of the remote host is listed under the key `fingerprint`.
It's a base64 encoded sha256 hash of the identity of the remote party. It can be
achieved by running `ssh-keygen -E sha256 -lf /etc/ssh/ssh_host_rsa_key.pub` on
mongodb.example.com (-E is supported since ssh 6.9). If the public key of
mongodb.example.com is located in some other file (i.e. your known_hosts), then
`ssh-keygen -E sha256 -lf ~/.ssh/known_hosts` can be run.

The identity of PersDB is listed under the key `key`. It is a private key on
pdb.example.com. Currently only RSA keys without a password are supported. A
new one can be created as follows:
`ssh-keygen -t rsa -f /home/john/.ssh/myidentity.key`. Leave the password empty.

A tunnel can be used by anyone on the machine, not only PersDB. To use a tunnel
with the mongodb adapter and connect to the database somedb on
mongodb.example.com one can use something like:
```
dbs: [
  {
    name: mydb
    ...
    source: {
      url: mongodb://127.0.0.1:1337/somedb
      coll: tyres
    }
  }
]
```


## Connecting with MongoDB

A special mongodb adapter is included that can be used to track changes in a
collection. See adapter/mongodb/README.md for further instructions.


## License

Copyright 2012-2016 Netsend.

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
