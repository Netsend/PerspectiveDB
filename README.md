# PersDB

*multi-master replication with transformations*

Features:
* built on the principles of owning your own data
* multi-master replication with transformations
* support for browsers via IndexedDB and WebSockets
* support systems that are built with privacy by design
* [security by design](https://github.com/Netsend/persdb/wiki/privilege-separation)

Alpha status. Currently working on a MongoDB adapter.

# Installation

Note: Node.js 4.x is required.

Clone this repo and change your working dir to it:
```
$ git clone https://github.com/Netsend/persdb.git
$ cd persdb
```

Install npm modules:
```
$ npm install
```

Ensure /var/empty exists for the chrooted processes:
```
$ sudo mkdir /var/empty
```

Ensure database directory exists, i.e. /srv/persdb/mydb:
```
$ sudo mkdir -p /srv/persdb/mydb
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
$ omask=$(umask); umask 077 && touch local/config/passwd.hjson; umask $omask
```

Grant access to a user for remote login, i.e. "john":
```
$ ./bin/adduser john >> local/config/passwd.hjson
```

Create a config file. I.e. copy and edit example.hjson:
```
$ cp config/example.hjson local/config/pdb.hjson
```

Start the server:
```
$ sudo bin/persdb local/config/pdb.hjson
```

# Adding a WebSocket server

By default only a tcp server is started on 127.0.0.1 port 2344. Communication
between different servers should be done over an ssh tunnel.

Communication with browsers can be done by using WebSockets. It is required to
start the WebSocket server over TLS with forward secrecy so a private key,
certificate and Diffie-Hellman parameters file must exist for this to work.

Generate DH parameters, a private key, a temporary csr and a self signed
certificate (or use a certificate signed by one of the major CAs):
```
$ umask 077
$ openssl dhparam -outform PEM -out config/dhparam.pem 2048
$ openssl genrsa -out config/key.pem 2048
$ umask 022
$ openssl req -new -sha256 -key config/key.pem -out config/csr.pem
$ openssl x509 -req -in config/csr.pem -signkey config/key.pem -out config/cert.pem
$ rm config/csr.pem
```

Edit your config to enable the WebSocket server and include these three files.
Relative paths are relative to the directory of the config file that specifies
them. An example is given in config/example-with-websocket.hjson.

# SSH tunnels

In order to communicate with external hosts an SSH tunnel must be used.
Different tunnels can be setup by PersDB. For those familiar, this works just
like the `ssh -L` command. Say PersDB runs on persdb.example.com. Then the
following is an example of a tunnel to the machine at mongodb.example.com. Every
connection on persdb.example.com to 127.0.0.1:1337 will be tunneled to
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
persdb.example.com. Currently only RSA keys without a password are supported. A
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

# License

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
