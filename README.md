# PersDB

*multi-master replication with transformations*

Features:
* built on the principles of owning your own data
* multi-master replication with transformations
* support systems that are built with privacy by design
* [security by design](https://github.com/Netsend/persdb/wiki/privilege-separation)

Work in progress, see [wiki](https://github.com/Netsend/persdb/wiki) for progress.

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

Add a new unprivileged user:
```
$ sudo useradd -d /var/empty -r -s /bin/false -U _pdbnull
```

Add a user for your database:
```
$ sudo useradd -d /var/empty -r -s /bin/false -U pdblevel
```

Edit config/example.hjson and start the server:
```
$ sudo bin/persdb.js config/example.hjson
```

# Adding a WebSocket server

By default only a tcp server is started on 127.0.0.1 port 2344. Communication
between different servers should be done over an ssh tunnel.

Communication with browsers can be done by using WebSockets. It is required to
start the WebSocket server over TLS so a key, certificate and Diffie-Hellman
parameters file must be generated in order for this to work.

Generate a private key, a temporary csr, a self signed certificate and a
DH params file:
```
$ openssl genrsa -out key.pem 2048
$ openssl req -new -sha256 -key key.pem -out csr.pem
$ openssl x509 -req -in csr.pem -signkey key.pem -out cert.pem
$ rm csr.pem
$ openssl dhparam -outform PEM -out dhparam.pem 2048
```

Edit your config to enable the WebSocket server and include these three files.

# License

Copyright 2012, 2013, 2014, 2015 Netsend.

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
