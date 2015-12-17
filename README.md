# PersDB

*multi-master replication with transformations*

Features:
* built on the principles of owning your own data
* multi-master replication with transformations
* support systems that are built with privacy by design
* [security by design](https://github.com/Netsend/persdb/wiki/privilege-separation)

Work in progress, see [wiki](https://github.com/Netsend/persdb/wiki) for progress.

# Installation

Assuming Node.js 4.x is installed and you are in the PersDB project root.

Install npm modules:
```
$ npm install
```

Add new unprivileged user:
```
$ sudo useradd -d /var/empty -r -s /bin/false -U _pdbnull
```

Add a user for your database:
```
$ sudo useradd -d /var/empty -r -s /bin/false -U pdblevel
```

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
