# PersDB-browser

Features:
* transparent versioning by using ES6 Proxy (Firefox only for now)

# Usage

Clone this repo and use browserify to create a bundle that can be loaded in the
browser.

```
$ git clone https://github.com/Netsend/persdb.git
$ cd persdb
$ npm install
$ browserify browser/example.js > browser/build/build.js
```

See config.json and example.html for a working example.

For instructions on how to start a server, see the main readme.

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
