/**
 * Copyright 2016 Netsend.
 *
 * This file is part of PerspectiveDB.
 *
 * PerspectiveDB is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PerspectiveDB is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with PerspectiveDB. If not, see <https://www.gnu.org/licenses/>.
 */

'use strict';

var stream = require('stream');

// create a PassThrough stream, loaded with data
// opts.reverse {Boolean, default false}
// opts.filter {Function}
// opts.map {Function}
function streamify(data, opts) {
  if (!Array.isArray(data)) throw new TypeError('data must be an array');

  opts = opts || {};
  if (opts == null || typeof opts !== 'object') throw new TypeError('opts must be an object');

  var filter = opts.filter || function() { return true; };
  var map = opts.map || function(item) { return item; };

  var pt = new stream.PassThrough({ objectMode: true });
  var i;
  if (opts.reverse) {
    for (i = data.length; i-- > 0;)
      if (filter(data[i]))
        pt.write(map(data[i]));
  } else {
    for (i = 0; i < data.length; i++)
      if (filter(data[i]))
        pt.write(map(data[i]));
  }

  pt.end();
  return pt;
}

module.exports = streamify;
