#!/bin/sh
#
# Copyright 2014 Netsend.
#
# This file is part of Mastersync.
#
# Mastersync is free software: you can redistribute it and/or modify it under the
# terms of the GNU Affero General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# Mastersync is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
# PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License along
# with Mastersync. If not, see <https://www.gnu.org/licenses/>.
#

if [ -z "$2" ]; then
  echo "usage: $(basename $0) database collection"
  exit 1
fi

if [ -n "$3" ]; then
  echo "usage: $(basename $0) database collection"
  exit 1
fi

echo "db.$2.drop()" | mongo "$1" &&
echo "db.createCollection('$2', { capped: true, size: 536870912 });" | mongo "$1"
echo "db.$2.ensureIndex({ '_id._id': 1, '_id._pe': 1, '_id._i': -1 }, { name: '_id_pe_i' })" | mongo "$1"
