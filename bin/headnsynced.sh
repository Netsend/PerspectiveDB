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

$(dirname $0)/mmlog.js --nsync -n 0 -d "$1" -c "$2" | awk '{ print $1 }' | uniq |  while read id; do
  $(dirname $0)/mmlog.js -n 1 -d "$1" -c "$2" --id $id
done
