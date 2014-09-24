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
  echo "usage: $(basename $0) start [end] file"
  exit 1
fi

if [ -n "$4" ]; then
  echo "usage: $(basename $0) start [end] file"
  exit 1
fi

start="$1"

if [ -z "$3" ]; then
  file="$2"
else
  file="$3"
  end="$2"
fi

if [ ! -e "$file" ]; then
  echo "file does not exist"
  exit 1
fi

if [ ! -r "$file" ]; then
  echo "file is not readable"
  exit 1
fi

# find start line number
start=$(grep -m 1 -n "$start" "$file" | head | awk 'BEGIN { FS = ":" } ; { print $1 }')
if [ -n "$end" ]; then
  end=$(grep -n "$end" "$file" | tail -n 1 | awk 'BEGIN { FS = ":" } ; { print $1 }')
else
  end=$(wc -l "$file" | awk '{ print $1 }')
fi

echo "extracting lines $start to $end from $file"
sed -n "${start},${end}p" "$file"
