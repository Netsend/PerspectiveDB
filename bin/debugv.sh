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
  echo "usage: $(basename $0) version logfile"
  exit 1
fi

if [ ! -e "$2" ]; then
  echo "logfile does not exist"
  exit 1
fi

if [ ! -r "$2" ]; then
  echo "logfile is not readable"
  exit 1
fi

# find start line number
start=$(grep -m 1 -n "$1" "$2" | head | awk 'BEGIN { FS = ":" } ; { print $1 }')
start=$(($start-10))
end=$(grep -n "$1" "$2" | tail -n 1 | awk 'BEGIN { FS = ":" } ; { print $1 }')

#timestamp=$(date +'%Y-%m-%d_%H-%M-%S')
#strippedv=$(echo $1 | tr -cd '[a-zA-Z0-9]')
#strippedl=$(echo $2 | tr -cd '[a-zA-Z0-9]')
#file=${strippedv}-${strippedl}-${timestamp}.log

#echo extracting lines $start to $end in $file
echo "extracting lines $start to $end from $2"
sed -n "${start},${end}p" "$2"
