#!/bin/sh

dir=$(dirname $0)
cd "$dir" || exit 1

for i in `ls assert_root/*.js`; do
  echo "\nTEST node root $i"
  node "$i" || exit "$?"
done

for i in `ls mocha_root/*.js`; do
  echo "\nTEST mocha root $i"
  sleep 2
  mocha "$i" || exit "$?"
done
