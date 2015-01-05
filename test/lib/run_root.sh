#!/bin/sh

dir=$(dirname $0)
cd "$dir" || exit 1

for i in `ls assert_root/*.js`; do
  echo "\nTEST node $i"
  node "$i"
done

for i in `ls mocha_root/*.js`; do
  echo "\nTEST mocha $i"
  mocha "$i"
done
