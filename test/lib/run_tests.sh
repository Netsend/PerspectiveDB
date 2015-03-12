#!/bin/sh

dir=$(dirname $0)
cd "$dir" || exit 1

echo "\nTEST mocha $i"
mocha mocha/

for i in `ls assert/*.js`; do
  echo "\nTEST node $i"
  node "$i"
done
