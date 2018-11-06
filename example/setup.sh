#!/bin/bash

set -e
dir=`dirname $(readlink -f "$0")`

rm -rf "$dir/node_modules/@servicemix"
tsc  --preserveConstEnums --strictNullChecks --sourceMap --target es2015 --module commonjs --allowJs --checkJs false --lib es2015 --rootDir "$dir/../src/runtime" --outDir "$dir/node_modules/@servicemix/runtime" "$dir/../src/runtime/Instrument.ts"



