#!/bin/bash

set -euo pipefail

for var in DHCPP NCPUS LD_LIBRARY_PATH; do
    if [ -z "${!var}" ]; then
        echo "$0: Environment variable $var is not set, aborting." 1>&2
        exit 1
    fi
done

if [ ! -d ./src ] || [ ! -f ./src/Makevars ] || [ ! -d ./R ] || [ ! -f ./DESCRIPTION ]; then
    echo "The current directory `pwd` does not look like an R package source directory, aborting." 1>&2
    exit 1
fi

# Ensure builds are always done from a clean slate.
trap 'rm -f src/*.o src/*.so' 1 2 15
rm -f src/*.o src/*.so

MAKE="make -j${NCPUS}" R --no-save --no-restore <<EOF
status = tryCatch(
  {
     install.packages(".", repos=NULL, type="source")
     0
  },
  error=function(e) 1,
  warning=function(w) 2
)
print(paste0('status=', status))
quit(save='no', status=status)
EOF

rm -f src/*.o src/*.so

exit 0
