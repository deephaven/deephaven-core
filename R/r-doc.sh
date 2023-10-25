#!/bin/bash

set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 output-dir" 1>&2
    exit 1
fi

if [ -z "${DH_PREFIX}" ]; then
    echo "$0: Environment variable DH_PREFIX is not set, aborting." 1>&2
    exit 1
fi

source $DH_PREFIX/env.sh

cd $DH_PREFIX/src/rdeephaven

OUT_DIR="$1"

R --no-save --no-restore <<EOF
library('roxygen2')
status = tryCatch(
  {
     roxygen2::roxygenize()
     0
  },
  error=function(e) 1
)
print(paste0('status=', status))
quit(save='no', status=status)
EOF

tar -zcf $OUT_DIR/man.tgz man

exit 0
