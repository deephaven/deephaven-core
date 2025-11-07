#!/bin/bash

set -euo pipefail

if [ -z "${DH_PREFIX}" ]; then
    echo "$0: Environment variable DH_PREFIX is not set, aborting." 1>&2
    exit 1
fi

source $DH_PREFIX/env.sh

cd $DH_PREFIX/src/rdeephaven

R --no-save --no-restore <<EOF
library('pkgdown')
status = tryCatch(
  {
     pkgdown::build_site(preview=FALSE)
     0
  },
  error=function(e) { 
    cat("ERROR occurred during pkgdown::build_site()\n")
    cat("Error message:", conditionMessage(e), "\n")
    cat("Error class:", class(e), "\n")
    print(e)
    1 
  }
)
print(paste0('status=', status))
quit(save='no', status=status)
EOF
