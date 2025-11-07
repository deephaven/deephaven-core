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
# Fix Pandoc error 23: disable definition_lists extension which is incompatible with gfm
Sys.setenv(PKGDOWN_PANDOC_ARGS = "--from=markdown-definition_lists --to=html")
status = tryCatch(
  {
     pkgdown::build_site(preview=FALSE, new_process=FALSE)
     0
  },
  error=function(e) { print(paste0('ERROR: ', e)); 1 }
)
print(paste0('status=', status))
quit(save='no', status=status)
EOF
