#!/usr/bin/env bash
set -x #echo on

# These are more or less the compiler and linker flags used by Python's distutils.core.setup()

CC_FLAGS="-pthread -g -fwrapv -O2 -Wall -fstack-protector --param=ssp-buffer-size=4 -Wformat -Werror=format-security -D_FORTIFY_SOURCE=2"
LD_FLAGS="-shared -fPIC -Wl,-Bsymbolic-functions -Wl,-z,relro -Wl,-O1"

gcc $CC_FLAGS mypy.c -ldl -o mypy
gcc $CC_FLAGS mypydl.c -I/usr/include/python3.4m -lpython3.4m $LD_FLAGS -o mypydl.so

python3 setup.py install --user

