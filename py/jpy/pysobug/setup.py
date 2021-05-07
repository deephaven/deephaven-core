#!/usr/bin/env python3

from distutils.core import setup
from distutils.extension import Extension

# This one works
#extension = Extension('mypymod', sources=['mypymod.c'], libraries=['python3.4m'])
# This one not
extension = Extension('mypymod', sources=['mypymod.c'])
setup(name='mypymod', ext_modules=[extension])
