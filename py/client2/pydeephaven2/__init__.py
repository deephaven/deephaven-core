#import gc as _gc
#import os as _os

# ARROW-8684: Disable GC while initializing Cython extension module,
# to workaround Cython bug in https://github.com/cython/cython/issues/3603
#_gc_enabled = _gc.isenabled()
#_gc.disable()
#import pydeephaven2.lib as _lib
#if _gc_enabled:
#    _gc.enable()

#from pydeephaven2.lib import (DateTime)

def get_include():
    """
    Return absolute path to directory containing Arrow C++ include
    headers. Similar to numpy.get_include
    """
    return _os.path.join(_os.path.dirname(__file__), 'include')

def get_libraries():
    """
    Return list of library names to include in the `libraries` argument for C
    or Cython extensions using pydeephaven2
    """
    return ['pydeephaven2']

def get_library_dirs():
    package_cwd = _os.path.dirname(__file__)
    library_dirs = [package_cwd]
