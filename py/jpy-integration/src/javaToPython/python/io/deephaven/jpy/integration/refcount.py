import ctypes

def refcount(address):
  return ctypes.c_long.from_address(address).value
