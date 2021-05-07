def noop(obj):
  pass

# the param t is just so we can get the method signature in java, and use an
# appropriate return type for the proxy handler
def identity(obj, t=None):
  return obj