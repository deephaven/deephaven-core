sentinel = object()

def some_function(x = sentinel):
  if x is None:
    return "None"
  if x == sentinel:
    return "sentinel"
  return x