import jpy

def ping_pong_java(result, remaining):
  if not jpy.has_jvm():
    raise Exception('Expected JVM to already be running')
  if remaining <= 0:
    return result
  PingPongStack = jpy.get_type('io.deephaven.jpy.integration.PingPongStack')
  return PingPongStack.pingPongPython('{}(python,{})'.format(result, remaining), remaining - 1)