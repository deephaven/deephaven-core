import jpy

if jpy.has_jvm():
  boolean = jpy.get_type('boolean')
  byte = jpy.get_type('byte')
  char = jpy.get_type('char')
  short = jpy.get_type('short')
  int = jpy.get_type('int')
  long = jpy.get_type('long')
  float = jpy.get_type('float')
  double = jpy.get_type('double')

  __primitives = frozenset([ boolean, byte, char, short, int, long, float, double ])

  def is_primitive_type(type):
    return type in __primitives
