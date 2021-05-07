import jpy

class ArrayTest:

  def __init__(self):
    self.jpy_boolean_array_type = type(jpy.array('boolean', []))
    self.jpy_char_array_type = type(jpy.array('char', []))
    self.jpy_byte_array_type = type(jpy.array('byte', []))
    self.jpy_short_array_type = type(jpy.array('short', []))
    self.jpy_int_array_type = type(jpy.array('int', []))
    self.jpy_long_array_type = type(jpy.array('long', []))
    self.jpy_float_array_type = type(jpy.array('float', []))
    self.jpy_double_array_type = type(jpy.array('double', []))

  def is_jpy_boolean_array(self, x):
    return isinstance(x, self.jpy_boolean_array_type)

  def is_jpy_char_array(self, x):
    return isinstance(x, self.jpy_char_array_type)

  def is_jpy_byte_array(self, x):
    return isinstance(x, self.jpy_byte_array_type)

  def is_jpy_short_array(self, x):
    return isinstance(x, self.jpy_short_array_type)

  def is_jpy_int_array(self, x):
    return isinstance(x, self.jpy_int_array_type)

  def is_jpy_long_array(self, x):
    return isinstance(x, self.jpy_long_array_type)

  def is_jpy_float_array(self, x):
    return isinstance(x, self.jpy_float_array_type)

  def is_jpy_double_array(self, x):
    return isinstance(x, self.jpy_double_array_type)