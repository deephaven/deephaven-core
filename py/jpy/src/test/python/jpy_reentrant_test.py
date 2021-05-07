import unittest

import jpyutil

jpyutil.init_jvm(jvm_maxmem='512M', jvm_classpath=['target/classes'])
import jpy

class TestReentrant(unittest.TestCase):
  def test_reentrant_function(self):
    PyObject = jpy.get_type('org.jpy.PyObject')
    PyInputMode = jpy.get_type('org.jpy.PyInputMode')
    PyModule = jpy.get_type('org.jpy.PyModule')

    PyObject.executeCode('def incByOne(x): return x + 1', PyInputMode.SCRIPT)

    mainModule = PyModule.getMain()
    res = mainModule.incByOne(41)
    self.assertEqual(res, 42)

  def test_reentrant_statement(self):
    PyObject = jpy.get_type('org.jpy.PyObject')
    PyInputMode = jpy.get_type('org.jpy.PyInputMode')
    PyObject.executeCode('print("hi from test_reentrant_statement")', PyInputMode.STATEMENT)

if __name__ == '__main__':
  print('\nRunning ' + __file__)
  unittest.main()
