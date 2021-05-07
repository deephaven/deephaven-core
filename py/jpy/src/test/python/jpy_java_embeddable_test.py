import unittest

import jpyutil

jpyutil.init_jvm(jvm_maxmem='512M', jvm_classpath=['target/test-classes', 'target/classes'])
import jpy

class TestJavaTests(unittest.TestCase):
  def testStartingAndStoppingIfAvailable(self):
    PyLibTest = jpy.get_type('org.jpy.EmbeddableTest')
    PyLibTest.testStartingAndStoppingIfAvailable()

  def testPassStatement(self):
    PyLibTest = jpy.get_type('org.jpy.EmbeddableTest')
    PyLibTest.testPassStatement()

  def testPrintStatement(self):
      PyLibTest = jpy.get_type('org.jpy.EmbeddableTest')
      PyLibTest.testPrintStatement()

  def testIncrementByOne(self):
    PyLibTest = jpy.get_type('org.jpy.EmbeddableTest')
    PyLibTest.testIncrementByOne()

if __name__ == '__main__':
  print('\nRunning ' + __file__)
  unittest.main()
