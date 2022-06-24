import jpyutil
jpyutil.preload_jvm_dll()

import unittest
import jpy


class TestSingleJpy(unittest.TestCase):
  def test_single_jpy_create_destroy_jvm(self):
    self.assertFalse(jpy.has_jvm())
    jpy.create_jvm([])
    self.assertTrue(jpy.has_jvm())
    jpy.destroy_jvm()
    self.assertFalse(jpy.has_jvm())

if __name__ == '__main__':
  unittest.main()
