import jpyutil
import unittest


class TestMultipleJpy(unittest.TestCase):
  # when debugging failures, enable these - but don't want to spam the test log

  #def setUp(self):
  #  jpy.diag.flags = jpy.diag.F_ALL

  #def tearDown(self):
  #  jpy.diag.flags = jpy.diag.F_OFF

  # THIS TEST CURRENTLY FAILS!
  @unittest.skip("IDS-4102")
  def test_multiple_jpy_create_destroy_jvm(self):
    jpyutil.preload_jvm_dll()
    import jpy
    self.assertFalse(jpy.has_jvm())
    jpy.create_jvm([])
    self.assertTrue(jpy.has_jvm())
    jpy.destroy_jvm()
    self.assertFalse(jpy.has_jvm())
    jpy.create_jvm([])
    self.assertTrue(jpy.has_jvm())
    jpy.destroy_jvm()
    self.assertFalse(jpy.has_jvm())


if __name__ == '__main__':
  unittest.main()
