import unittest

import jpyutil


jpyutil.init_jvm(jvm_maxmem='512M')
import jpy


class TestJavaArrays(unittest.TestCase):
    def test_diag_flags_constants(self):
        self.assertIsNotNone(jpy.diag)
        self.assertIsNotNone(jpy.diag.flags)
        self.assertEqual(jpy.diag.F_OFF, 0x00)
        self.assertEqual(jpy.diag.F_TYPE, 0x01)
        self.assertEqual(jpy.diag.F_METH, 0x02)
        self.assertEqual(jpy.diag.F_EXEC, 0x04)
        self.assertEqual(jpy.diag.F_MEM, 0x08)
        self.assertEqual(jpy.diag.F_JVM, 0x10)
        self.assertEqual(jpy.diag.F_ERR, 0x20)
        self.assertEqual(jpy.diag.F_ALL, 0xff)


    def test_diag_flags_value(self):
        self.assertIsNotNone(jpy.diag)
        self.assertEqual(jpy.diag.flags, 0)
        jpy.diag.flags = 1
        self.assertEqual(jpy.diag.flags, 1)
        jpy.diag.flags = 0
        self.assertEqual(jpy.diag.flags, 0)
        jpy.diag.flags = jpy.diag.F_EXEC + jpy.diag.F_MEM
        self.assertEqual(jpy.diag.flags, 12)
        jpy.diag.flags = 0
        self.assertEqual(jpy.diag.flags, 0)
        jpy.diag.flags += jpy.diag.F_EXEC
        jpy.diag.flags += jpy.diag.F_MEM
        self.assertEqual(jpy.diag.flags, 12)


if __name__ == '__main__':
    print('\nRunning ' + __file__)
    unittest.main()
