import unittest

import jpyutil


jpyutil.init_jvm(jvm_maxmem='32M', jvm_classpath=['target/test-classes'])
import jpy


class TestJavaArrays(unittest.TestCase):
    def setUp(self):
        self.Fixture = jpy.get_type('org.jpy.fixtures.ConstructionTestFixture')
        self.assertIsNotNone(self.Fixture)

    def test_large_obj_by_constructor_alloc(self):
        # 100 * 1MB
        for _ in range(100):
            fixture = self.Fixture(1000000) # 1MB

    def test_large_obj_by_static_alloc(self):
        # 100 * 1MB
        for _ in range(100):
            fixture = self.Fixture.viaStatic(1000000) # 1MB

if __name__ == '__main__':
    print('\nRunning ' + __file__)
    unittest.main()
