import unittest

import jpyutil


jpyutil.init_jvm(jvm_maxmem='512M', jvm_classpath=['target/test-classes'])
import jpy


class TestMethodReturnValues(unittest.TestCase):
    def setUp(self):
        self.Fixture = jpy.get_type('org.jpy.fixtures.MethodReturnValueTestFixture')
        self.assertIsNotNone(self.Fixture)
        self.Thing = jpy.get_type('org.jpy.fixtures.Thing')
        self.assertIsNotNone(self.Thing)


    def test_void(self):
        fixture = self.Fixture()
        self.assertEqual(fixture.getVoid(), None)


    def test_primitive_values(self):
        fixture = self.Fixture()
        self.assertEqual(fixture.getValue_boolean(True), True)
        self.assertEqual(fixture.getValue_byte(11), 11)
        self.assertEqual(fixture.getValue_short(12), 12)
        self.assertEqual(fixture.getValue_int(13), 13)
        self.assertEqual(fixture.getValue_long(14), 14)
        self.assertAlmostEqual(fixture.getValue_float(15.1), 15.1, places=5)
        self.assertEqual(fixture.getValue_double(16.2), 16.2)


    def test_objects(self):
        fixture = self.Fixture()
        obj = self.Thing()
        self.assertEqual(fixture.getString('Hi!'), 'Hi!')
        self.assertEqual(fixture.getObject(obj), obj)


    def test_array1d_boolean(self):
        fixture = self.Fixture()
        array = fixture.getArray1D_boolean(True, False, True)
        self.assertEqual(len(array), 3)
        self.assertEqual(array[0], True)
        self.assertEqual(array[1], False)
        self.assertEqual(array[2], True)


    def test_array1d_byte(self):
        fixture = self.Fixture()
        array = fixture.getArray1D_byte(-10, 20, 30)
        self.assertEqual(len(array), 3)
        self.assertEqual(array[0], -10)
        self.assertEqual(array[1], 20)
        self.assertEqual(array[2], 30)


    def test_array1d_short(self):
        fixture = self.Fixture()
        array = fixture.getArray1D_short(-10001, 20001, 30001)
        self.assertEqual(len(array), 3)
        self.assertEqual(array[0], -10001)
        self.assertEqual(array[1], 20001)
        self.assertEqual(array[2], 30001)


    def test_array1d_int(self):
        fixture = self.Fixture()
        array = fixture.getArray1D_int(-100001, 200001, 300001)
        self.assertEqual(len(array), 3)
        self.assertEqual(array[0], -100001)
        self.assertEqual(array[1], 200001)
        self.assertEqual(array[2], 300001)


    def test_array1d_long(self):
        fixture = self.Fixture()
        array = fixture.getArray1D_long(-10000000001, 20000000001, 30000000001)
        self.assertEqual(len(array), 3)
        self.assertEqual(array[0], -10000000001)
        self.assertEqual(array[1], 20000000001)
        self.assertEqual(array[2], 30000000001)


    def test_array1d_float(self):
        fixture = self.Fixture()
        array = fixture.getArray1D_float(-1.01, 2.01, 3.01)
        self.assertEqual(len(array), 3)
        self.assertAlmostEqual(array[0], -1.01, places=5)
        self.assertAlmostEqual(array[1], 2.01, places=5)
        self.assertAlmostEqual(array[2], 3.01, places=5)


    def test_array1d_double(self):
        fixture = self.Fixture()
        array = fixture.getArray1D_double(-1.01, 2.01, 3.01)
        self.assertEqual(len(array), 3)
        self.assertEqual(array[0], -1.01)
        self.assertEqual(array[1], 2.01)
        self.assertEqual(array[2], 3.01)


    def test_array1d_String(self):
        fixture = self.Fixture()
        array = fixture.getArray1D_String('A', 'B', 'C')
        self.assertEqual(len(array), 3)
        self.assertEqual(array[0], 'A')
        self.assertEqual(array[1], 'B')
        self.assertEqual(array[2], 'C')


    def test_array1d_Object(self):
        fixture = self.Fixture()
        array = fixture.getArray1D_Object(self.Thing(7), self.Thing(8), self.Thing(9))
        self.assertEqual(len(array), 3)
        self.assertEqual(array[0], self.Thing(7))
        self.assertEqual(array[1], self.Thing(8))
        self.assertEqual(array[2], self.Thing(9))


if __name__ == '__main__':
    print('\nRunning ' + __file__)
    unittest.main()
