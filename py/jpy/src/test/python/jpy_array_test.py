import unittest
import sys

import jpyutil


jpyutil.init_jvm(jvm_maxmem='32M', jvm_classpath=['target/test-classes'])
import jpy


class TestJavaArrays(unittest.TestCase):
    def do_test_basic_array_protocol_with_length(self, type, initial, expected):
        a = jpy.array(type, 3)
        self.assertEqual(len(a), 3)
        self.assertEqual(a[0], initial[0])
        self.assertEqual(a[1], initial[1])
        self.assertEqual(a[2], initial[2])
        a[0] = expected[0]
        a[1] = expected[1]
        a[2] = expected[2]
        return a


    def do_test_array_with_initializer(self, type, expected):
        a = jpy.array(type, expected)
        self.assertEqual(len(a), 3)
        self.assertEqual(a[0], expected[0])
        self.assertEqual(a[1], expected[1])
        self.assertEqual(a[2], expected[2])


    def do_test_array_protocol(self, type_name, initial, expected):
        self.do_test_array_protocol2(type_name, initial, expected)
        self.do_test_array_protocol2(jpy.get_type(type_name), initial, expected)
        self.do_test_array_with_initializer(type_name, expected)
        # self.do_test_array_with_initializer(jpy.get_type(type_name), expected)


    def do_test_array_protocol2(self, type, initial, expected):
        a = self.do_test_basic_array_protocol_with_length(type, initial, expected)
        self.assertEqual(a[0], expected[0])
        self.assertEqual(a[1], expected[1])
        self.assertEqual(a[2], expected[2])


    def do_test_array_protocol_float(self, type_name, initial, expected, places):
        self.do_test_array_protocol_float2(type_name, initial, expected, places)
        self.do_test_array_protocol_float2(jpy.get_type(type_name), initial, expected, places)


    def do_test_array_protocol_float2(self, type, initial, expected, places):
        a = self.do_test_basic_array_protocol_with_length(type, initial, expected)
        self.assertAlmostEqual(a[0], expected[0], places=places)
        self.assertAlmostEqual(a[1], expected[1], places=places)
        self.assertAlmostEqual(a[2], expected[2], places=places)


    def test_array_boolean(self):
        self.do_test_array_protocol('boolean', [False, False, False], [True, False, True])


    def test_array_char(self):
        self.do_test_array_protocol('char', [0, 0, 0], [0, 100, 32767])


    def test_array_byte(self):
        self.do_test_array_protocol('byte', [0, 0, 0], [-128, 100, 127])


    def test_array_short(self):
        self.do_test_array_protocol('short', [0, 0, 0], [-32768, 100, 32767])


    def test_array_int(self):
        self.do_test_array_protocol('int', [0, 0, 0], [-2147483648, 100, 2147483647])


    def test_array_long(self):
        self.do_test_array_protocol('long', [0, 0, 0], [-9223372036854775808, 100, 9223372036854775807])


    def test_array_float(self):
        self.do_test_array_protocol_float('float', [0, 0, 0], [-1.001, 0.001, 1.001], places=5)


    def test_array_double(self):
        self.do_test_array_protocol_float('double', [0, 0, 0], [-1.001, 0.001, 1.001], places=10)


    def test_array_object(self):
        File = jpy.get_type('java.io.File')
        String = jpy.get_type('java.lang.String')
        Integer = jpy.get_type('java.lang.Integer')
        self.do_test_array_protocol('java.lang.Integer', [None, None, None], [1, None, 3])
        self.do_test_array_protocol('java.lang.String', [None, None, None], ['A', 'B', 'C'])
        self.do_test_array_protocol('java.io.File', [None, None, None], [File('A'), File('B'), File('C')])
        self.do_test_array_protocol('java.lang.Object', [None, None, None], [None, None, None])
        self.do_test_array_protocol('java.lang.Object', [None, None, None], [File('A'), 'B', 3])


    # see https://github.com/bcdev/jpy/issues/52
    def test_array_item_del(self):
        Integer = jpy.get_type('java.lang.Integer')
        a = jpy.array(Integer, 3)
        try:
            del a[1]
        except RuntimeError as err:
            self.assertEqual(err.args[0], 'cannot delete items of Java arrays')


    def do_test_basic_buffer_protocol(self, type, itemsize, values):

        a = jpy.array(type, 4)
        self.assertEqual(len(a), 4)

        a[0] = values[0]
        a[1] = values[1]
        a[2] = values[2]
        a[3] = values[3]

        m = memoryview(a)
        self.assertEqual(len(m), 4)
        self.assertEqual(m.ndim, 1)
        self.assertEqual(m.itemsize, itemsize)
        self.assertEqual(m.shape, (4,))
        self.assertEqual(m.strides, (itemsize,))
        self.assertEqual(m.readonly, True)
        if sys.version_info >= (3, 0, 0):
            # Python 2.7: AttributeError: 'memoryview' object has no attribute 'nbytes'
            self.assertEqual(m.nbytes, 4 * itemsize)
            # Python 2.7: AttributeError: 'memoryview' object has no attribute 'contiguous'
            self.assertEqual(m.contiguous, True)
            # Python 2.7: AttributeError: 'memoryview' object has no attribute 'c_contiguous'
            self.assertEqual(m.c_contiguous, True)
        return m


    def do_test_buffer_protocol(self, type_name, itemsize, values):
        self.do_test_buffer_protocol2(type_name, itemsize, values)
        self.do_test_buffer_protocol2(jpy.get_type(type_name), itemsize, values)


    def do_test_buffer_protocol2(self, type, itemsize, values):
        m = self.do_test_basic_buffer_protocol(type, itemsize, values)
        # With Python 2.7, we cannot use the returned memoryview object for further tests
        if sys.version_info >= (3, 0, 0):
            # Python 2.7: NotImplementedError: tolist() only supports byte views
            self.assertEqual(m.tolist(), values)
            # Python 2.7: AttributeError: 'memoryview' object has no attribute 'release'
            m.release()


    def do_test_buffer_protocol_float(self, type_name, itemsize, values, places):
        self.do_test_buffer_protocol_float2(type_name, itemsize, values, places)
        self.do_test_buffer_protocol_float2(jpy.get_type(type_name), itemsize, values, places)
        pass


    def do_test_buffer_protocol_float2(self, type, itemsize, values, places):
        m = self.do_test_basic_buffer_protocol(type, itemsize, values)
        # With Python 2.7, we cannot use the returned memoryview object for further tests
        if sys.version_info >= (3, 0, 0):
            # Python 2.7: TypeError: unsupported operand type(s) for -: 'float' and 'str'
            self.assertAlmostEqual(m[0], values[0], places=places)
            self.assertAlmostEqual(m[1], values[1], places=places)
            self.assertAlmostEqual(m[2], values[2], places=places)
            self.assertAlmostEqual(m[3], values[3], places=places)
            # Python 2.7: AttributeError: 'memoryview' object has no attribute 'release'
            m.release()


    def test_buffer_boolean(self):
        self.do_test_buffer_protocol('boolean', 1, [True, False, True, True])


    def test_buffer_char(self):
        self.do_test_buffer_protocol('char', 2, [65, 0, 67, 32])


    def test_buffer_byte(self):
        self.do_test_buffer_protocol('byte', 1, [65, 0, -110, -1])


    def test_buffer_short(self):
        self.do_test_buffer_protocol('short', 2, [651, 0, -1102, -1])


    def test_buffer_int(self):
        self.do_test_buffer_protocol('int', 4, [65123, 0, -110123, -1])


    def test_buffer_long(self):
        self.do_test_buffer_protocol('long', 8, [65123456789, 0, -110123456789, -1])


    def test_buffer_float(self):
        self.do_test_buffer_protocol_float('float', 4, [0.12345, 0.0, -100.123, 54.3], 5)


    def test_buffer_double(self):
        self.do_test_buffer_protocol_float('double', 8, [0.12345678, 0.0, -100.123456, 54.3], 8)

    def test_large_array_by_size_alloc(self):
        # 100 * 1MB
        for _ in range(100):
            java_array = jpy.array('byte', 1000000) # 1MB

    def test_large_array_by_sequence_alloc(self):
        sequence = list(range(250000)) # 1MB
        # 100 * 1MB
        for _ in range(100):
            java_array = jpy.array('int', sequence)

    def test_java_constructed_array_alloc(self):
        fixture = jpy.get_type('org.jpy.fixtures.JavaArrayTestFixture')
        # 100 * 1MB
        for _ in range(100):
            java_array = fixture.createByteArray(1000000) # 1MB

    def test_leak(self):
        '''
        This isn't a very good "unit"-test - the failure of this test depends
        on the amount of RAM and specifics of the OS. On my machine I've been
        able to demonstrate failure with the following constants.
        '''
        # skip the test unless you need to stress test array release logic
        if True:
            return
        j_int_array = jpy.array('int', range(100000))
        # ensure that the bufferExportCount doesn't go to 0
        keep_around = memoryview(j_int_array)
        for i in range(1000000):
            memory_view = memoryview(j_int_array)

if __name__ == '__main__':
    print('\nRunning ' + __file__)
    unittest.main()
