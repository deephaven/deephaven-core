import unittest
import array
import sys

import jpyutil


jpyutil.init_jvm(jvm_maxmem='512M', jvm_classpath=['target/test-classes'])
import jpy

try:
    import numpy as np
except:
    np = None


def annotate_fixture_methods(type, method):
    # print('annotate_fixture_methods: type =', type, ', method =', method.name)
    if method.name == 'modifyThing':
        method.set_param_mutable(0, True)
    elif method.name == 'returnThing':
        method.set_param_return(0, True)
    elif method.name == 'modifyAndReturnThing':
        method.set_param_mutable(0, True)
        method.set_param_return(0, True)
    elif method.name == 'modifyIntArray':
        method.set_param_mutable(0, True)
    elif method.name == 'returnIntArray':
        method.set_param_return(0, False)
        method.set_param_return(0, True)
    elif method.name == 'modifyAndReturnIntArray':
        method.set_param_mutable(0, True)
        method.set_param_return(0, True)
    elif method.name == 'modifyAndOutputIntArray':
        method.set_param_mutable(0, True)
        method.set_param_output(0, True)
    return True


jpy.type_callbacks['org.jpy.fixtures.ModifyAndReturnParametersTestFixture'] = annotate_fixture_methods


class TestMutableAndReturnParameters(unittest.TestCase):
    def setUp(self):
        self.Fixture = jpy.get_type('org.jpy.fixtures.ModifyAndReturnParametersTestFixture')
        self.assertIsNotNone(self.Fixture)
        self.Thing = jpy.get_type('org.jpy.fixtures.Thing')
        self.assertIsNotNone(self.Thing)


    def test_modifyThing(self):
        fixture = self.Fixture()
        t = self.Thing()
        self.assertEqual(t.getValue(), 0)
        fixture.modifyThing(t, 11)
        self.assertEqual(t.getValue(), 11)


    def test_returnThing(self):
        fixture = self.Fixture()

        t1 = self.Thing(21)
        t2 = fixture.returnThing(t1)
        self.assertEqual(t1.getValue(), 21)
        self.assertEqual(t2.getValue(), 21)
        self.assertEqual(t1, t2)
        self.assertTrue(t1 is t2)

        t1 = None
        t2 = fixture.returnThing(t1)
        self.assertIsNotNone(t2)
        self.assertEqual(t2.getValue(), 0)


    def test_modifyAndReturnThing(self):
        fixture = self.Fixture()

        t1 = self.Thing()
        t2 = fixture.modifyAndReturnThing(t1, 31)
        self.assertEqual(t1.getValue(), 31)
        self.assertEqual(t2.getValue(), 31)
        self.assertEqual(t1, t2)
        self.assertTrue(t1 is t2)

        t1 = None
        t2 = fixture.modifyAndReturnThing(t1, 32)
        self.assertIsNotNone(t2)
        self.assertEqual(t2.getValue(), 32)


    def test_modifyIntArray(self):
        fixture = self.Fixture()

        # See https://docs.python.org/2/c-api/buffer.html
        # "An array can only expose its contents via the old-style buffer interface.
        #    This limitation does not apply to Python 3, where memoryview objects can be
        #    constructed from arrays, too."
        # >>> import array
        # >>> a = array.array('i', [1,2,3])
        # >>> m = memoryview(a)
        # TypeError: cannot make memory view because object does not have the buffer interface
        #
        if sys.version_info >= (3, 0, 0):
            a = array.array('i', [0, 0, 0])
            fixture.modifyIntArray(a, 12, 13, 14)
            self.assertEqual(a[0], 12)
            self.assertEqual(a[1], 13)
            self.assertEqual(a[2], 14)

        if np:
            a = np.array([0, 0, 0], dtype='int32')
            fixture.modifyIntArray(a, 10, 11, 12)
            self.assertEqual(a[0], 10)
            self.assertEqual(a[1], 11)
            self.assertEqual(a[2], 12)

        a = jpy.array('int', 3)
        fixture.modifyIntArray(a, 12, 13, 14)
        self.assertEqual(a[0], 12)
        self.assertEqual(a[1], 13)
        self.assertEqual(a[2], 14)

        a = [0, 0, 0]
        fixture.modifyIntArray(a, 12, 13, 14)
        self.assertEqual(a[0], 0)
        self.assertEqual(a[1], 0)
        self.assertEqual(a[2], 0)

        with self.assertRaises(RuntimeError, msg='RuntimeError expected') as e:
            a = None
            fixture.modifyIntArray(a, 14, 15, 16)
        self.assertEqual(str(e.exception), 'java.lang.NullPointerException')


    def test_returnIntArray(self):
        fixture = self.Fixture()

        # See https://docs.python.org/2/c-api/buffer.html
        # "An array can only expose its contents via the old-style buffer interface.
        #    This limitation does not apply to Python 3, where memoryview objects can be
        #    constructed from arrays, too."
        # >>> import array
        # >>> a = array.array('i', [1,2,3])
        # >>> m = memoryview(a)
        # TypeError: cannot make memory view because object does not have the buffer interface
        #
        if sys.version_info >= (3, 0, 0):
            a1 = array.array('i', [0, 0, 0])
            a2 = fixture.returnIntArray(a1)
            # AssertionError: array('i', [0, 0, 0]) is not [I(objectRef=0x0778C364)
            self.assertIs(a1, a2)

        if np:
            a1 = np.array([0, 0, 0], dtype='int32')
            a2 = fixture.returnIntArray(a1)
            self.assertIs(a1, a2)

        a1 = jpy.array('int', 3)
        a2 = fixture.returnIntArray(a1)
        self.assertIs(a1, a2)

        a1 = None
        a2 = fixture.returnIntArray(a1)
        self.assertEqual(type(a2), jpy.get_type('[I'))

        a1 = [0, 0, 0]
        a2 = fixture.returnIntArray(a1)
        self.assertEqual(type(a2), jpy.get_type('[I'))


    def test_modifyAndReturnIntArray(self):
        fixture = self.Fixture()

        # See https://docs.python.org/2/c-api/buffer.html
        # "An array can only expose its contents via the old-style buffer interface.
        #    This limitation does not apply to Python 3, where memoryview objects can be
        #    constructed from arrays, too."
        # >>> import array
        # >>> a = array.array('i', [1,2,3])
        # >>> m = memoryview(a)
        # TypeError: cannot make memory view because object does not have the buffer interface
        #
        if sys.version_info >= (3, 0, 0):
            a1 = array.array('i', [0, 0, 0])
            # Python 2.7: TypeError: must be impossible<bad format char>, not bool
            a2 = fixture.modifyAndReturnIntArray(a1, 16, 17, 18)
            self.assertIs(a1, a2)
            self.assertEqual(a2[0], 16)
            self.assertEqual(a2[1], 17)
            self.assertEqual(a2[2], 18)

        if np:
            a1 = np.array([0, 0, 0], dtype='int32')
            a2 = fixture.modifyAndReturnIntArray(a1, 10, 11, 12)
            self.assertIs(a1, a2)
            self.assertEqual(a2[0], 10)
            self.assertEqual(a2[1], 11)
            self.assertEqual(a2[2], 12)

        a1 = jpy.array('int', 3)
        a2 = fixture.modifyAndReturnIntArray(a1, 16, 17, 18)
        self.assertIs(a1, a2)
        self.assertEqual(a2[0], 16)
        self.assertEqual(a2[1], 17)
        self.assertEqual(a2[2], 18)

        a1 = None
        a2 = fixture.modifyAndReturnIntArray(a1, 16, 17, 18)
        self.assertEqual(type(a2), jpy.get_type('[I'))
        self.assertEqual(a2[0], 16)
        self.assertEqual(a2[1], 17)
        self.assertEqual(a2[2], 18)

        a1 = [0, 0, 0]
        a2 = fixture.modifyAndReturnIntArray(a1, 16, 17, 18)
        self.assertEqual(type(a2), jpy.get_type('[I'))
        self.assertEqual(a2[0], 16)
        self.assertEqual(a2[1], 17)
        self.assertEqual(a2[2], 18)


    # See https://github.com/bcdev/jpy/issues/36
    #
    def test_modifyAndOutputIntArray(self):
        fixture = self.Fixture()

        # See https://docs.python.org/2/c-api/buffer.html
        # "An array can only expose its contents via the old-style buffer interface.
        #    This limitation does not apply to Python 3, where memoryview objects can be
        #    constructed from arrays, too."
        # >>> import array
        # >>> a = array.array('i', [1,2,3])
        # >>> m = memoryview(a)
        # TypeError: cannot make memory view because object does not have the buffer interface
        #
        if sys.version_info >= (3, 0, 0):
            a = array.array('i', [0, 0, 0])
            fixture.modifyAndOutputIntArray(a, 16, 17, 18)
            self.assertEqual(a[0], 16)
            self.assertEqual(a[1], 17)
            self.assertEqual(a[2], 18)

        if np:
            a = np.array([0, 0, 0], dtype='int32')
            fixture.modifyIntArray(a, 10, 11, 12)
            self.assertEqual(a[0], 10)
            self.assertEqual(a[1], 11)
            self.assertEqual(a[2], 12)

        a = jpy.array('int', 3)
        fixture.modifyAndOutputIntArray(a, 15, 16, 17)
        self.assertEqual(a[0], 15)
        self.assertEqual(a[1], 16)
        self.assertEqual(a[2], 17)

        a = None
        fixture.modifyAndOutputIntArray(a, 16, 17, 18)

        a = [0, 0, 0]
        fixture.modifyAndOutputIntArray(a, 16, 17, 18)
        self.assertEqual(a[0], 0)
        self.assertEqual(a[1], 0)
        self.assertEqual(a[2], 0)


if __name__ == '__main__':
    print('\nRunning ' + __file__)
    unittest.main()
