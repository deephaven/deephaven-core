import unittest
import sys

import jpyutil


jpyutil.init_jvm(jvm_maxmem='512M')
import jpy


class TestString(unittest.TestCase):
    """
    Tests various Java SE classes from rt.jar
    """

    def setUp(self):
        self.String = jpy.get_type('java.lang.String')
        self.assertIsNotNone(self.String)


    def test_constructor(self):
        s = self.String('Bibo')
        self.assertEqual(type(s), self.String)
        self.assertEqual(str(s), 'Bibo')


    def test_unicode_constructor_with_py27(self):
        # This test is actually the same as test_constructor(), but 'str' is not 'unicode' in Python 2.7
        s = self.String(u'Bibo')
        self.assertEqual(type(s), self.String)
        self.assertEqual(str(s), 'Bibo')


    def test_toString(self):
        s = self.String('Bibo')
        self.assertTrue('toString' in self.String.__dict__)
        s = s.toString()
        self.assertEqual(s, 'Bibo')


    def test_substring(self):
        s = self.String('Bibo')
        self.assertTrue('substring' in self.String.__dict__)
        s2 = s.substring(0, 2)
        self.assertEqual(s2, 'Bi')
        s2 = s.substring(2)
        self.assertEqual(s2, 'bo')


    def test_split(self):
        s = self.String('/usr/local/bibo')
        self.assertTrue('split' in self.String.__dict__)
        array = s.split('/')
        self.assertEqual(len(array), 4)
        self.assertEqual(array[0], '')
        self.assertEqual(array[1], 'usr')
        self.assertEqual(array[2], 'local')
        self.assertEqual(array[3], 'bibo')
        array = s.split('/', 2)
        self.assertEqual(array[0], '')
        self.assertEqual(array[1], 'usr/local/bibo')


    def test_getBytes(self):
        s = self.String('Bibo')
        self.assertTrue('getBytes' in self.String.__dict__)
        array = s.getBytes()
        if sys.version_info >= (3, 0, 0):
            self.assertEqual(str(type(array)), "<class '[B'>")
        else:
            self.assertEqual(str(type(array)), "<type '[B'>")
        self.assertEqual(len(array), 4)
        self.assertEqual(array[0], 66)
        self.assertEqual(array[1], 105)
        self.assertEqual(array[2], 98)
        self.assertEqual(array[3], 111)


    def test_getClass(self):
        s = self.String()
        c = s.getClass()
        self.assertEqual('java.lang.String', c.getName())


import os


class TestFile(unittest.TestCase):
    def setUp(self):
        self.File = jpy.get_type('java.io.File')
        self.assertIsNotNone(self.File)


    def test_constructor(self):
        f = self.File('/usr/local/bibo')
        self.assertEqual(type(f), self.File)
        self.assertEqual(str(f).split(os.sep), ['', 'usr', 'local', 'bibo'])


    def test_getPath(self):
        f = self.File('/usr/local/bibo')
        self.assertTrue('getPath' in self.File.__dict__)
        path = f.getPath()
        self.assertEqual(path.split(os.sep), ['', 'usr', 'local', 'bibo'])


    def test_getName(self):
        f = self.File('/usr/local/bibo')
        self.assertTrue('getName' in self.File.__dict__)
        name = f.getName()
        self.assertEqual(name, 'bibo')


    def test_toPath(self):
        f = self.File('/usr/local/bibo')
        self.assertTrue('toPath' in self.File.__dict__)
        path = f.toPath()
        if sys.version_info >= (3, 0, 0):
            self.assertEqual(str(type(path)), '<class \'java.nio.file.Path\'>')
        else:
            self.assertEqual(str(type(path)), '<type \'java.nio.file.Path\'>')

        jpy.get_type('java.nio.file.Path')
        n1 = path.getName(0)
        n2 = path.getName(1)
        n3 = path.getName(2)
        self.assertEqual(str(n1), 'usr')
        self.assertEqual(str(n2), 'local')
        self.assertEqual(str(n3), 'bibo')


    def test_toString(self):
        f = self.File('bibo')
        self.assertTrue('toString' in self.File.__dict__)
        s = f.toString()
        self.assertEqual(s, 'bibo')


class TestArrayList(unittest.TestCase):
    def setUp(self):
        self.ArrayList = jpy.get_type('java.util.ArrayList')
        self.File = jpy.get_type('java.io.File')


    def test_ArrayList(self):
        f = self.File('/usr/local/bibo')

        array_list = self.ArrayList()
        array_list.add('A')
        array_list.add(12)
        array_list.add(3.4)
        array_list.add(f)

        self.assertEqual(array_list.size(), 4)
        self.assertEqual(array_list.get(0), 'A')
        self.assertEqual(array_list.get(1), 12)
        self.assertEqual(array_list.get(2), 3.4)
        self.assertEqual(array_list.get(3), f)
        self.assertEqual(type(array_list.get(3)), type(f))

        array_list = self.ArrayList(array_list)
        self.assertEqual(array_list.size(), 4)
        self.assertEqual(array_list.get(0), 'A')
        self.assertEqual(array_list.get(1), 12)
        self.assertEqual(array_list.get(2), 3.4)
        self.assertEqual(array_list.get(3), f)
        self.assertEqual(type(array_list.get(3)), type(f))

        array = array_list.toArray()
        self.assertEqual(len(array), 4)
        self.assertEqual(array[0], 'A')
        self.assertEqual(array[1], 12)
        self.assertEqual(array[2], 3.4)
        self.assertEqual(array[3], f)
        self.assertEqual(type(array[3]), type(f))


class TestHashMap(unittest.TestCase):
    def setUp(self):
        self.HashMap = jpy.get_type('java.util.HashMap')
        self.File = jpy.get_type('java.io.File')


    def test_HashMap(self):
        f = self.File('/usr/local/bibo')
        fa = jpy.array('java.io.File', 2)
        fa[0] = f
        fa[1] = f

        hash_map = self.HashMap()
        hash_map.put(0, 'A')
        hash_map.put(1, 12)
        hash_map.put(2, 3.4)
        hash_map.put(3, f)
        hash_map.put(4, fa)

        self.assertEqual(hash_map.size(), 5)
        self.assertEqual(hash_map.get(0), 'A')
        self.assertEqual(hash_map.get(1), 12)
        self.assertEqual(hash_map.get(2), 3.4)
        self.assertEqual(hash_map.get(3), f)
        self.assertEqual(type(hash_map.get(3)), type(f))
        self.assertEqual(hash_map.get(4), fa)

        hash_map = self.HashMap(hash_map)
        self.assertEqual(hash_map.size(), 5)
        self.assertEqual(hash_map.get(0), 'A')
        self.assertEqual(hash_map.get(1), 12)
        self.assertEqual(hash_map.get(2), 3.4)
        self.assertEqual(hash_map.get(3), f)
        self.assertEqual(type(hash_map.get(3)), type(f))
        self.assertEqual(hash_map.get(4), fa)


if __name__ == '__main__':
    print('\nRunning ' + __file__)
    unittest.main()
