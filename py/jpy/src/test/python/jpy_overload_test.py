# This file was modified by Deephaven Data Labs.
import unittest

import jpyutil

jpyutil.init_jvm(jvm_maxmem='512M', jvm_classpath=['target/test-classes'])
import jpy


class TestConstructorOverloads(unittest.TestCase):
    def setUp(self):
        self.Fixture = jpy.get_type('org.jpy.fixtures.ConstructorOverloadTestFixture')
        self.assertIsNotNone(self.Fixture)

    def test_FloatConstructors(self):
        fixture = self.Fixture()
        self.assertEqual(fixture.getState(), '')

        fixture = self.Fixture(12)
        self.assertEqual(fixture.getState(), 'Integer(12)')

        fixture = self.Fixture(12, 34)
        self.assertEqual(fixture.getState(), 'Integer(12),Integer(34)')

        fixture = self.Fixture(0.12)
        self.assertEqual(fixture.getState(), 'Float(0.12)')

        fixture = self.Fixture(0.12, 0.34)
        self.assertEqual(fixture.getState(), 'Float(0.12),Float(0.34)')

        fixture = self.Fixture(0.12, 34)
        self.assertEqual(fixture.getState(), 'Float(0.12),Integer(34)')

        fixture = self.Fixture(12, 0.34)
        self.assertEqual(fixture.getState(), 'Integer(12),Float(0.34)')

        with self.assertRaises(RuntimeError, msg='RuntimeError expected') as e:
            fixture = self.Fixture(12, '34')
        self.assertEqual(str(e.exception), 'no matching Java method overloads found')


class TestMethodOverloads(unittest.TestCase):
    def setUp(self):
        self.Fixture = jpy.get_type('org.jpy.fixtures.MethodOverloadTestFixture')
        self.assertIsNotNone(self.Fixture)

    def test_2ArgOverloadsWithVaryingTypes(self):
        fixture = self.Fixture()

        self.assertEqual(fixture.join(12, 32), 'Integer(12),Integer(32)')
        self.assertEqual(fixture.join(12, 3.2), 'Integer(12),Double(3.2)')
        self.assertEqual(fixture.join(12, 'abc'), 'Integer(12),String(abc)')
        self.assertEqual(fixture.join(1.2, 32), 'Double(1.2),Integer(32)')
        self.assertEqual(fixture.join(1.2, 3.2), 'Double(1.2),Double(3.2)')
        self.assertEqual(fixture.join(1.2, 'abc'), 'Double(1.2),String(abc)')
        self.assertEqual(fixture.join('efg', 32), 'String(efg),Integer(32)')
        self.assertEqual(fixture.join('efg', 3.2), 'String(efg),Double(3.2)')
        self.assertEqual(fixture.join('efg', 'abc'), 'String(efg),String(abc)')

        with self.assertRaises(RuntimeError, msg='RuntimeError expected') as e:
            fixture.join(object(), 32)
        self.assertEqual(str(e.exception), 'no matching Java method overloads found')

    def test_nArgOverloads(self):
        fixture = self.Fixture()

        self.assertEqual(fixture.join('x'), 'String(x)')
        self.assertEqual(fixture.join('x', 'y'), 'String(x),String(y)')
        self.assertEqual(fixture.join('x', 'y', 'z'), 'String(x),String(y),String(z)')

        with self.assertRaises(RuntimeError, msg='RuntimeError expected') as e:
            fixture.join('x', 'y', 'z', 'u')
        self.assertEqual(str(e.exception), 'no matching Java method overloads found')

    def test_nArgOverloadsAreFoundInBaseClass(self):
        Fixture = jpy.get_type('org.jpy.fixtures.MethodOverloadTestFixture$MethodOverloadTestFixture2')
        fixture = Fixture()

        self.assertEqual(fixture.join('x'), 'String(x)')
        self.assertEqual(fixture.join('x', 'y'), 'String(x),String(y)')
        self.assertEqual(fixture.join('x', 'y', 'z'), 'String(x),String(y),String(z)')
        self.assertEqual(fixture.join('x', 'y', 'z', 'u'), 'String(x),String(y),String(z),String(u)')

        with self.assertRaises(RuntimeError, msg='RuntimeError expected') as e:
            fixture.join('x', 'y', 'z', 'u', 'v')
        self.assertEqual(str(e.exception), 'no matching Java method overloads found')

    def test_stringAsComparable(self):
        fixture = self.Fixture()
        self.assertEqual(fixture.join2("a", 1, "c", "d"), 'String(a),Integer(1),String(c),String(d)')

    def test_stringAsNumber(self):
        fixture = self.Fixture()
        with self.assertRaises(RuntimeError, msg='RuntimeError expected') as e:
            fixture.join3('x', 2)
        self.assertEqual(str(e.exception), 'no matching Java method overloads found')

    def test_numbersAsNumber(self):
        fixture = self.Fixture()
        self.assertEqual(fixture.join3(1, 2), 'Integer(1),Integer(2)')
        self.assertEqual(fixture.join3(1.1, 2), 'Double(1.1),Integer(2)')

    def test_numbersAsComparable(self):
        fixture = self.Fixture()
        self.assertEqual(fixture.join2(1, 2, "c", "d"), 'Integer(1),Integer(2),String(c),String(d)')
        self.assertEqual(fixture.join2(1.1, 2, "c", "d"), 'Double(1.1),Integer(2),String(c),String(d)')

class TestVarArgs(unittest.TestCase):
    def setUp(self):
        self.Fixture = jpy.get_type('org.jpy.fixtures.VarArgsTestFixture')
        self.assertIsNotNone(self.Fixture)

    def test_varargsEmpty(self):
        fixture = self.Fixture()

        self.assertEqual(fixture.joinFloat("Prefix"), 'String(Prefix),float[]()')

        with self.assertRaises(RuntimeError, msg='RuntimeError expected') as e:
            res = fixture.join("Prefix")
        self.assertEqual(str(e.exception), 'ambiguous Java method call, too many matching method overloads found')

    def test_varargs(self):
        fixture = self.Fixture()

        self.assertEqual(fixture.join("Prefix", "a", "b", "c"), 'String(Prefix),String[](String(a),String(b),String(c))')
        self.assertEqual(fixture.join("Prefix", 1, 2, 3), 'String(Prefix),int[](1,2,3)')
        self.assertEqual(fixture.join("Prefix", 1.1, 2.1, 3.1), 'String(Prefix),double[](1.1,2.1,3.1)')

        self.assertEqual(fixture.joinFloat("Prefix", 1.1, 2.1, 3.1), 'String(Prefix),float[](1.1,2.1,3.1)')

        self.assertEqual(fixture.joinLong("Prefix", 1, 2, 3), 'String(Prefix),long[](1,2,3)')
        bignum = 8589934592
        self.assertEqual(fixture.joinLong("Prefix", 1, 2, 3, bignum), 'String(Prefix),long[](1,2,3,'+str(bignum)+')')

        self.assertEqual(fixture.joinByte("Prefix", 1, 2, 3), 'String(Prefix),byte[](1,2,3)')
        self.assertEqual(fixture.joinShort("Prefix", 1, 2, 3, 4), 'String(Prefix),short[](1,2,3,4)')
        self.assertEqual(fixture.joinChar("Prefix", 65, 66), 'String(Prefix),char[](A,B)')

        self.assertEqual(fixture.joinBoolean("Prefix", True, False), 'String(Prefix),boolean[](true,false)')
        self.assertEqual(fixture.joinObjects("Prefix", True, "A String", 3), 'String(Prefix),Object[](Boolean(true),String(A String),Integer(3))')

    def test_fixedArity(self):
        fixture = self.Fixture()

        self.assertEqual(fixture.chooseFixedArity(), 1)
        self.assertEqual(fixture.chooseFixedArity(1), 2)
        self.assertEqual(fixture.chooseFixedArity(1, 2), 2)

    def test_stringVsObject(self):
        fixture = self.Fixture()
        self.assertEqual(fixture.stringOrObjectVarArgs(["a", "b"]), 3)
        self.assertEqual(fixture.stringOrObjectVarArgs([1, 2, 3]), 5)



class TestOtherMethodResolutionCases(unittest.TestCase):

    # see https://github.com/bcdev/jpy/issues/55
    def test_toReproduceAndFixIssue55(self):
        Paths = jpy.get_type('java.nio.file.Paths')
        # The following statement will execute the var args method without any arguments
        p = Paths.get('testfile')
        # This is the workaround that was previously required
        p = Paths.get('testfile', [])

    # see https://github.com/bcdev/jpy/issues/56
    def test_toReproduceAndFixIssue56(self):
        Paths = jpy.get_type('java.nio.file.Paths')
        p = Paths.get('testfile', [])
        s = str(p)
        self.assertEqual(s, 'testfile')
        # The following call crashed the Python interpreter with JDK/JRE 1.8.0 < update 60.
        s = p.toString()
        self.assertEqual(s, 'testfile')

    # see https://github.com/bcdev/jpy/issues/57
    def test_toReproduceAndFixIssue57(self):
        HashMap = jpy.get_type('java.util.HashMap')
        Map = jpy.get_type('java.util.Map')
        m = HashMap()
        c = m.getClass()
        self.assertEqual(c.getName(), 'java.util.HashMap')
        m = jpy.cast(m, Map)
        # without the fix, we get "AttributeError: 'java.util.Map' object has no attribute 'getClass'"
        c = m.getClass()
        self.assertEqual(c.getName(), 'java.util.HashMap')

    # see https://github.com/bcdev/jpy/issues/54
    def test_toReproduceAndFixIssue54(self):
        String = jpy.get_type('java.lang.String')
        Arrays = jpy.get_type('java.util.Arrays')
        a = jpy.array(String, ['A', 'B', 'C'])
        # jpy.diag.flags = jpy.diag.F_METH
        s = Arrays.toString(a)
        # jpy.diag.flags = 0
        # without the fix, we get str(s) = "java.lang.String@xxxxxx"
        self.assertEqual(str(s), '[A, B, C]')

class TestDefaultMethods(unittest.TestCase):
    def setUp(self):
        self.Fixture = jpy.get_type('org.jpy.fixtures.DefaultInterfaceImplTestFixture')
        self.assertIsNotNone(self.Fixture)

    # see https://github.com/bcdev/jpy/issues/102
    def test_defaultedInterfaces(self):
        fixture = self.Fixture()
        self.assertEqual(fixture.doItPlusOne(), 3)


class TestCovariantReturn(unittest.TestCase):
    def setUp(self):
        self.Fixture = jpy.get_type('org.jpy.fixtures.CovariantOverloadExtendTestFixture')
        self.assertIsNotNone(self.Fixture)

    def test_covariantReturn(self):
        fixture = self.Fixture(1)
        self.assertEqual(fixture.foo(4, 1).getX(), 6)


if __name__ == '__main__':
    print('\nRunning ' + __file__)
    unittest.main()
