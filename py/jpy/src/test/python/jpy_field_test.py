import unittest

import jpyutil


jpyutil.init_jvm(jvm_maxmem='512M', jvm_classpath=['target/test-classes'])
import jpy


class TestFields(unittest.TestCase):
    def setUp(self):
        self.Fixture = jpy.get_type('org.jpy.fixtures.FieldTestFixture')
        self.assertIsNotNone(self.Fixture)

        self.Thing = jpy.get_type('org.jpy.fixtures.Thing')
        self.assertIsNotNone(self.Thing)

        self.String = jpy.get_type('java.lang.String')
        self.assertIsNotNone(self.String)


    def test_static_fields(self):
        self.assertEqual(self.Fixture.z_STATIC_FIELD, True)
        self.assertEqual(self.Fixture.c_STATIC_FIELD, 65)
        self.assertEqual(self.Fixture.b_STATIC_FIELD, 123)
        self.assertEqual(self.Fixture.s_STATIC_FIELD, 12345)
        self.assertEqual(self.Fixture.i_STATIC_FIELD, 123456789)
        self.assertEqual(self.Fixture.j_STATIC_FIELD, 1234567890123456789)
        self.assertAlmostEqual(self.Fixture.f_STATIC_FIELD, 0.12345, places=5)
        self.assertAlmostEqual(self.Fixture.d_STATIC_FIELD, 0.123456789)

        self.assertEqual(self.Fixture.S_OBJ_STATIC_FIELD, 'ABC')
        self.assertEqual(self.Fixture.l_OBJ_STATIC_FIELD, self.Thing(123))


    def test_primitive_instance_fields(self):
        fixture = self.Fixture()
        self.assertEqual(fixture.zInstField, False)
        self.assertEqual(fixture.cInstField, 0)
        self.assertEqual(fixture.bInstField, 0)
        self.assertEqual(fixture.sInstField, 0)
        self.assertEqual(fixture.iInstField, 0)
        self.assertEqual(fixture.jInstField, 0)
        self.assertEqual(fixture.fInstField, 0)
        self.assertEqual(fixture.dInstField, 0)

        fixture.zInstField = True
        fixture.cInstField = 65
        fixture.bInstField = 123
        fixture.sInstField = 12345
        fixture.iInstField = 123456789
        fixture.jInstField = 1234567890123456789
        fixture.fInstField = 0.12345
        fixture.dInstField = 0.123456789

        self.assertEqual(fixture.zInstField, True)
        self.assertEqual(fixture.cInstField, 65)
        self.assertEqual(fixture.bInstField, 123)
        self.assertEqual(fixture.sInstField, 12345)
        self.assertEqual(fixture.iInstField, 123456789)
        self.assertEqual(fixture.jInstField, 1234567890123456789)
        self.assertAlmostEqual(fixture.fInstField, 0.12345, places=5)
        self.assertAlmostEqual(fixture.dInstField, 0.123456789)


    def test_object_instance_fields(self):
        fixture = self.Fixture()
        self.assertEqual(fixture.zObjInstField, None)
        self.assertEqual(fixture.cObjInstField, None)
        self.assertEqual(fixture.bObjInstField, None)
        self.assertEqual(fixture.sObjInstField, None)
        self.assertEqual(fixture.iObjInstField, None)
        self.assertEqual(fixture.jObjInstField, None)
        self.assertEqual(fixture.fObjInstField, None)
        self.assertEqual(fixture.dObjInstField, None)
        self.assertEqual(fixture.SObjInstField, None)
        self.assertEqual(fixture.lObjInstField, None)

        fixture.zObjInstField = True
        fixture.cObjInstField = 65
        fixture.bObjInstField = 123
        fixture.sObjInstField = 12345
        fixture.iObjInstField = 123456789
        fixture.jObjInstField = 1234567890123456789
        fixture.fObjInstField = 0.12345
        fixture.dObjInstField = 0.123456789
        fixture.SObjInstField = 'ABC'
        fixture.lObjInstField = self.Thing(123)

        self.assertEqual(fixture.zObjInstField, True)
        self.assertEqual(fixture.cObjInstField, 65)
        self.assertEqual(fixture.bObjInstField, 123)
        self.assertEqual(fixture.sObjInstField, 12345)
        self.assertEqual(fixture.iObjInstField, 123456789)
        self.assertEqual(fixture.jObjInstField, 1234567890123456789)
        self.assertAlmostEqual(fixture.fObjInstField, 0.12345, places=5)
        self.assertAlmostEqual(fixture.dObjInstField, 0.123456789)
        self.assertEqual(fixture.SObjInstField, 'ABC')
        self.assertEqual(fixture.lObjInstField, self.Thing(123))


if __name__ == '__main__':
    print('\nRunning ' + __file__)
    unittest.main()
