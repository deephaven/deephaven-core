#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

##############################################################################
# NOTE: the jvm should have been initialized, or this test will certainly fail
##############################################################################

import sys
import jpy
import numpy
import pandas
from datetime import datetime

from deephaven import TableTools, tableToDataFrame, dataFrameToTable, createTableFromData
from deephaven.conversion_utils import NULL_BYTE, NULL_SHORT, NULL_INT, NULL_LONG, \
    convertToJavaArray, convertToJavaList, convertToJavaArrayList, convertToJavaHashSet, \
    convertToJavaHashMap


if sys.version_info[0] < 3:
    int = long  # cheap python2/3 compliance - probably only necessary for 32 bit system?
    import unittest2 as unittest
    # not part of the standard library, installed via pip (or the like)
    # it provides backward compatibility with python3 style subTest context manager (handy for complex tests)
else:
    import unittest


class TestTableDataframeConversion(unittest.TestCase):
    """
    Test cases for table <-> dataframe conversions, really a functionality test
    """

    def testTableToDataframeNoNulls(self):
        """
        Test for converting a basic table with no null values to a dataframe
        """

        tab_reg = TableTools.emptyTable(1).update("boolCol=(boolean)false",
                                                  "byteCol=(byte)0",
                                                  "shortCol=(short)0",
                                                  "intCol=(int)0",
                                                  "longCol=(long)0",
                                                  "floatCol=(float)0",
                                                  "doubleCol=(double)0",
                                                  "datetimeCol=new DateTime(0)",
                                                  "stringCol=`test`")
        # there are no nulls here, so all three conversion options should work, and result in identical dataframes
        with self.subTest(msg="convert null when no null values"):
            df = tableToDataFrame(tab_reg, convertNulls='ERROR', categoricals=None)
            df_reg = tableToDataFrame(tab_reg, convertNulls='PASS', categoricals=None)
            df_reg_nc = tableToDataFrame(tab_reg, convertNulls='CONVERT', categoricals=None)

        # EQUALITY CHECK
        with self.subTest(msg='converted dfs are equal'):
            self.assertTrue(df.equals(df_reg))  # equals is transitive
            self.assertTrue(df_reg.equals(df_reg_nc))

        # DATA TYPE TEST
        for col, dtyp in [('boolCol', numpy.bool_),
                          ('byteCol', numpy.int8),
                          ('shortCol', numpy.int16),
                          ('intCol', numpy.int32),
                          ('longCol', numpy.int64),
                          ('floatCol', numpy.float32),
                          ('doubleCol', numpy.float64),
                          ('datetimeCol', numpy.dtype('datetime64[ns]')),
                          ('stringCol', numpy.object)]:
            # NB: I'm confident that dtype is not checked for df.equals(), so it's not redundant to do both
            with self.subTest(msg='dtype nulls_convert=ERROR for {}'.format(col)):
                self.assertEqual(df[col].values.dtype, dtyp)
            with self.subTest(msg='dtype nulls_convert=PASS for {}'.format(col)):
                self.assertEqual(df_reg[col].values.dtype, dtyp)
            with self.subTest(msg='dtype nulls_convert=CONVERT for {}'.format(col)):
                self.assertEqual(df_reg_nc[col].values.dtype, dtyp)  # there are no nulls -> no dumb type casts

        # VALUES TEST
        for col, val in [('boolCol', False),
                         ('byteCol', 0),
                         ('shortCol', 0),
                         ('intCol', 0),
                         ('longCol', 0),
                         ('floatCol', 0),
                         ('doubleCol', 0),
                         ('datetimeCol', numpy.datetime64(0, 'ns')),
                         ('stringCol', u'test')]:
            # NB: raw unicode string should be simultaneously python2/3 compliant
            with self.subTest(msg='entries for {}'.format(col)):
                self.assertEqual(df[col].values[0], val)

    def testTableToDataframeWithNulls(self):
        """
        Test for converting a basic table with null values to a dataframe
        """

        tab_nulls = TableTools.emptyTable(2).update("boolCol=((i==0) ? true : null)",
                                                    "byteCol=(byte)((i==0) ? 0 : NULL_BYTE)",
                                                    "shortCol=(short)((i==0) ? 2 : NULL_SHORT)",
                                                    "intCol=(int)((i==0) ? 0 : NULL_INT)",
                                                    "longCol=(long)((i==0) ? 0 : NULL_LONG)",
                                                    "floatCol=(float)((i==0) ? 2 : NULL_FLOAT)",
                                                    "doubleCol=(double)((i==0) ? 2 : NULL_DOUBLE)",
                                                    "datetimeCol=((i==0) ? new DateTime(0) : null)")
        with self.subTest(msg="Does not convert if convertNulls=ERROR and nulls present"):
            self.assertRaises(ValueError, tableToDataFrame,
                              tab_nulls, convertNulls='ERROR', categoricals=None)
        with self.subTest(msg="Converts if convertNulls in [PASS, CONVERT] and nulls present"):
            df_nulls = tableToDataFrame(tab_nulls, convertNulls='PASS', categoricals=None)
            df_nulls_nc = tableToDataFrame(tab_nulls, convertNulls='CONVERT', categoricals=None)

        # EQUALITY CHECK
        self.assertFalse(df_nulls.equals(df_nulls_nc))

        # DATA TYPES TEST
        # verify that the dtypes are as expected when we DO NOT convert the nulls
        for col, dtyp in [('boolCol', numpy.bool_),
                          ('byteCol', numpy.int8),
                          ('shortCol', numpy.int16),
                          ('intCol', numpy.int32),
                          ('longCol', numpy.int64),
                          ('floatCol', numpy.float32),
                          ('doubleCol', numpy.float64),
                          ('datetimeCol', numpy.dtype('datetime64[ns]'))
                          ]:
            with self.subTest(msg='data type, nulls_convert=False, for {}'.format(col)):
                self.assertEqual(df_nulls[col].values.dtype, dtyp)  # as before
        # verify that the dtypes are as expected when we DO convert the nulls
        for col, dtyp in [('boolCol', numpy.object),
                          ('byteCol', numpy.float32),
                          ('shortCol', numpy.float32),
                          ('intCol', numpy.float64),
                          ('longCol', numpy.float64),
                          ('floatCol', numpy.float32),
                          ('doubleCol', numpy.float64),
                          ('datetimeCol', numpy.dtype('datetime64[ns]'))
                          ]:
            with self.subTest(msg='data type, nulls_convert=True, for {}'.format(col)):
                self.assertEqual(df_nulls_nc[col].values.dtype, dtyp)

        # VALUES TEST
        # verify that the null entries are as expected when we DO NOT convert the nulls
        for col, val in [('boolCol', False),
                         ('byteCol', NULL_BYTE),
                         ('shortCol', NULL_SHORT),
                         ('intCol', NULL_INT),
                         ('longCol', NULL_LONG),
                         ]:
            with self.subTest(msg='null entry, nulls_convert=False, for {}'.format(col)):
                self.assertEqual(df_nulls[col].values[1], val)
        # floating point types & time converted to NaN/T regardless of null conversion
        with self.subTest(msg='null entry, nulls_convert=False, for floatCol'):
            self.assertTrue(numpy.isnan(df_nulls['floatCol'].values[1]))
        with self.subTest(msg='null entry, nulls_convert=False, for doubleCol'):
            self.assertTrue(numpy.isnan(df_nulls['doubleCol'].values[1]))
        with self.subTest(msg='null entry, nulls_convert=False, for datetimeCol'):
            self.assertTrue(numpy.isnat(df_nulls['datetimeCol'].values[1]))
        # verify that the null entries are as expected when we DO convert the nulls
        with self.subTest(msg='entries nulls_convert=True for bool'):
            self.assertIsNone(df_nulls_nc['boolCol'][1])
        for col in ['byteCol',
                    'shortCol',
                    'intCol',
                    'longCol',
                    'floatCol',
                    'doubleCol']:
            with self.subTest(msg='regular entry, nulls_convert=True, for {}'.format(col)):
                self.assertFalse(numpy.isnan(df_nulls_nc[col].values[0]))
            with self.subTest(msg='null entry, nulls_convert=True, for {}'.format(col)):
                self.assertTrue(numpy.isnan(df_nulls_nc[col].values[1]))
        with self.subTest(msg='regular entry, nulls_convert=True, for datetimeCol'):
            self.assertEqual(df_nulls_nc['datetimeCol'].values[0], numpy.datetime64(0, 'ns'))
        with self.subTest(msg='null entry, nulls_convert=False, for {}'.format(col)):
            self.assertTrue(numpy.isnat(df_nulls['datetimeCol'].values[1]))

    def testDataframeToTable(self):
        """
        Test for converting dataframe to a table
        """

        getElement = lambda tab, col: tab.getColumnSource(col).get(0)

        df = pandas.DataFrame({'boolCol': numpy.zeros((1, ), dtype=numpy.bool_),
                               'byteCol': numpy.zeros((1,), dtype=numpy.int8),
                               'shortCol': numpy.zeros((1,), dtype=numpy.int16),
                               'intCol': numpy.zeros((1,), dtype=numpy.int32),
                               'longCol': numpy.zeros((1,), dtype=numpy.int64),
                               'floatCol': numpy.zeros((1,), dtype=numpy.float32),
                               'doubleCol': numpy.zeros((1,), dtype=numpy.float64),
                               'datetimeCol': numpy.zeros((1, ), dtype='datetime64[ns]'),
                               'stringCol': numpy.array([u'test', ], dtype=numpy.unicode_)
                               })

        # NB: use a raw unicode string should be simultaneously python2/3 compliant
        tab = dataFrameToTable(df)
        tabDef = tab.getDefinition()  # get the meta-data for the table
        # check that the datatypes make sense
        for col, typ in [('boolCol', 'class java.lang.Boolean'),
                         ('byteCol', 'byte'),
                         ('shortCol', 'short'),
                         ('intCol', 'int'),
                         ('longCol', 'long'),
                         ('floatCol', 'float'),
                         ('doubleCol', 'double'),
                         ('datetimeCol', 'class io.deephaven.time.DateTime'),
                         ('stringCol', 'class java.lang.String')
                         ]:
            with self.subTest(msg="data type for column {}".format(col)):
                self.assertEqual(typ, tabDef.getColumn(col).getDataType().toString())

        # Checking equality of the entry on the java side, to the best of my ability...
        with self.subTest(msg="entry for column boolCol"):
            self.assertEqual(getElement(tab, 'boolCol'), False)  # I'm guessing that Boolean() -> False
        with self.subTest(msg="entry for column byteCol"):
            self.assertEqual(getElement(tab, 'byteCol'), 0)  # I'm guessing that Byte() -> 0
        with self.subTest(msg="entry for column shortCol"):
            self.assertEqual(getElement(tab, 'shortCol'), 0)  # I'm guessing that Short() -> 0
        with self.subTest(msg="entry for column intCol"):
            self.assertEqual(getElement(tab, 'intCol'), 0)  # I'm guessing that Integer() -> 0
        with self.subTest(msg="entry for column longCol"):
            self.assertEqual(getElement(tab, 'longCol'), 0)  # I'm guessing that Long() -> 0
        with self.subTest(msg="entry for column floatCol"):
            self.assertEqual(getElement(tab, 'floatCol'), 0)  # I'm guessing that Float() -> 0
        with self.subTest(msg="entry for column doubleCol"):
            self.assertEqual(getElement(tab, 'doubleCol'), 0)  # I'm guessing that Double() -> 0
        with self.subTest(msg="entry for column datetimeCol"):
            cls = jpy.get_type('io.deephaven.time.DateTime')
            self.assertEqual(getElement(tab, 'datetimeCol'), cls(0))
        with self.subTest(msg="entry for column stringCol"):
            self.assertEqual(getElement(tab, 'stringCol'), u'test')

    def testUnsupportedPrimitiveTest(self):
        """
        Test for behavior of unsupported column type conversion
        """

        for dtypename in ['uint8', 'uint16', 'uint32', 'uint64', 'complex64', 'complex128', 'float16']:
            with self.subTest(msg="dtype={}".format(dtypename)):
                df = pandas.DataFrame({'test': numpy.zeros((1, ), dtype=dtypename)})
                self.assertRaises(ValueError, dataFrameToTable, df)

    def testArrayColumnConversion(self):
        """
        Test for behavior when one of the columns is of array type (in each direction)
        """

        firstTable = TableTools.emptyTable(10).update("MyString=new String(`a`+i)",
                                                      "MyChar=new Character((char) ((i%26)+97))",
                                                      "MyBoolean=new Boolean(i%2==0)",
                                                      "MyByte=new java.lang.Byte(Integer.toString(i%127))",
                                                      "MyShort=new Short(Integer.toString(i%32767))",
                                                      "MyInt=new Integer(i)",
                                                      "MyLong=new Long(i)",
                                                      "MyFloat=new Float(i+i/10)",
                                                      "MyDouble=new Double(i+i/10)"
                                                      )
        arrayTable = firstTable.update("A=i%3").groupBy("A")
        dataFrame = tableToDataFrame(arrayTable, convertNulls='PASS', categoricals=None)

        for colName, arrayType in [
            ('MyString', 'io.deephaven.vector.ObjectVector'),
            ('MyChar', 'io.deephaven.vector.CharVector'),
            ('MyBoolean', 'io.deephaven.vector.ObjectVector'),  # NB: BooleanVector is deprecated
            ('MyByte', 'io.deephaven.vector.ByteVector'),
            ('MyShort', 'io.deephaven.vector.ShortVector'),
            ('MyInt', 'io.deephaven.vector.IntVector'),
            ('MyLong', 'io.deephaven.vector.LongVector'),
            ('MyFloat', 'io.deephaven.vector.FloatVector'),
            ('MyDouble', 'io.deephaven.vector.DoubleVector'),
        ]:
            with self.subTest(msg="type for original column {}".format(colName)):
                self.assertEqual(arrayTable.getColumn(colName).getType().getName(), arrayType)
                self.assertEqual(dataFrame[colName].values.dtype, numpy.object)

        for colName, dtype in [
            ('MyBoolean', numpy.bool_),
            ('MyByte', numpy.int8),
            ('MyShort', numpy.int16),
            ('MyInt', numpy.int32),
            ('MyLong', numpy.int64),
            ('MyFloat', numpy.float32),
            ('MyDouble', numpy.float64),
        ]:
            with self.subTest(msg="type of converted array for {}".format(colName)):
                self.assertTrue(isinstance(dataFrame[colName].values[0], numpy.ndarray))
                self.assertEqual(dataFrame[colName].values[0].dtype, dtype)

        with self.subTest(msg="type of converted array for MyString"):
            self.assertTrue(isinstance(dataFrame['MyString'].values[0], numpy.ndarray))
            self.assertTrue(dataFrame['MyString'].values[0].dtype.name.startswith('unicode') or
                            dataFrame['MyString'].values[0].dtype.name.startswith('str'))

        # NB: numpy really doesn't have a char type, so it gets treated like an uninterpretted type
        with self.subTest(msg="type of converted array for MyChar"):
            self.assertTrue(isinstance(dataFrame['MyChar'].values[0], numpy.ndarray))
            self.assertTrue(dataFrame['MyChar'].values[0].dtype.name.startswith('unicode') or
                            dataFrame['MyChar'].values[0].dtype.name.startswith('str'))

        # convert back
        backTable = dataFrameToTable(dataFrame, convertUnknownToString=True)
        for colName, arrayType in [
            ('MyString', 'io.deephaven.vector.ObjectVectorDirect'),
            ('MyChar', 'io.deephaven.vector.CharVectorDirect'),
            ('MyBoolean', 'io.deephaven.vector.ObjectVectorDirect'),
            ('MyByte', 'io.deephaven.vector.ByteVectorDirect'),
            ('MyShort', 'io.deephaven.vector.ShortVectorDirect'),
            ('MyInt', 'io.deephaven.vector.IntVectorDirect'),
            ('MyLong', 'io.deephaven.vector.LongVectorDirect'),
            ('MyFloat', 'io.deephaven.vector.FloatVectorDirect'),
            ('MyDouble', 'io.deephaven.vector.DoubleVectorDirect'),
        ]:
            with self.subTest(msg="type for reverted column for {}".format(colName)):
                self.assertEqual(backTable.getColumn(colName).getType().getName(), arrayType)
        with self.subTest(msg="element type for reverted column MyBoolean"):
            self.assertEqual(backTable.getColumn('MyBoolean').get(0).getComponentType().getName(), 'java.lang.Boolean')
        with self.subTest(msg="element type for reverted column MyString"):
            self.assertEqual(backTable.getColumn('MyString').get(0).getComponentType().getName(), 'java.lang.String')

    def testListColumnVersion(self):
        """
        Test for behavior when one of the data frame columns contains tuples or lists
        """

        def1 = {('a', 'b'): {('A', 'B'): 1, ('A', 'C'): 2},
                ('a', 'a'): {('A', 'C'): 3, ('A', 'B'): 4},
                ('a', 'c'): {('A', 'B'): 5, ('A', 'C'): 6},
                ('b', 'a'): {('A', 'C'): 7, ('A', 'B'): 8},
                ('b', 'b'): {('A', 'D'): 9, ('A', 'B'): 10}}
        dataframe1 = pandas.DataFrame(def1)
        table1 = dataFrameToTable(dataframe1)
        print("dataframe1 = \n{}".format(dataframe1))
        print("table1 = {}\n".format(TableTools.html(table1)))

        def2 = {'one': [(1, 2), (2, 3), (3, ), (4, 5, 6, 7)],
                'two': [(4, 5), (6, 5, 3), (7, 6), (8, 7)],
                'thing': [None, None, None, None]}
        dataframe2 = pandas.DataFrame(def2)

        table2 = dataFrameToTable(dataframe2, convertUnknownToString=True)
        print("dataframe2 = \n{}".format(dataframe2))
        print("table2 = {}\n".format(TableTools.html(table2)))

        def3 = {'one': [[1, 2], [2, 3], [3, 4], [4, 5, 6, 7]],
                'two': [[4, 5], [6, 5], [7, 6], [8, 7]],
                'thing': [None, None, None, None]}
        dataframe3 = pandas.DataFrame(def3)

        table3 = dataFrameToTable(dataframe3, convertUnknownToString=True)
        print("dataframe3 = \n{}".format(dataframe3))
        print("table3 = {}\n".format(TableTools.html(table3)))

    def testMultidimensionalArray(self):
        """
        Test suite for behavior of converting a dataframe with multi-dimensional column
        """

        for dtypename, array_type in [('int8', '[[B'), ('int16', '[[S'), ('int32', '[[I'), ('int64', '[[J'),
                                      ('float32', '[[F'), ('float64', '[[D'), ('U1', '[[C'),
                                      ('U3', '[[Ljava.lang.String;'),
                                      ('datetime64[ns]', '[[Lio.deephaven.time.DateTime;')]:
            with self.subTest(msg="dtype={}".format(dtypename)):
                nparray = numpy.empty((2, ), dtype=numpy.object)
                nparray[:] = [numpy.zeros((3, 4), dtype=dtypename) for i in range(2)]
                df = pandas.DataFrame({'test': nparray})
                tab = dataFrameToTable(df)
                self.assertTrue(tab.getColumn('test').getType().getName(), 'io.deephaven.vector.ObjectVectorDirect')
                self.assertEqual(tab.getColumn('test').get(0).getClass().getName(), array_type)

        with self.subTest(msg="nested array exception check"):
            nparray = numpy.empty((2, ), dtype=numpy.object)
            arr1 = numpy.empty((3, 4), dtype=numpy.object)
            arr1[:] = 'junk'
            nparray[:] = [arr1 for i in range(2)]
            df = pandas.DataFrame({'test': nparray})
            self.assertRaises(ValueError, dataFrameToTable, df, convertUnknownToString=False)

    def testConversionUtility(self):
        """
        Test suite for convertToJava* methods in conversion_utils module
        """

        # mostly I'm just going to create a coverage test for a couple simple basic cases
        with self.subTest(msg="convertToJavaArray for string"):
            junk = convertToJavaArray("abc")
        with self.subTest(msg="convertToJavaList for string"):
            junk = convertToJavaList("abc")
        with self.subTest(msg="convertToJavaArrayList for string"):
            junk = convertToJavaArrayList("abc")
        with self.subTest(msg="convertToJavaHashSet for string"):
            junk = convertToJavaHashSet("abc")

        with self.subTest(msg="convertToJavaArray for int list"):
            junk = convertToJavaArray([0, 1, 2])
        with self.subTest(msg="convertToJavaList for int list"):
            junk = convertToJavaList([0, 1, 2])
        with self.subTest(msg="convertToJavaArrayList for int list"):
            junk = convertToJavaArrayList([0, 1, 2])
        with self.subTest(msg="convertToJavaHashSet for int list"):
            junk = convertToJavaHashSet([0, 1, 2])

        with self.subTest(msg="convertToJavaHashMap for dict"):
            junk = convertToJavaHashMap({'one': 1, 'two': 2})
        with self.subTest(msg="convertToJavaHashMap for two lists"):
            junk = convertToJavaHashMap(['one', 'two'], [1, 2])

    def testCreateTableFromData(self):
        """
        Test suite for createTbaleFRomData method
        """

        data_names = ['intList', 'floatList', 'charList', 'stringList', 'booleanList', 'timeList']
        data_list = [[1, 2, None], [1., 2., None], ['A', 'B', None], [u'one', u'two', None],
                     [True, False, None], [datetime.utcnow(), datetime.utcnow(), datetime.utcnow()]]

        with self.subTest(msg="createTableFromData with lists"):
            tab = createTableFromData(data_list, columns=data_names)
            print("tableFromList = {}\n".format(TableTools.html(tab)))

        data_dict = {}
        for nm, da in zip(data_names, data_list):
            data_dict[nm] = da
        with self.subTest(msg="createTableFromData with dict"):
            tab = createTableFromData(data_dict, columns=data_names)
            print("tableFromDict = {}\n".format(TableTools.html(tab)))
