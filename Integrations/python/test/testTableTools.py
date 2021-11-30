#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

##############################################################################
# NOTE: the jvm should have been initialized, or this test will certainly fail
##############################################################################

import sys
import jpy
import numpy
from datetime import datetime, date

from deephaven import TableTools

if sys.version_info[0] < 3:
    import unittest2 as unittest
    # not part of the standard library, installed via pip (or the like)
    # it provides backward compatibility with python3 style subTest context manager (handy for complex tests)
else:
    import unittest


class TestTableTools(unittest.TestCase):
    """
    Test cases for the deephaven.TableTools module (performed locally) -
    """

    @classmethod
    def setUpClass(self):
        self.nByteArray = numpy.array([1, 2, 3], dtype=numpy.int8)
        self.nShortArray = numpy.array([1, 2, 3], dtype=numpy.int16)
        self.nIntArray = numpy.array([1, 2, 3], dtype=numpy.int32)
        self.nLongArray = numpy.array([1, 2, 3], dtype=numpy.int64)
        self.nFloatArray = numpy.array([1, 2, 3], dtype=numpy.float32)
        self.nDoubleArray = numpy.array([1, 2, 3], dtype=numpy.float64)
        self.nCharArray = numpy.array(['A', 'B', 'C'])
        self.nStringArray = numpy.array([u'one', u'two', u'three'])
        self.nBooleanArray = numpy.array([True, False, True], dtype=numpy.bool)
        self.nTimeArray = numpy.array([1, 2, 3], dtype='datetime64[s]')
        self.intList = [1, 2, None]
        self.floatList = [1., 2., None]
        self.charList = ['A', 'B', None]
        self.stringList = [u'one', u'two', None]
        self.booleanList = [True, False, None]
        self.timeList = [datetime.utcnow(), datetime.utcnow(), datetime.utcnow()]

    def testTableBasics(self):
        """
        Test cases for table creation, and a few other basic table methods:
            diff(), html(), show(), showCommaDelimited(), showWithRowSet(), string(),
            roundDecimalColumns(), roundDecimalColumnsExcept(), merge(), mergeSorted()
        """

        tab, tab2, tab3, tab4, tab5, tab6 = None, None, None, None, None, None

        with self.subTest(msg="emptyTable(long)"):
            tab = TableTools.emptyTable(3)
            # set some cols which aren't dumb
            tab = tab.update("intCol=(int)i", "fltCol=(float)i*0.5", "dblCol=(double)i*0.3")

        with self.subTest(msg="newTable(TableDefinition)"):
            # assuming the first test passed...
            tab3 = TableTools.newTable(tab.getDefinition())

        # Essentially table to string methods
        with self.subTest(msg="html test"):
            print("html rendering = \n{}".format(TableTools.html(tab)))

        with self.subTest(msg="show(Table, *cols)"):
            print("show =")
            TableTools.show(tab, "intCol", "dblCol")
        with self.subTest(msg="show(Table, 2, *cols)"):
            print("show & row limit =")
            TableTools.show(tab, 2, "intCol", "dblCol")

        with self.subTest(msg="showCommaDelimited(Table, *cols)"):
            print("showCommaDelimited =")
            TableTools.showCommaDelimited(tab, "intCol", "dblCol")
        with self.subTest(msg="showCommaDelimited(Table, 2, *cols)"):
            print("showCommaDelimited & row limit =")
            TableTools.showCommaDelimited(tab, 2, "intCol", "dblCol")

        with self.subTest(msg="showWithRowSet(Table, *cols)"):
            print("showWithRowSet =")
            TableTools.showWithRowSet(tab, "intCol", "dblCol")
        with self.subTest(msg="showWithRowSet(Table, 2, *cols)"):
            print("showWithRowSet & row limit =")
            TableTools.showWithRowSet(tab, 2, "intCol", "dblCol")

        with self.subTest(msg="string(Table, *cols)"):
            print("string =\n {}".format(TableTools.string(tab, "intCol", "dblCol")))
        with self.subTest(msg="string(Table, 2, *cols)"):
            print("string & row limit =\n {}".format(TableTools.string(tab, 2, "intCol", "dblCol")))

        with self.subTest(msg="roundDecimalColumns"):
            tab4 = TableTools.roundDecimalColumns(tab)
        with self.subTest(msg="roundDecimalColumns(*cols)"):
            tab5 = TableTools.roundDecimalColumns(tab, "fltCol", "dblCol")
        with self.subTest(msg="roundDecimalColumnsExcept(*cols)"):
            tab6 = TableTools.roundDecimalColumns(tab, "fltCol")

        with self.subTest(msg="diff test of a table with itself"):
            print("diff output of table with itself = \n{}".format(TableTools.diff(tab, tab, 3)))
        with self.subTest(msg="diff test of a table with rounded version of itself"):
            print("diff output of table with rounded version of itself = \n{}".format(TableTools.diff(tab, tab4, 3)))

        with self.subTest(msg="merge(*tables)"):
            tab4 = TableTools.merge(tab, tab)
        with self.subTest(msg="merge([tables])"):
            tab4 = TableTools.merge([tab, tab])
        with self.subTest(msg="mergeSorted(col, [tables])"):
            tab4 = TableTools.mergeSorted("intCol", [tab, tab])
        with self.subTest(msg="merge(col, *tables)"):
            tab4 = TableTools.mergeSorted("intCol", tab, tab)

        del tab, tab2, tab3, tab4, tab5, tab6

    def testColumnHolder(self):
        """
        Test cases for <primitive>Col() methods & associated newTable() method
        """

        holders = []
        junk, tab = None, None
        with self.subTest(msg="byteCol"):
            holders.append(TableTools.byteCol("byteCol", self.nByteArray))
        with self.subTest(msg="shortCol"):
            holders.append(TableTools.shortCol("shortCol", self.nShortArray))
        with self.subTest(msg="intCol"):
            holders.append(TableTools.intCol("intCol", self.nIntArray))
        with self.subTest(msg="longCol"):
            holders.append(TableTools.longCol("longCol", self.nLongArray))
        with self.subTest(msg="floatCol"):
            holders.append(TableTools.floatCol("floatCol", self.floatList))
        with self.subTest(msg="doubleCol"):
            holders.append(TableTools.doubleCol("doubleCol", self.nDoubleArray))
        with self.subTest(msg="charCol"):
            holders.append(TableTools.charCol("charCol", self.nCharArray))

        with self.subTest(msg="newTable with column holders"):
            self.assertGreater(len(holders), 0)  # make sure that we even do something useful
            tab = TableTools.newTable(*holders)
            print("tab =\n{}".format(TableTools.html(tab)))
        del holders, tab


        holders = []
        with self.subTest(msg="byteCol with list"):
            holders.append(TableTools.byteCol("byteList", self.intList))
        with self.subTest(msg="shortCol with list"):
            holders.append(TableTools.shortCol("shortList", self.intList))
        with self.subTest(msg="intCol with list"):
            holders.append(TableTools.intCol("intList", self.intList))
        with self.subTest(msg="longCol with list"):
            holders.append(TableTools.longCol("longList", self.intList))
        with self.subTest(msg="floatCol with list"):
            holders.append(TableTools.doubleCol("floatList", self.floatList))
        with self.subTest(msg="doubleCol with list"):
            holders.append(TableTools.doubleCol("doubleList", self.floatList))
        with self.subTest(msg="charCol with list"):
            holders.append(TableTools.charCol("charList", self.charList))
        print('prim col from list = \n{}'.format(TableTools.html(TableTools.newTable(*holders))))
        del holders


        holders = []
        with self.subTest(msg="col with string array"):
            holders.append(TableTools.col("stringCol", self.nStringArray))
        with self.subTest(msg="col with boolean array"):
            holders.append(TableTools.col("booleanCol", self.nBooleanArray))
        with self.subTest(msg="col with time array"):
            holders.append(TableTools.col("timeCol", self.nTimeArray))
        print('obj col = \n{}'.format(TableTools.html(TableTools.newTable(*holders))))
        del holders


        holders = []
        with self.subTest(msg="col with int list"):
            holders.append(TableTools.col("intList2", self.intList))
        with self.subTest(msg="col with double list"):
            holders.append(TableTools.col("doubleList2", self.floatList))
        with self.subTest(msg="col with char list"):
            holders.append(TableTools.col("stringList", self.stringList))
        with self.subTest(msg="col with char list"):
            holders.append(TableTools.col("charList2", self.charList))
        with self.subTest(msg="col with boolean list"):
            holders.append(TableTools.col("booleanList", self.booleanList))
        with self.subTest(msg="col with time list"):
            holders.append(TableTools.col("timeList", self.timeList))
        print('col from list = \n{}'.format(TableTools.html(TableTools.newTable(*holders))))
        del holders


        holders = []
        with self.subTest(msg="col with byte"):
            holders.append(TableTools.col("byteCol", self.nByteArray))
        with self.subTest(msg="col with short"):
            holders.append(TableTools.col("shortCol", self.nShortArray))
        with self.subTest(msg="col with int"):
            holders.append(TableTools.col("intCol", self.nIntArray))
        with self.subTest(msg="col with long"):
            holders.append(TableTools.col("longCol", self.nLongArray))
        with self.subTest(msg="col with float"):
            holders.append(TableTools.col("floatCol", self.nFloatArray))
        with self.subTest(msg="col with double"):
            holders.append(TableTools.col("doubleCol", self.nDoubleArray))
        with self.subTest(msg="col with char"):
            holders.append(TableTools.col("charCol", self.nCharArray))
        print("primitive from col =\n{}".format(TableTools.html(TableTools.newTable(*holders))))
        del holders

    def testColumnSource(self):
        """
        Test cases for colSource(), objColSource() methods & associated newTable() method
        """

        # type inference does not work for list and dicts (i.e. not converted to java collections & maps)
        colSources = []
        names = []
        mapSources = {}

        with self.subTest(msg="colSource with byte"):
            key, val = "byte", TableTools.colSource(self.nByteArray)
            names.append(key)
            colSources.append(val)
            mapSources[key] = val
        with self.subTest(msg="colSource with short"):
            key, val = "short", TableTools.colSource(self.nShortArray)
            names.append(key)
            colSources.append(val)
            mapSources[key] = val
        with self.subTest(msg="colSource with int"):
            key, val = "int", TableTools.colSource(self.nIntArray)
            names.append(key)
            colSources.append(val)
            mapSources[key] = val
        with self.subTest(msg="colSource with long"):
            key, val = "long", TableTools.colSource(self.nLongArray)
            names.append(key)
            colSources.append(val)
            mapSources[key] = val
        with self.subTest(msg="colSource with float"):
            key, val = "float", TableTools.colSource(self.nFloatArray)
            names.append(key)
            colSources.append(val)
            mapSources[key] = val
        with self.subTest(msg="colSource with double"):
            key, val = "double", TableTools.colSource(self.nDoubleArray)
            names.append(key)
            colSources.append(val)
            mapSources[key] = val
        with self.subTest(msg="colSource with char"):
            key, val = "char", TableTools.colSource(self.nCharArray)
            names.append(key)
            colSources.append(val)
            mapSources[key] = val
        with self.subTest(msg="colSource with string"):
            key, val = "string", TableTools.colSource(self.nStringArray)
            names.append(key)
            colSources.append(val)
            mapSources[key] = val
        with self.subTest(msg="colSource with boolean"):
            key, val = "boolean", TableTools.colSource(self.nBooleanArray)
            names.append(key)
            colSources.append(val)
            mapSources[key] = val
        with self.subTest(msg="colSource with time"):
            key, val = "time", TableTools.colSource(self.nTimeArray)
            names.append(key)
            colSources.append(val)
            mapSources[key] = val

        with self.subTest(msg="newTable with name and column source list"):
            self.assertGreater(len(colSources), 0)  # make sure that we even do something useful
            print("table from [names], [sources] = \n{}".format(TableTools.html(TableTools.newTable(3, names, colSources))))

        with self.subTest(msg="newTable with {name: column source} map"):
            self.assertGreater(len(mapSources), 0)  # make sure that we even do something useful
            print("table from [names : sources] = \n{}".format(TableTools.html(TableTools.newTable(3, mapSources))))
        del names, colSources, mapSources

        names, colSources = [], []
        with self.subTest(msg="colSource with int list"):
            key, val = "intList", TableTools.colSource(self.intList)
            names.append(key)
            colSources.append(val)
        with self.subTest(msg="colSource with float list"):
            key, val = "floatList", TableTools.colSource(self.floatList)
            names.append(key)
            colSources.append(val)
        with self.subTest(msg="colSource with string list"):
            key, val = "strList", TableTools.colSource(self.stringList)
            names.append(key)
            colSources.append(val)
        with self.subTest(msg="colSource with char list"):
            key, val = "charList", TableTools.colSource(self.charList)
            names.append(key)
            colSources.append(val)
        with self.subTest(msg="colSource with boolean list"):
            key, val = "boolList", TableTools.colSource(self.booleanList)
            names.append(key)
            colSources.append(val)
        with self.subTest(msg="colSource with time list"):
            key, val = "time", TableTools.colSource(self.timeList)
            names.append(key)
            colSources.append(val)
        print("table from colSource with lists = \n{}".format(TableTools.html(TableTools.newTable(3, names, colSources))))
        del names, colSources

        names, colSources = [], []
        with self.subTest(msg="objColSource with string"):
            key, val = "string", TableTools.objColSource(self.nStringArray)
            names.append(key)
            colSources.append(val)
        with self.subTest(msg="objColSource with boolean"):
            key, val = "boolean", TableTools.objColSource(self.nBooleanArray)
            names.append(key)
            colSources.append(val)
        with self.subTest(msg="objColSource with time"):
            key, val = "time", TableTools.objColSource(self.nTimeArray)
            names.append(key)
            colSources.append(val)
        # NOTE: this one is kinda dumb...probably not what anyone wants
        with self.subTest(msg="objColSource with double primitive"):
            key, val = "double", TableTools.objColSource(self.nDoubleArray)
            names.append(key)
            colSources.append(val)
        print("table from objColSource = \n{}".format(TableTools.html(TableTools.newTable(3, names, colSources))))
        del names, colSources

        names, colSources = [], []
        with self.subTest(msg="objColSource with string list"):
            key, val = "string", TableTools.objColSource(self.stringList)
            names.append(key)
            colSources.append(val)
        with self.subTest(msg="objColSource with boolean list"):
            key, val = "boolean", TableTools.objColSource(self.booleanList)
            names.append(key)
            colSources.append(val)
        with self.subTest(msg="objColSource with time list"):
            key, val = "time", TableTools.objColSource(self.timeList)
            names.append(key)
            colSources.append(val)
        # NOTE: this one is kinda dumb...probably not what anyone wants
        with self.subTest(msg="objColSource with float list"):
            key, val = "double", TableTools.objColSource(self.floatList)
            names.append(key)
            colSources.append(val)
        print("table from objColSource with lists = \n{}".format(TableTools.html(TableTools.newTable(3, names, colSources))))
        del names, colSources

    @unittest.skip("what to do?")
    def testGetKey(self):
        """
        Test for getKey() & getPrevKey() methods?
        """

        # TODO: getKey(), getPrevKey()
        pass

    def testPrimitiveColCases(self):
        """
        Testing column construction from primitive cases
        """

        with self.subTest(msg="col with bool"):
            col1 = TableTools.col('bool1', numpy.array([True], dtype=numpy.bool))
            self.assertEqual(col1.dataType.toString(), 'class java.lang.Boolean')
            # print("table from bool = \n{}".format(TableTools.html(TableTools.newTable(col1))))

            col2 = TableTools.col('bool2', True, False, None)
            self.assertEqual(col2.dataType.toString(), 'class java.lang.Boolean')
            # print("table from bool varargs = \n{}".format(TableTools.html(TableTools.newTable(col2))))

        with self.subTest(msg="col with int"):
            col1 = TableTools.col('int1', 1)
            self.assertEqual(col1.dataType.toString(), 'long')
            # print("table from int = \n{}".format(TableTools.html(TableTools.newTable(col1))))

            col2 = TableTools.col('int2', 1, 2, None)
            self.assertEqual(col2.dataType.toString(), 'long')
            # print("table from int varargs = \n{}".format(TableTools.html(TableTools.newTable(col2))))

        with self.subTest(msg="col with float"):
            col1 = TableTools.col('float1', 1.0)
            self.assertEqual(col1.dataType.toString(), 'double')
            # print("table from float = \n{}".format(TableTools.html(TableTools.newTable(col1))))

            col2 = TableTools.col('float2', 1.0, 2.0, None)
            self.assertEqual(col2.dataType.toString(), 'double')
            # print("table from float varargs = \n{}".format(TableTools.html(TableTools.newTable(col2))))

        with self.subTest(msg="col with string"):
            col1 = TableTools.col('string1', 'one')
            self.assertEqual(col1.dataType.toString(), 'class java.lang.String')
            # print("table from string = \n{}".format(TableTools.html(TableTools.newTable(col1))))

            col2 = TableTools.col('string2', 'one', 'two', None)
            self.assertEqual(col2.dataType.toString(), 'class java.lang.String')
            # print("table from string varargs = \n{}".format(TableTools.html(TableTools.newTable(col2))))

        with self.subTest(msg="col with datetime"):
            col1 = TableTools.col('datetime1', datetime.utcnow())
            self.assertEqual(col1.dataType.toString(), 'class io.deephaven.time.DateTime')
            # print("table from string = \n{}".format(TableTools.html(TableTools.newTable(col1))))

            col2 = TableTools.col('datetime2', datetime.utcnow(), datetime.utcnow(), None)
            self.assertEqual(col2.dataType.toString(), 'class io.deephaven.time.DateTime')
            # print("table from datetime varargs = \n{}".format(TableTools.html(TableTools.newTable(col2))))

        with self.subTest(msg="col with date"):
            col1 = TableTools.col('date1', date.today())
            self.assertEqual(col1.dataType.toString(), 'class io.deephaven.time.DateTime')
            # print("table from string = \n{}".format(TableTools.html(TableTools.newTable(col1))))

            col2 = TableTools.col('date2', date.today(), date.today(), None)
            self.assertEqual(col2.dataType.toString(), 'class io.deephaven.time.DateTime')
            # print("table from date varargs = \n{}".format(TableTools.html(TableTools.newTable(col2))))

        with self.subTest(msg="col with no argument"):
            col1 = TableTools.col('empty', [])
            self.assertEqual(col1.dataType.toString(), 'class java.lang.Object')
            # print("table from empty = \n{}".format(TableTools.html(TableTools.newTable(col1))))

    def testPrimitiveColSourceCases(self):
        """
        Testing column source construction from primitive cases
        """

        with self.subTest(msg="colSource with bool"):
            col1 = TableTools.colSource(True)
            self.assertEqual(col1.getType().toString(), 'class java.lang.Boolean')
            # print("table from bool = \n{}".format(TableTools.html(TableTools.newTable(1, {'bool1': col1}))))

            col2 = TableTools.colSource(True, False, None)
            self.assertEqual(col2.getType().toString(), 'class java.lang.Boolean')
            # print("table from bool varargs = \n{}".format(TableTools.html(TableTools.newTable(3, {'bool2': col2}))))

        with self.subTest(msg="colSource with int"):
            col1 = TableTools.colSource(1)
            self.assertEqual(col1.getType().toString(), 'long')
            # print("table from int = \n{}".format(TableTools.html(TableTools.newTable(1, {'int1': col1}))))

            col2 = TableTools.colSource(1, 2, None)
            self.assertEqual(col2.getType().toString(), 'long')
            # print("table from int varargs = \n{}".format(TableTools.html(TableTools.newTable(3, {'int2': col2}))))

        with self.subTest(msg="colSource with float"):
            col1 = TableTools.colSource(1.0)
            self.assertEqual(col1.getType().toString(), 'double')
            # print("table from float = \n{}".format(TableTools.html(TableTools.newTable(1, {'float1': col1}))))

            col2 = TableTools.colSource(1.0, 2.0, None)
            self.assertEqual(col2.getType().toString(), 'double')
            # print("table from float varargs = \n{}".format(TableTools.html(TableTools.newTable(3, {'float2': col2}))))

        with self.subTest(msg="colSource with string"):
            col1 = TableTools.colSource('one')
            self.assertEqual(col1.getType().toString(), 'class java.lang.String')
            # print("table from string = \n{}".format(TableTools.html(TableTools.newTable(1, {'string1': col1}))))

            col2 = TableTools.colSource('one', 'two', None)
            self.assertEqual(col2.getType().toString(), 'class java.lang.String')
            # print("table from string varargs = \n{}".format(TableTools.html(TableTools.newTable(3, {'string2': col2}))))

        with self.subTest(msg="colSource with datetime"):
            col1 = TableTools.colSource(datetime.utcnow())
            self.assertEqual(col1.getType().toString(), 'class io.deephaven.time.DateTime')
            # print("table from string = \n{}".format(TableTools.html(TableTools.newTable(1, {'datetime1': col1}))))

            col2 = TableTools.colSource(datetime.utcnow(), datetime.utcnow(), None)
            self.assertEqual(col2.getType().toString(), 'class io.deephaven.time.DateTime')
            # print("table from datetime varargs = \n{}".format(TableTools.html(TableTools.newTable(3, {'datetime2': col2}))))

        with self.subTest(msg="colSource with date"):
            col1 = TableTools.colSource(date.today())
            self.assertEqual(col1.getType().toString(), 'class io.deephaven.time.DateTime')
            # print("table from string = \n{}".format(TableTools.html(TableTools.newTable(1, {'date1': col1}))))

            col2 = TableTools.colSource(date.today(), date.today(), None)
            self.assertEqual(col2.getType().toString(), 'class io.deephaven.time.DateTime')
            # print("table from date varargs = \n{}".format(TableTools.html(TableTools.newTable(3, {'date2': col2}))))

    def testBreakingCases(self):
        """
        Testing some cases observed to fail previously
        """

        tab1, tab2, tab3 = None, None, None
        with self.subTest(msg="charCol with list"):
            tab1 = TableTools.newTable(TableTools.charCol('tab1', ['A', 'B', 'C', 'D']))
            print("tab1 = \n{}".format(TableTools.html(tab1)))

        with self.subTest(msg="charCol with string"):
            tab2 = TableTools.newTable(TableTools.charCol('tab2', 'EFGH'))
            print("tab2 = \n{}".format(TableTools.html(tab2)))

        with self.subTest(msg="col with single string"):
            tab3 = TableTools.newTable(TableTools.col('tab3', 'EFGH'))
            print("tab3 = \n{}".format(TableTools.html(tab3)))
        del tab1, tab2, tab3

