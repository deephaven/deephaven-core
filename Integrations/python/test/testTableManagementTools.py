#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

##############################################################################
# NOTE: the jvm should have been initialized, or this test will certainly fail
##############################################################################

import sys
import jpy
import os
import time
import shutil

from deephaven import TableTools, TableManagementTools


if sys.version_info[0] < 3:
    import unittest2 as unittest
    # not part of the standard library, installed via pip (or the like)
    # it provides backward compatibility with python3 style subTest context manager (handy for complex tests)
else:
    import unittest


class TestTableManagementTools(unittest.TestCase):
    """
    Test cases for the deephaven.TableManagementTools module (performed locally) -
    """

    @classmethod
    def setUpClass(cls):
        # define a junk table workspace directory
        cls.rootDir = os.path.join(TableManagementTools.getWorkspaceRoot(), 'junk')

    def testCreation(self):
        """
        Test suite for reading, writing, and deleting a table to disk
        """

        table = TableTools.emptyTable(3).update("x=i", "y=(double)(i/10.0)", "z=(double)(i*i)")
        definition = table.getDefinition()
        fileLocation = os.path.join(self.rootDir, 'table1')
        fileLocation2 = os.path.join(self.rootDir, 'table2')

        # make sure that the test workspace is clean
        if os.path.exists(fileLocation):
            shutil.rmtree(fileLocation)
        if os.path.exists(fileLocation2):
            shutil.rmtree(fileLocation2)
        time.sleep(0.01)  # avoid race condition on file existence...

        # Writing
        with self.subTest(msg="writeTable(Table, String)"):
            TableManagementTools.writeTableToDir(table, fileLocation)
            time.sleep(0.01)  # avoid race condition on file existence...
            self.assertTrue(os.path.exists(fileLocation))
            shutil.rmtree(fileLocation)
            time.sleep(0.01)  # avoid race condition on file existence...
        with self.subTest(msg="writeTable(Table, File)"):
            TableManagementTools.writeTableToDir(table, TableManagementTools.getFileObject(fileLocation))
            time.sleep(0.01)  # avoid race condition on file existence...
            self.assertTrue(os.path.exists(fileLocation))
            shutil.rmtree(fileLocation)
            time.sleep(0.01)  # avoid race condition on file existence...
        with self.subTest(msg="writeTable(Table, String, StorageFormat) - Parquet"):
            TableManagementTools.writeTableToDir(table, fileLocation, 'Parquet')
            time.sleep(0.01)  # avoid race condition on file existence...
            self.assertTrue(os.path.exists(fileLocation))
            shutil.rmtree(fileLocation)
            time.sleep(0.01)  # avoid race condition on file existence...
        with self.subTest(msg="writeTables(Table[], TableDefinition, File[]"):
            TableManagementTools.writeTables([table, table], definition, [fileLocation, fileLocation2])
            time.sleep(0.01)  # avoid race condition on file existence...
            self.assertTrue(os.path.exists(fileLocation))
            self.assertTrue(os.path.exists(fileLocation2))

        # Reading
        with self.subTest(msg="readTable(File)"):
            table2 = TableManagementTools.readTableFromDir(fileLocation)

        # Delete
        with self.subTest(msg="delete(File)"):
            if os.path.exists(fileLocation):
                TableManagementTools.deleteTable(fileLocation)
                time.sleep(0.01)  # avoid race condition on file existence...
                self.assertFalse(os.path.exists(fileLocation))
            if os.path.exists(fileLocation2):
                TableManagementTools.deleteTable(fileLocation2)
                time.sleep(0.01)  # avoid race condition on file existence...
                self.assertFalse(os.path.exists(fileLocation2))

    @classmethod
    def tearDownClass(cls):
        # remove the junk definitions created in the tests, if they exist...
        if os.path.exists(cls.rootDir):
            try:
                shutil.rmtree(cls.rootDir)
            except Exception as e:
                print("Tried removing directory {}, but failed with error {}. "
                      "Manual clean-up may be necessary".format(cls.rootDir, e))
