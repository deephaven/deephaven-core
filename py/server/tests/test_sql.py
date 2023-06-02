#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

import jpy

from deephaven import DHError, read_csv, empty_table
from deephaven.experimental import sql
from tests.testbase import BaseTestCase

_JTableSpec = jpy.get_type("io.deephaven.qst.table.TableSpec")

some_global_table = empty_table(42)


class SqlTest(BaseTestCase):
    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_eval_global(self):
        result_table = sql.evaluate("SELECT * FROM some_global_table")
        self.assertEqual(result_table.size, 42)

    def test_eval_local(self):
        # noinspection PyUnusedLocal
        test_table = read_csv("tests/data/test_table.csv")
        result_table = sql.evaluate("SELECT * FROM test_table LIMIT 5")
        self.assertEqual(result_table.size, 5)

    def test_dry_run_global(self):
        result_spec = sql.evaluate("SELECT * FROM some_global_table", dry_run=True)
        self.assertTrue(isinstance(result_spec, jpy.JType))
        # Might be nice to extend jpy like this in the future?
        # self.assertTrue(isinstance(result_spec, _JTableSpec))
        self.assertTrue(_JTableSpec.jclass.isInstance(result_spec))

    def test_dry_run_local(self):
        # noinspection PyUnusedLocal
        test_table = read_csv("tests/data/test_table.csv")
        result_spec = sql.evaluate("SELECT * FROM test_table LIMIT 7", dry_run=True)
        self.assertTrue(isinstance(result_spec, jpy.JType))
        # Might be nice to extend jpy like this in the future?
        # self.assertTrue(isinstance(result_spec, _JTableSpec))
        self.assertTrue(_JTableSpec.jclass.isInstance(result_spec))

    def test_local_takes_precedence(self):
        # noinspection PyUnusedLocal,PyShadowingNames
        some_global_table = empty_table(13)
        result_table = sql.evaluate("SELECT * FROM some_global_table")
        self.assertEqual(result_table.size, 13)

    def test_nested_non_local(self):
        # noinspection PyUnusedLocal
        test_table = read_csv("tests/data/test_table.csv")

        def do_sql():
            nonlocal test_table
            return sql.evaluate("SELECT * FROM test_table LIMIT 3")

        result_table = do_sql()
        self.assertEqual(result_table.size, 3)

    def test_current_timestamp(self):
        result_table = sql.evaluate("SELECT CURRENT_TIMESTAMP")
        self.assertEqual(result_table.size, 1)

    def test_inner_func_local(self):
        def inner_func(my_table):
            return sql.evaluate("SELECT * FROM my_table LIMIT 13")

        result_table = inner_func(some_global_table)
        self.assertEqual(result_table.size, 13)


if __name__ == "__main__":
    unittest.main()
