#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

import jpy

from deephaven import DHError, read_csv, empty_table
from deephaven.experimental import sql
from deephaven.execution_context import make_user_exec_ctx

from tests.testbase import BaseTestCase

_JTableSpec = jpy.get_type("io.deephaven.qst.table.TableSpec")

some_global_table = empty_table(42)


class SqlTest(BaseTestCase):
    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_eval_global_implicit(self):
        result_table = sql.eval("SELECT * FROM some_global_table")
        self.assertEqual(result_table.size, 42)

    def test_eval_global_explicit(self):
        result_table = sql.eval(
            "SELECT * FROM my_global_table",
            globals={"my_global_table": some_global_table},
        )
        self.assertEqual(result_table.size, 42)

    def test_eval_local_implicit(self):
        # noinspection PyUnusedLocal
        test_table = read_csv("tests/data/test_table.csv")
        result_table = sql.eval("SELECT * FROM test_table LIMIT 5")
        self.assertEqual(result_table.size, 5)

    def test_eval_local_explicit(self):
        test_table = read_csv("tests/data/test_table.csv")
        result_table = sql.eval(
            "SELECT * FROM my_table LIMIT 6", locals={"my_table": test_table}
        )
        self.assertEqual(result_table.size, 6)

    def test_dry_run_global_implicit(self):
        result_spec = sql.eval("SELECT * FROM some_global_table", dry_run=True)
        self.assertTrue(isinstance(result_spec, jpy.JType))
        # Might be nice to extend jpy like this in the future?
        # self.assertTrue(isinstance(result_spec, _JTableSpec))
        self.assertTrue(_JTableSpec.jclass.isInstance(result_spec))

    def test_dry_run_global_explicit(self):
        result_spec = sql.eval(
            "SELECT * FROM my_global_table",
            dry_run=True,
            globals={"my_global_table": some_global_table},
        )
        self.assertTrue(isinstance(result_spec, jpy.JType))
        # Might be nice to extend jpy like this in the future?
        # self.assertTrue(isinstance(result_spec, _JTableSpec))
        self.assertTrue(_JTableSpec.jclass.isInstance(result_spec))

    def test_dry_run_local_implicit(self):
        # noinspection PyUnusedLocal
        test_table = read_csv("tests/data/test_table.csv")
        result_spec = sql.eval("SELECT * FROM test_table LIMIT 7", dry_run=True)
        self.assertTrue(isinstance(result_spec, jpy.JType))
        # Might be nice to extend jpy like this in the future?
        # self.assertTrue(isinstance(result_spec, _JTableSpec))
        self.assertTrue(_JTableSpec.jclass.isInstance(result_spec))

    def test_dry_run_local_explicit(self):
        # noinspection PyUnusedLocal
        test_table = read_csv("tests/data/test_table.csv")
        result_spec = sql.eval(
            "SELECT * FROM my_table LIMIT 8",
            dry_run=True,
            locals={"my_table": test_table},
        )
        self.assertTrue(isinstance(result_spec, jpy.JType))
        # Might be nice to extend jpy like this in the future?
        # self.assertTrue(isinstance(result_spec, _JTableSpec))
        self.assertTrue(_JTableSpec.jclass.isInstance(result_spec))

    def test_local_takes_precedence(self):
        # noinspection PyUnusedLocal,PyShadowingNames
        some_global_table = empty_table(13)
        result_table = sql.eval("SELECT * FROM some_global_table")
        self.assertEqual(result_table.size, 13)

    def test_no_implicit_global_if_local_explicit(self):
        test_table = read_csv("tests/data/test_table.csv")
        with self.assertRaises(DHError):
            sql.eval(
                "SELECT * FROM my_global_table",
                locals={"test_table": test_table},
            )

    def test_no_implicit_local_if_global_explicit(self):
        # noinspection PyUnusedLocal
        test_table = read_csv("tests/data/test_table.csv")
        with self.assertRaises(DHError):
            sql.eval(
                "SELECT * FROM test_table",
                globals={"my_global_table": some_global_table},
            )

    def test_nested_non_local(self):
        # noinspection PyUnusedLocal
        test_table = read_csv("tests/data/test_table.csv")

        def do_sql():
            nonlocal test_table
            return sql.eval("SELECT * FROM test_table LIMIT 3")

        result_table = do_sql()
        self.assertEqual(result_table.size, 3)

if __name__ == "__main__":
    unittest.main()
