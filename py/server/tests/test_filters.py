#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import DHError, empty_table, new_table, read_csv
from deephaven.column import string_col
from deephaven.concurrency_control import Barrier
from deephaven.filters import (
    ColumnName,
    Filter,
    PatternMode,
    and_,
    eq,
    ge,
    gt,
    in_,
    incremental_release,
    is_not_null,
    is_null,
    le,
    lt,
    ne,
    not_,
    or_,
    pattern,
)
from tests.testbase import BaseTestCase


class FilterTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None
        super().tearDown()

    def test_incremental_release(self):
        filtered_table = self.test_table.where(filters=incremental_release(5, 10))
        self.assertEqual(filtered_table.size, 5)

    def test_pattern_filter(self):
        new_test_table = self.test_table.update("X = String.valueOf(d)")
        regex_filter = pattern(PatternMode.MATCHES, "X", "(?s)...")
        with self.assertRaises(DHError):
            filtered_table = self.test_table.where(filters=regex_filter)

        filtered_table = new_test_table.where(filters=regex_filter)
        self.assertLessEqual(filtered_table.size, new_test_table.size)

        new_test_table = new_test_table.update("Y = String.valueOf(e)")
        regex_filter1 = pattern(PatternMode.MATCHES, "Y", "(?s).0.")
        filtered_table = new_test_table.where(filters=[regex_filter, regex_filter1])
        self.assertLessEqual(filtered_table.size, new_test_table.size)

    def test_filter(self):
        conditions = ["a > 100", "b < 1000", "c < 0"]
        filters = Filter.from_(conditions)
        filtered_table = self.test_table.where(filters)
        filter_and = and_(filters)
        filtered_table_and = self.test_table.where(filter_and)
        self.assert_table_equals(filtered_table, filtered_table_and)

        filter_or = or_(filters)
        filtered_table_or = self.test_table.where(filter_or)
        self.assertGreater(filtered_table_or.size, filtered_table_and.size)

        filter_not = not_(filter_or)
        filtered_table_not = self.test_table.where(filter_not)
        self.assertEqual(
            filtered_table_or.size + filtered_table_not.size, self.test_table.size
        )

        filtered_table_mixed = self.test_table.where(
            ["a > 100", "b < 1000", Filter.from_("c < 0")]
        )
        self.assert_table_equals(filtered_table_mixed, filtered_table_and)

    def test_is_null(self):
        x = new_table([string_col("X", ["a", "b", "c", None, "e", "f"])])
        x_is_null = new_table([string_col("X", [None])])
        x_not_is_null = new_table([string_col("X", ["a", "b", "c", "e", "f"])])
        self.assert_table_equals(x.where(is_null("X")), x_is_null)
        self.assert_table_equals(x.where(not_(is_null("X"))), x_not_is_null)

    def test_is_not_null(self):
        x = new_table([string_col("X", ["a", "b", "c", None, "e", "f"])])
        x_is_not_null = new_table([string_col("X", ["a", "b", "c", "e", "f"])])
        x_not_is_not_null = new_table([string_col("X", [None])])
        self.assert_table_equals(x.where(is_not_null("X")), x_is_not_null)
        self.assert_table_equals(x.where(not_(is_not_null("X"))), x_not_is_not_null)

    def test_filters_with_concurrency_control(self):
        conditions = ["a > 100", "b < 1000", "c < 0"]
        filters = Filter.from_(conditions)
        filters[0] = filters[0].with_serial()
        barrier = Barrier()
        filters[1] = filters[1].with_declared_barriers([barrier])
        filters[2] = filters[2].with_respected_barriers([barrier])

        filtered_table = self.test_table.where(filters)
        filter_and = and_(filters)
        filter_and.with_serial()
        filtered_table_and = self.test_table.where(filter_and)
        self.assert_table_equals(filtered_table, filtered_table_and)

    def test_filter_in(self):
        t = empty_table(100).update(["A = i", "B = String.valueOf(i)"])
        with self.subTest("integer values"):
            var_2 = 2
            filter_in = in_("A", [1, var_2, 3])

            rt = t.where(filter_in)
            self.assertEqual(3, rt.size)

            rt = t.where(not_(filter_in))
            self.assertEqual(97, rt.size)

        with self.subTest("string values"):
            filter_in = in_("B", ["2", "3"])
            rt = t.where(filter_in)
            self.assertEqual(2, rt.size)

        with self.subTest("mixed values"):
            with self.assertRaises(DHError) as cm:
                filter_in = in_("A", [2, "3"])
                rt = t.where(filter_in)
            print(cm.exception)

            # inconsistent ?
            filter_in = in_("B", [2, "3"])
            rt = t.where(filter_in)
            self.assertEqual(1, rt.size)

        t1 = t.update(["C = (java.lang.Object)B"]).update("C = C == `2`? 2: C")
        with self.subTest("object values"):
            filter_in = in_("C", [2, "3"])
            rt = t1.where(filter_in)
            self.assertEqual(2, rt.size)

    def test_filter_comparison(self):
        t = empty_table(100).update(["A = i", "B = String.valueOf(i)", "C = i * 2"])

        with self.subTest("eq/ne"):
            filter_eq = eq(ColumnName("A"), 10)
            rt = t.where(filter_eq)
            self.assertEqual(1, rt.size)
            filter_neq = ne(ColumnName("A"), 10)
            rt = t.where(filter_neq)
            self.assertEqual(99, rt.size)
            filter_neq = not_(filter_eq)
            rt = t.where(filter_neq)
            self.assertEqual(99, rt.size)

        with self.subTest("lt/ge"):
            filter_lt = lt(ColumnName("A"), 10)
            rt = t.where(filter_lt)
            self.assertEqual(10, rt.size)
            filter_ge = ge(ColumnName("A"), 10)
            rt = t.where(filter_ge)
            self.assertEqual(90, rt.size)
            filter_ge = not_(filter_lt)
            rt = t.where(filter_ge)
            self.assertEqual(90, rt.size)

        with self.subTest("gt/le"):
            filter_gt = gt(ColumnName("A"), 10)
            rt = t.where(filter_gt)
            self.assertEqual(89, rt.size)
            filter_le = le(ColumnName("A"), 10)
            rt = t.where(filter_le)
            self.assertEqual(11, rt.size)
            filter_le = not_(filter_gt)
            rt = t.where(filter_le)
            self.assertEqual(11, rt.size)


if __name__ == "__main__":
    unittest.main()
