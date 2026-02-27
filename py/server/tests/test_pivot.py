#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

from tests.testbase import BaseTestCase
from deephaven import DHError, new_table
from deephaven.column import int_col
from deephaven.experimental.pivot import pivot
from deephaven.table import table_diff


class PivotTestCase(BaseTestCase):

    def test_pivot_1_row(self):
        input = new_table([
            int_col("Row", [1, 2, 3, 1, 2, 3]),
            int_col("Col", [1, 1, 1, 2, 2, 2]),
            int_col("Value", [10, 20, 30, 40, 50, 60])
        ])

        with self.subTest("pivot - 1 row col"):
            p = pivot(input, "Row", "Col", "Value")

            target = new_table([
                int_col("Row", [1, 2, 3]),
                int_col("_1", [10, 20, 30]),
                int_col("_2", [40, 50, 60]),
            ])

            d = table_diff(p, target)
            self.assertEqual(d, "")

        with self.subTest("pivot - 1 row col - value_to_col_name"):
            p = pivot(input, "Row", "Col", "Value", value_to_col_name=lambda x: f"Col_{x}")

            target = new_table([
                int_col("Row", [1, 2, 3]),
                int_col("Col_1", [10, 20, 30]),
                int_col("Col_2", [40, 50, 60]),
            ])

            d = table_diff(p, target)
            self.assertEqual(d, "")

    def test_pivot_2_row(self):
        input = new_table([
            int_col("Row1", [1, 2, 2, 1, 2, 2]),
            int_col("Row2", [1, 2, 3, 1, 2, 3]),
            int_col("Col", [1, 1, 1, 2, 2, 2]),
            int_col("Value", [10, 20, 30, 40, 50, 60])
        ])

        with self.subTest("pivot - 2 row col"):
            p = pivot(input, ["Row1", "Row2"], "Col", "Value")

            target = new_table([
                int_col("Row1", [1, 2, 2]),
                int_col("Row2", [1, 2, 3]),
                int_col("_1", [10, 20, 30]),
                int_col("_2", [40, 50, 60]),
            ])

            d = table_diff(p, target)
            self.assertEqual(d, "")

        with self.subTest("pivot - 2 row col - value_to_col_name"):
            p = pivot(input, ["Row1", "Row2"], "Col", "Value", value_to_col_name=lambda x: f"Col_{x}")

            target = new_table([
                int_col("Row1", [1, 2, 2]),
                int_col("Row2", [1, 2, 3]),
                int_col("Col_1", [10, 20, 30]),
                int_col("Col_2", [40, 50, 60]),
            ])

            d = table_diff(p, target)
            self.assertEqual(d, "")

    def test_pivot_errors(self):
        input = new_table([
            int_col("Row", [1, 2, 3, 1, 2, 3]),
            int_col("Col", [1, 1, 1, 2, 2, 2]),
            int_col("Value", [10, 20, 30, 40, 50, 60])
        ])

        with self.subTest("pivot - non-string col_name"):
            with self.assertRaises(DHError) as cm:
                p = pivot(input, "Row", "Col", "Value", value_to_col_name=lambda x: 1.0)
            self.assertIn("Value does not map to a string", str(cm.exception))

        with self.subTest("pivot - invalid col_name"):
            with self.assertRaises(DHError) as cm:
                p = pivot(input, "Row", "Col", "Value", value_to_col_name=lambda x: f".{x}#")
            self.assertIn("Value maps to an invalid column name", str(cm.exception))

        with self.subTest("pivot - duplicate col_name"):
            with self.assertRaises(DHError) as cm:
                p = pivot(input, "Row", "Col", "Value", value_to_col_name=lambda x: "Col")
            self.assertIn("Value maps to a duplicate column name", str(cm.exception))
