#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import os
import tempfile
import unittest
from types import MappingProxyType

from deephaven import DHError, dtypes, new_table, read_csv, write_csv
from deephaven.column import (
    bool_col,
    byte_col,
    char_col,
    double_col,
    float_col,
    int_col,
    long_col,
    short_col,
)
from tests.testbase import BaseTestCase


class CsvTestCase(BaseTestCase):
    def test_read_simple(self):
        t = read_csv("tests/data/small_sample.csv")
        self.assertTrue(t.columns)

    def test_read_header(self):
        col_names = ["Strings", "Longs", "Floats"]
        col_types = [dtypes.string, dtypes.long, dtypes.float64]
        table_header = {k: v for k, v in zip(col_names, col_types)}
        t = read_csv("tests/data/test_csv.csv", header=table_header)
        self.assertEqual(col_names, t.column_names)

    def test_read_error_col_type(self):
        col_names = ["Strings", "Longs", "Floats"]
        col_types = [dtypes.string, dtypes.float64, dtypes.long]
        table_header = {k: v for k, v in zip(col_names, col_types)}
        with self.assertRaises(DHError) as cm:
            t = read_csv("tests/data/test_csv.csv", header=table_header)

        self.assertIsNotNone(cm.exception.compact_traceback)

    def test_read_error_quote(self):
        col_names = ["Strings", "Longs", "Floats"]
        col_types = [dtypes.string, dtypes.long, dtypes.float64]
        table_header = {k: v for k, v in zip(col_names, col_types)}
        with self.assertRaises(DHError) as cm:
            t = read_csv("tests/data/test_csv.csv", header=table_header, quote=",")

        self.assertIsNotNone(cm.exception.compact_traceback)

    def test_write(self):
        t = read_csv("tests/data/small_sample.csv")
        write_csv(t, "./test_write.csv")
        t_cols = t.column_names
        t = read_csv("./test_write.csv")
        self.assertEqual(t_cols, t.column_names)

        col_names = ["Strings", "Longs", "Floats"]
        col_types = [dtypes.string, dtypes.long, dtypes.float64]
        table_header = {k: v for k, v in zip(col_names, col_types)}
        t = read_csv("tests/data/test_csv.csv", header=table_header)
        write_csv(t, "./test_write.csv", cols=col_names)
        t = read_csv("./test_write.csv")
        self.assertEqual(col_names, t.column_names)

        import os

        os.remove("./test_write.csv")

    def test_read_header_row(self):
        t = read_csv("tests/data/small_sample.csv")
        t1 = read_csv("tests/data/small_sample.csv", header_row=2)
        self.assertEqual(t.size, t1.size + 2)

        with self.assertRaises(DHError):
            t1 = read_csv("tests/data/small_sample.csv", headless=True, header_row=2)

    def test_primitive_types(self):
        actual = read_csv(
            "tests/data/primitive_types.csv",
            {
                "Bool": dtypes.bool_,
                "Byte": dtypes.byte,
                "Char": dtypes.char,
                "Short": dtypes.short,
                "Int": dtypes.int32,
                "Long": dtypes.long,
                "Float": dtypes.float32,
                "Double": dtypes.double,
            },
        )
        expected = new_table(
            [
                bool_col("Bool", [True]),
                byte_col("Byte", [42]),
                char_col("Char", [ord("a")]),
                short_col("Short", [42]),
                int_col("Int", [42]),
                long_col("Long", [42]),
                float_col("Float", [42.42]),
                double_col("Double", [42.42]),
            ]
        )
        self.assert_table_equals(actual, expected)

    def test_read_col_types(self) -> None:
        # The keys are in a different order than the file's columns, and cover only two of them. Each type goes to
        # the column of that name, and the column left out keeps its name and inferred type.
        t = read_csv(
            "tests/data/test_csv.csv",
            col_types={"Long": dtypes.double, "String": dtypes.string},
        )
        self.assertEqual(["String", "Long", "Float"], t.column_names)
        self.assertEqual(dtypes.string, t.definition["String"].data_type)
        self.assertEqual(dtypes.double, t.definition["Long"].data_type)
        self.assertEqual(dtypes.double, t.definition["Float"].data_type)

    def test_read_col_types_unknown_name(self) -> None:
        with self.assertRaises(DHError) as cm:
            read_csv("tests/data/test_csv.csv", col_types={"Flaot": dtypes.double})
        self.assertIn(
            "col_types names not found in the CSV header: 'Flaot'", str(cm.exception)
        )

    def test_read_col_types_legalized_name(self) -> None:
        # The table shows the header 'my venue' as the column 'my_venue', but col_types takes the header name.
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "legalized.csv")
            with open(path, "w") as f:
                f.write("Sym,my venue,my-venue\nAAPL,X,Y\n")

            t = read_csv(path, col_types={"my venue": dtypes.string})
            self.assertEqual(["Sym", "my_venue", "my_venue2"], t.column_names)
            self.assertEqual(dtypes.string, t.definition["my_venue"].data_type)

            with self.assertRaises(DHError) as cm:
                read_csv(path, col_types={"my_venue": dtypes.string})
            self.assertIn(
                "'my_venue' (use the header name 'my venue')", str(cm.exception)
            )

            # 'my-venue' also legalizes to 'my_venue', so it gets a suffix. The hint must still name its own header.
            with self.assertRaises(DHError) as cm:
                read_csv(path, col_types={"my_venue2": dtypes.string})
            self.assertIn(
                "'my_venue2' (use the header name 'my-venue')", str(cm.exception)
            )

    def test_read_header_mapping(self) -> None:
        # A header that is a Mapping but not a dict still sets both names and types.
        header = MappingProxyType(
            {"S": dtypes.string, "L": dtypes.double, "F": dtypes.double}
        )
        t = read_csv("tests/data/test_csv.csv", header=header)
        self.assertEqual(["S", "L", "F"], t.column_names)
        self.assertEqual(dtypes.double, t.definition["L"].data_type)

    def test_read_header_list(self) -> None:
        t = read_csv("tests/data/test_csv.csv", header=["A", "B", "C"])
        self.assertEqual(["A", "B", "C"], t.column_names)
        self.assertEqual(dtypes.long, t.definition["B"].data_type)
        self.assertEqual(dtypes.double, t.definition["C"].data_type)

    def test_read_header_list_with_col_types(self) -> None:
        # The names in header replace the file's, so col_types must use them.
        t = read_csv(
            "tests/data/test_csv.csv",
            header=["A", "B", "C"],
            col_types={"B": dtypes.double},
        )
        self.assertEqual(dtypes.double, t.definition["B"].data_type)

        with self.assertRaises(DHError) as cm:
            read_csv(
                "tests/data/test_csv.csv",
                header=["A", "B", "C"],
                col_types={"Long": dtypes.double},
            )
        self.assertIn(
            "col_types names not found in the CSV header: 'Long'", str(cm.exception)
        )

    def test_read_headless_col_types(self) -> None:
        # skip_rows=1 skips the file's header line, which a headless read takes as data.
        t = read_csv(
            "tests/data/test_table.csv",
            headless=True,
            skip_rows=1,
            col_types={"Column3": dtypes.float32},
        )
        self.assertEqual(
            ["Column1", "Column2", "Column3", "Column4", "Column5"], t.column_names
        )
        self.assertEqual(dtypes.float32, t.definition["Column3"].data_type)
        self.assertEqual(dtypes.int32, t.definition["Column4"].data_type)

        t = read_csv(
            "tests/data/test_table.csv",
            headless=True,
            skip_rows=1,
            header=["A", "B", "C", "D", "E"],
            col_types={"E": dtypes.double, "C": dtypes.float32},
        )
        self.assertEqual(["A", "B", "C", "D", "E"], t.column_names)
        self.assertEqual(dtypes.float32, t.definition["C"].data_type)
        self.assertEqual(dtypes.double, t.definition["E"].data_type)

    def test_read_col_types_errors(self) -> None:
        with self.assertRaises(DHError) as cm:
            read_csv(
                "tests/data/test_csv.csv",
                header={
                    "String": dtypes.string,
                    "Long": dtypes.long,
                    "Float": dtypes.double,
                },
                col_types={"Float": dtypes.double},
            )
        self.assertIn(
            "a header dict cannot be combined with col_types", str(cm.exception)
        )

        with self.assertRaises(DHError) as cm:
            read_csv("tests/data/test_csv.csv", col_types={"Long": dtypes.int64_array})
        self.assertIn("col_types has unsupported data types", str(cm.exception))

        with self.assertRaises(DHError) as cm:
            read_csv(
                "tests/data/test_csv.csv",
                header={
                    "String": dtypes.string,
                    "Long": dtypes.int64_array,
                    "Float": dtypes.double,
                },
            )
        self.assertIn("header has unsupported data types: 'Long'", str(cm.exception))

        with self.assertRaises(DHError) as cm:
            read_csv("tests/data/test_csv.csv", header="String")
        self.assertIn(
            "header must be a sequence of column names or a dict", str(cm.exception)
        )


if __name__ == "__main__":
    unittest.main()
