#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

# Test python sort syntax

import unittest
from unittest import TestCase
from deephaven import as_list, SortColumn, ColumnName
from deephaven.TableTools import newTable, intCol, diff

class SortTestCase(TestCase):
    def test_sort_syntax(self):
        source = newTable(
            intCol("A", 1, 2, 3, 1, 2, 3),
            intCol("B", 0, 1, 0, 1, 0, 1),
            intCol("C", 1, 2, 3, 4, 5, 6),
        )

        sort_columns = as_list([
            SortColumn.asc(ColumnName.of("A")),
            SortColumn.desc(ColumnName.of("B"))
        ])

        actual = source.sort(sort_columns)

        target = newTable(
            intCol("A", 1, 1, 2, 2, 3, 3),
            intCol("B", 1, 0, 1, 0, 1, 0),
            intCol("C", 4, 1, 2, 5, 6, 3),
        )

        diff_string = diff(actual, target, 1)
        self.assertEqual("", diff_string)



if __name__ == '__main__':
    unittest.main()
