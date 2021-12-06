#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import unittest

from deephaven.TableTools import newTable, intCol, diff
from deephaven.filter import or_
from unittest import TestCase


class FilterTestCase(TestCase):
    def test_or(self):
        t = newTable(
            intCol("A", 1, 2, 3, 4, 5),
            intCol("B", 11, 12, 13, 14, 15)
        )

        t_target = newTable(
            intCol("A", 1, 4, 5),
            intCol("B", 11, 14, 15)
        )

        t_actual = t.where(or_("A<2", "B>=14"))

        diff_string = diff(t_actual, t_target, 1)
        self.assertEqual("", diff_string)


if __name__ == '__main__':
    unittest.main()