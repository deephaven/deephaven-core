#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import unittest

from deephaven.TableTools import newTable, intCol
from deephaven.filter import or_

from deephaven import Types
from deephaven import read_csv
from unittest import TestCase


class FilterTestCase(TestCase):
    def test_or(self):
        t = newTable(
            intCol("A", 1, 2, 3, 4, 5),
            intCol("B", 11, 12, 13, 14, 15)
        )

        t_target = newTable(
            intCol("A", 3, 4),
            intCol("B", 13, 14)
        )

        t_actual = t.where(or_("A>2", "B<=14"))
        self.assertEqual(t_target, t_actual)


if __name__ == '__main__':
    unittest.main()