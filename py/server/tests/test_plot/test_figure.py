#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import read_csv, DHError
from deephaven.plot import Figure
from tests.testbase import BaseTestCase


class FigureTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None
        super().tearDown()

    def test_figure(self):
        with self.subTest("this should work too"):
            figure = Figure()
            new_f = figure.plot_xy("plot1", self.test_table, x="a", y="b", by=['e'])
            self.assertIsNotNone(new_f)

        with self.subTest("this should work"):
            figure = Figure()
            new_f = figure.plot_xy("plot1", self.test_table, x="a", y="b")
            plot1 = new_f.show()
            new_f = figure.plot_xy("plot2", x=[1, 2, 3], y=[1.0, 2.0, 3.0])
            plot2 = new_f.show()
            self.assertIsNotNone(new_f)


if __name__ == '__main__':
    unittest.main()
