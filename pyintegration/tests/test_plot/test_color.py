#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import unittest

from deephaven2 import read_csv
from deephaven2.plot import color, Color
from deephaven2.plot.figure import Figure
from deephaven2.plot.linestyle import LineEndStyle, LineStyle
from tests.testbase import BaseTestCase


class ColorTestCase(BaseTestCase):
    def setUp(self):
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None

    def test_color(self):
        figure = Figure()
        new_f = figure.plot_xy("plot1", self.test_table, x="a", y="b")
        line = new_f.line(color=color.RED, style=LineStyle(width=1.0, end_style=LineEndStyle.ROUND))
        self.assertIsNotNone(line)

    def test_color_hsl(self):
        figure = Figure()
        custom_color = Color.of_hsl(h=128, s=58, l=68, alpha=0.6)
        new_f = figure.plot_xy("plot1", self.test_table, x="a", y="b")
        line = new_f.line(color=custom_color, style=LineStyle(width=1.0, end_style=LineEndStyle.ROUND))
        self.assertIsNotNone(line)


if __name__ == '__main__':
    unittest.main()
