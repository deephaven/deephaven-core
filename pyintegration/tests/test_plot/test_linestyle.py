#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import random
import unittest

from deephaven2 import read_csv
from deephaven2.plot import color
from deephaven2.plot.figure import Figure
from deephaven2.plot.linestyle import LineEndStyle, LineJoinStyle, LineStyle
from tests.testbase import BaseTestCase


class LineStyleTestCase(BaseTestCase):
    def setUp(self):
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None

    def test_default_line_style(self):
        line_style = LineStyle()
        self.assertIsNotNone(line_style)

    def test_line_style_no_dash_pattern(self):
        for end_style in LineEndStyle:
            for join_style in LineJoinStyle:
                with self.subTest("No dash pattern."):
                    line_style = LineStyle(width=random.random(), end_style=end_style,
                                           join_style=join_style)
                    self.assertIsNotNone(line_style)

    def test_line_style_dash_pattern(self):
        dash_pattern = [1, 2, 3, 5.0]

        for end_style in LineEndStyle:
            for join_style in LineJoinStyle:
                with self.subTest("No dash pattern."):
                    line_style = LineStyle(width=random.random(), end_style=end_style,
                                           join_style=join_style, dash_pattern=dash_pattern)
                    self.assertIsNotNone(line_style)

    def test_line(self):
        figure = Figure()
        new_f = figure.plot_xy("plot1", self.test_table, x="a", y="b")
        line = new_f.line(color=color.RED, style=LineStyle(width=1.0, end_style=LineEndStyle.ROUND))
        self.assertIsNotNone(line)


if __name__ == '__main__':
    unittest.main()
