#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import random
import unittest

from deephaven import read_csv, DHError
from deephaven.plot import Colors
from deephaven.plot import Figure
from deephaven.plot import LineEndStyle, LineJoinStyle, LineStyle
from tests.testbase import BaseTestCase


class LineStyleTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None
        super().tearDown()

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
        line = new_f.line(color=Colors.ANTIQUEWHITE, style=LineStyle(width=1.0, end_style=LineEndStyle.ROUND))
        self.assertIsNotNone(line)

        with self.assertRaises(DHError):
            line = new_f.line(color=Colors.ANTIQUEWHITE,
                              style=LineStyle(width=1.0, end_style=LineEndStyle.ROUND, dash_pattern=[-1]))


if __name__ == '__main__':
    unittest.main()
