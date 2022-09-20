#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import read_csv, DHError
from deephaven.plot import Color, Colors
from deephaven.plot import Figure
from deephaven.plot import LineEndStyle, LineStyle
from tests.testbase import BaseTestCase


class ColorTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None
        super().tearDown()

    def test_color(self):
        figure = Figure()
        new_f = figure.plot_xy("plot1", self.test_table, x="a", y="b")
        line = new_f.line(color=Colors.RED, style=LineStyle(width=1.0, end_style=LineEndStyle.ROUND))
        self.assertIsNotNone(line)

    def test_color_hsl(self):
        figure = Figure()
        custom_color = Color.of_hsl(h=128, s=58, l=68, alpha=0.6)
        new_f = figure.plot_xy("plot1", self.test_table, x="a", y="b")
        line = new_f.line(color=custom_color, style=LineStyle(width=1.0, end_style=LineEndStyle.ROUND))
        self.assertIsNotNone(line)

    def test_color_factory(self):
        Color.of_name("RED")
        Color.of_rgb(12, 16, 188, 200)
        Color.of_rgb_f(0.2, 0.6, 0.88, alpha=0.2)
        Color.of_hsl(h=128, s=58, l=68, alpha=0.6)
        with self.assertRaises(DHError):
            Color.of_name("REDDER")
        with self.assertRaises(DHError):
            Color.of_rgb(12, 16, 288)
        with self.assertRaises(DHError):
            Color.of_rgb_f(1.2, 0.6, 0.88, alpha=0.2)
        with self.assertRaises(DHError):
            Color.of_hsl(h=377, s=58, l=168, alpha=10)


if __name__ == '__main__':
    unittest.main()
