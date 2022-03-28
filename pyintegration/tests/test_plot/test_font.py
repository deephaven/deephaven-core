#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import unittest

from deephaven2 import read_csv
from deephaven2.plot import Color
from deephaven2.plot import Figure
from deephaven2.plot import FontFamilyNames, Font, FontStyle
from tests.testbase import BaseTestCase


class FontTestCase(BaseTestCase):
    def setUp(self):
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None

    def test_font_family_names(self):
        self.assertIn("Serif", FontFamilyNames)

    def test_font(self):
        custom_font = Font()
        self.assertIsNotNone(custom_font)
        custom_font = Font(size=16)
        self.assertIsNotNone(custom_font)
        custom_font = Font(style=FontStyle.BOLD_ITALIC)
        self.assertIsNotNone(custom_font)

    def test_chart_tile_with_font(self):
        figure = Figure()
        custom_color = Color.of_hsl(h=128, s=58, l=68, alpha=0.6)
        custom_font = Font(family="SansSerif", style=FontStyle.BOLD_ITALIC, size=18)
        new_f = figure.plot_xy("plot1", self.test_table, x="a", y="b")
        new_chart_title = new_f.chart_title(title="Dummy Char", color=custom_color, font=custom_font)
        self.assertIsNotNone(new_chart_title)


if __name__ == '__main__':
    unittest.main()
