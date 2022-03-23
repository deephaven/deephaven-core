#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import random
import unittest

from deephaven2.plot.linestyle import LineEndStyle, LineJoinStyle, LineStyle
from tests.testbase import BaseTestCase


class LineStyleTestCase(BaseTestCase):

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


if __name__ == '__main__':
    unittest.main()
