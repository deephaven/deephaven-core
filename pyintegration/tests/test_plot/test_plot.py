#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import unittest

from deephaven2 import read_csv
from deephaven2.plot import PlotStyle, Shape
from deephaven2.plot.figure import Figure
from tests.testbase import BaseTestCase


class PlotTestCase(BaseTestCase):
    def setUp(self):
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None

    def test_plot_style(self):
        figure = Figure()
        new_f = figure.plot_xy("plot1", self.test_table, x="a", y="b")
        # TODO got an exception, potentially an autogen issue
        # axes = new_f.axes(name="X", axes=0, plot_style=PlotStyle.PIE)
        axes = new_f.axes(plot_style=PlotStyle.PIE)
        self.assertIsNotNone(axes)

    def test_shape(self):
        figure = Figure()
        new_f = figure.plot_xy("plot1", self.test_table, x="a", y="b")
        new_p = new_f.point(shape=Shape.SQUARE, size=10, label="Big Point")
        self.assertIsNotNone(new_p)


if __name__ == '__main__':
    unittest.main()
