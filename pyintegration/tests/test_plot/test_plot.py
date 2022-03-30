#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import unittest

from deephaven2 import read_csv
from deephaven2.plot import axis_transform_names, axis_transform_by_name, DecimalAxisFormat, \
    NanosAxisFormat
from deephaven2.plot import Shape
from deephaven2.plot import PlotStyle
from deephaven2.plot import Figure
from deephaven2.time import TimeZone
from tests.testbase import BaseTestCase


class PlotTestCase(BaseTestCase):
    def setUp(self):
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None

    def test_plot_style(self):
        figure = Figure()
        new_f = figure.plot_xy("plot1", self.test_table, x="a", y="b")
        axes = new_f.axes(axes=0, plot_style=PlotStyle.PIE)
        self.assertIsNotNone(axes)

    def test_shape(self):
        figure = Figure()
        new_f = figure.plot_xy("plot1", self.test_table, x="a", y="b")
        new_p = new_f.point(shape=Shape.SQUARE, size=10, label="Big Point")
        self.assertIsNotNone(new_p)

    def test_axis_transform(self):
        figure = Figure()
        new_f = figure.plot_xy("plot1", self.test_table, x="a", y="b")
        for name in axis_transform_names():
            axis = new_f.axis(transform=axis_transform_by_name(name))
            self.assertIsNotNone(axis)

    def test_axis_format(self):
        figure = Figure()
        new_f = figure.plot_xy("plot1", self.test_table, x="a", y="b")

        dec_aix_format = DecimalAxisFormat()
        axis = new_f.axis(format=dec_aix_format)
        self.assertIsNotNone(axis)

        nanos_aix_format = NanosAxisFormat()
        axis = new_f.axis(format=nanos_aix_format)
        self.assertIsNotNone(axis)

        nanos_aix_format = NanosAxisFormat(tz=TimeZone.PT)
        nanos_aix_format.set_pattern("yyyy-MM-dd'T'HH:mm")
        axis = new_f.axis(format=nanos_aix_format)
        self.assertIsNotNone(axis)


if __name__ == '__main__':
    unittest.main()
