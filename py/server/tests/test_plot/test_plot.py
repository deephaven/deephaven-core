#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import read_csv
from deephaven.plot import axis_transform_names, axis_transform, DecimalAxisFormat, \
    NanosAxisFormat
from deephaven.plot import Shape
from deephaven.plot import PlotStyle
from deephaven.plot import Figure
from deephaven.time import to_j_time_zone
from tests.testbase import BaseTestCase


class PlotTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None
        super().tearDown()

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
            axis = new_f.axis(transform=axis_transform(name))
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

        nanos_aix_format = NanosAxisFormat(tz=to_j_time_zone("PT"))
        nanos_aix_format.set_pattern("yyyy-MM-dd'T'HH:mm")
        axis = new_f.axis(format=nanos_aix_format)
        self.assertIsNotNone(axis)


if __name__ == '__main__':
    unittest.main()
