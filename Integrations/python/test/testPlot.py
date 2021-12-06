#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#


##############################################################################
# NOTE: the jvm should have been initialized, or this test will certainly fail
##############################################################################

import sys
import tempfile
import os
import numpy

from deephaven import TableTools, Aggregation

# NB: these two modules cannot even be imported successfully without the jvm initialized
import deephaven.Plot as Plot

if sys.version_info[0] < 3:
    import unittest2 as unittest
else:
    import unittest


class TestPlot(unittest.TestCase):
    """
    Test cases for the deephaven.Plot module
    """

    @classmethod
    def setUpClass(cls):
        """
        Inherited method allowing initialization of test environment
        """
        cls.table = TableTools.emptyTable(200).update("timestamp=new DateTime((long)(i/2)*1000000000)",
                                                      "Sym=((i%2 == 0) ? `MSFT` : `AAPL`)",
                                                      "price=(double)((i%2 == 0) ? 100.0 + (i/2) + 5*Math.random() : 250.0 + (i/2) + 10*Math.random())")

        longs = numpy.arange(0, 86401, 60, dtype=numpy.int64)
        cls.arrays = {
            'DateTime[]': longs.astype('datetime64[s]'),
            'long[]': longs,
            'int[]': longs.astype(numpy.int32),
            'float[]': longs.astype(numpy.float32),
            'double[]': longs.astype(numpy.float64),
        }

    def testColor(self):
        """
        Test suite for color methods
        """

        with self.subTest(msg="color(string)"):
            color = Plot.color("#000000")

        with self.subTest(msg="colorHSL(float, float, float)"):
            color = Plot.colorHSL(0.0, 0.0, 0.0)
        with self.subTest(msg="colorHSL(float, float, float, float)"):
            color = Plot.colorHSL(0.0, 0.0, 0.0, 0.0)

        with self.subTest(msg="colorRGB(int)"):
            color = Plot.colorRGB(0)
        with self.subTest(msg="colorRGB(int, boolean)"):
            color = Plot.colorRGB(0, True)
        with self.subTest(msg="colorRGB(int, int, int)"):
            color = Plot.colorRGB(0, 0, 0)
        with self.subTest(msg="colorRGB(int, int, int)"):
            color = Plot.colorRGB(0, 0, 0, 255)
        with self.subTest(msg="colorRGB(float, float, float)"):
            color = Plot.colorRGB(0.0, 0.0, 0.0)
        with self.subTest(msg="colorRGB(float, float, float, float)"):
            color = Plot.colorRGB(0.0, 0.0, 0.0, 1.0)
        del color

        with self.subTest(msg="colorNames()"):
            names = Plot.colorNames()
            self.assertTrue(isinstance(names, list) and len(names) > 0)
            print("colorNames() values {}".format(names))

    def testFont(self):
        """
        Test suite for font methods
        """

        fstyle = None
        with self.subTest(msg="fontStyle(string)"):
            fstyle = Plot.fontStyle("bold_italic")  # not case sensitive

        with self.subTest(msg="font(string, string, int)"):
            font = Plot.font("Arial", "p", 10)  # (family, style, size)
        with self.subTest(msg="font(string, fontstyle, int)"):
            font = Plot.font("Arial", fstyle, 10)

        with self.subTest(msg="fontFamilyNames()"):
            names = Plot.fontFamilyNames()
            self.assertTrue(isinstance(names, list) and len(names) > 0)
            print("fontFamilyNames() values {}".format(names))
        with self.subTest(msg="fontStyleNames()"):
            names = Plot.fontStyleNames()
            self.assertTrue(isinstance(names, list) and len(names) > 0)
            print("fontStyleNames() values {}".format(names))

    def testLineStyle(self):
        """
        Test suite for line style construction
        """

        with self.subTest(msg="lineEndStyleNames()"):
            names = Plot.lineEndStyleNames()
            self.assertTrue(isinstance(names, list) and len(names) > 0)
            print("lineEndStyleNames() values {}".format(names))

        with self.subTest(msg="lineJoinStyleNames()"):
            names = Plot.lineJoinStyleNames()
            self.assertTrue(isinstance(names, list) and len(names) > 0)
            print("lineJoinStyleNames() values {}".format(names))

        endStyle, joinStyle = None, None
        with self.subTest(msg="lineEndStyle(string)"):  # should be [BUTT, ROUND, SQUARE], not case sensitive
            endStyle = Plot.lineEndStyle("Butt")
        with self.subTest(msg="lineJoinStyle(string)"):  # should be [BEVEL, MITER, ROUND]
            joinStyle = Plot.lineJoinStyle("Bevel")

        with self.subTest(msg="lineStyle(double, lineEndStyle, lineJoinStyle, double...)"):
            ls = Plot.lineStyle(4.0, endStyle, joinStyle, 3.0, 3.0)
        with self.subTest(msg="lineStyle(double, string, string, double...)"):
            ls = Plot.lineStyle(4.0, "butt", "bevel", 3.0, 3.0)
        with self.subTest(msg="lineStyle(double)"):
            ls = Plot.lineStyle(4.0)
        with self.subTest(msg="lineStyle(double, int[])"):
            ls = Plot.lineStyle(4.0, numpy.array([3, 3], dtype=numpy.int32))
        with self.subTest(msg="lineStyle(double, long[])"):
            ls = Plot.lineStyle(4.0, numpy.array([3, 3], dtype=numpy.int64))
        with self.subTest(msg="lineStyle(double, float[])"):
            ls = Plot.lineStyle(4.0, numpy.array([3, 3], dtype=numpy.float32))
        with self.subTest(msg="lineStyle(double, double[])"):
            ls = Plot.lineStyle(4.0, [3.0, 3.0])
        with self.subTest(msg="lineStyle(double...)"):
            ls = Plot.lineStyle(3.0, 3.0)

        with self.subTest(msg="lineStyle(string, string)"):
            ls = Plot.lineStyle("butt", "bevel")
        with self.subTest(msg="lineStyle()"):
            ls = Plot.lineStyle()
        with self.subTest(msg="lineStyle()"):
            ls = Plot.lineStyle()

        # NOTE: These patterns will fail.. [3, 3] -> int[], which doesn't match any function signature
        #   note that [3.0, 3.0] -> double[] would match a (different) function signature
        # with self.subTest(msg="lineStyle(double, lineEndStyle, lineJoinStyle, List<T>)"):
        #     ls = Plot.lineStyle(4.0, endStyle, joinStyle, [3, 3])
        # with self.subTest(msg="lineStyle(double, string, string, List<T>)"):
        #     ls = Plot.lineStyle(4.0, "butt", "bevel", [3, 3])
        # with self.subTest(msg="lineStyle(List<T>)"):
        #     ls = Plot.lineStyle([3, 3])



    def testPlot(self):
        """
        plot method calls - possibly expand over time
        """

        # perform basic Plot.plot(name, x, y), where x, y span every pairwise choice from the self.arrays dictionary
        figure = None
        typs = sorted(self.arrays.keys())
        for i, xtyp in enumerate(typs):
            xarray = self.arrays[xtyp]
            for j, ytyp in enumerate(typs):
                yarray = self.arrays[ytyp]
                with self.subTest(msg="plot({}, {})".format(xtyp, ytyp)):
                    series_name = '{}_{}'.format(xtyp, ytyp)
                    figure = Plot.plot(series_name, xarray, yarray).show()
        del figure

    def testCatPiePlotTroubles(self):

        val = [20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 30, 40, 50, 60]
        cat = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"]
        fig = Plot.piePlot("pp2", cat, val)
        del fig

