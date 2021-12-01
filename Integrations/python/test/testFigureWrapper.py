#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#


##############################################################################
# NOTE: the jvm should have been initialized, or this test will certainly fail
##############################################################################

import sys
import jpy

from deephaven import TableTools, Aggregation, Plot, Calendars
from deephaven.Plot import figure_wrapper

_JArrayList = jpy.get_type("java.util.ArrayList")

if sys.version_info[0] < 3:
    import unittest2 as unittest
    # not part of the standard library, installed via pip (or the like)
    # it provides backward compatibility with python3 style subTest context manager (handy for complex tests)
else:
    import unittest


class TestFigureWrapper(unittest.TestCase):
    """
    Test cases for the deephaven.Plot.figure_wrapper module
    """

    @classmethod
    def setUpClass(self):
        """
        Inherited method allowing initialization of test environment
        """
        self.table = TableTools.emptyTable(200).update("timestamp=new DateTime((long)(i/2)*1000000000)",
                                                       "Sym=((i%2 == 0) ? `MSFT` : `AAPL`)",
                                                       "price=(double)((i%2 == 0) ? 100.0 + (i/2) + 5*Math.random() : 250.0 + (i/2) + 10*Math.random())")
        # TODO: maybe we should test the direct data plotting functionality? vs table reference?

    def testBasicMethods(self):
        """
        Test suite for some basic FigureWrapper methods
        """
        figure1, figure2, figure3, figure4 = None, None, None, None
        with self.subTest(msg="FigureWrapper()"):
            figure1 = figure_wrapper.FigureWrapper()
        with self.subTest(msg="FigureWrapper(int, int)"):
            figure2 = figure_wrapper.FigureWrapper(1, 2)
        with self.subTest(msg="FigureWrapper.show()"):
            figure4 = figure2.show()  # NB: figure3.figure_ is a FigureWidget versus Figure...
        with self.subTest(msg="FigureWrapper.getWidget()"):
            # NB: method name should have been switched to getWidget() from getwidget()
            self.assertIsNone(figure2.getWidget())
            self.assertIsNotNone(figure4.getWidget())

        # TODO: I'm fairly sure that this is not working as I would hope...I can't call figure3.show()
        with self.subTest(msg="FigureWrapper(figure=figure)"):
            figure3 = figure_wrapper.FigureWrapper(figure=figure2)

        # tidy up by destroying these objects - probably only necessary after show, but JIC
        del figure1, figure2, figure3, figure4
        # NB: setting to None should also do it, where that is more convenient

    def testBaseFigure(self):
        """
        Test suite for methods inherited from BaseFigure
        """

        figure = figure_wrapper.FigureWrapper(2, 2)
        with self.subTest(msg="figureTitle(string)"):
            figure = figure.figureTitle("Super Title")
        with self.subTest(msg="figureTitleFont(string, string, int)"):
            figure = figure.figureTitleFont("Arial", "B", 24)
        with self.subTest(msg="figureTitleColor(string)"):
            figure = figure.figureTitleColor("#FF0000")  # named color or RGB hex-string
        with self.subTest(msg="figureTitleColor(Paint)"):
            figure = figure.figureTitleColor(Plot.colorRGB(0.0, 1.0, 0.0))  # create an RGB color using plot convenience function
        with self.subTest(msg="updateInterval(long)"):
            figure = figure.updateInterval(1000)  # in milliseconds

        # Maybe the wrapping for these is dumb?
        chart1, chart2, chart3 = None, None, None
        with self.subTest(msg="newChart()"):
            chart1 = figure.newChart()
        with self.subTest(msg="newChart(int)"):
            chart2 = figure.newChart(0)
        with self.subTest(msg="newChart(int, int)"):
            chart3 = figure.newChart(0, 1)

        with self.subTest(msg="chart(int)"):
            chart1 = chart2.chart(0)
        with self.subTest(msg="chart(int, int)"):
            chart1 = chart3.chart(0, 1)

        with self.subTest(msg="removeChart(int, int)"):
            chart1 = chart3.removeChart(0, 1)
        with self.subTest(msg="removeChart(int)"):
            chart1 = chart2.removeChart(0)
        del chart1, chart2, chart3

        # I have to put a series in here
        figure = figure.plot("Microsoft", self.table.where("Sym=`MSFT`"), "timestamp", "price")

        with self.subTest(msg="figureRemoveSeries(*string)"):
            figure = figure.figureRemoveSeries("Microsoft")
        del figure

    def testPlottingMethods(self):
        """
        Test suite for the plotting methods inherited from Axes
        """

        figure = figure_wrapper.FigureWrapper(1, 1)
        with self.subTest("plot"):
            figure = figure.plot("XY Series", self.table.where("Sym=`MSFT`"), "timestamp", "price")

        figure = figure_wrapper.FigureWrapper(1, 1)
        with self.subTest("catPlot"):
            figure = figure.catPlot("Category", self.table, "Sym", "price")

        figure = figure_wrapper.FigureWrapper(1, 1)
        with self.subTest("histPlot"):
            figure = figure.histPlot("Histogram", self.table.where("Sym=`MSFT`"), "price", 10)

        figure = figure_wrapper.FigureWrapper(1, 1)
        with self.subTest("catHistPlot"):
            figure = figure.catHistPlot("Category Histogram", self.table, "Sym")

        figure = figure_wrapper.FigureWrapper(1, 1)
        with self.subTest("piePlot"):
            figure = figure.piePlot("Pie", self.table.aggBy(Aggregation.AggAvg("price"), "Sym"), "Sym", "price")

        figure = figure_wrapper.FigureWrapper(1, 1)
        with self.subTest("ohlcPlot"):
            # dumbest ohlc ever
            figure = figure.ohlcPlot("OHLC", self.table.where("Sym=`MSFT`"), "timestamp", "price", "price", "price", "price")

        figure = figure_wrapper.FigureWrapper(1, 1)
        with self.subTest("errorBarX"):
            figure = figure.errorBarX("Error X", self.table.where("Sym=`MSFT`"), "timestamp", "price", "timestamp", "timestamp")

        figure = figure_wrapper.FigureWrapper(1, 1)
        with self.subTest("errorBarY"):
            figure = figure.errorBarY("Error Y", self.table.where("Sym=`MSFT`"), "timestamp", "price", "price", "price")

        figure = figure_wrapper.FigureWrapper(1, 1)
        with self.subTest("errorBarXY"):
            figure = figure.errorBarXY("Error XY", self.table.where("Sym=`MSFT`"), "timestamp", "timestamp", "timestamp", "price", "price", "price")

        figure = figure_wrapper.FigureWrapper(1, 1)
        aggs = [
            Aggregation.AggAvg("avgPrice=price"),
            Aggregation.AggMin("minPrice=price"),
            Aggregation.AggMax("maxPrice=price")]
        j_agg_list = _JArrayList()
        for agg in aggs:
            j_agg_list.add(agg)

        with self.subTest("catErrorBar"):
            figure = figure.catErrorBar("Cat Error Bar",
                                        self.table.aggBy(j_agg_list,"Sym"),
                                        "Sym", "avgPrice", "minPrice", "maxPrice")
        del figure

    def testAxesMethods(self):
        """
        Test suite for methods for non-plotting methods inherited from Axes
        """

        # TODO: x/yTransform(AxisTransform)?, x/yBusinessTime(BusinessCalendar)?

        figure = figure_wrapper.FigureWrapper()  # is there an axes at this point?

        axis = None
        # maybe the wrapping for these is dumb?
        with self.subTest(msg="axis fetchers"):
            axis = figure.axis(0)
            axis = figure.xAxis()
            axis = figure.yAxis()
        del axis

        axes = None
        # maybe the wrapping for these is dumb?
        with self.subTest(msg="twin axis methods"):
            axes = figure.twin()
            axes = figure.twin("new")
            axes = figure.twin(0)
            axes = figure.twin("new", 0)
            axes = figure.twinX()
            axes = figure.twinX("new")
            axes = figure.twinY()
            axes = figure.twinY("new")
        del axes

        with self.subTest(msg="axis formatter methods"):
            figure = figure.xFormatPattern("###,###.00").yFormatPattern("###,###.00")
        with self.subTest(msg="axis color methods"):
            figure = figure.xColor("#202020").yColor("#202020")
            figure.xColor(Plot.colorRGB(1.0, 0.0, 0.0)).yColor(Plot.colorRGB(1.0, 0.0, 0.0))
        with self.subTest(msg="axis labelling methods"):
            figure = figure.xLabel("x axis").yLabel("y axis")
        with self.subTest(msg="axis label font methods"):
            figure = figure.xLabelFont("Arial", "P", 11).yLabelFont("Arial", "P", 11)
        with self.subTest(msg="axis tick font methods"):
            figure = figure.xTicksFont("Arial", "I", 9).yTicksFont("Arial", "I", 9)
        with self.subTest(msg="axis range methods"):
            figure = figure.xRange(1.0, 10.0).yRange(1.0, 10.0)
            figure.xMin(1.0).yMin(1.0)
            figure.xMax(10.0).yMax(10.0)
        with self.subTest(msg="axis ticks methods"):
            figure = figure.xTicks(1.0).yTicks(1.0)
            figure.xTicks([1.0, 2.5, 5.0, 7.5, 10.0]).yTicks([1.0, 2.5, 5.0, 7.5, 10.0])
        with self.subTest(msg="tick visibility methods"):
            figure = figure.xTicksVisible(True).yTicksVisible(True)
            figure = figure.xMinorTicksVisible(True).yMinorTicksVisible(True)
        with self.subTest(msg="minor ticks"):
            figure = figure.xMinorTicks(2).yMinorTicks(2)
        with self.subTest(msg="tick label angles"):
            figure = figure.xTickLabelAngle(45.0).yTickLabelAngle(45.0)

        with self.subTest(msg="axis business time methods"):
            figure.xBusinessTime().yBusinessTime()

        with self.subTest(msg="axis log methods"):
            figure.xLog().yLog()
        with self.subTest(msg="axis inversion methods"):
            figure = figure.xInvert().yInvert()
            figure = figure.xInvert(True).yInvert(True)

        # I have to put a series in here
        figure = figure.plot("Microsoft", self.table.where("Sym=`MSFT`"), "timestamp", "price")
        with self.subTest(msg="plotStyle"):
            figure = figure.plotStyle("Area")  # does this just apply the style to all applicable series? Or?

        # maybe the wrapping for these is dumb?
        series = None
        with self.subTest(msg="series(int)"):
            series = figure.series(0)  # I'm guessing that the int id starts at 0?
        with self.subTest(msg="series(string"):
            series = figure.series("Microsoft")
        del series

        with self.subTest(msg="axesRemoveSeries(*string)"):
            figure = figure.axesRemoveSeries("Microsoft")

        del figure

    @unittest.skip("These all fail, because no axes is selected. Not presently sure how to resolve?")
    def testAxisMethods(self):
        """
        Test suite for methods inherited from Axis - do these apply said methods to every axis? Seems silly.
        """

        figure = figure_wrapper.FigureWrapper()
        # How do I get it to select an axes?

        with self.subTest(msg="axisColor(string)"):
            figure = figure.axisColor("#000000")
        with self.subTest(msg="axisColor(Paint)"):
            figure = figure.axisColor(Plot.colorRGB(0, 0, 255))
        with self.subTest(msg="axisFormatPattern()"):
            figure = figure.axisFormat("###,###.00")  # decimal formatting pattern
        with self.subTest(msg="axisLabel(string)"):
            figure = figure.axisLabel("axis")  # decimal formatting pattern
        with self.subTest(msg="axisLabelFont(string, string, int)"):
            figure = figure.axisLabelFont("Arial", "P", 11)

        with self.subTest(msg="businessTime()"):
            figure = figure.businessTime()

        with self.subTest(msg="businessTime(calendar)"):
            figure = figure.businessTime(Calendars.calendar())

        with self.subTest(msg="min(double)"):
            figure = figure.min(1.0)
        with self.subTest(msg="max(double)"):
            figure = figure.max(10.0)
        with self.subTest(msg="range(double, double)"):
            figure = figure.range(1.0, 10.0)
        with self.subTest(msg="ticks(double)"):
            figure = figure.ticks(1.0)
        with self.subTest(msg="ticks(double[])"):
            figure = figure.ticks([1.0, 2.5, 5.0, 7.5, 10.0])
        with self.subTest(msg="tickFont(string, string, int)"):
            figure = figure.ticksFont("Arial", "I", 9)
        with self.subTest(msg="ticksVisible(boolean)"):
            figure = figure.ticksVisible(True)
        with self.subTest(msg="tickLabelAngle(double)"):
            figure = figure.tickLabelAngle(45.0)  # I'm guessing degrees?
        with self.subTest(msg="minorTicks(int)"):
            figure = figure.minorTicks(2)
        with self.subTest(msg="minorTicksVisible(boolean)"):
            figure = figure.minorTicksVisible(True)

        with self.subTest(msg="log()"):
            figure = figure.log()
        # TODO: where would I get an AxisTransform object?
        # with self.subTest(msg="transform(AxisTransform)"):
        #     figure = figure.transform(what)

        with self.subTest(msg="invert()"):
            figure = figure.invert()
        with self.subTest(msg="invert(boolean)"):
            figure = figure.invert(False)

        del figure

    def testChartMethods(self):
        """
        Test suite for methods inherited from Chart
        """

        figure = figure_wrapper.FigureWrapper(2, 2)
        with self.subTest(msg="chartTitle(string)"):
            figure = figure.chartTitle("Chart Title")
        with self.subTest(msg="chartTitleColor(string"):
            figure = figure.chartTitleColor("BLUE")
        with self.subTest(msg="chartTitleColor(Paint)"):
            figure = figure.chartTitleColor(Plot.colorRGB(0, 0, 255))
        with self.subTest(msg="chartTitleFont(string, string, int)"):
            figure = figure.chartTitleFont("Arial", "B", 20)
        with self.subTest(msg="span(int, int"):
            figure.span(2, 2)
        with self.subTest(msg="colSpan(int)"):
            figure.colSpan(2)
        with self.subTest(msg="rowSpan(int)"):
            figure.rowSpan(2)

        axes = None
        # maybe the wrapping for these is dumb? Should be returning an axes reference?
        with self.subTest(msg="newAxes()"):
            axes = figure.newAxes()
        with self.subTest(msg="newAxes(string)"):
            axes = figure.newAxes("new_axis")
        with self.subTest(msg="newAxes(int)"):
            axes = figure.newAxes(2)
        with self.subTest(msg="newAxes(string, int)"):
            axes = figure.newAxes("new_axis", 2)
        with self.subTest(msg="axes(string)"):
            axes.axes("new_axis")
        with self.subTest(msg="axes(int)"):
            axes.axes(0)  # I'm assuming that 0 will always work?
        del axes

        # TODO: what are the possibilities here? I'm guessing ["horizontal", "vertical"]? Documentation?
        with self.subTest(msg="plotOrientation(string)"):
            figure = figure.plotOrientation("vertical")

        # I have to put a series in here
        figure = figure.plot("Microsoft", self.table.where("Sym=`MSFT`"), "timestamp", "price")
        with self.subTest(msg="legendVisible(boolean)"):
            figure = figure.legendVisible(True)
        with self.subTest(msg="legendFont(string, string, int)"):
            figure = figure.legendFont("Arial", "P", 8)
        with self.subTest(msg="legendColor(string)"):
            # I'm guessing that this is the background color?
            figure = figure.legendColor("#A0A0A0")
        with self.subTest(msg="legendColor(Paint)"):
            figure = figure.legendColor(Plot.colorRGB(200, 200, 200))
        with self.subTest(msg="chartRemoveSeries(*string)"):
            figure.chartRemoveSeries("Microsoft")
        del figure

    def testDataSeriesMethods(self):
        """
        Test suite for methods inherited from DataSeries
        """

        # TODO: pointColorByY(SerializableFunction)?, pointColorByY(Closure)?

        figure = Plot.plot("Microsoft", self.table.where("Sym=`MSFT`"), "timestamp", "price")
        with self.subTest(msg="linesVisible(boolean)"):
            figure = figure.linesVisible(True)
        with self.subTest(msg="lineColor(Paint)"):
            figure = figure.lineColor(Plot.colorRGB(0.2, 1.0, 0.2))
        with self.subTest(msg="lineStyle(LineStyle)"):
            figure = figure.lineStyle(Plot.lineStyle(4, 4))
        with self.subTest(msg="pointsVisible(boolean)"):
            figure = figure.pointsVisible(True)
        with self.subTest(msg="pointSize(double)"):
            figure = figure.pointSize(2.0)
        with self.subTest(msg="pointLabel(object)"):
            figure = figure.pointLabel("label")
        with self.subTest(msg="pointLabelFormat(string)"):
            figure = figure.pointLabelFormat("{0}: ({1}, {2})")
        with self.subTest(msg="pointShape(string)"):
            figure = figure.pointShape("CIRCLE")
        with self.subTest(msg="seriesColor(Paint)"):
            figure = figure.seriesColor(Plot.colorRGB(0.1, 0.1, 0.1))
        with self.subTest(msg="pointColor(Paint)"):
            figure = figure.pointColor(Plot.colorRGB(1.0, 0.0, 0.0))
        with self.subTest(msg="gradientVisible(boolean)"):
            figure.gradientVisible(False)
        with self.subTest(msg="toolTipPattern(string)"):
            figure = figure.toolTipPattern("###,###.00")
        with self.subTest(msg="xToolTipPattern(string)"):
            figure = figure.xToolTipPattern("###,###.00")
        with self.subTest(msg="yToolTipPattern(string)"):
            figure = figure.yToolTipPattern("###,###.00")
        del figure

    @unittest.skip("what to do?")
    def testCategoryDataseriesMethods(self):
        """
        Test suite for methods inherited from CategoryDataSeries - bah...
        """

        # TODO: this is terrible
        pass

    @unittest.skip("what to do?")
    def testXYDataSeriesMethods(self):
        """
        Test suite for methods inherited from XYDataSeries - bah...
        """

        # TODO: various extensions of pointSize(*args), pointColor(*args), pointLabel(*args), pointShape(*args)
        pass

    @unittest.skip("These all fail with predictable error message. Wrapping appears to be correct, but I'm calling on"
                   "something inappropriate. Not presently sure how to resolve?")
    def testMultiSeries(self):
        """
        Test suite for methods inherited from MultiSeries - bah...
        """

        # NB: the error message:
        #   java.lang.UnsupportedOperationException: Series type does not support this method.
        #   seriesType=class io.deephaven.plot.datasets.xy.XYDataSeriesTableArray
        #   method='@Override public  FigureImpl pointsVisible( java.lang.Boolean visible, java.lang.Object... keys )'

        # TODO: seriesNamingFunction(*args)?,pointColorByY(func, *keys)?
        # TODO: a ton of other call signatures for basically XYDataSeriesMethods

        figure = Plot.plot("Microsoft", self.table.where("Sym=`MSFT`"), "timestamp", "price")\
            .plot("Apple", self.table.where("Sym=`AAPL`"), "timestamp", "price")

        with self.subTest(msg="gradientVisible(boolean, *keys)"):
            figure = figure.gradientVisible(True, "Microsoft")
        with self.subTest(msg="lineColor(Paint/int/string, *keys)"):
            figure = figure.lineColor("RED", "Apple")
        with self.subTest(msg="lineStyle(LineStyle, *keys)"):
            figure = figure.lineStyle(Plot.lineStyle(4.0, 4.0), "Microsoft", "Apple")
        with self.subTest(msg="linesVisible(boolean, *keys)"):
            figure = figure.linesVisible(True, "Microsoft", "Apple")
        with self.subTest(msg="pointColor(Paint/int/string, *keys)"):
            figure = figure.pointColor("BLUE", "Microsoft", "Apple")
        with self.subTest(msg="pointLabel(object, *keys)"):
            figure = figure.pointLabel("label", "Microsoft", "Apple")
        with self.subTest(msg="pointLabelFormat(string, *keys)"):
            figure = figure.pointLabelFormat("{0}: ({1}, {2})", "Microsoft", "Apple")
        with self.subTest(msg="pointShape(string, *keys)"):
            figure = figure.pointShape("SQUARE", "Microsoft", "Apple")
        with self.subTest(msg="pointSize(double, *keys)"):
            figure = figure.pointSize(2.0, "Microsoft", "Apple")
        with self.subTest(msg="pointsVisible(boolean, *keys)"):
            figure = figure.pointsVisible(True, "Microsoft", "Apple")
        with self.subTest(msg="seriesColor(Paint/int/string, *keys)"):
            figure = figure.seriesColor(Plot.colorRGB(255, 0, 0), "Microsoft", "Apple")
        with self.subTest(msg="tool tips"):
            figure = figure.toolTipPattern("###,###.00", "Apple")\
                .xToolTipPattern("###,###.00", "Apple")\
                .yToolTipPattern("###,###.00", "Apple")
        with self.subTest(msg="group(int, *keys)"):
            figure = figure.group(0, "Microsoft", "Apple")
        del figure

