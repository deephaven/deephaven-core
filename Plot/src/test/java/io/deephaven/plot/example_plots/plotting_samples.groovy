/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

//auto imported

import io.deephaven.time.DateTime
import io.deephaven.engine.util.TableTools

import static io.deephaven.plot.colors.ColorMaps.heatMap
import static io.deephaven.plot.colors.ColorMaps.rangeMap


/**
 * Simple open high low close chart
 */

time = 1491946585000000000L

//Sample plot OHLCChart
date = [new DateTime(time + DAY * 1),
        new DateTime(time + DAY * 2),
        new DateTime(time + DAY * 3),
        new DateTime(time + DAY * 4)] as DateTime[]
open = [3, 4, 3, 5]as Number[]
high = [5, 6, 5, 7]as Number[]
low = [2, 3, 1, 4]as Number[]
close = [4, 5, 4, 6]as Number[]
ohlcChart = figure().figureTitle("OHLC Chart").ohlcPlot("OHLC", date, open, high, low, close).show()
////////////////////////////////////////////////////////////////////////////////////////////////////


//Sample plotPieChartArray.java
categories = ["Samsung", "Others", "Nokia", "Apple"]as String[]
values = [27.8, 55.3, 16.8, 17.1]as Number[]
pieChart = figure().figureTitle("Pie Chart").xLabel("X").yLabel("Y").piePlot("Phones", categories,values).show()
////////////////////////////////////////////////////////////////////////////////////////////////////


//Sample Area Chart
red = java.awt.Color.decode("#d62728")
darkBlue = java.awt.Color.decode("#1f77b4")
lighterRed = colorRGB(red.getRed(), red.getGreen(), red.getBlue(), 50)
lighterDarkBlue = colorRGB(darkBlue.getRed(), darkBlue.getGreen(), darkBlue.getBlue(), 100)
date1 = [
        new DateTime(time + DAY * 2),
        new DateTime(time + DAY * 4),
        new DateTime(time + DAY * 5),
        new DateTime(time + DAY * 8),
        new DateTime(time + DAY * 9),
        new DateTime(time + DAY * 11),
        new DateTime(time + DAY * 12),
        new DateTime(time + DAY * 13),
        new DateTime(time + DAY * 14),
        new DateTime(time + DAY * 15),
        new DateTime(time + DAY * 18),
        new DateTime(time + DAY * 19),
        new DateTime(time + DAY * 21),
        new DateTime(time + DAY * 22),
        new DateTime(time + DAY * 23),
        new DateTime(time + DAY * 24),
        new DateTime(time + DAY * 25),
        new DateTime(time + DAY * 28),
        new DateTime(time + DAY * 30),
        new DateTime(time + DAY * 31),
        new DateTime(time + DAY * 32),
        new DateTime(time + DAY * 33),
        new DateTime(time + DAY * 36),
        new DateTime(time + DAY * 38),
        new DateTime(time + DAY * 40),
        new DateTime(time + DAY * 43),
        new DateTime(time + DAY * 44),
        new DateTime(time + DAY * 46)] as DateTime[]
date3 = [new DateTime(time + DAY),
         new DateTime(time + DAY * 3),
         new DateTime(time + DAY * 4),
         new DateTime(time + DAY * 6),
         new DateTime(time + DAY * 8),
         new DateTime(time + DAY * 10),
         new DateTime(time + DAY * 11),
         new DateTime(time + DAY * 13),
         new DateTime(time + DAY * 15),
         new DateTime(time + DAY * 17),
         new DateTime(time + DAY * 18),
         new DateTime(time + DAY * 19),
         new DateTime(time + DAY * 20),
         new DateTime(time + DAY * 21),
         new DateTime(time + DAY * 23),
         new DateTime(time + DAY * 24),
         new DateTime(time + DAY * 26),
         new DateTime(time + DAY * 27),
         new DateTime(time + DAY * 28),
         new DateTime(time + DAY * 30),
         new DateTime(time + DAY * 32),
         new DateTime(time + DAY * 33),
         new DateTime(time + DAY * 34),
         new DateTime(time + DAY * 36),
         new DateTime(time + DAY * 38),
         new DateTime(time + DAY * 40),
         new DateTime(time + DAY * 42),
         new DateTime(time + DAY * 43),
         new DateTime(time + DAY * 44),
         new DateTime(time + DAY * 46)] as DateTime[]
y1 = [100,
         102,
         98,
         101,
         101,
         102,
         103,
         104,
         105,
         106,
         103,
         105,
         107,
         108,
         105,
         109,
         110,
         113,
         115,
         114,
         114,
         114,
         113,
         116,
         117,
         118,
         119,
         123] as Number[]
y3 = [100,
         102,
         98,
         97,
         98,
         99,
         96,
         95,
         92,
         93,
         90,
         89,
         88,
         86,
         88,
         85,
         85,
         86,
         83,
         81,
         82,
         80,
         81,
         79,
         78,
         77,
         78,
         76,
         76,
         75] as Number[]
y1Higher = new Number[y1.length]
y1Lower = new Number[y1.length]
for(ii = 0; ii < y1.length; ii++) {
    d = y1[ii].doubleValue()
    y1Higher[ii] = d + ((2 + ii) * 0.3)
    y1Lower[ii] = d - ((2 + ii) * 0.3)
}
y3Higher = new Number[y3.length]
y3Lower = new Number[y3.length]
for(ii = 0; ii < y3.length; ii++) {
    d = y3[ii].doubleValue()
    y3Higher[ii] = d + ((2 + ii) * 0.3)
    y3Lower[ii] = d - ((2 + ii) * 0.3)
}
fig = figure().plotStyle(PlotStyle.LINE).yLabel("Predicted Index")
        .plot("S1", date3,y3).pointsVisible(false)
        .plot("S2", date1,y1).pointsVisible(false)
axs2 = fig.twin()
PrettyChart = axs2.plotStyle(PlotStyle.AREA)
        .plot("S3", date3,y3Lower).seriesColor(colorRGB(250, 250, 250))
        .plot("S4", date3,y3Higher).seriesColor(lighterRed)
        .plot("S5", date1,y1Lower).seriesColor(colorRGB(250, 250, 250))
        .plot("S6", date1,y1Higher).seriesColor(lighterDarkBlue)
        .legendVisible(false)
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////


//Sample Scatter Plot Matrix
x1 = [1, 2, 3, 4]as Number[]
x2 = [2, 3, 1, 9]as Number[]
x3 = [0, 1.5, 4.5, 7.5]as Number[]
x4 = [1.3, 3.2, 3.4, 3.8]as Number[]
t = TableTools.newTable(TableTools.col("x1", x1),
        TableTools.col("x2", x2),
        TableTools.col("x3", x3),
        TableTools.col("x4", x4))
scatterPlotMatrix = scatterPlotMatrix(t, "x1", "x2", "x3", "x4").show()
////////////////////////////////////////////////////////////////////////////////////////////////////


//Sample bar chart
x1 = ["A", "B", "C", "D"]as String[]
y1 = [2, 3, 1, 9]as Number[]
x2 = ["A", "C", "Z"]as String[]
y2 = [1.3, 3.2, 3.4]as Number[]
SimpleCategoryPlotChart = figure().figureTitle("Category Bar Chart")
        .xLabel("X")
        .yLabel("Y")
        .catPlot("S1", x1,y1).lineColor(colorRGB(100,100,100))
        .catPlot("S2", x2,y2)
        .axis(0).axisLabelFont(font("Courier", "BOLD_ITALIC", 25))
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////

//Sample categorical line chart
x1 = ["A", "B", "C", "D", "E"]as String[]
y1 = [2, 3, 1, 9, 5]as Number[]
x2 = ["A", "B", "C", "E"]as String[]
y2 = [1.3, 3.2, 3.4, -1]as Number[]
SimpleCategoryPlot2Chart = figure().figureTitle("Category Line Chart")
        .xLabel("X")
        .yLabel("Y")
        .plotStyle(PlotStyle.LINE)
        .catPlot("S1", x1, y1).lineColor(colorRGB(100, 100, 100))
        .twin()
        .catPlot("S2", x2, y2)
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////

//Sample Histogram
x1 = [1, 2, 2, 3, 3, 3, 4]as Number[]
t = TableTools.newTable(TableTools.col("x1", x1))
SimpleHistoTableChart = figure().figureTitle("Histogram").histPlot("Histogram", t, "x1", 4).pointColor("red").show()
////////////////////////////////////////////////////////////////////////////////////////////////////

//Sample Time Series Plot
x1 = [new DateTime(time),
      new DateTime(time + MINUTE),
      new DateTime(time + 2 * MINUTE),
      new DateTime(time + 3 * MINUTE)] as DateTime[]
y1 = [2, 3, 1, 9] as Number[]
x2 = [new DateTime(time + MINUTE),
      new DateTime(time + (3 * MINUTE)),
      new DateTime(time + (4 * MINUTE))] as DateTime[]
y2 = [1.3, 3.2, 3.4] as Number[]
SimpleTsDBDatePlotChart = figure().figureTitle("Time Series")
        .plot("S1", x1,y1)
        .plot("S2", x2,y2)
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////

//Sample Time Series Plot- Dates formatted as yyyy-MM-dd
x1 = [new Date(2016-1900,11,12),
         new Date(2016-1900,12,31),
         new Date(2017-1900,1,2),
         new Date(2017-1900,3,3)] as Date[]
y1 = [2, 3, 1, 9]as Number[]
x2 = [new Date(2016-1900,6,14),
         new Date(2016-1900, 12, 12),
         new Date(2017-1900,4,1)] as Date[]
y2 = [1.3, 3.2, 3.4]as Number[]
SimpleTsPlotChart = figure().figureTitle("Time Series Plot- Custom Date Formatting")
        .figureTitleColor("RED")
        .figureTitleFont(font("Arial", "B", 30))
        .chartTitle("Dates Formatted as yyyy-MM-dd")
        .xLabel("Time")
        .yLabel("Values")
        .plot("S1", x1,y1)
        .plot("S2", x2,y2)
        .xFormatPattern("yyyy-MM-dd")
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////

//Sample Area Plot 2
x1 = [1, 2, 3, 4]as Number[]
y1 = [2, 3, 1, 9]as Number[]
x2 = [0, 1.5, 4.5]as Number[]
y2 = [1.3, 3.2, 3.4]as Number[]
AreaChart = figure().figureTitle("Sample Area Plot 2")
        .plotStyle(PlotStyle.AREA)
        .plot("S1", x1, y1)
        .plot("S2", x2, y2)
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////

// Sample XY Bar chart
x1 = [1, 2, 3, 4]as Number[]
y1 = [2, 3, 1, 9]as Number[]
x2 = [0, 1.5, 4.5]as Number[]
y2 = [1.3, 3.2, 3.4]as Number[]
SimpleXYBarChart = figure().chartTitle("Numerical Bar Chart")
        .plotStyle("BAR")
        .plot("S1", x1, y1).pointColor("red")
        .plot("S2", x2, y2).pointColor("green")
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////

//Sample XY plot with points different colors
x1 = [1, 2, 3, 4]as Number[]
y1 = [2, 3, 1, 9]as Number[]
c1 = [COLOR_RED, COLOR_RED, COLOR_YELLOW, COLOR_YELLOW]as io.deephaven.gui.color.Color[]
x2 = [0, 1.5, 4.5]as Number[]
y2 = [1.3, 3.2, 3.4]as Number[]
SimpleXYColorChart = figure()
        .plot("S1", x1,y1).pointColor(COLOR_RED, COLOR_RED, COLOR_YELLOW, COLOR_YELLOW).pointsVisible(true)
        .plot("S2", x2,y2).pointsVisible(true).pointColor(COLOR_GREEN)
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////

//Sample XY Histogram
x1 = [1, 2, 2, 3, 3, 3, 4]as Number[]
SimpleXYHistoChart = figure().figureTitle("Numerical Histogram")
        .histPlot("Test1", x1, 4).pointColor("green")
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////

//Sample- Changing Axis Ticks
x1 = [1, 2, 3, 4]as Number[]
y1 = [2, 3, 1, 9]as Number[]
x2 = [0, 1.5, 15.5]as Number[]
y2 = [1.3, 3.2, 3.4]as Number[]
SimpleXYPlotChartTicksChanged = figure().figureTitle("Modified Tick Positions")
        .xTicks([0.0, 1.0, 3.0, 5.0, 60.0] as double[])
        .xLabel("X")
        .yLabel("Y")
        .plot("S1", x1,y1)
        .plot("S2", x2,y2)
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////

//Sample plotSimpleXYPlot2.java
x1 = [1, 2, 3, 4]as Number[]
y1 = [2, 3, 1, 9]as Number[]
x2 = [10, 11.5, 14.5]as Number[]
y2 = [21.3, 23.2, 23.4]as Number[]
SimpleXYPlot2Chart = figure().xLabel("X1")
        .yLabel("Y1")
        .plot("S1", x1,y1)
        .newAxes()
        .xLabel("X2")
        .yLabel("Y2")
        .plot("S2", x2,y2)
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////

//Sample Logarithmic plot
x1 = [1, 2, 3, 4]as Number[]
y1 = [2, 3, 1, 9]as Number[]
x2 = [10, 11.5, 14.5]as Number[]
y2 = [1.3, 123.2, 1223.4]as Number[]
SimpleXYPlot2bChart = figure().figureTitle("Logarithmic Chart")
        .xLabel("X1")
        .yLabel("Y1")
        .xLog()
        .plot("S1", x1,y1)
        .newAxes()
        .xLabel("X2")
        .yLabel("Y2")
        .yLog()
        .plot("S2", x2,y2)
SimpleXYPlot2bChart = fig.show()
////////////////////////////////////////////////////////////////////////////////////////////////////

//Sample Plot with Modified LineStyle
thickDash = lineStyle(2.0, LineStyle.LineEndStyle.BUTT, LineStyle.LineJoinStyle.MITER, 10.0)
simpleXYPlot4 = figure()
        .figureTitle("Line Modified")
        .figureTitleFont(font("Courier", "PLAIN", 40))
        .xMin(0)
        .xMax(10)
        .plot("Func0", {x->-5})
        .plot("Func1", {x->-4})
        .plot("Func2", {x->-3})
        .plot("Func3", {x->x*x}).lineStyle(thickDash)
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////

//Sample: Many Carts in one figure
x1 = [1, 2, 3, 4]as Number[]
y1 = [2, 3, 1, 9]as Number[]
x2 = [0, 1.5, 4.5]as Number[]
y2 = [1.3, 3.2, 3.4]as Number[]
x3 = [10, 11.5, 12.5]as Number[]
y3 = [11.3, 2.2, 8.4]as Number[]
fig = figure(2,2)
        .newChart(0).rowSpan(2)
        .chartTitle("Chart 1")
        .chartTitleFont(font("Courier", "PLAIN", 40))
        .plotStyle("scatter")
        .plot("S1", x1, y1)
fig = fig.newChart(0, 1)
        .chartTitle("Chart 1")
        .chartTitleFont(font("Courier", "PLAIN", 40))
        .plotStyle("scatter")
        .plot("S1", x2, y2)
fig = fig.newChart(1, 1)
        .chartTitle("My 3")
        .chartTitleFont(font("Courier", "PLAIN", 40))
        .plotStyle("scatter")
        .plot("S1", x3, y3)
MultipleCharts  = fig.show()
////////////////////////////////////////////////////////////////////////////////////////////////////

//Sample: Point Labels
x = [1, 2, 3, 4]as Number[]
y = [2, 3, 1, 9]as Number[]
labels = ["A", "B", "C"]as String[]
PointLabels = figure().plotStyle("scatter").plot("Test1", x, y).pointLabel(labels).show()
////////////////////////////////////////////////////////////////////////////////////////////////////

//Sample: Point Size
x1 = [1, 2, 3, 4]as Number[]
y1 = [2, 3, 1, 9]as Number[]
s1 = [1.0, Double.NaN, 2.0, 2.0] as double[]
x2 = [0, 1.5, 4.5]as Number[]
y2 = [1.3, 3.2, 3.4]as Number[]
x3 = [0, 1, 2]as Number[]
y3 = [1.5, 2.8, 4.5]as Number[]
SampleXYSizeChart = figure().figureTitle("Sample Size Chart")
        .plotStyle("scatter")
        .plot("S1", x1,y1).pointSize(s1)
        .plot("S2", x2,y2).pointSize(0.5d)
        .plot("S3", x3,y3)
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////

//Sample XY Stacked Area chart
x1 = [5.0, 10.0, 15.0 , 20.0]as Number[]
y1 = [5.0, 15.5, 9.5, 7.5]as Number[]
y2 = [7.0, 15.5, 9.5, 3.5]as Number[]
SampleXYStackedAreaChart = figure().figureTitle("Stacked Area Chart")
        .xColor("blue").yColor("red")
        .plotStyle(PlotStyle.STACKED_AREA)
        .plot("S1", x1, y1)
        .plot("S2", x1, y2)
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////

//Sample XY Plot from a Table
x1 = [1, 2, 3, 4]as Number[]
y1 = [2, 3, 1, 9]as Number[]
x2 = [0, 1.5, 4.5, Double.NaN]as Number[]
y2 = [1.3, 3.2, 3.4, Double.NaN]as Number[]
t = TableTools.newTable(TableTools.col("x1", x1),
        TableTools.col("y1", y1),
        TableTools.col("x2", x2),
        TableTools.col("y2", y2))
SimpleXYTableChart = figure().figureTitle("Plotting Table Data")
        .plot("S1", t, "x2","y2")
        .plot("S2", t, "x1","y1")
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////

//Sample: Changing the Number of Axis Ticks Drawn
x1 = [1, 2, 3, 4]as Number[]
y1 = [2, 3, 1, 9]as Number[]
x2 = [0, 1.5, 4.5]as Number[]
y2 = [1.3, 3.2, 3.4]as Number[]
SimpleXYTicksChart = figure().xTicks(2)
        .xMinorTicks(3)
        .yTicksVisible(false)
        .yMinorTicks(3)
        .plot("S1", x1,y1)
        .plot("S2", x2,y2).seriesColor(colorRGB(0, 255, 255))
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////



//simplecolormaps.java
x = new Number[50]
y = new Number[x.length]
y2 = new Number[x.length]
randy = new Random()

for (ii = 0; ii < x.length; ii++) {
    x[ii] = ii
    pos = (int) randy.nextInt(2)
    d = randy.nextDouble() % 1
    y[ii] = ii + (pos < 1 ? d : -d)
    y2[ii] = y[ii].doubleValue() - 10
}

ColorMapPlot = figure().figureTitle("Color Maps").plotStyle("scatter")
        .plot("Test1", x, y).pointColorByY(heatMap(0, 50, COLOR_GREEN, COLOR_RED))

ranges = [new Range(Double.NEGATIVE_INFINITY, 10, true, false),
          new Range(10, 30, true, false), new Range(30, Double.POSITIVE_INFINITY, true, true)] as Range[]

newColors = [COLOR_BLUE, COLOR_GREEN, COLOR_RED] as io.deephaven.gui.color.Color[]
colorMap = [(ranges[0]):(newColors[0]), (ranges[1]):(newColors[1]), (ranges[2]):(newColors[2])]
ColorMapPlot = ColorMapPlot.plot("S1", x, y2).pointColorByY(rangeMap(colorMap))
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////

//liveplot1
t = TableTools.timeTable("00:00:01")
        .updateView("I=i","J=sin(I*3.14/25)")
LivePlot = plot("S1",t,"I","J").show()
////////////////////////////////////////////////////////////////////////////////////////////////////


//liveplot2
t = TableTools.timeTable("00:00:01")
        .updateView("I=i","J=sin(I*3.14/25)","K=sin(I*3.14/25+3.14/4)")
LivePlot2 = figure().updateInterval(500)
        .plot("S1",t,"I","J")
        .plot("S2",t,"I","K")
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////


//business transform
x = new DateTime[500]
y = new double[500]
now = 1493305755000000000L
for(ii = 0; ii < 250; ii++) {
    now = now + MINUTE
    x[ii] = new DateTime(now)
    y[ii] = Math.sin(ii);
}

now = 1493305755000000000L + DAY
for(ii = 250; ii < x.length; ii++) {
    now = now + MINUTE
    x[ii] = new DateTime(now)
    y[ii] = Math.sin(ii)
}

businessPlot = figure(2,1)
        .chartTitle("Business")
        .xTransform(new io.deephaven.plot.axistransformations.AxisTransformBusinessCalendar(CALENDAR_USNYSE))
        .plot("Business", x, y)
        .newChart(1)
        .chartTitle("NonBusiness")
        .plot("NonBusiness", x, y)
        .show()
////////////////////////////////////////////////////////////////////////////////////////////////////


//one click
t = TableTools.timeTable("00:00:01")
        .updateView("Cat = i%2 == 0 ? `A` : `B`", "I=i", "J = i % 2 == 0 ? sin(I*3.14/10) : cos(I*3.14/10)")
LivePlot = plot("S1",oneClick(t, "Cat"),"I","J").show()