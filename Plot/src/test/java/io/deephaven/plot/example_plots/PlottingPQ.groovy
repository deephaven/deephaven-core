/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

/**
 * Automated persistent query test for plotting methods
 */
t = emptyTable(50)
        .update("X = i + 5", "XLow = X -1", "XHigh = X + 1", "Y = Math.random() * 5", "YLow = Y - 1", "YHigh = Y + 1", "USym = i % 2 == 0 ? `AAPL` : `MSFT`")


p = plot("S1", t, "X", "Y").lineColor("black").show()
p2 = plot("S1", t, "X", "Y").plotStyle("bar").gradientVisible(true).show()
p3 = plot("S1", t, "X", "Y").plotStyle("scatter").pointColor("black").pointSize(2).show()
p4 = plot("S1", t, "X", "Y").plotStyle("area").seriesColor("red").show()


p4 = plot3d("S1", t, "X", "X", "Y").show()


pBy = plotBy("S1", t, "X", "Y", "USym").show()
pBy = plot3dBy("S1", t, "X",  "X", "Y", "USym").show()

cp = catPlot("S1", t, "X", "Y").lineColor("black").show()
cp2 = catPlot("S1", t, "X", "Y").plotStyle("bar").gradientVisible(true).show()
cp3 = catPlot("S1", t, "X", "Y").plotStyle("scatter").pointColor("black").pointSize(2).show()
cp4 = catPlot("S1", t, "X", "Y").plotStyle("area").seriesColor("red").show()

cp = catPlot3d("S1", t, "X", "X", "Y").show()

cpBy = catPlotBy("S1", t, "X", "Y", "USym").show()

cpBy = catPlot3dBy("S1", t, "X", "X", "Y", "USym").show()

pp = piePlot("S1", t, "X", "Y")


chp = catHistPlot("S1", t, "X").show()

hp = histPlot("S1", t, "X", 5).show()
hp = histPlot("S1", t, "X", 0, 10, 5).show()

ep = errorBarXY("S1", t, "X", "XLow", "XHigh", "Y", "YLow", "YHigh").show()
epBy = errorBarXYBy("S1", t, "X", "XLow", "XHigh", "Y", "YLow", "YHigh", "USym").show()
ep2 = errorBarX("S1", t, "X", "XLow", "XHigh", "Y").show()
epBy2 = errorBarXBy("S1", t, "X", "XLow", "XHigh", "Y", "USym").show()
ep3 = errorBarY("S1", t, "X", "Y", "YLow", "YHigh").show()
epBy3 = errorBarYBy("S1", t, "X", "Y", "YLow", "YHigh", "USym").show()





doubles = [3, 4, 3, 5, 4, 5] as double[]
time = 1491946585000000000L;
t = newTable(col("USym", ["A", "B", "A", "B", "A", "B"] as String[]),
        doubleCol("Open", doubles),
        doubleCol("High", doubles),
        doubleCol("Low", doubles),
        doubleCol("Close", doubles))

t = t.updateView("Time = new DateTime(time + (MINUTE * i))")

ohlc = ohlcPlot("Test1", t, "Time", "Open", "High", "Low", "Close")

ohlcPlotBy = figure().newChart(0)
        .chartTitle("Chart Title")
        .newAxes()
        .xLabel("X")
        .yLabel("Y")
        .ohlcPlotBy("Test1", t, "Time", "Open", "High", "Low", "Close", "USym")




categories = ["Samsung", "Others", "Nokia", "Apple", "MSFT"] as String[]
valuesD = [27.8, 55.3, 16.8, 17.1, 23.1] as double[]
valuesI = [27, 55, 16, 17, 15] as int[]

ap = plot("S1", valuesD, valuesI).show()
ap = plot3d("S1", valuesI, valuesI, valuesI).show()

acp = catPlot("S1", categories, valuesI).show()
acp2 = catPlot3d("S1", categories, categories, valuesD).show()

achp = catHistPlot("S1", categories).show()

app = figure().xLabel("X").yLabel("Y").piePlot("S1", categories, valuesI).pointLabelFormat("{0}").show()

aep = errorBarXY("S1", valuesD, valuesD, valuesD, valuesD, valuesD, valuesD).show()
aep2 = errorBarX("S1", valuesD, valuesD, valuesD, valuesD).show()
aep3 = errorBarY("S1", valuesD, valuesD, valuesD, valuesD).show()


hp = histPlot("S1", valuesD, 5).show()
hp = histPlot("S1", valuesD, 0, 10, 5).show()
hp = histPlot("S1", valuesI, 5).show()
