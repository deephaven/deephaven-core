import deephaven.TableTools as tt
import deephaven.Plot as plt


t = tt.emptyTable(50)\
    .update("X = i + 5", "XLow = X -1", "XHigh = X + 1", "Y = Math.random() * 5", "YLow = Y - 1", "YHigh = Y + 1", "USym = i % 2 == 0 ? `AAPL` : `MSFT`")


p = plt.plot("S1", t, "X", "Y").lineColor("black").show()
p2 = plt.plot("S1", t, "X", "Y").plotStyle("bar").gradientVisible(True).show()
p3 = plt.plot("S1", t, "X", "Y").plotStyle("scatter").pointColor("black").pointSize(2).show()
p4 = plt.plot("S1", t, "X", "Y").plotStyle("area").seriesColor("red").show()


p4 = plt.plot3d("S1", t, "X", "X", "Y").show()


pBy = plt.plotBy("S1", t, "X", "Y", "USym").show()
pBy = plt.plot3dBy("S1", t, "X",  "X", "Y", "USym").show()

cp = plt.catPlot("S1", t, "X", "Y").lineColor("black").show()
cp2 = plt.catPlot("S1", t, "X", "Y").plotStyle("bar").gradientVisible(True).show()
cp3 = plt.catPlot("S1", t, "X", "Y").plotStyle("scatter").pointColor("black").pointSize(2).show()
cp4 = plt.catPlot("S1", t, "X", "Y").plotStyle("area").seriesColor("red").show()

cp = plt.catPlot3d("S1", t, "X", "X", "Y").show()

cpBy = plt.catPlotBy("S1", t, "X", "Y", "USym").show()

cpBy = plt.catPlot3dBy("S1", t, "X", "X", "Y", "USym").show()

pp = plt.piePlot("S1", t, "X", "Y")


chp = plt.catHistPlot("S1", t, "X").show()

hp = plt.histPlot("S1", t, "X", 5).show()
hp = plt.histPlot("S1", t, "X", 0, 10, 5).show()

ep = plt.errorBarXY("S1", t, "X", "XLow", "XHigh", "Y", "YLow", "YHigh").show()
epBy = plt.errorBarXYBy("S1", t, "X", "XLow", "XHigh", "Y", "YLow", "YHigh", "USym").show()
ep2 = plt.errorBarX("S1", t, "X", "XLow", "XHigh", "Y").show()
epBy2 = plt.errorBarXBy("S1", t, "X", "XLow", "XHigh", "Y", "USym").show()
ep3 = plt.errorBarY("S1", t, "X", "Y", "YLow", "YHigh").show()
epBy3 = plt.errorBarYBy("S1", t, "X", "Y", "YLow", "YHigh", "USym").show()


doubles = [3, 4, 3, 5, 4, 5]
time = 1491946585000000000
t = tt.newTable(tt.col("USym", ["A", "B", "A", "B", "A", "B"]),
                tt.doubleCol("Open", doubles), tt.doubleCol("High", doubles),
                tt.doubleCol("Low", doubles), tt.doubleCol("Close", doubles))

t = t.updateView("Time = new DateTime(time + (MINUTE * i))")

ohlc = plt.ohlcPlot("Test1", t, "Time", "Open", "High", "Low", "Close")

ohlcPlotBy = plt.figure().newChart(0)\
    .chartTitle("Chart Title")\
    .newAxes()\
    .xLabel("X")\
    .yLabel("Y")\
    .ohlcPlotBy("Test1", t, "Time", "Open", "High", "Low", "Close", "USym")


categories = ["Samsung", "Others", "Nokia", "Apple", "MSFT"]
valuesD = [27.8, 55.3, 16.8, 17.1, 23.1]
valuesI = [27, 55, 16, 17, 15]

ap = plt.plot("S1", valuesD, valuesI).show()
ap = plt.plot3d("S1", valuesI, valuesI, valuesI).show()

acp = plt.catPlot("S1", categories, valuesI).show()
acp2 = plt.catPlot3d("S1", categories, categories, valuesD).show()

achp = plt.catHistPlot("S1", categories).show()

app = plt.figure().xLabel("X").yLabel("Y").piePlot("S1", categories, valuesI).pointLabelFormat("{0}").show()

aep = plt.errorBarXY("S1", valuesD, valuesD, valuesD, valuesD, valuesD, valuesD).show()
aep2 = plt.errorBarX("S1", valuesD, valuesD, valuesD, valuesD).show()
aep3 = plt.errorBarY("S1", valuesD, valuesD, valuesD, valuesD).show()


hp = plt.histPlot("S1", valuesD, 5).show()
hp = plt.histPlot("S1", valuesD, 0, 10, 5).show()
hp = plt.histPlot("S1", valuesI, 5).show()
