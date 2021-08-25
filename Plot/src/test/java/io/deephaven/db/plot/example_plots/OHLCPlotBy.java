/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;
import io.deephaven.db.plot.axistransformations.AxisTransformLambda;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.TableTools;


public class OHLCPlotBy {

    public static void main(String[] args) {
        final String[] cats = {"A", "B", "A", "B", "A", "B"};
        final String[] cats2 = {"AA", "BB", "A", "B", "AA", "BB"};
        final double[] open = {3, 4, 3, 5, 4, 5};
        final double[] high = {5, 6, 5, 7, 6, 8};
        final double[] low = {2, 3, 1, 4, 2, 3};
        final double[] close = {4, 5, 4, 6, 5, 6};
        final long time = 1491946585000000000L;
        Table t = TableTools.newTable(TableTools.col("USym", cats), TableTools.col("USym2", cats2),
            TableTools.doubleCol("Open", open),
            TableTools.doubleCol("High", high),
            TableTools.doubleCol("Low", low),
            TableTools.doubleCol("Close", close));

        QueryScope.addParam("time", time);
        t = t.updateView("Time = new DBDateTime(time + (MINUTE * i))");

        Figure fig = FigureFactory.figure();
        Figure cht = fig.newChart(0)
            .chartTitle("Chart Title");
        Figure axs = cht.newAxes().xTransform(new AxisTransformLambda())
            .xLabel("X")
            .yLabel("Y")
            .ohlcPlotBy("Test1", t, "Time", "Open", "High", "Low", "Close", "USym")
            .pointColor("black", "A");

        ExamplePlotUtils.display(axs);
    }
}
