/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.TableTools;


public class CatPlotBy {

    public static void main(String[] args) {
        final String[] usym = {"A", "B", "A", "B"};
        final String[] cats = {"A", "B", "C", "D"};
        final double[] values = {5, 4, 4, 3};

        Table t = TableTools.newTable(TableTools.col("USym", usym),
                TableTools.col("Cats", cats),
                TableTools.doubleCol("Values", values));

        t = t.update("Timestamp = DBDateTime.now() + (HOUR * i)");
        Figure fig = FigureFactory.figure();
        for (int i = 0; i < 1; i++) {
            fig = fig.newChart()
                    .newAxes()
                    .catPlot("Test1", t, "Timestamp", "Values");
        }

        ExamplePlotUtils.display(fig);
    }
}
