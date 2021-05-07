/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.composite.ScatterPlotMatrix;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.TableTools;


public class ScatterPlotTable {

    public static void main(String[] args) {
        final Number[] x1 = {1, 2, 3, 4};
        final Number[] x2 = {2, 3, 1, 9};
        final Number[] x3 = {0, 1.5, 4.5, 7.5};
        final Number[] x4 = {1.3, 3.2, 3.4, 3.8};

        Table t = TableTools.newTable(TableTools.col("x1", x1),
                TableTools.col("x2", x2),
                TableTools.col("x3", x3),
                TableTools.col("x4", x4));

        ScatterPlotMatrix f = ScatterPlotMatrix.scatterPlotMatrix(t, "x1", "x2", "x3", "x4");
        ExamplePlotUtils.display(f);
    }

}
