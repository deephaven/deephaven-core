/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.example_plots;

import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureFactory;
import io.deephaven.plot.axistransformations.AxisTransforms;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;

import static io.deephaven.plot.filters.Selectables.oneClick;

public class SimpleXYTable {

    public static void main(String[] args) {

        Table t = TableTools.emptyTable(10).updateView("Timestamp = new DateTime(i * HOUR)", "Open = i",
                "High = i + 2", "Low = i - 2", "Close = i + 1", "By = i % 5");
        Table t2 = TableTools.emptyTable(5000).updateView("Timestamp = new DateTime(0) + (i * HOUR)",
                "Open = i + 100", "High = i + 2 + 100", "Low = i - 2 + 100", "Close = i + 1 + 100", "By = i % 5");

        final Figure f =
                FigureFactory.figure().ohlcPlotBy("Test1", t, "Timestamp", "Open", "High", "Low", "Close", "By")
                        .yTransform(AxisTransforms.SQRT)
                        .lineColor("black")
                        .pointLabel("A")
                        .newChart()
                        .ohlcPlotBy("Test2", t2, "Timestamp", "Open", "High", "Low", "Close", "By")
                        .show();

        ExamplePlotUtils.display(f);
    }

}
