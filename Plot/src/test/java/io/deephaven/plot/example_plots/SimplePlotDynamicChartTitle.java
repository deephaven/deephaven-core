/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.example_plots;

import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureFactory;
import io.deephaven.plot.filters.SelectableDataSetOneClick;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;


import static io.deephaven.plot.filters.Selectables.oneClick;

/**
 * Plot examples that demonstrates dynamic chart titles
 */
public class SimplePlotDynamicChartTitle {
    public static void main(String[] args) {
        final Number[] x1 = {1, 2, 3, 4};
        final Number[] y1 = {2, 3, 1, 9};
        final Number[] x2 = {0, 1.5, 15.5};
        final Number[] y2 = {1.3, 3.2, 3.4};

        final Table table = TableTools.emptyTable(2).updateView("by = i%2==0 ? `A` : `B`", "title=i", "title2=2*i");
        final SelectableDataSetOneClick by = oneClick(table, "by");

        Figure fig = FigureFactory.figure()
                .xInvert(true)
                .plot("TestF", x1, y1)
                .chartTitle(false, table, "title", "title2");
        ExamplePlotUtils.display(fig);
    }
}
