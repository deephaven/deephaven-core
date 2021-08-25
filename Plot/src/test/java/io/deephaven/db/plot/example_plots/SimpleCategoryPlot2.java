/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;


public class SimpleCategoryPlot2 {

    public static void main(String[] args) {
        final String[] x1 = {"CatA", "CatB", "CatC", "CatD", "CatE"};
        final Number[] y1 = {2, 3, 1, 9, 5};
        final String[] x2 = {"CatA", "CatB", "CatC", "CatE"};
        final Number[] y2 = {1.3, 3.2, 3.4, -1};

        Figure fig = FigureFactory.figure();
        Figure cht = fig.newChart(0)
                .chartTitle("Chart Title");
        Figure axs1 = cht.newAxes()
                .xLabel("X")
                .yLabel("Y").plotStyle("SCATTER")

                .catPlot("Test1", x1, y1)
                .pointShape("circle")
                .pointSize(2);

        Figure axs2 = axs1.twin().plotStyle("SCATTER")
                .catPlot("Test2", x2, y2)
                .pointShape("up_triangle")
                .pointSize(2)
                .axis(0).axisLabelFont("Courier", "BOLD_ITALIC", 25)
                .xTickLabelAngle(45);

        ExamplePlotUtils.display(axs2);
    }

}
