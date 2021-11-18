/*
 *
 * * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 *
 */

package io.deephaven.plot.example_plots;

import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureFactory;
import io.deephaven.gui.color.Color;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimpleCategoryColor {

    public static void main(String[] args) {
        final String[] x1 = {"A", "B", "C", "D"};
        final Number[] y1 = {2, 3, 1, 9};
        final Color[] c1 = {new Color(0, 255, 0),
                new Color(0, 0, 255), new Color(255, 0, 0), new Color(255, 255, 0)};
        final String[] x2 = {"A", "B", "C", "E", "F"};
        final Number[] y2 = {5, 2, 1, 7, 5};
        final Color[] c2 = {new Color(0, 255, 0),
                new Color(0, 0, 255), new Color(0, 0, 0), new Color(255, 0, 0), new Color(255, 255, 0)};

        final Map<String, Color> c1m = IntStream.range(0, c1.length).boxed()
                .collect(Collectors.toMap(i -> x1[i], i -> c1[i]));

        final Map<String, Color> c2m = IntStream.range(0, c2.length).boxed()
                .collect(Collectors.toMap(i -> x2[i], i -> c2[i]));
        c2m.put("C", null);

        Figure fig = FigureFactory.figure();
        Figure cht = fig.newChart(0)
                .chartTitle("Chart Title");
        Figure axs = cht.newAxes()
                .xLabel("X")
                .yLabel("Y")
                .catPlot("Test2", x2, y2)
                .pointColor(new Color(0, 0, 0))
                .pointColor(c2m)
                .catPlot("Test1", x1, y1)
                .pointColor(c1m);


        ExamplePlotUtils.display(axs);
    }
}
