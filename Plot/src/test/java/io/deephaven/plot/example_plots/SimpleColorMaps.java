/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.example_plots;

import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureFactory;
import io.deephaven.gui.color.Color;
import io.deephaven.plot.util.Range;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import static io.deephaven.plot.colors.ColorMaps.heatMap;
import static io.deephaven.plot.colors.ColorMaps.rangeMap;

public class SimpleColorMaps {

    public static void main(String[] args) {
        final Number[] x = new Number[50];
        final Number[] y = new Number[x.length];
        final Number[] y2 = new Number[x.length];
        final Random randy = new Random();
        final Color red = Color.color("red");
        final Color green = Color.color("green");
        final Color blue = Color.color("blue");

        for (int i = 0; i < x.length; i++) {
            x[i] = i;
            int pos = randy.nextInt(2);
            double d = randy.nextDouble() % 1;
            y[i] = i + (pos < 1 ? d : -d);
            y2[i] = y[i].doubleValue() - 10;
        }

        Figure fig = FigureFactory.figure();
        Figure cht = fig.newChart(0)
                .chartTitle("Chart Title");
        Figure axs1 = cht.newAxes()
                .xLabel("X")
                .yLabel("Y")
                .plotStyle("LINE")
                .plot("Test1", x, y)
                .pointColorByY(val -> heatMap(0, 50, green, red).apply(val));

        Range[] ranges = new Range[] {
                new Range(Double.NEGATIVE_INFINITY, 10, true, false),
                new Range(10, 30, true, false),
                new Range(30, Double.POSITIVE_INFINITY, true, true)};

        Color[] colors = {blue, green, red};

        final Map<Range, Color> m = new LinkedHashMap<>();
        m.put(ranges[0], colors[0]);
        m.put(ranges[1], colors[1]);
        m.put(ranges[2], colors[2]);
        axs1 = axs1.plot("Test2", x, y2)
                .pointColorByY(val -> rangeMap(m).apply(val));

        ExamplePlotUtils.display(axs1);
    }

}
