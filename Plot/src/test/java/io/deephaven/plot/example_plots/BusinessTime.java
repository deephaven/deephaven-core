/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.example_plots;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureFactory;
import io.deephaven.plot.axistransformations.AxisTransformBusinessCalendar;
import io.deephaven.time.DateTime;
import io.deephaven.time.calendar.Calendars;


public class BusinessTime {
    public static void main(String[] args) {
        DateTime[] x = new DateTime[500];
        double[] y = new double[500];
        long time = 1493305755000000000L;
        for (int i = 0; i < 250; i++) {
            time = time + DateTimeUtils.MINUTE;
            x[i] = new DateTime(time);
            y[i] = Math.sin(i);
        }

        time = 1493305755000000000L + DateTimeUtils.DAY;
        for (int i = 250; i < x.length; i++) {
            time = time + DateTimeUtils.MINUTE;
            x[i] = new DateTime(time);
            y[i] = Math.sin(i);
        }

        Figure fig = FigureFactory.figure(2, 1);
        Figure cht = fig.newChart(0)
                .chartTitle("Business");
        Figure axs = cht.newAxes().xTransform(new AxisTransformBusinessCalendar(Calendars.calendar("USNYSE")))
                .xTicksVisible(false)
                .xLabel("X")
                .yLabel("Y")
                .plot("Test", x, y);

        Figure cht2 = axs.newChart(1)
                .chartTitle("NonBusiness");
        Figure axs2 = cht2.newAxes()
                .xLabel("X")
                .yLabel("Y")
                .plot("Test", x, y);


        ExamplePlotUtils.display(axs2);
    }
}
