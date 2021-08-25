/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;
import io.deephaven.db.plot.axistransformations.AxisTransformBusinessCalendar;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.util.calendar.Calendars;


public class BusinessTime {
    public static void main(String[] args) {
        DBDateTime[] x = new DBDateTime[500];
        double[] y = new double[500];
        long time = 1493305755000000000L;
        for (int i = 0; i < 250; i++) {
            time = time + DBTimeUtils.MINUTE;
            x[i] = new DBDateTime(time);
            y[i] = Math.sin(i);
        }

        time = 1493305755000000000L + DBTimeUtils.DAY;
        for (int i = 250; i < x.length; i++) {
            time = time + DBTimeUtils.MINUTE;
            x[i] = new DBDateTime(time);
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
