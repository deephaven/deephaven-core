/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.Figure;
import io.deephaven.db.plot.FigureFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class SimpleTsPlot2 {

    public static void main(String[] args) throws IOException, ParseException {
        final String fileName = args[0];
        final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        BufferedReader reader = new BufferedReader(new FileReader(fileName));

        final ArrayList<Date> dates = new ArrayList<>();
        final ArrayList<Number> values = new ArrayList<>();

        String line = reader.readLine();
        line = reader.readLine(); // skip header
        while (line != null) {
            final String[] sline = line.split(",");
            final String dateString = sline[0];
            final String priceString = sline[6];
            final Date d = df.parse(dateString);
            final Double v = Double.parseDouble(priceString);
            dates.add(d);
            values.add(v);
            line = reader.readLine();
        }

        final Date[] x1 = dates.toArray(new Date[dates.size()]);
        final Number[] y1 = values.toArray(new Number[values.size()]);

        Figure fig = FigureFactory.figure()
            .newChart(0)
            .chartTitle(fileName)
            .newAxes()
            .xLabel("X")
            .yLabel("Y")
            .plot("Test1", x1, y1)
            .pointsVisible(false);

        ExamplePlotUtils.display(fig);
    }

}
