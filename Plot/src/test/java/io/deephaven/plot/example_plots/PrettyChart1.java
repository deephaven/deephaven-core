/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.example_plots;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.plot.*;
import io.deephaven.time.DateTime;
import io.deephaven.gui.color.Color;


public class PrettyChart1 {

    public static void main(String[] args) {
        final java.awt.Color red = java.awt.Color.decode("#d62728");
        final java.awt.Color darkBlue = java.awt.Color.decode("#1f77b4");
        final Color lighterRed = new Color(red.getRed(), red.getGreen(), red.getBlue(), 50);
        final Color lighterDarkBlue = new Color(darkBlue.getRed(), darkBlue.getGreen(), darkBlue.getBlue(), 100);
        final long time = 1491946585000000000L;

        DateTime[] date1 = {
                new DateTime(time + DateTimeUtils.DAY * 2),
                new DateTime(time + DateTimeUtils.DAY * 4),
                new DateTime(time + DateTimeUtils.DAY * 5),
                new DateTime(time + DateTimeUtils.DAY * 8),
                new DateTime(time + DateTimeUtils.DAY * 9),
                new DateTime(time + DateTimeUtils.DAY * 11),
                new DateTime(time + DateTimeUtils.DAY * 12),
                new DateTime(time + DateTimeUtils.DAY * 13),
                new DateTime(time + DateTimeUtils.DAY * 14),
                new DateTime(time + DateTimeUtils.DAY * 15),
                new DateTime(time + DateTimeUtils.DAY * 18),
                new DateTime(time + DateTimeUtils.DAY * 19),
                new DateTime(time + DateTimeUtils.DAY * 21),
                new DateTime(time + DateTimeUtils.DAY * 22),
                new DateTime(time + DateTimeUtils.DAY * 23),
                new DateTime(time + DateTimeUtils.DAY * 24),
                new DateTime(time + DateTimeUtils.DAY * 25),
                new DateTime(time + DateTimeUtils.DAY * 28),
                new DateTime(time + DateTimeUtils.DAY * 30),
                new DateTime(time + DateTimeUtils.DAY * 31),
                new DateTime(time + DateTimeUtils.DAY * 32),
                new DateTime(time + DateTimeUtils.DAY * 33),
                new DateTime(time + DateTimeUtils.DAY * 36),
                new DateTime(time + DateTimeUtils.DAY * 38),
                new DateTime(time + DateTimeUtils.DAY * 40),
                new DateTime(time + DateTimeUtils.DAY * 43),
                new DateTime(time + DateTimeUtils.DAY * 44),
                new DateTime(time + DateTimeUtils.DAY * 46),
        };


        DateTime[] date3 = {
                new DateTime(time + DateTimeUtils.DAY),
                new DateTime(time + DateTimeUtils.DAY * 3),
                new DateTime(time + DateTimeUtils.DAY * 4),
                new DateTime(time + DateTimeUtils.DAY * 6),
                new DateTime(time + DateTimeUtils.DAY * 8),
                new DateTime(time + DateTimeUtils.DAY * 10),
                new DateTime(time + DateTimeUtils.DAY * 11),
                new DateTime(time + DateTimeUtils.DAY * 13),
                new DateTime(time + DateTimeUtils.DAY * 15),
                new DateTime(time + DateTimeUtils.DAY * 17),
                new DateTime(time + DateTimeUtils.DAY * 18),
                new DateTime(time + DateTimeUtils.DAY * 19),
                new DateTime(time + DateTimeUtils.DAY * 20),
                new DateTime(time + DateTimeUtils.DAY * 21),
                new DateTime(time + DateTimeUtils.DAY * 23),
                new DateTime(time + DateTimeUtils.DAY * 24),
                new DateTime(time + DateTimeUtils.DAY * 26),
                new DateTime(time + DateTimeUtils.DAY * 27),
                new DateTime(time + DateTimeUtils.DAY * 28),
                new DateTime(time + DateTimeUtils.DAY * 30),
                new DateTime(time + DateTimeUtils.DAY * 32),
                new DateTime(time + DateTimeUtils.DAY * 33),
                new DateTime(time + DateTimeUtils.DAY * 34),
                new DateTime(time + DateTimeUtils.DAY * 36),
                new DateTime(time + DateTimeUtils.DAY * 38),
                new DateTime(time + DateTimeUtils.DAY * 40),
                new DateTime(time + DateTimeUtils.DAY * 42),
                new DateTime(time + DateTimeUtils.DAY * 43),
                new DateTime(time + DateTimeUtils.DAY * 44),
                new DateTime(time + DateTimeUtils.DAY * 46),
        };

        Number[] y1 = {
                100,
                102,
                98,
                101,
                101,
                102,
                103,
                104,
                105,
                106,
                103,
                105,
                107,
                108,
                105,
                109,
                110,
                113,
                115,
                114,
                114,
                114,
                113,
                116,
                117,
                118,
                119,
                123,
        };

        Number[] y3 = {
                100,
                102,
                98,
                97,
                98,
                99,
                96,
                95,
                92,
                93,
                90,
                89,
                88,
                86,
                88,
                85,
                85,
                86,
                83,
                81,
                82,
                80,
                81,
                79,
                78,
                77,
                78,
                76,
                76,
                75,
        };

        Number[] y1Higher = new Number[y1.length];
        Number[] y1Lower = new Number[y1.length];
        for (int i = 0; i < y1.length; i++) {
            double d = y1[i].doubleValue();
            y1Higher[i] = d + ((2 + i) * 0.3);
            y1Lower[i] = d - ((2 + i) * 0.3);
        }

        Number[] y3Higher = new Number[y3.length];
        Number[] y3Lower = new Number[y3.length];
        for (int i = 0; i < y3.length; i++) {
            double d = y3[i].doubleValue();
            y3Higher[i] = d + ((2 + i) * 0.3);
            y3Lower[i] = d - ((2 + i) * 0.3);
        }

        Figure fig = FigureFactory.figure();
        Figure cht = fig.newChart(0)
                .chartTitle("Chart Title");
        Figure axs = cht.newAxes().plotStyle("LINE")
                .yLabel("Predicted Index")
                .plot("Test2", date3, y3).pointsVisible(false)
                .plot("Test1", date1, y1).pointsVisible(false);


        Figure axs2 = axs.twin()
                .plotStyle(PlotStyle.AREA)
                .plot("Test1", date3, y3Lower).seriesColor(new Color(250, 250, 250))
                .plot("Test2", date3, y3Higher).seriesColor(lighterRed)
                .plot("Test3", date1, y1Lower).seriesColor(new Color(250, 250, 250))
                .plot("Test4", date1, y1Higher).seriesColor(lighterDarkBlue);


        ExamplePlotUtils.display(axs2);
    }
}
