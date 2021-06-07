/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.example_plots;

import io.deephaven.db.plot.*;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.gui.color.Color;


public class PrettyChart1 {

    public static void main(String[] args) {
        final java.awt.Color red = java.awt.Color.decode("#d62728");
        final java.awt.Color darkBlue = java.awt.Color.decode("#1f77b4");
        final Color lighterRed = new Color(red.getRed(), red.getGreen(), red.getBlue(), 50);
        final Color lighterDarkBlue = new Color(darkBlue.getRed(), darkBlue.getGreen(), darkBlue.getBlue(), 100);
        final long time = 1491946585000000000L;

        DBDateTime[] date1 = {
                new DBDateTime(time + DBTimeUtils.DAY * 2),
                new DBDateTime(time + DBTimeUtils.DAY * 4),
                new DBDateTime(time + DBTimeUtils.DAY * 5),
                new DBDateTime(time + DBTimeUtils.DAY * 8),
                new DBDateTime(time + DBTimeUtils.DAY * 9),
                new DBDateTime(time + DBTimeUtils.DAY * 11),
                new DBDateTime(time + DBTimeUtils.DAY * 12),
                new DBDateTime(time + DBTimeUtils.DAY * 13),
                new DBDateTime(time + DBTimeUtils.DAY * 14),
                new DBDateTime(time + DBTimeUtils.DAY * 15),
                new DBDateTime(time + DBTimeUtils.DAY * 18),
                new DBDateTime(time + DBTimeUtils.DAY * 19),
                new DBDateTime(time + DBTimeUtils.DAY * 21),
                new DBDateTime(time + DBTimeUtils.DAY * 22),
                new DBDateTime(time + DBTimeUtils.DAY * 23),
                new DBDateTime(time + DBTimeUtils.DAY * 24),
                new DBDateTime(time + DBTimeUtils.DAY * 25),
                new DBDateTime(time + DBTimeUtils.DAY * 28),
                new DBDateTime(time + DBTimeUtils.DAY * 30),
                new DBDateTime(time + DBTimeUtils.DAY * 31),
                new DBDateTime(time + DBTimeUtils.DAY * 32),
                new DBDateTime(time + DBTimeUtils.DAY * 33),
                new DBDateTime(time + DBTimeUtils.DAY * 36),
                new DBDateTime(time + DBTimeUtils.DAY * 38),
                new DBDateTime(time + DBTimeUtils.DAY * 40),
                new DBDateTime(time + DBTimeUtils.DAY * 43),
                new DBDateTime(time + DBTimeUtils.DAY * 44),
                new DBDateTime(time + DBTimeUtils.DAY * 46),
        };


        DBDateTime[] date3 = {
                new DBDateTime(time + DBTimeUtils.DAY),
                new DBDateTime(time + DBTimeUtils.DAY * 3),
                new DBDateTime(time + DBTimeUtils.DAY * 4),
                new DBDateTime(time + DBTimeUtils.DAY * 6),
                new DBDateTime(time + DBTimeUtils.DAY * 8),
                new DBDateTime(time + DBTimeUtils.DAY * 10),
                new DBDateTime(time + DBTimeUtils.DAY * 11),
                new DBDateTime(time + DBTimeUtils.DAY * 13),
                new DBDateTime(time + DBTimeUtils.DAY * 15),
                new DBDateTime(time + DBTimeUtils.DAY * 17),
                new DBDateTime(time + DBTimeUtils.DAY * 18),
                new DBDateTime(time + DBTimeUtils.DAY * 19),
                new DBDateTime(time + DBTimeUtils.DAY * 20),
                new DBDateTime(time + DBTimeUtils.DAY * 21),
                new DBDateTime(time + DBTimeUtils.DAY * 23),
                new DBDateTime(time + DBTimeUtils.DAY * 24),
                new DBDateTime(time + DBTimeUtils.DAY * 26),
                new DBDateTime(time + DBTimeUtils.DAY * 27),
                new DBDateTime(time + DBTimeUtils.DAY * 28),
                new DBDateTime(time + DBTimeUtils.DAY * 30),
                new DBDateTime(time + DBTimeUtils.DAY * 32),
                new DBDateTime(time + DBTimeUtils.DAY * 33),
                new DBDateTime(time + DBTimeUtils.DAY * 34),
                new DBDateTime(time + DBTimeUtils.DAY * 36),
                new DBDateTime(time + DBTimeUtils.DAY * 38),
                new DBDateTime(time + DBTimeUtils.DAY * 40),
                new DBDateTime(time + DBTimeUtils.DAY * 42),
                new DBDateTime(time + DBTimeUtils.DAY * 43),
                new DBDateTime(time + DBTimeUtils.DAY * 44),
                new DBDateTime(time + DBTimeUtils.DAY * 46),
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
