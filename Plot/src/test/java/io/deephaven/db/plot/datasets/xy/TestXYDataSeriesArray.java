/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.xy;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.db.plot.BaseFigureImpl;
import io.deephaven.db.plot.ChartImpl;
import io.deephaven.db.plot.errors.PlotIllegalArgumentException;
import io.deephaven.db.plot.util.PlotUtils;
import io.deephaven.db.plot.util.tables.TableBackedTableMapHandle;
import io.deephaven.gui.color.Color;
import io.deephaven.db.plot.datasets.data.IndexableNumericData;
import io.deephaven.db.plot.datasets.data.IndexableNumericDataArrayDouble;
import io.deephaven.db.plot.util.tables.SwappableTable;
import io.deephaven.db.plot.util.tables.TableHandle;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.TableTools;
import junit.framework.TestCase;

import java.util.ArrayList;

public class TestXYDataSeriesArray extends BaseArrayTestCase {

    public void testXYDataSeriesArray() {
        ChartImpl chart = new BaseFigureImpl().newChart();
        final double[] valueArray = {1, 2, 3};
        final double[] valueArray2 = {4, 5, 6};
        final IndexableNumericData values = new IndexableNumericDataArrayDouble(valueArray, null);
        final IndexableNumericData values2 = new IndexableNumericDataArrayDouble(valueArray2, null);
        final XYDataSeriesInternal x1 = new XYDataSeriesArray(chart.newAxes(), 1, "Test", values, values2);
        final XYDataSeriesInternal x2 = new XYDataSeriesArray(chart.newAxes(), 2, "Test2", values2, values);

        assertEquals(x1.size(), valueArray.length);
        assertEquals(x2.size(), valueArray.length);
        for (int i = 0; i < valueArray.length; i++) {
            assertEquals(x1.getX(i), valueArray[i]);
            assertEquals(x1.getY(i), valueArray2[i]);
            assertEquals(x2.getX(i), valueArray2[i]);
            assertEquals(x2.getY(i), valueArray[i]);
        }

        final double[] misSized = {1, 2};
        final IndexableNumericData misSizedValues = new IndexableNumericDataArrayDouble(misSized, null);

        try {
            new XYDataSeriesArray(new BaseFigureImpl().newChart().newAxes(), 3, "Test", misSizedValues, values2);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("inconsistent size"));
        }

        try {
            new XYDataSeriesArray(new BaseFigureImpl().newChart().newAxes(), 4, "Test", null, values2);
            TestCase.fail("Expected an exception");
        } catch (PlotIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }

        try {
            new XYDataSeriesArray(new BaseFigureImpl().newChart().newAxes(), 5, "Test", misSizedValues, null);
            TestCase.fail("Expected an exception");
        } catch (PlotIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }


        x1.seriesColor(1);
        assertEquals(x1.getPointColor(0), PlotUtils.intToColor(1));
        assertEquals(x1.getLineColor(), PlotUtils.intToColor(1));

        final Color color = Color.color("red");
        x1.seriesColor(color);
        assertEquals(x1.getPointColor(0), color);
        assertEquals(x1.getLineColor(), color);
    }


    public void testCopy() {
        final double[] valueArray = {1, 2, 3};
        final double[] valueArray2 = {4, 5, 6};
        final IndexableNumericData values = new IndexableNumericDataArrayDouble(valueArray, null);
        final IndexableNumericData values2 = new IndexableNumericDataArrayDouble(valueArray2, null);

        final XYDataSeriesArray x1 =
                new XYDataSeriesArray(new BaseFigureImpl().newChart().newAxes(), 1, "Test", values, values2);
        final XYDataSeriesArray x2 =
                new XYDataSeriesArray(new BaseFigureImpl().newChart().newAxes(), 2, "Test2", values2, values);

        x1.addTableHandle(new TableHandle(TableTools.emptyTable(2).updateView("A=i", "B=i"), "A", "B"));
        x1.addTableHandle(new TableHandle(TableTools.emptyTable(2).updateView("C=i"), "C"));
        final SwappableTable swappableTable =
                new SwappableTable(new TableBackedTableMapHandle(TableTools.emptyTable(2).updateView("A=i", "B=i"),
                        new ArrayList<>(), new String[0], null)) {
                    @Override
                    public void addColumn(String column) {

                    }
                };
        x1.addSwappableTable(swappableTable);

        final XYDataSeriesArray x1Copy = x1.copy(new BaseFigureImpl().newChart().newAxes());
        TestAbstractXYDataSeries.testCopy(x1, x1Copy);


        x2.pointsVisible(false);
        x2.linesVisible(true);
        x2.pointLabelFormat("{0}: {1}, {2}");
        x2.xToolTipPattern("0.0E0");
        x2.yToolTipPattern("0.0E1");
        x2.seriesColor("blue");
        x2.lineColor("red");
        x2.pointSize(0.5, 4.2, 3.0);

        final Color c1 = new Color(0, 0, 0);
        final Color c2 = new Color(100, 100, 100);
        final Color c3 = new Color(255, 255, 255);
        final Table tableColors = TableTools.newTable(TableTools.col("Color", c1, c2, c3));
        x2.pointColor(tableColors, "Color");

        final XYDataSeriesArray x2Copy = x2.copy(new BaseFigureImpl().newChart().newAxes());
        TestAbstractXYDataSeries.testCopy(x2, x2Copy);
    }
}
