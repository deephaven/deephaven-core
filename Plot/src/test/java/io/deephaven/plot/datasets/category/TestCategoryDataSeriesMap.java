/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.category;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.BaseFigureImpl;
import io.deephaven.plot.datasets.data.*;
import io.deephaven.engine.util.TableTools;
import junit.framework.TestCase;

public class TestCategoryDataSeriesMap extends BaseArrayTestCase {
    private static final int SIZE = 500;
    private static final BaseFigureImpl f = new BaseFigureImpl();
    private static final AxesImpl a = f.newChart().newAxes();
    private static final int[] dataX = new int[SIZE];
    private static final double[] dataY = new double[dataX.length];
    static {
        for (int i = 0; i < dataX.length; i++) {
            dataX[i] = i;
            dataY[i] = i % 10 == 0 ? Double.NaN : 2 * i;
        }
    }

    private static final int[] dataXMisMatched = new int[SIZE - 1];
    private static final String[] invalidY = new String[SIZE];
    static {
        for (int i = 0; i < dataXMisMatched.length; i++) {
            dataXMisMatched[i] = i;
            invalidY[i] = null;
        }
    }

    private static final IndexableDataDouble dx = new IndexableDataDouble(dataX, false, null);
    private static final IndexableData<String> dyInvalid = new IndexableDataArray<>(invalidY, null);
    private static final IndexableDataDouble dxMismatched = new IndexableDataDouble(dataXMisMatched, false, null);
    private static final IndexableNumericData dy = new IndexableNumericDataArrayDouble(dataY, null);

    public void testCategoryDataSeriesMap() {
        CategoryDataSeriesMap map = new CategoryDataSeriesMap(a, 1, "Test", dx, dy);

        assertEquals(map.getValue(0.0), Double.NaN);
        assertEquals(map.getValue(5.0), 10.0);
        assertEquals(map.getValue(55.0), 110.0);
        assertNull(map.getValue(dataX.length));


        try {
            new CategoryDataSeriesMap(a, 2, "Test", null, dy);
            TestCase.fail("Expected an Exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }

        try {
            new CategoryDataSeriesMap(a, 3, "Test", dx, null);
            TestCase.fail("Expected an Exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }

        try {
            new CategoryDataSeriesMap(a, 4, "Test", dxMismatched, dy);
            TestCase.fail("Expected an Exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("match"));
        }

        for (int i = 0; i < dataXMisMatched.length; i++) {
            invalidY[i] = "S";
        }


        try {
            new CategoryDataSeriesMap(a, 6, "Test", dyInvalid, dy);
            TestCase.fail("Expected an Exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("repeated"));
        }
    }

    public void testCopy() {
        final CategoryDataSeriesMap map1 = new CategoryDataSeriesMap(a, 1, "Test", dx, dy);
        final CategoryDataSeriesMap map1Copy = map1.copy(new BaseFigureImpl().newChart().newAxes());
        testCopy(map1, map1Copy);


        final CategoryDataSeriesMap map2 = new CategoryDataSeriesMap(a, 1, "Test2", dx, dy);
        map2.pointLabel(p -> "Label");
        map2.pointsVisible(true);
        map2.linesVisible(true);
        map2.gradientVisible(true);
        map2.lineColor("green");
        map2.pointColor("red");
        map2.pointSize(TableTools.newTable(TableTools.intCol("Values", dataX), TableTools.intCol("Sizes", dataX)),
                "Values", "Sizes");
        map2.pointLabelFormat("{1}");
        map2.toolTipPattern("0.00E0");
        map2.seriesColor(2);
        final CategoryDataSeriesMap map2Copy = map2.copy(new BaseFigureImpl().newChart().newAxes());

        // initialize the AssociativeDataTable dataset holding the point sizes
        map2Copy.getPointSize(dataX[0]);

        testCopy(map2, map2Copy);

    }

    static void testCopy(final CategoryDataSeriesInternal original, final CategoryDataSeriesInternal copy) {
        assertEquals(original.name(), copy.name());
        assertEquals(original.size(), copy.size());
        for (int aDataX : dataX) {
            assertEquals(original.getValue(aDataX), copy.getValue(aDataX));
            assertEquals(original.getLabel(aDataX), copy.getLabel(aDataX));
            assertEquals(original.getColor(aDataX), copy.getColor(aDataX));
            assertEquals(original.getPointSize(aDataX), copy.getPointSize(aDataX));
        }

        assertEquals(original.getPointsVisible(), copy.getPointsVisible());
        assertEquals(original.getGradientVisible(), copy.getGradientVisible());
        assertEquals(original.getLineStyle(), copy.getLineStyle());
        assertEquals(original.getLineColor(), copy.getLineColor());
        assertEquals(original.getPointLabelFormat(), copy.getPointLabelFormat());
        assertEquals(original.getXToolTipPattern(), copy.getXToolTipPattern());
        assertEquals(original.getYToolTipPattern(), copy.getYToolTipPattern());
        assertEquals(original.getSeriesColor(), copy.getSeriesColor());
    }
}
