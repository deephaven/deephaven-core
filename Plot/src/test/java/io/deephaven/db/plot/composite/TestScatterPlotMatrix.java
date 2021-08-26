/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.composite;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.db.plot.FigureImpl;
import io.deephaven.db.plot.datasets.xy.XYDataSeriesInternal;
import io.deephaven.db.plot.filters.SelectableDataSetOneClick;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.utils.ColumnHolder;
import junit.framework.TestCase;

import java.util.concurrent.atomic.AtomicInteger;

import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public class TestScatterPlotMatrix extends BaseArrayTestCase {
    private final int length = 10;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        LiveTableMonitor.DEFAULT.resetForUnitTests(false);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        LiveTableMonitor.DEFAULT.resetForUnitTests(true);
    }

    public void testScatterPlotMatrix() {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        final int[][] ints = new int[length][length];
        final double[][] doubles = new double[length][length];
        final long[][] longs = new long[length][length];
        final float[][] floats = new float[length][length];
        final AtomicInteger[][] atomicIntegers = new AtomicInteger[length][length];
        final String[] names = new String[length];
        for (int i = 0; i < length; i++) {
            for (int j = 0; j < length; j++) {
                ints[i][j] = i;
                doubles[i][j] = i;
                longs[i][j] = i;
                floats[i][j] = i;
                atomicIntegers[i][j] = new AtomicInteger(i);
            }
            names[i] = ((char) i + 65) + "";
        }

        testScatterPlotMatrix(ScatterPlotMatrix.scatterPlotMatrix(atomicIntegers));
        testScatterPlotMatrix(ScatterPlotMatrix.scatterPlotMatrix(names, atomicIntegers));

        testScatterPlotMatrix(ScatterPlotMatrix.scatterPlotMatrix(ints));
        testScatterPlotMatrix(ScatterPlotMatrix.scatterPlotMatrix(names, ints));

        testScatterPlotMatrix(ScatterPlotMatrix.scatterPlotMatrix(doubles));
        testScatterPlotMatrix(ScatterPlotMatrix.scatterPlotMatrix(names, doubles));

        testScatterPlotMatrix(ScatterPlotMatrix.scatterPlotMatrix(longs));
        testScatterPlotMatrix(ScatterPlotMatrix.scatterPlotMatrix(names, longs));

        testScatterPlotMatrix(ScatterPlotMatrix.scatterPlotMatrix(floats));
        testScatterPlotMatrix(ScatterPlotMatrix.scatterPlotMatrix(names, floats));


        ColumnHolder[] columns = new ColumnHolder[ints[0].length];
        String[] columnNames = new String[columns.length];
        int i = 0;
        for (int[] ints1 : ints) {
            columnNames[i] = "int" + i;
            columns[i] = TableTools.intCol(columnNames[i], ints1);
            i++;
        }
        Table t = TableTools.newTable(columns);
        testScatterPlotMatrix(ScatterPlotMatrix.scatterPlotMatrix(t, columnNames));


        i = 0;
        for (double[] doubles1 : doubles) {
            columnNames[i] = "double" + i;
            columns[i] = TableTools.doubleCol(columnNames[i], doubles1);
            i++;
        }
        t = TableTools.newTable(columns).updateView("Cat = i == 0 ? `A` : `B`");
        SelectableDataSetOneClick oneClick = new SelectableDataSetOneClick(
            t.byExternal(columnNames), t.getDefinition(), new String[] {"Cat"});
        final ScatterPlotMatrix matrix = ScatterPlotMatrix.scatterPlotMatrix(oneClick, columnNames);
        final XYDataSeriesInternal series =
            (XYDataSeriesInternal) matrix.getFigure().chart(0).axes(0).series(0);
        for (int j = 0; j < series.size(); j++) {
            assertEquals(Double.NaN, series.getX(i));
            assertEquals(Double.NaN, series.getY(i));
        }


        try {
            ScatterPlotMatrix.scatterPlotMatrix(null, floats);
            TestCase.fail("Expected an exception");
        } catch (RequirementFailure e) {
            assertTrue(e.getMessage().contains("null"));
        }

        try {
            ScatterPlotMatrix.scatterPlotMatrix(new String[5], new int[4][5]);
            TestCase.fail("Expected an exception");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("dimension"));
        }
        LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
    }

    public void testPointSize() {
        final double[][] doubles = new double[length][length];
        final int[] pointSizesInt = new int[length * length];
        final double[] pointSizesDouble = new double[length * length];
        final long[] pointSizesLong = new long[length * length];
        final Number[] pointSizesNumber = new Number[length * length];
        final String[] names = new String[length];
        for (int i = 0; i < length; i++) {
            for (int j = 0; j < length; j++) {
                doubles[i][j] = i;

                pointSizesInt[length * i + j] = i;
                pointSizesDouble[length * i + j] = i;
                pointSizesLong[length * i + j] = i;
                pointSizesNumber[length * i + j] = i;
            }
            names[i] = ((char) i + 65) + "";
        }

        ScatterPlotMatrix matrix = ScatterPlotMatrix.scatterPlotMatrix(names, doubles);

        matrix = matrix.pointSize(2.0);
        testPointSize(new double[] {2.0, 2.0}, matrix);

        matrix = matrix.pointSize(3L);
        testPointSize(new double[] {3.0, 3.0}, matrix);

        matrix = matrix.pointSize(4);
        testPointSize(new double[] {4.0, 4.0}, matrix);

        matrix = matrix.pointSize(Integer.valueOf(5));
        testPointSize(new double[] {5.0, 5.0}, matrix);

        matrix = matrix.pointSize(0, 1);
        testPointSize(new double[] {1.0, 5.0}, matrix);

        matrix = matrix.pointSize(0, 2.0);
        testPointSize(new double[] {2.0, 5.0}, matrix);

        matrix = matrix.pointSize(0, 3L);
        testPointSize(new double[] {3.0, 5.0}, matrix);

        matrix = matrix.pointSize(0, Integer.valueOf(4));
        testPointSize(new double[] {4.0, 5.0}, matrix);


        try {
            matrix.pointSize((length + 1) * length, 2.0);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("out of bounds"));
        }

        try {
            matrix.pointSize((length + 1) * length, Integer.valueOf(4));
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("out of bounds"));
        }

        matrix = matrix.pointSize(0, 1, 1);
        testPointSize(new double[] {4.0, 1.0}, matrix);

        matrix = matrix.pointSize(0, 1, 2.0);
        testPointSize(new double[] {4.0, 2.0}, matrix);

        matrix = matrix.pointSize(0, 1, 3L);
        testPointSize(new double[] {4.0, 3.0}, matrix);

        matrix = matrix.pointSize(0, 1, Integer.valueOf(4));
        testPointSize(new double[] {4.0, 4.0}, matrix);

        matrix = matrix.pointSize(pointSizesDouble);
        testPointSize(new double[] {pointSizesDouble[0], pointSizesDouble[1]}, matrix);

        matrix = matrix.pointSize(pointSizesInt);
        testPointSize(new double[] {pointSizesInt[0], pointSizesInt[1]}, matrix);

        matrix = matrix.pointSize(pointSizesLong);
        testPointSize(new double[] {pointSizesLong[0], pointSizesLong[1]}, matrix);

        matrix = matrix.pointSize(pointSizesNumber);
        testPointSize(
            new double[] {pointSizesNumber[0].doubleValue(), pointSizesNumber[1].doubleValue()},
            matrix);



        matrix = matrix.pointSize(NULL_INT);
        testPointSize(matrix);

        matrix = matrix.pointSize(NULL_LONG);
        testPointSize(matrix);
    }

    private void testPointSize(final double[] size, final ScatterPlotMatrix matrix) {
        final XYDataSeriesInternal series1 =
            (XYDataSeriesInternal) matrix.getFigure().chart(0).axes(0).series(0);
        final XYDataSeriesInternal series2 =
            (XYDataSeriesInternal) matrix.getFigure().chart(1).axes(0).series(0);
        final XYDataSeriesInternal[] seriesArray = new XYDataSeriesInternal[] {series1, series2};

        for (int i = 0; i < size.length; i++) {
            final XYDataSeriesInternal series = seriesArray[i];
            for (int j = 0; j < series.size(); j++) {
                assertEquals(size[i], series.getPointSize(j));
            }
        }
    }

    private void testPointSize(final ScatterPlotMatrix matrix) {
        final XYDataSeriesInternal series1 =
            (XYDataSeriesInternal) matrix.getFigure().chart(0).axes(0).series(0);
        final XYDataSeriesInternal series2 =
            (XYDataSeriesInternal) matrix.getFigure().chart(1).axes(0).series(0);

        for (int j = 0; j < series1.size(); j++) {
            assertNull(series1.getPointSize(j));
        }
        for (int j = 0; j < series2.size(); j++) {
            assertNull(series1.getPointSize(j));
        }
    }

    private void testScatterPlotMatrix(ScatterPlotMatrix matrix) {
        XYDataSeriesInternal series =
            (XYDataSeriesInternal) matrix.getFigure().chart(0).axes(0).series(0);
        for (int i = 0; i < length; i++) {
            assertEquals(0.0, series.getX(i));
            assertEquals(0.0, series.getY(i));
        }
        new FigureImpl(matrix).show();
    }
}
