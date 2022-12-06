/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot.datasets.ohlc;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.gui.color.Color;
import io.deephaven.plot.BaseFigureImpl;
import io.deephaven.plot.datasets.data.IndexableNumericData;
import io.deephaven.plot.datasets.data.IndexableNumericDataArrayDateTime;
import io.deephaven.plot.datasets.data.IndexableNumericDataArrayDouble;
import io.deephaven.plot.datasets.xy.TestAbstractXYDataSeries;
import io.deephaven.plot.util.tables.SwappableTable;
import io.deephaven.plot.util.tables.TableBackedPartitionedTableHandle;
import io.deephaven.plot.util.tables.TableHandle;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;

import java.util.ArrayList;

public class TestOHLCDataSeries extends BaseArrayTestCase {
    private final DateTime[] datesA = {new DateTime(DateTimeUtils.DAY), new DateTime(2 * DateTimeUtils.DAY),
            new DateTime(3 * DateTimeUtils.DAY), new DateTime(4 * DateTimeUtils.DAY)};
    private final double[] openA = {1.0, 2.0, 1.5, 2.0};
    private final double[] closeA = {1.8, 1.8, 1.7, 2.2};
    private final double[] highA = {2.0, 2.0, 1.8, 2.5};
    private final double[] lowA = {0.9, 1.5, 1.5, 1.8};
    private final IndexableNumericData dates = new IndexableNumericDataArrayDateTime(datesA, null);
    private final IndexableNumericData open = new IndexableNumericDataArrayDouble(openA, null);
    private final IndexableNumericData close = new IndexableNumericDataArrayDouble(closeA, null);
    private final IndexableNumericData high = new IndexableNumericDataArrayDouble(highA, null);
    private final IndexableNumericData low = new IndexableNumericDataArrayDouble(lowA, null);

    private final OHLCDataSeriesInternal dataSeries = new OHLCDataSeriesArray(new BaseFigureImpl().newChart().newAxes(),
            1, "Test", dates, open, high, low, close);
    private final OHLCDataSeriesInternal dataSeries2 = new OHLCDataSeriesArray(
            new BaseFigureImpl().newChart().newAxes(), 1, "Test2", dates, close, high, low, open);

    private SafeCloseable executionContext;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        executionContext = TestExecutionContext.createForUnitTests().open();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        executionContext.close();
    }

    public void testOHLCDataSeriesArray() {
        checkOHLCDataSeriesArray(dataSeries, datesA, openA, highA, lowA, closeA);
        checkOHLCDataSeriesArray(dataSeries2, datesA, closeA, highA, lowA, openA);
    }

    private void checkOHLCDataSeriesArray(OHLCDataSeriesInternal dataSeries, DateTime[] time, double[] open,
            double[] high, double[] low, double[] close) {
        assertEquals(dataSeries.size(), time.length);

        for (int i = 0; i < dataSeries.size(); i++) {
            assertEquals(dataSeries.getX(i), (double) time[i].getNanos());
            assertEquals(dataSeries.getY(i), close[i]);
            assertEquals(dataSeries.getOpen(i), open[i]);
            assertEquals(dataSeries.getHigh(i), high[i]);
            assertEquals(dataSeries.getLow(i), low[i]);
            assertEquals(dataSeries.getClose(i), close[i]);
        }
    }

    public void testCopy() {
        final OHLCDataSeriesArray ohlc1 = new OHLCDataSeriesArray(new BaseFigureImpl().newChart().newAxes(), 1, "Test",
                dates, open, high, low, close);
        testCopy(ohlc1, ohlc1.copy(new BaseFigureImpl().newChart().newAxes()));

        final OHLCDataSeriesArray ohlc2 = new OHLCDataSeriesArray(new BaseFigureImpl().newChart().newAxes(), 1, "Test2",
                dates, close, high, low, open);
        ohlc2.pointsVisible(true);
        ohlc2.linesVisible(false);
        ohlc2.pointLabelFormat("{0}: {1}, {2}");
        ohlc2.xToolTipPattern("0.0E0");
        ohlc2.yToolTipPattern("0.0E1");
        ohlc2.seriesColor("blue");
        ohlc2.lineColor("red");
        ohlc2.pointSize(0.5, 4.2, 3.0);
        ohlc2.addTableHandle(new TableHandle(TableTools.emptyTable(2).updateView("A=i", "B=i"), "A", "B"));
        ohlc2.addTableHandle(new TableHandle(TableTools.emptyTable(2).updateView("C=i"), "C"));
        final SwappableTable swappableTable =
                new SwappableTable(new TableBackedPartitionedTableHandle(
                        TableTools.emptyTable(2).updateView("A=i", "B=i"), new ArrayList<>(), new String[0], null)) {
                    @Override
                    public void addColumn(String column) {}
                };
        ohlc2.addSwappableTable(swappableTable);

        final Color c1 = new Color(0, 0, 0);
        final Color c2 = new Color(100, 100, 100);
        final Color c3 = new Color(255, 255, 255);
        final Table tableColors = TableTools.newTable(TableTools.col("Color", c1, c2, c3));
        ohlc2.pointColor(tableColors, "Color");
        testCopy(ohlc2, ohlc2.copy(new BaseFigureImpl().newChart().newAxes()));
    }

    private void testCopy(OHLCDataSeriesArray original, final OHLCDataSeriesArray copy) {
        TestAbstractXYDataSeries.testCopy(original, copy, false);

        for (int i = 0; i < original.size(); i++) {
            assertEquals(original.getOpen(i), copy.getOpen(i));
            assertEquals(original.getHigh(i), copy.getHigh(i));
            assertEquals(original.getLow(i), copy.getLow(i));
            assertEquals(original.getClose(i), copy.getClose(i));
        }
    }
}
