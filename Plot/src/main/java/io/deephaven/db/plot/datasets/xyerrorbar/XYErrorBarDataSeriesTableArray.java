/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.xyerrorbar;

import io.deephaven.db.plot.AxesImpl;
import io.deephaven.db.plot.TableSnapshotSeries;
import io.deephaven.db.plot.datasets.data.IndexableNumericDataTable;
import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.tables.TableHandle;

import org.jetbrains.annotations.NotNull;

public class XYErrorBarDataSeriesTableArray extends XYErrorBarDataSeriesArray
    implements TableSnapshotSeries {

    private final TableHandle tableHandle;
    private final String x;
    private final String xLow;
    private final String xHigh;
    private final String y;
    private final String yLow;
    private final String yHigh;

    public XYErrorBarDataSeriesTableArray(final AxesImpl axes, final int id, final Comparable name,
        @NotNull final TableHandle tableHandle, final String x, final String xLow,
        final String xHigh, final String y, final String yLow, final String yHigh,
        final boolean drawXError, final boolean drawYError) {
        super(axes, id, name,
            new IndexableNumericDataTable(tableHandle, x, new PlotInfo(axes, name)),
            xLow == null ? null
                : new IndexableNumericDataTable(tableHandle, xLow, new PlotInfo(axes, name)),
            xHigh == null ? null
                : new IndexableNumericDataTable(tableHandle, xHigh, new PlotInfo(axes, name)),
            new IndexableNumericDataTable(tableHandle, y, new PlotInfo(axes, name)),
            yLow == null ? null
                : new IndexableNumericDataTable(tableHandle, yLow, new PlotInfo(axes, name)),
            yHigh == null ? null
                : new IndexableNumericDataTable(tableHandle, yHigh, new PlotInfo(axes, name)),
            drawXError, drawYError);

        this.tableHandle = tableHandle;
        this.x = x;
        this.xLow = xLow;
        this.xHigh = xHigh;
        this.y = y;
        this.yLow = yLow;
        this.yHigh = yHigh;
    }

    private XYErrorBarDataSeriesTableArray(final XYErrorBarDataSeriesTableArray series,
        final AxesImpl axes) {
        super(series, axes);
        this.tableHandle = series.tableHandle;
        this.x = series.x;
        this.xLow = series.xLow;
        this.xHigh = series.xHigh;
        this.y = series.y;
        this.yLow = series.yLow;
        this.yHigh = series.yHigh;
    }

    @Override
    public XYErrorBarDataSeriesTableArray copy(final AxesImpl axes) {
        return new XYErrorBarDataSeriesTableArray(this, axes);
    }
}
