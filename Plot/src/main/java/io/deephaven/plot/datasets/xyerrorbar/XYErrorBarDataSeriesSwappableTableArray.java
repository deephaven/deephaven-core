/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.xyerrorbar;

import io.deephaven.plot.AxesImpl;
import io.deephaven.plot.TableSnapshotSeries;
import io.deephaven.plot.datasets.data.IndexableNumericDataSwappableTable;
import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.util.tables.SwappableTable;
import io.deephaven.engine.table.Table;

import org.jetbrains.annotations.NotNull;

public class XYErrorBarDataSeriesSwappableTableArray extends XYErrorBarDataSeriesArray implements TableSnapshotSeries {

    private final SwappableTable swappableTable;
    private final String x;
    private final String xLow;
    private final String xHigh;
    private final String y;
    private final String yLow;
    private final String yHigh;
    private Table localTable;

    public XYErrorBarDataSeriesSwappableTableArray(final AxesImpl axes, final int id, final Comparable name,
            @NotNull final SwappableTable swappableTable, final String x, final String xLow, final String xHigh,
            final String y, final String yLow, final String yHigh, final boolean drawXError, final boolean drawYError) {
        super(axes, id, name, new IndexableNumericDataSwappableTable(swappableTable, x, new PlotInfo(axes, name)),
                xLow == null ? null
                        : new IndexableNumericDataSwappableTable(swappableTable, xLow, new PlotInfo(axes, name)),
                xHigh == null ? null
                        : new IndexableNumericDataSwappableTable(swappableTable, xHigh, new PlotInfo(axes, name)),
                new IndexableNumericDataSwappableTable(swappableTable, y, new PlotInfo(axes, name)),
                yLow == null ? null
                        : new IndexableNumericDataSwappableTable(swappableTable, yLow, new PlotInfo(axes, name)),
                yHigh == null ? null
                        : new IndexableNumericDataSwappableTable(swappableTable, yHigh, new PlotInfo(axes, name)),
                drawXError, drawYError);

        this.swappableTable = swappableTable;
        this.x = x;
        this.xLow = xLow;
        this.xHigh = xHigh;
        this.y = y;
        this.yLow = yLow;
        this.yHigh = yHigh;
    }

    private XYErrorBarDataSeriesSwappableTableArray(final XYErrorBarDataSeriesSwappableTableArray series,
            final AxesImpl axes) {
        super(series, axes);
        this.swappableTable = series.swappableTable;
        this.x = series.x;
        this.xLow = series.xLow;
        this.xHigh = series.xHigh;
        this.y = series.y;
        this.yLow = series.yLow;
        this.yHigh = series.yHigh;
    }

    @Override
    public XYErrorBarDataSeriesSwappableTableArray copy(final AxesImpl axes) {
        return new XYErrorBarDataSeriesSwappableTableArray(this, axes);
    }

}
