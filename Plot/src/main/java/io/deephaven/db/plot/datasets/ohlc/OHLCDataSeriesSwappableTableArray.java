/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.ohlc;

import io.deephaven.db.plot.AxesImpl;
import io.deephaven.db.plot.TableSnapshotSeries;
import io.deephaven.db.plot.datasets.data.IndexableNumericDataSwappableTable;
import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.plot.util.tables.SwappableTable;
import io.deephaven.db.tables.Table;

import org.jetbrains.annotations.NotNull;

public class OHLCDataSeriesSwappableTableArray extends OHLCDataSeriesArray
    implements TableSnapshotSeries {

    private final SwappableTable swappableTable;
    private final String timeCol;
    private final String openCol;
    private final String highCol;
    private final String lowCol;
    private final String closeCol;
    private Table localTable;

    public OHLCDataSeriesSwappableTableArray(final AxesImpl axes, final int id,
        final Comparable name, @NotNull final SwappableTable swappableTable, final String timeCol,
        final String openCol, final String highCol, final String lowCol, final String closeCol) {
        super(axes, id, name,
            new IndexableNumericDataSwappableTable(swappableTable, timeCol,
                new PlotInfo(axes, name)),
            new IndexableNumericDataSwappableTable(swappableTable, openCol,
                new PlotInfo(axes, name)),
            new IndexableNumericDataSwappableTable(swappableTable, highCol,
                new PlotInfo(axes, name)),
            new IndexableNumericDataSwappableTable(swappableTable, lowCol,
                new PlotInfo(axes, name)),
            new IndexableNumericDataSwappableTable(swappableTable, closeCol,
                new PlotInfo(axes, name)));

        ArgumentValidations.assertNotNull(timeCol, "timeCol", getPlotInfo());
        ArgumentValidations.assertNotNull(openCol, "openCol", getPlotInfo());
        ArgumentValidations.assertNotNull(highCol, "highCol", getPlotInfo());
        ArgumentValidations.assertNotNull(lowCol, "lowCol", getPlotInfo());
        ArgumentValidations.assertNotNull(closeCol, "closeCol", getPlotInfo());

        this.swappableTable = swappableTable;
        this.timeCol = timeCol;
        this.openCol = openCol;
        this.highCol = highCol;
        this.lowCol = lowCol;
        this.closeCol = closeCol;
    }

    private OHLCDataSeriesSwappableTableArray(final OHLCDataSeriesSwappableTableArray series,
        final AxesImpl axes) {
        super(series, axes);
        this.swappableTable = series.swappableTable;
        this.timeCol = series.timeCol;
        this.openCol = series.openCol;
        this.highCol = series.highCol;
        this.lowCol = series.lowCol;
        this.closeCol = series.closeCol;
    }

    @Override
    public OHLCDataSeriesSwappableTableArray copy(AxesImpl axes) {
        return new OHLCDataSeriesSwappableTableArray(this, axes);
    }

}
