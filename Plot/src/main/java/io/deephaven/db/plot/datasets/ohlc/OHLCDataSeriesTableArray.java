/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.ohlc;

import io.deephaven.db.plot.AxesImpl;
import io.deephaven.db.plot.TableSnapshotSeries;
import io.deephaven.db.plot.datasets.data.IndexableNumericDataTable;
import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.plot.util.tables.TableHandle;

import org.jetbrains.annotations.NotNull;

public class OHLCDataSeriesTableArray extends OHLCDataSeriesArray implements TableSnapshotSeries {

    private final TableHandle tableHandle;
    private final String timeCol;
    private final String openCol;
    private final String highCol;
    private final String lowCol;
    private final String closeCol;

    public OHLCDataSeriesTableArray(final AxesImpl axes, final int id, final Comparable name,
        @NotNull final TableHandle th, final String timeCol, final String openCol,
        final String highCol, final String lowCol, final String closeCol) {
        super(axes, id, name, new IndexableNumericDataTable(th, timeCol, new PlotInfo(axes, name)),
            new IndexableNumericDataTable(th, openCol, new PlotInfo(axes, name)),
            new IndexableNumericDataTable(th, highCol, new PlotInfo(axes, name)),
            new IndexableNumericDataTable(th, lowCol, new PlotInfo(axes, name)),
            new IndexableNumericDataTable(th, closeCol, new PlotInfo(axes, name)));

        ArgumentValidations.assertNotNull(timeCol, "timeCol", getPlotInfo());
        ArgumentValidations.assertNotNull(openCol, "openCol", getPlotInfo());
        ArgumentValidations.assertNotNull(highCol, "highCol", getPlotInfo());
        ArgumentValidations.assertNotNull(lowCol, "lowCol", getPlotInfo());
        ArgumentValidations.assertNotNull(closeCol, "closeCol", getPlotInfo());

        this.tableHandle = th;
        this.timeCol = timeCol;
        this.openCol = openCol;
        this.highCol = highCol;
        this.lowCol = lowCol;
        this.closeCol = closeCol;
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    private OHLCDataSeriesTableArray(final OHLCDataSeriesTableArray series, final AxesImpl axes) {
        super(series, axes);
        this.tableHandle = series.tableHandle;
        this.timeCol = series.timeCol;
        this.openCol = series.openCol;
        this.highCol = series.highCol;
        this.lowCol = series.lowCol;
        this.closeCol = series.closeCol;
    }

    @Override
    public OHLCDataSeriesTableArray copy(AxesImpl axes) {
        return new OHLCDataSeriesTableArray(this, axes);
    }
}
