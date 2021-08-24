/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.interval;

import io.deephaven.db.plot.AxesImpl;
import io.deephaven.db.plot.TableSnapshotSeries;
import io.deephaven.db.plot.datasets.data.IndexableNumericData;
import io.deephaven.db.plot.datasets.xy.XYDataSeriesArray;
import io.deephaven.db.plot.errors.PlotIllegalArgumentException;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.plot.util.tables.SwappableTable;
import io.deephaven.db.plot.util.tables.TableHandle;

import org.jetbrains.annotations.NotNull;

/**
 * An {@link XYDataSeriesArray} suitable for bar charts.
 */
public class IntervalXYDataSeriesArray extends XYDataSeriesArray
    implements IntervalXYDataSeriesInternal, TableSnapshotSeries {

    private static final long serialVersionUID = 5911383536377254715L;

    public static final String BIN_MIN = "BinMin";
    public static final String BIN_MID = "BinMid";
    public static final String BIN_MAX = "BinMax";
    public static final String COUNT = "Count";
    private final IndexableNumericData startX;
    private final IndexableNumericData midX;
    private final IndexableNumericData endX;
    private final IndexableNumericData startY;
    private final IndexableNumericData midY;
    private final IndexableNumericData endY;
    private final TableHandle tableHandle;
    private final SwappableTable swappableTable;

    public IntervalXYDataSeriesArray(final AxesImpl axes, final int id, final Comparable name,
        @NotNull final TableHandle tableHandle,
        final IndexableNumericData startX, final IndexableNumericData midX,
        final IndexableNumericData endX,
        final IndexableNumericData startY, final IndexableNumericData midY,
        final IndexableNumericData endY) {
        this(axes, id, name, tableHandle, null, startX, midX, endX, startY, midY, endY);
    }

    public IntervalXYDataSeriesArray(final AxesImpl axes, final int id, final Comparable name,
        @NotNull final SwappableTable swappableTable,
        final IndexableNumericData startX, final IndexableNumericData midX,
        final IndexableNumericData endX,
        final IndexableNumericData startY, final IndexableNumericData midY,
        final IndexableNumericData endY) {
        this(axes, id, name, null, swappableTable, startX, midX, endX, startY, midY, endY);
    }

    private IntervalXYDataSeriesArray(final AxesImpl axes, final int id, final Comparable name,
        final TableHandle tableHandle, final SwappableTable swappableTable,
        final IndexableNumericData startX, final IndexableNumericData midX,
        final IndexableNumericData endX,
        final IndexableNumericData startY, final IndexableNumericData midY,
        final IndexableNumericData endY) {
        this(axes, id, name, tableHandle, swappableTable, startX, midX, endX, startY, midY, endY,
            null);
    }

    /**
     * Creates an instance of IntervalXYDataSeriesArray with the specified data points.
     * <p>
     * {@code startX}, {@code midX}, and {@code endX} at each index define the location of a bar in
     * the chart.
     *
     * @param axes axes displaying the plot
     * @param id data series id
     * @param name series name
     * @param startX lowest x-coordinate of the bar at the given index
     * @param midX middle x-coordinate of the bar at the given index
     * @param endX highest x-coordinate of the bar at the given index
     * @param startY lowest y-coordinate of the bar at the given index
     * @param midY middle y-coordinate of the bar at the given index
     * @param endY highest y-coordinate of the bar at the given index
     * @throws io.deephaven.base.verify.RequirementFailure {@code startX}, {@code midX},
     *         {@code endX}, {@code startY}, {@code midY}, and {@code endY} must not be null
     * @throws IllegalArgumentException {@code startX}, {@code midX}, {@code endX}, {@code startY},
     *         {@code midY}, and {@code endY} must be the same size
     */
    public IntervalXYDataSeriesArray(final AxesImpl axes, final int id, final Comparable name,
        final TableHandle tableHandle, final SwappableTable swappableTable,
        final IndexableNumericData startX, final IndexableNumericData midX,
        final IndexableNumericData endX,
        final IndexableNumericData startY, final IndexableNumericData midY,
        final IndexableNumericData endY,
        final XYDataSeriesArray series) {
        super(axes, id, name, midX, midY, series);

        ArgumentValidations.assertNotNull(startX, "startX", getPlotInfo());
        ArgumentValidations.assertNotNull(startY, "startY", getPlotInfo());
        ArgumentValidations.assertNotNull(midX, "midX", getPlotInfo());
        ArgumentValidations.assertNotNull(midY, "midY", getPlotInfo());
        ArgumentValidations.assertNotNull(endX, "endX", getPlotInfo());
        ArgumentValidations.assertNotNull(endY, "endY", getPlotInfo());

        if (tableHandle == null && swappableTable == null) {
            throw new PlotIllegalArgumentException(
                "One of tableHandle or swappableTable must be non null!", this);
        }

        this.tableHandle = tableHandle;
        this.swappableTable = swappableTable;
        this.startX = startX;
        this.midX = midX;
        this.endX = endX;
        this.startY = startY;
        this.midY = midY;
        this.endY = endY;

        ArgumentValidations.assertSameSize(
            new IndexableNumericData[] {startX, midX, endX, startY, midY, endY},
            new String[] {"startX", "midX", "endX", "startY", "midY", "endY"}, getPlotInfo());
    }

    /**
     * Creates a copy of a series using a different Axes.
     *
     * @param series series to copy.
     * @param axes new axes to use.
     */
    private IntervalXYDataSeriesArray(final IntervalXYDataSeriesArray series, final AxesImpl axes) {
        super(series, axes);

        this.tableHandle = series.tableHandle;
        this.swappableTable = series.swappableTable;
        this.startX = series.startX;
        this.midX = series.midX;
        this.endX = series.endX;
        this.startY = series.startY;
        this.midY = series.midY;
        this.endY = series.endY;
    }

    @Override
    public IntervalXYDataSeriesArray copy(AxesImpl axes) {
        return new IntervalXYDataSeriesArray(this, axes);
    }

    @Override
    public double getStartX(final int item) {
        return startX.get(item);
    }

    @Override
    public double getEndX(final int item) {
        return endX.get(item);
    }

    @Override
    public double getStartY(final int item) {
        return startY.get(item);
    }

    @Override
    public double getEndY(final int item) {
        return endY.get(item);
    }

    public IndexableNumericData getStartX() {
        return startX;
    }

    public IndexableNumericData getEndX() {
        return endX;
    }

    public IndexableNumericData getStartY() {
        return startY;
    }

    public IndexableNumericData getEndY() {
        return endY;
    }
}
