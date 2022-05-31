/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.table.ColumnSource;

import java.util.Collections;
import java.util.List;

/**
 * Utilities for downsampling non-ticking time series data within a query. The input table must be sorted by the
 * {@link DateTime} column to be used for binning rows.
 * <p>
 * </p>
 * <p>
 * Usage is of the form:
 * {@code downsampledX = x.where(new DownsampledWhereFilter("Timestamp", 5 * DateTimeUtils.MINUTE));}
 * </p>
 */

public class DownsampledWhereFilter extends WhereFilterImpl {
    private final String column;
    private final long binSize;
    private final SampleOrder order;

    /**
     * Enum to use when selecting downsampling behavior:
     * <p>
     * LOWERFIRST is the constant for lowerBin/firstBy.
     * </p>
     * <p>
     * UPPERLAST is the constant for upperBin/lastBy. (Default)
     * </p>
     */
    public enum SampleOrder {
        UPPERLAST, LOWERFIRST
    }

    /**
     * Creates a {@link DownsampledWhereFilter} which can be used in a .where clause to downsample time series rows.
     * 
     * @param column {@link DateTime} column to use for filtering.
     * @param binSize Size in nanoseconds for the time bins. Constants like {@link DateTimeUtils#MINUTE} are typically
     *        used.
     * @param order {@link SampleOrder} to set desired behavior.
     */
    public DownsampledWhereFilter(String column, long binSize, SampleOrder order) {
        this.column = column;
        this.binSize = binSize;
        this.order = order;
    }

    /**
     * Creates a {@link DownsampledWhereFilter} which can be used in a .where clause to downsample time series rows.
     * 
     * @param column {@link DateTime} column to use for filtering.
     * @param binSize Size in nanoseconds for the time bins. Constants like {@link DateTimeUtils#MINUTE} are typically
     *        used.
     */
    public DownsampledWhereFilter(String column, long binSize) {
        this.column = column;
        this.binSize = binSize;
        this.order = SampleOrder.UPPERLAST;
    }

    @Override
    public List<String> getColumns() {
        return Collections.singletonList(column);
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @Override
    public void init(TableDefinition tableDefinition) {}

    @Override
    public WritableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        if (DynamicNode.isDynamicAndIsRefreshing(table)) {
            throw new UnsupportedOperationException("Can not do a DownsampledWhereFilter on a refreshing table!");
        }

        // NB: because our source is not refreshing, we don't care about the previous values

        ColumnSource<DateTime> timestampColumn = table.getColumnSource(column);

        RowSetBuilderSequential builder = RowSetFactory.builderSequential();


        RowSet.Iterator it = selection.iterator();
        boolean hasNext = it.hasNext();

        long lastKey = -1;
        DateTime lastBin = null;

        while (hasNext) {
            long next = it.nextLong();
            hasNext = it.hasNext();

            DateTime timestamp = timestampColumn.get(next);
            DateTime bin = (order == SampleOrder.UPPERLAST) ? DateTimeUtils.upperBin(timestamp, binSize)
                    : DateTimeUtils.lowerBin(timestamp, binSize);
            if (!hasNext) {
                if (order == SampleOrder.UPPERLAST) {
                    if (lastKey != -1 && (lastBin != null && !lastBin.equals(bin))) {
                        builder.appendKey(lastKey);
                    }
                    builder.appendKey(next);
                } else if (lastBin != null && !lastBin.equals(bin)) {
                    builder.appendKey(next);
                }
            } else {
                if (lastBin == null || !bin.equals(lastBin)) {
                    if (order == SampleOrder.UPPERLAST && lastKey != -1) {
                        builder.appendKey(lastKey);
                    } else if (order == SampleOrder.LOWERFIRST) {
                        builder.appendKey(next);
                    }
                    lastBin = bin;
                }
                lastKey = next;
            }
        }

        return builder.build();
    }

    @Override
    public boolean isSimpleFilter() {
        return true;
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {

    }

    @Override
    public DownsampledWhereFilter copy() {
        return new DownsampledWhereFilter(column, binSize, order);
    }
}
