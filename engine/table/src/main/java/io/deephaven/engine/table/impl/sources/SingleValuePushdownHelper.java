//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BasePushdownFilterContext;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.util.SafeCloseable;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class SingleValuePushdownHelper {
    private static final long[] singleRowKeyArray = new long[] {0};
    private static final LongChunk<OrderedRowKeys> singleRowKeyChunk = LongChunk.chunkWrap(singleRowKeyArray);

    public static LongChunk<OrderedRowKeys> singleRowKeyChunk() {
        return singleRowKeyChunk;
    }

    /**
     * Returns a new chunk containing the single byte value.
     */
    public static Chunk<Values> makeChunk(byte value) {
        final byte[] arr = new byte[] {value};
        return ByteChunk.chunkWrap(arr);
    }

    /**
     * Returns a new chunk containing the single char value.
     */
    public static Chunk<Values> makeChunk(char value) {
        final char[] arr = new char[] {value};
        return CharChunk.chunkWrap(arr);
    }

    /**
     * Returns a new chunk containing the single short value.
     */
    public static Chunk<Values> makeChunk(short value) {
        final short[] arr = new short[] {value};
        return ShortChunk.chunkWrap(arr);
    }

    /**
     * Returns a new chunk containing the single int value.
     */
    public static Chunk<Values> makeChunk(int value) {
        final int[] arr = new int[] {value};
        return IntChunk.chunkWrap(arr);
    }

    /**
     * Returns a new chunk containing the single long value.
     */
    public static Chunk<Values> makeChunk(long value) {
        final long[] arr = new long[] {value};
        return LongChunk.chunkWrap(arr);
    }

    /**
     * Returns a new chunk containing the single float value.
     */
    public static Chunk<Values> makeChunk(float value) {
        final float[] arr = new float[] {value};
        return FloatChunk.chunkWrap(arr);
    }

    /**
     * Returns a new chunk containing the single double value.
     */
    public static Chunk<Values> makeChunk(double value) {
        final double[] arr = new double[] {value};
        return DoubleChunk.chunkWrap(arr);
    }

    /**
     * Returns a new chunk containing the single boolean value.
     */
    public static Chunk<Values> makeChunk(boolean value) {
        final boolean[] arr = new boolean[] {value};
        return BooleanChunk.chunkWrap(arr);
    }

    /**
     * Returns a new chunk containing the single object value.
     */
    public static Chunk<Values> makeChunk(Object value) {
        final Object[] arr = new Object[] {value};
        return ObjectChunk.chunkWrap(arr);
    }

    /**
     * A pushdown filter context for row key agnostic chunk sources.
     */
    public static class FilterContext extends BasePushdownFilterContext {
        public FilterContext(
                final WhereFilter filter,
                final List<ColumnSource<?>> columnSources) {
            super(filter, columnSources);
        }
    }

    /**
     * Execute the {@link #chunkFilter(BasePushdownFilterContext, Supplier)} or
     * {@link #tableFilter(WhereFilter, RowSet, boolean, ColumnSource)} to test whether the single value matches the
     * supplied filter.
     */
    public static boolean filter(
            final RowSet selection,
            final boolean usePrev,
            final BasePushdownFilterContext context,
            final Supplier<Chunk<Values>> valueChunkSupplier,
            final ColumnSource<?> columnSource) {

        // Chunk filtering has lower overhead than creating a dummy table.
        if (context.supportsChunkFiltering()) {
            return chunkFilter(context, valueChunkSupplier);
        }

        return tableFilter(context.filter(), selection, usePrev, columnSource);
    }

    /**
     * Execute the chunk filter from the context and return {@code true} if the filter matches any rows.
     */
    public static boolean chunkFilter(
            final BasePushdownFilterContext context,
            final Supplier<Chunk<Values>> valueChunkSupplier) {

        try (final BasePushdownFilterContext.UnifiedChunkFilter chunkFilter = context.createChunkFilter(1)) {
            final LongChunk<OrderedRowKeys> resultChunk =
                    chunkFilter.filter(valueChunkSupplier.get(), SingleValuePushdownHelper.singleRowKeyChunk());
            return resultChunk.size() > 0;
        }
    }

    /**
     * Execute the filter against a dummy table and return {@code true} if the filter matches any rows.
     */
    public static boolean tableFilter(
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final ColumnSource<?> columnSource) {
        // Create a single row table, execute the filter, and return `selection` or empty depending on the result.
        final String columnName = filter.getColumns().get(0);

        try (final SafeCloseable ignored = LivenessScopeStack.open();
                final TrackingWritableRowSet rowSet = RowSetFactory.fromKeys(selection.firstRowKey()).toTracking()) {

            // Create a dummy table containing only this column source.
            final Map<String, ColumnSource<?>> columnSourceMap = Map.of(columnName, columnSource);
            final Table dummyTable = new QueryTable(rowSet, columnSourceMap);

            // Execute the filter on the dummy table.
            try (final RowSet result = filter.filter(rowSet, rowSet, dummyTable, usePrev)) {
                return !result.isEmpty();
            }
        }
    }
}
