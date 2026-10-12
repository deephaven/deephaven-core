//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.annotations.VisibleForTesting;

import java.util.function.LongSupplier;

public interface ChunkFilter {
    /**
     * Filter a chunk of values, setting parallel values in results to "true" or "false".
     * <p>
     * The results chunk must have capacity at least as large as values.size(); and the result size will be set to
     * values.size() on return.
     * 
     * @param values the values to filter
     * @param results a chunk with the key values where the result of the filter is true
     */
    void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
            WritableLongChunk<OrderedRowKeys> results);

    /**
     * Filter a chunk of values, setting parallel values in {@code results} to the output of the filter.
     * 
     * @param values the values to filter
     * @param results a boolean chunk containing the result of the filter
     *
     * @return the number of values that are {@code true} in {@code results} after the filter is applied.
     */
    int filter(Chunk<? extends Values> values, WritableBooleanChunk<Values> results);

    /**
     * Filter a chunk of values, setting parallel values in {@code results} to {@code false} when the filter result is
     * {@code false}. The filter will not be evaluated for values that are already {@code false} in the results chunk.
     * <p>
     * To use this method effectively, the results chunk should be initialized by a call to
     * {@link #filter(Chunk, WritableBooleanChunk)} or by setting all values {@code true} before the first call.
     * Successive calls will have the effect of AND'ing the filter results with existing results.
     *
     * @param values the values to filter
     * @param results a boolean chunk containing the result of the filter
     *
     * @return the number of values that are {@code true} in {@code results} after the filter is applied.
     */
    int filterAnd(Chunk<? extends Values> values, WritableBooleanChunk<Values> results);

    /**
     * A filter that always returns false.
     */
    ChunkFilter FALSE_FILTER_INSTANCE = new ChunkFilter() {
        @Override
        public void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final int len = values.size();
            results.fillWithValue(0, len, false);
            return 0;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            results.fillWithValue(0, values.size(), false);
            return 0;
        }
    };

    /**
     * A filter that always returns true.
     */
    ChunkFilter TRUE_FILTER_INSTANCE = new ChunkFilter() {
        @Override
        public void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(values.size());
            results.copyFromChunk(keys, 0, 0, keys.size());
        }

        @Override
        public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            final int len = values.size();
            results.fillWithValue(0, len, true);
            return len;
        }

        @Override
        public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
            // No values are set to false, so every value that was true remains true
            final int len = values.size();
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                count += results.get(ii) ? 1 : 0;
            }
            return count;
        }
    };

    /**
     * How many values we read from a column source into a chunk before applying a filter.
     */
    int FILTER_CHUNK_SIZE = 2048;
    /**
     * How many values we wait for before checking for interruption and throwing a cancellation exception
     */
    long INITIAL_INTERRUPTION_SIZE =
            Configuration.getInstance().getLongWithDefault("ChunkFilter.initialInterruptionSize", 1 << 20);
    /**
     * How long we would like to take, in milliseconds between interruption checks
     */
    long INTERRUPTION_GOAL_MILLIS =
            Configuration.getInstance().getLongWithDefault("ChunkFilter.interruptionGoalMillis", 100);

    /**
     * Apply a chunk filter to a RowSet and column source, producing a new WritableRowSet that is responsive to the
     * filter.
     *
     * @param selection the RowSet to filter
     * @param columnSource the column source to filter
     * @param usePrev should we use previous values from the column source?
     * @param chunkFilter the chunk filter to apply
     *
     * @return A new WritableRowSet representing the filtered values, owned by the caller
     */
    static WritableRowSet applyChunkFilter(RowSet selection, ColumnSource<?> columnSource, boolean usePrev,
            ChunkFilter chunkFilter) {
        return applyChunkFilter(selection, columnSource, usePrev, chunkFilter, System::currentTimeMillis);
    }

    /**
     * Apply a chunk filter to a RowSet and column source, reading the time from {@code clockMillis} when tuning how
     * often to check for interruption. Exists so that tests can drive the tuning with a deterministic clock instead of
     * wall time.
     *
     * @param selection the RowSet to filter
     * @param columnSource the column source to filter
     * @param usePrev should we use previous values from the column source?
     * @param chunkFilter the chunk filter to apply
     * @param clockMillis the source of the current time, in milliseconds
     *
     * @return A new WritableRowSet representing the filtered values, owned by the caller
     */
    @VisibleForTesting
    static WritableRowSet applyChunkFilter(RowSet selection, ColumnSource<?> columnSource, boolean usePrev,
            ChunkFilter chunkFilter, LongSupplier clockMillis) {
        return ChunkFilterApplier.applyChunkFilter(selection, columnSource, usePrev, chunkFilter, clockMillis);
    }
}
