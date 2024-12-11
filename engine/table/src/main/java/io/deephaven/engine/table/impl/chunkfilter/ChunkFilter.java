//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;

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
     * Filter a chunk of values, setting parallel values in results to {@code false} when the filter result is
     * {@code false}. The filter is not evaluated for values that are already {@code false} in the results chunk.
     * <p>
     * To use this method effectively, the results chunk should be initialized to {@code true} before the first call.
     * Successive calls will have the effect of AND'ing the filter results with the existing results.
     *
     * @param values the values to filter
     * @param results a boolean chunk containing the result of the filter
     *
     * @return the number of values that were set to {@code false} during this call.
     */
    int filter(Chunk<? extends Values> values, WritableBooleanChunk<Values> results);

    interface CharChunkFilter extends ChunkFilter {
        /**
         * Test if a value matches the filter.
         */
        boolean matches(char value);
    }

    interface ByteChunkFilter extends ChunkFilter {
        /**
         * Test if a value matches the filter.
         */
        boolean matches(byte value);
    }

    interface ShortChunkFilter extends ChunkFilter {
        /**
         * Test if a value matches the filter.
         */
        boolean matches(short value);
    }

    interface IntChunkFilter extends ChunkFilter {
        /**
         * Test if a value matches the filter.
         */
        boolean matches(int value);
    }

    interface LongChunkFilter extends ChunkFilter {
        /**
         * Test if a value matches the filter.
         */
        boolean matches(long value);
    }

    interface FloatChunkFilter extends ChunkFilter {
        /**
         * Test if a value matches the filter.
         */
        boolean matches(float value);
    }

    interface DoubleChunkFilter extends ChunkFilter {
        /**
         * Test if a value matches the filter.
         */
        boolean matches(double value);
    }

    interface ObjectChunkFilter<T> extends ChunkFilter {
        /**
         * Test if a value matches the filter.
         */
        boolean matches(T value);
    }

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
        public int filter(Chunk<? extends Values> values, WritableBooleanChunk<Values> results) {
            final int len = values.size();

            // need to count the values we changed to false
            int count = 0;
            for (int ii = 0; ii < len; ++ii) {
                if (results.get(ii)) {
                    results.set(ii, false);
                    count++;
                }
            }
            return count;
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
        public int filter(Chunk<? extends Values> values, WritableBooleanChunk<Values> results) {
            results.fillWithValue(0, values.size(), true);
            return 0;
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
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();

        final int contextSize = (int) Math.min(FILTER_CHUNK_SIZE, selection.size());
        long chunksBetweenChecks = INITIAL_INTERRUPTION_SIZE / FILTER_CHUNK_SIZE;
        long filteredChunks = 0;
        long lastInterruptCheck = System.currentTimeMillis();

        try (final ColumnSource.GetContext getContext = columnSource.makeGetContext(contextSize);
                final WritableLongChunk<OrderedRowKeys> longChunk = WritableLongChunk.makeWritableChunk(contextSize);
                final RowSequence.Iterator rsIt = selection.getRowSequenceIterator()) {
            while (rsIt.hasMore()) {
                if (filteredChunks++ == chunksBetweenChecks) {
                    if (Thread.interrupted()) {
                        throw new CancellationException("interrupted while filtering data");
                    }

                    final long now = System.currentTimeMillis();
                    final long checkDuration = now - lastInterruptCheck;

                    // tune so that we check at the desired interval, never less than one chunk
                    chunksBetweenChecks = Math.max(1, Math.min(1, checkDuration <= 0 ? chunksBetweenChecks * 2
                            : chunksBetweenChecks * INTERRUPTION_GOAL_MILLIS / checkDuration));
                    lastInterruptCheck = now;
                    filteredChunks = 0;
                }
                final RowSequence okChunk = rsIt.getNextRowSequenceWithLength(contextSize);
                final LongChunk<OrderedRowKeys> keyChunk = okChunk.asRowKeyChunk();

                final Chunk<? extends Values> dataChunk;
                if (usePrev) {
                    dataChunk = columnSource.getPrevChunk(getContext, okChunk);
                } else {
                    dataChunk = columnSource.getChunk(getContext, okChunk);
                }
                chunkFilter.filter(dataChunk, keyChunk, longChunk);

                builder.appendOrderedRowKeysChunk(longChunk);
            }
        }
        return builder.build();
    }
}
