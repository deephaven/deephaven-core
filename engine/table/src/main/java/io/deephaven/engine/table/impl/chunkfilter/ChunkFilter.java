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

import java.util.function.LongConsumer;

public interface ChunkFilter {
    /**
     * Filter a chunk of values, setting parallel values in results to "true" or "false".
     * <p>
     * The results chunk must have capacity at least as large as values.size(); and the result size will be set to
     * values.size() on return.
     * 
     * @param values the values to filter
     * @param results a boolean chunk with true values for items that match the filter, and false otherwise
     */
    void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
            WritableLongChunk<OrderedRowKeys> results);

    /**
     * Filter a chunk of values, reading keys from an iterator and writing keys to a consumer.
     *
     * @param values the values to filter
     * @param rows a {@link RowSequence} that provides the keys for the values, must be exactly the length of the values
     *        in {@code values}
     * @param consumer a consumer that will be called with the keys that pass the filter
     */
    void filter(Chunk<? extends Values> values, RowSequence rows, LongConsumer consumer);

    interface CharChunkFilter extends ChunkFilter {
        void filter(CharChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results);

        default void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            filter(values.asCharChunk(), keys, results);
        }

        void filter(CharChunk<? extends Values> values, RowSequence rows, LongConsumer consumer);

        default void filter(Chunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            filter(values.asCharChunk(), rows, consumer);
        }
    }

    interface ByteChunkFilter extends ChunkFilter {
        void filter(ByteChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results);

        default void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            filter(values.asByteChunk(), keys, results);
        }

        void filter(ByteChunk<? extends Values> values, RowSequence rows, LongConsumer consumer);

        default void filter(Chunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            filter(values.asByteChunk(), rows, consumer);
        }
    }

    interface ShortChunkFilter extends ChunkFilter {
        void filter(ShortChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results);

        default void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            filter(values.asShortChunk(), keys, results);
        }

        void filter(ShortChunk<? extends Values> values, RowSequence rows, LongConsumer consumer);

        default void filter(Chunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            filter(values.asShortChunk(), rows, consumer);
        }
    }

    interface IntChunkFilter extends ChunkFilter {
        void filter(IntChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results);

        default void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            filter(values.asIntChunk(), keys, results);
        }

        void filter(IntChunk<? extends Values> values, RowSequence rows, LongConsumer consumer);

        default void filter(Chunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            filter(values.asIntChunk(), rows, consumer);
        }
    }

    interface LongChunkFilter extends ChunkFilter {
        void filter(LongChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results);

        default void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            filter(values.asLongChunk(), keys, results);
        }

        void filter(LongChunk<? extends Values> values, RowSequence rows, LongConsumer consumer);

        default void filter(Chunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            filter(values.asLongChunk(), rows, consumer);
        }
    }

    interface FloatChunkFilter extends ChunkFilter {
        void filter(FloatChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results);

        default void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            filter(values.asFloatChunk(), keys, results);
        }

        void filter(FloatChunk<? extends Values> values, RowSequence rows, LongConsumer consumer);

        default void filter(Chunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            filter(values.asFloatChunk(), rows, consumer);
        }
    }

    interface DoubleChunkFilter extends ChunkFilter {
        void filter(DoubleChunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results);

        default void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            filter(values.asDoubleChunk(), keys, results);
        }

        void filter(DoubleChunk<? extends Values> values, RowSequence rows, LongConsumer consumer);

        default void filter(Chunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            filter(values.asDoubleChunk(), rows, consumer);
        }
    }

    interface ObjectChunkFilter<T> extends ChunkFilter {
        void filter(ObjectChunk<T, ? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results);

        default void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            filter(values.asObjectChunk(), keys, results);
        }

        void filter(ObjectChunk<T, ? extends Values> values, RowSequence rows, LongConsumer consumer);

        default void filter(Chunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            filter(values.asObjectChunk(), rows, consumer);
        }
    }

    /**
     * A filter that always returns false.
     */
    ChunkFilter FALSE_FILTER_INSTANCE = new ChunkFilter() {
        @Override
        public void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                io.deephaven.chunk.WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
        }

        @Override
        public void filter(Chunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            // No work to perform
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
        public void filter(Chunk<? extends Values> values, RowSequence rows, LongConsumer consumer) {
            rows.forAllRowKeys(consumer);
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
                final Chunk<? extends Values> dataChunk;
                if (usePrev) {
                    dataChunk = columnSource.getPrevChunk(getContext, okChunk);
                } else {
                    dataChunk = columnSource.getChunk(getContext, okChunk);
                }
                chunkFilter.filter(dataChunk, okChunk, builder::appendKey);
            }
        }
        return builder.build();
    }
}
