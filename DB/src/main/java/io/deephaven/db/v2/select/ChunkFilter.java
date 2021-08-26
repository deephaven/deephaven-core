package io.deephaven.db.v2.select;

import io.deephaven.configuration.Configuration;
import io.deephaven.db.exceptions.QueryCancellationException;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;

public interface ChunkFilter {
    /**
     * Filter a chunk of values, setting parallel values in results to "true" or "false".
     *
     * The results chunk must have capacity at least as large as values.size(); and the result size will be set to
     * values.size() on return.
     * 
     * @param values the values to filter
     * @param results a boolean chunk with true values for items that match the filter, and false otherwise
     */
    void filter(Chunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys,
            WritableLongChunk<OrderedKeyIndices> results);

    interface CharChunkFilter extends ChunkFilter {
        void filter(CharChunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results);

        default void filter(Chunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results) {
            filter(values.asCharChunk(), keys, results);
        }
    }

    interface ByteChunkFilter extends ChunkFilter {
        void filter(ByteChunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results);

        default void filter(Chunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results) {
            filter(values.asByteChunk(), keys, results);
        }
    }

    interface ShortChunkFilter extends ChunkFilter {
        void filter(ShortChunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results);

        default void filter(Chunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results) {
            filter(values.asShortChunk(), keys, results);
        }
    }

    interface IntChunkFilter extends ChunkFilter {
        void filter(IntChunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results);

        default void filter(Chunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results) {
            filter(values.asIntChunk(), keys, results);
        }
    }

    interface LongChunkFilter extends ChunkFilter {
        void filter(LongChunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results);

        default void filter(Chunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results) {
            filter(values.asLongChunk(), keys, results);
        }
    }

    interface FloatChunkFilter extends ChunkFilter {
        void filter(FloatChunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results);

        default void filter(Chunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results) {
            filter(values.asFloatChunk(), keys, results);
        }
    }

    interface DoubleChunkFilter extends ChunkFilter {
        void filter(DoubleChunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results);

        default void filter(Chunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results) {
            filter(values.asDoubleChunk(), keys, results);
        }
    }

    interface ObjectChunkFilter<T> extends ChunkFilter {
        void filter(ObjectChunk<T, ? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results);

        default void filter(Chunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys,
                WritableLongChunk<OrderedKeyIndices> results) {
            filter(values.asObjectChunk(), keys, results);
        }
    }

    /**
     * A filter that always returns false.
     */
    ChunkFilter FALSE_FILTER_INSTANCE = (values, keys, results) -> results.setSize(0);

    /**
     * A filter that always returns true.
     */
    ChunkFilter TRUE_FILTER_INSTANCE = (values, keys, results) -> {
        results.setSize(values.size());
        results.copyFromChunk(keys, 0, 0, keys.size());
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
     * Apply a chunk filter to an Index and column source, producing a new Index that is responsive to the filter.
     *
     * @param selection the Index to filter
     * @param columnSource the column source to filter
     * @param usePrev should we use previous values from the column source?
     * @param chunkFilter the chunk filter to apply
     *
     * @return a new Index representing the filtered values
     */
    static Index applyChunkFilter(Index selection, ColumnSource<?> columnSource, boolean usePrev,
            ChunkFilter chunkFilter) {
        final Index.SequentialBuilder builder = Index.FACTORY.getSequentialBuilder();

        final int contextSize = (int) Math.min(FILTER_CHUNK_SIZE, selection.size());
        long chunksBetweenChecks = INITIAL_INTERRUPTION_SIZE / FILTER_CHUNK_SIZE;
        long filteredChunks = 0;
        long lastInterruptCheck = System.currentTimeMillis();

        try (final ColumnSource.GetContext getContext = columnSource.makeGetContext(contextSize);
                final WritableLongChunk<OrderedKeyIndices> longChunk = WritableLongChunk.makeWritableChunk(contextSize);
                final OrderedKeys.Iterator okIt = selection.getOrderedKeysIterator()) {
            while (okIt.hasMore()) {
                if (filteredChunks++ == chunksBetweenChecks) {
                    if (Thread.interrupted()) {
                        throw new QueryCancellationException("interrupted while filtering data");
                    }

                    final long now = System.currentTimeMillis();
                    final long checkDuration = now - lastInterruptCheck;

                    // tune so that we check at the desired interval, never less than one chunk
                    chunksBetweenChecks = Math.max(1, Math.min(1, checkDuration <= 0 ? chunksBetweenChecks * 2
                            : chunksBetweenChecks * INTERRUPTION_GOAL_MILLIS / checkDuration));
                    lastInterruptCheck = now;
                    filteredChunks = 0;
                }
                final OrderedKeys okChunk = okIt.getNextOrderedKeysWithLength(contextSize);
                final LongChunk<OrderedKeyIndices> keyChunk = okChunk.asKeyIndicesChunk();

                final Chunk<? extends Values> dataChunk;
                if (usePrev) {
                    dataChunk = columnSource.getPrevChunk(getContext, okChunk);
                } else {
                    dataChunk = columnSource.getChunk(getContext, okChunk);
                }
                chunkFilter.filter(dataChunk, keyChunk, longChunk);

                builder.appendOrderedKeyIndicesChunk(longChunk);
            }
        }
        return builder.getIndex();
    }
}
