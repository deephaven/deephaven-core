package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

/**
 * Data structure for mapping one "outer" row key space to another "inner" row key space. Query engine components use
 * this when a {@link RowSet} must be flattened or re-ordered.
 */
public interface RowRedirection {

    /**
     * Simple redirected lookup.
     *
     * @param outerRowKey The "outer" row key
     * @return The corresponding "inner" row key, or {@link RowSet#NULL_ROW_KEY} if no mapping exists
     */
    long get(long outerRowKey);

    /**
     * Simple redirected lookup, using previous values.
     *
     * @param outerRowKey The "outer" row key
     * @return The corresponding "inner" row key, or {@link RowSet#NULL_ROW_KEY} if no mapping exists
     */
    long getPrev(long outerRowKey);

    /**
     * A basic, empty, singleton default FillContext instance.
     */
    ChunkSource.FillContext DEFAULT_FILL_INSTANCE = new ChunkSource.FillContext() {
        @Override
        public boolean supportsUnboundedFill() {
            return true;
        }
    };

    /**
     * Make a FillContext for this RowRedirection. The default implementation supplied {@link #DEFAULT_FILL_INSTANCE},
     * suitable for use with the default implementation of
     * {@link #fillChunk(ChunkSource.FillContext, WritableLongChunk, RowSequence)},
     * {@link #fillChunkUnordered(ChunkSource.FillContext, WritableLongChunk, LongChunk)}, and
     * {@link #fillPrevChunk(ChunkSource.FillContext, WritableLongChunk, RowSequence)}.
     *
     * @param chunkCapacity The maximum number of mappings that will be retrieved in one operation
     * @param sharedContext A {@link SharedContext} for use when the same RowRedirection will be used with the same
     *        {@code keysToMap} by multiple consumers
     * @return The FillContext to use
     */
    default ChunkSource.FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return DEFAULT_FILL_INSTANCE;
    }

    /**
     * Lookup each element in a {@link RowSequence} and write the result to a {@link WritableLongChunk}.
     * 
     * @param fillContext The FillContext
     * @param innerRowKeys The result chunk
     * @param outerRowKeys The row keys to lookup in this RowRedirection
     */
    default void fillChunk(
            @NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableLongChunk<? extends RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        innerRowKeys.setSize(0);
        outerRowKeys.forAllRowKeys((final long outerRowKey) -> {
            innerRowKeys.add(get(outerRowKey));
        });
    }

    /**
     * Lookup each element in a {@link LongChunk} and write the result to a {@link WritableLongChunk}.
     * 
     * @param fillContext The FillContext
     * @param innerRowKeys The result chunk
     * @param outerRowKeys The row keys to lookup in this RowRedirection
     */
    default void fillChunkUnordered(
            @NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableLongChunk<? extends RowKeys> innerRowKeys,
            @NotNull final LongChunk<RowKeys> outerRowKeys) {
        innerRowKeys.setSize(0);
        for (int oki = 0; oki < outerRowKeys.size(); ++oki) {
            innerRowKeys.add(get(outerRowKeys.get(oki)));
        }
    }

    /**
     * Lookup each element in a {@link RowSequence} and write the result to a {@link WritableLongChunk}, using previous
     * values.
     *
     * @param fillContext The FillContext
     * @param innerRowKeys The result chunk
     * @param outerRowKeys The row keys to lookup in this RowRedirection
     */
    default void fillPrevChunk(
            @NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableLongChunk<? extends RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        innerRowKeys.setSize(0);
        outerRowKeys.forAllRowKeys((final long outerRowKey) -> {
            innerRowKeys.add(getPrev(outerRowKey));
        });
    }

    /**
     * @return Whether this RowRedirection is actually {@link WritableRowRedirection writable}
     */
    default boolean isWritable() {
        return this instanceof WritableRowRedirection;
    }

    /**
     * <p>
     * Cast this RowRedirection reference to a {@link WritableRowRedirection}.
     *
     * @return {@code this} cast to a {@link WritableRowRedirection}
     * @throws ClassCastException If {@code this} is not a {@link WritableRowRedirection}
     */
    default WritableRowRedirection writableCast() {
        return (WritableRowRedirection) this;
    }
}
