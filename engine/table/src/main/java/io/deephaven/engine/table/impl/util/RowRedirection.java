/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.DefaultChunkSource;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import org.jetbrains.annotations.NotNull;

/**
 * Data structure for mapping one "outer" row key space to another "inner" row key space. Query engine components use
 * this when a {@link RowSet} must be flattened or re-ordered.
 */
public interface RowRedirection extends DefaultChunkSource.WithPrev<RowKeys>, FillUnordered<RowKeys> {

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

    @Override
    default ChunkType getChunkType() {
        return ChunkType.Long;
    }

    @Override
    default boolean providesFillUnordered() {
        return true;
    }

    /**
     * Lookup each element in a {@link RowSequence} and write the result to a {@link WritableLongChunk}.
     *
     * @param fillContext The {@link io.deephaven.engine.table.ChunkSource.FillContext fill context}
     * @param innerRowKeys The result {@link WritableLongChunk}
     * @param outerRowKeys The row keys to lookup in this RowRedirection
     */
    @Override
    default void fillChunk(
            @NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final WritableLongChunk<? super RowKeys> innerRowKeysTyped = innerRowKeys.asWritableLongChunk();
        innerRowKeysTyped.setSize(0);
        outerRowKeys.forAllRowKeys((final long outerRowKey) -> innerRowKeysTyped.add(get(outerRowKey)));
    }

    /**
     * Lookup each element in a {@link LongChunk} and write the result to a {@link WritableLongChunk}.
     *
     * @param fillContext The FillContext
     * @param innerRowKeys The result chunk
     * @param outerRowKeys The row keys to lookup in this RowRedirection
     */
    @Override
    default void fillChunkUnordered(
            @NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final LongChunk<? extends RowKeys> outerRowKeys) {
        final WritableLongChunk<? super RowKeys> innerRowKeysTyped = innerRowKeys.asWritableLongChunk();
        innerRowKeysTyped.setSize(0);
        for (int oki = 0; oki < outerRowKeys.size(); ++oki) {
            innerRowKeysTyped.add(get(outerRowKeys.get(oki)));
        }
    }

    /**
     * Lookup each element in a {@link RowSequence} using <em>previous</em> values and write the result to a
     * {@link WritableLongChunk}.
     *
     * @param fillContext The {@link io.deephaven.engine.table.ChunkSource.FillContext fill context}
     * @param innerRowKeys The result {@link WritableLongChunk}
     * @param outerRowKeys The row keys to lookup in this RowRedirection
     */
    @Override
    default void fillPrevChunk(
            @NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final WritableLongChunk<? super RowKeys> innerRowKeysTyped = innerRowKeys.asWritableLongChunk();
        innerRowKeysTyped.setSize(0);
        outerRowKeys.forAllRowKeys((final long outerRowKey) -> innerRowKeysTyped.add(getPrev(outerRowKey)));
    }



    /**
     * Lookup each element in a {@link LongChunk} using <em>previous</em> values and write the result to a
     * {@link WritableLongChunk}.
     *
     * @param fillContext The FillContext
     * @param innerRowKeys The result chunk
     * @param outerRowKeys The row keys to lookup in this RowRedirection
     */
    @Override
    default void fillPrevChunkUnordered(
            @NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final LongChunk<? extends RowKeys> outerRowKeys) {
        final WritableLongChunk<? super RowKeys> innerRowKeysTyped = innerRowKeys.asWritableLongChunk();
        innerRowKeysTyped.setSize(0);
        for (int oki = 0; oki < outerRowKeys.size(); ++oki) {
            innerRowKeysTyped.add(getPrev(outerRowKeys.get(oki)));
        }
    }

    /**
     * If this RowRedirection is guaranteed to map outer keys in ascending order to inner keys in ascending order; then
     * return true; all other redirections must return false.
     *
     * @return if our output maintains ascending order
     */
    default boolean ascendingMapping() {
        return false;
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
