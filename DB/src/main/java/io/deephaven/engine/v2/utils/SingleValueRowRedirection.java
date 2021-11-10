/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils;

import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.chunk.Attributes.RowKeys;
import io.deephaven.engine.rftable.ChunkSource;
import io.deephaven.engine.chunk.WritableLongChunk;
import org.jetbrains.annotations.NotNull;

/**
 * {@link RowRedirection} that redirects all outer row keys to the same inner row key.
 */
public class SingleValueRowRedirection implements RowRedirection {

    protected long value;

    public SingleValueRowRedirection(final long value) {
        this.value = value;
    }

    @Override
    public long get(final long outerRowKey) {
        return value;
    }

    @Override
    public long getPrev(final long outerRowKey) {
        return value;
    }

    @Override
    public String toString() {
        return "SingleValueRowRedirection{" + value + "}";
    }

    @Override
    public void fillChunk(
            @NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableLongChunk<? extends RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final int sz = outerRowKeys.intSize();
        innerRowKeys.setSize(sz);
        innerRowKeys.fillWithValue(0, sz, value);
    }

    @Override
    public void fillPrevChunk(
            @NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableLongChunk<? extends RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        // no prev
        fillChunk(fillContext, innerRowKeys, outerRowKeys);
    }

    public long getValue() {
        return value;
    }

    /**
     * @return Whether this SingleValueRowRedirection is actually {@link MutableSingleValueRowRedirection mutable}
     */
    public final boolean isMutableSingleValue() {
        return this instanceof MutableSingleValueRowRedirection;
    }

    /**
     * <p>
     * Cast this SingleValueRowRedirection reference to a {@link MutableSingleValueRowRedirection}.
     *
     * @return {@code this} cast to a {@link MutableSingleValueRowRedirection}
     * @throws ClassCastException If {@code this} is not a {@link MutableSingleValueRowRedirection}
     */
    public final MutableSingleValueRowRedirection mutableSingleValueCast() {
        return (MutableSingleValueRowRedirection) this;
    }
}
