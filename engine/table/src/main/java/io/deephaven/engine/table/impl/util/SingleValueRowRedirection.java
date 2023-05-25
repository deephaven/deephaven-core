/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
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
            @NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final int sz = outerRowKeys.intSize();
        innerRowKeys.setSize(sz);
        innerRowKeys.asWritableLongChunk().fillWithValue(0, sz, value);
    }

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        // no prev
        fillChunk(fillContext, innerRowKeys, outerRowKeys);
    }

    public long getValue() {
        return value;
    }

    /**
     * @return Whether this SingleValueRowRedirection is actually {@link WritableSingleValueRowRedirection writable}
     */
    public final boolean isWritableSingleValue() {
        return this instanceof WritableSingleValueRowRedirection;
    }

    /**
     * <p>
     * Cast this SingleValueRowRedirection reference to a {@link WritableSingleValueRowRedirection}.
     *
     * @return {@code this} cast to a {@link WritableSingleValueRowRedirection}
     * @throws ClassCastException If {@code this} is not a {@link WritableSingleValueRowRedirection}
     */
    public final WritableSingleValueRowRedirection writableSingleValueCast() {
        return (WritableSingleValueRowRedirection) this;
    }
}
