/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import org.jetbrains.annotations.NotNull;

/**
 * A column source that returns the row key as a long.
 */
public class RowKeySource extends AbstractColumnSource<Long> implements ImmutableColumnSourceGetDefaults.ForLong {

    public static final RowKeySource INSTANCE = new RowKeySource();

    private RowKeySource() {
        super(Long.class);
    }

    @Override
    public long getLong(long rowKey) {
        return rowKey;
    }

    @Override
    public void fillChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        rowSequence.fillRowKeyChunk(destination.asWritableLongChunk());
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(
            @NotNull final GetContext context,
            @NotNull final RowSequence rowSequence) {
        return getChunk(context, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(
            @NotNull final GetContext context,
            final long firstKey,
            final long lastKey) {
        return getChunk(context, firstKey, lastKey);
    }

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        fillChunk(context, destination, rowSequence);
    }
}
