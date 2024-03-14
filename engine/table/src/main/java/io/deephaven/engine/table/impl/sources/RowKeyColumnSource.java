//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

/**
 * This is a column source that uses no additional memory and is an identity mapping from row key to row key.
 */
public class RowKeyColumnSource extends AbstractColumnSource<Long>
        implements ImmutableColumnSourceGetDefaults.ForLong, FillUnordered<Values> {
    public static final RowKeyColumnSource INSTANCE = new RowKeyColumnSource();

    public RowKeyColumnSource() {
        super(Long.class);
    }

    @Override
    public long getLong(long rowKey) {
        return rowKey < 0 ? QueryConstants.NULL_LONG : rowKey;
    }

    @Override
    public void fillChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        doFillChunk(destination, rowSequence);
    }

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        doFillChunk(destination, rowSequence);
    }

    static void doFillChunk(
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        final WritableLongChunk<? super Values> longChunk = destination.asWritableLongChunk();
        if (rowSequence.isContiguous()) {
            final int size = rowSequence.intSize();
            longChunk.setSize(size);
            final long firstRowKey = rowSequence.firstRowKey();
            for (int ii = 0; ii < size; ++ii) {
                longChunk.set(ii, firstRowKey + ii);
            }
        } else {
            rowSequence.fillRowKeyChunk(longChunk);
        }
    }

    @Override
    public boolean providesFillUnordered() {
        return true;
    }

    @Override
    public void fillChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        doFillUnordered(dest, keys);
    }


    @Override
    public void fillPrevChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        doFillUnordered(dest, keys);
    }

    private void doFillUnordered(
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        final WritableLongChunk<? super Values> longChunk = dest.asWritableLongChunk();
        longChunk.setSize(keys.size());
        for (int ii = 0; ii < keys.size(); ++ii) {
            long key = keys.get(ii);
            longChunk.set(ii, key < 0 ? QueryConstants.NULL_LONG : key);
        }
    }
}
