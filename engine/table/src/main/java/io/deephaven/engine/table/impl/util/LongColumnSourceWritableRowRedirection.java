/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

/**
 * {@link WritableRowRedirection} implementation that wraps a {@link WritableColumnSource} of {@code longs}.
 */
public final class LongColumnSourceWritableRowRedirection
        extends LongColumnSourceRowRedirection<WritableColumnSource<Long>>
        implements WritableRowRedirection {

    public LongColumnSourceWritableRowRedirection(WritableColumnSource<Long> columnSource) {
        super(columnSource);
    }

    @Override
    public long put(long outerRowKey, long innerRowKey) {
        final long previous = columnSource.getLong(outerRowKey);

        columnSource.set(outerRowKey, innerRowKey);

        return previous == QueryConstants.NULL_LONG ? RowSequence.NULL_ROW_KEY : previous;
    }

    @Override
    public void putVoid(long outerRowKey, long innerRowKey) {
        columnSource.set(outerRowKey, innerRowKey);
    }

    @Override
    public long remove(long outerRowKey) {
        final long previous = columnSource.getLong(outerRowKey);
        if (previous == QueryConstants.NULL_LONG) {
            return RowSequence.NULL_ROW_KEY;
        }
        columnSource.set(outerRowKey, QueryConstants.NULL_LONG);
        return previous;
    }

    @Override
    public void removeVoid(long outerRowKey) {
        columnSource.set(outerRowKey, QueryConstants.NULL_LONG);
    }

    @Override
    public void removeAll(final RowSequence outerRowKeys) {
        final int numKeys = outerRowKeys.intSize();
        try (final ChunkSink.FillFromContext fillFromContext = columnSource.makeFillFromContext(numKeys);
             final WritableLongChunk<Values> values = WritableLongChunk.makeWritableChunk(numKeys)) {
            values.fillWithNullValue(0, numKeys);
            columnSource.fillFromChunk(fillFromContext, values, outerRowKeys);
        }
    }

    @Override
    public ChunkSink.FillFromContext makeFillFromContext(int chunkCapacity) {
        return columnSource.makeFillFromContext(chunkCapacity);
    }

    @Override
    public void fillFromChunk(
            @NotNull final ChunkSink.FillFromContext context,
            @NotNull final Chunk<? extends RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        columnSource.fillFromChunk(context, innerRowKeys, outerRowKeys);
    }

    @Override
    public void startTrackingPrevValues() {
        columnSource.startTrackingPrevValues();
    }
}
