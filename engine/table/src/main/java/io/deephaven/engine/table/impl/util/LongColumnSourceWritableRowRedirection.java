/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.Chunk;
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
        columnSource.setNull(outerRowKey);
        return previous;
    }

    @Override
    public void removeVoid(long outerRowKey) {
        columnSource.setNull(outerRowKey);
    }

    @Override
    public void removeAll(final RowSequence rowSequence) {
        columnSource.setNull(rowSequence);
    }

    @Override
    public void removeAllUnordered(LongChunk<RowKeys> outerRowKeys) {
        for (int ii = 0; ii < outerRowKeys.size(); ++ii) {
            columnSource.setNull(outerRowKeys.get(ii));
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
