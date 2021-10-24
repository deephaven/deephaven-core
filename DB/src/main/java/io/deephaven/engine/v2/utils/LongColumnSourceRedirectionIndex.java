package io.deephaven.engine.v2.utils;

import io.deephaven.engine.structures.RowSequence;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.v2.sources.WritableChunkSink;
import io.deephaven.engine.v2.sources.WritableSource;
import io.deephaven.engine.v2.sources.chunk.Attributes.Values;
import io.deephaven.engine.v2.sources.chunk.Chunk;
import io.deephaven.engine.v2.sources.chunk.WritableLongChunk;
import org.jetbrains.annotations.NotNull;

/**
 * {@link RedirectionIndex} implementation that wraps a {@link WritableSource} of {@code longs}.
 */
public final class LongColumnSourceRedirectionIndex extends ReadOnlyLongColumnSourceRedirectionIndex<WritableSource<Long>> {

    public LongColumnSourceRedirectionIndex(WritableSource<Long> columnSource) {
        super(columnSource);
    }

    @Override
    public final long put(long key, long index) {
        final long previous = columnSource.getLong(key);

        columnSource.set(key, index);

        return previous == QueryConstants.NULL_LONG ? TrackingMutableRowSet.NULL_ROW_KEY : previous;
    }

    @Override
    public final void putVoid(long key, long index) {
        columnSource.set(key, index);
    }

    @Override
    public final long remove(long key) {
        final long previous = columnSource.getLong(key);
        if (previous == QueryConstants.NULL_LONG) {
            return TrackingMutableRowSet.NULL_ROW_KEY;
        }
        columnSource.set(key, QueryConstants.NULL_LONG);
        return previous;
    }

    @Override
    public final void removeVoid(long key) {
        columnSource.set(key, QueryConstants.NULL_LONG);
    }

    @Override
    public void removeAll(final RowSequence keys) {
        final int numKeys = keys.intSize();
        try(final WritableChunkSink.FillFromContext fillFromContext = columnSource.makeFillFromContext(numKeys);
            final WritableLongChunk<Values> values = WritableLongChunk.makeWritableChunk(numKeys)) {
            values.fillWithNullValue(0, numKeys);
            columnSource.fillFromChunk(fillFromContext, values, keys);
        }
    }

    @Override
    public WritableChunkSink.FillFromContext makeFillFromContext(int chunkCapacity) {
        return columnSource.makeFillFromContext(chunkCapacity);
    }

    @Override
    public void fillFromChunk(@NotNull WritableChunkSink.FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull RowSequence rowSequence) {
        columnSource.fillFromChunk(context, src, rowSequence);
    }

    @Override
    public final void startTrackingPrevValues() {
        columnSource.startTrackingPrevValues();
    }
}
