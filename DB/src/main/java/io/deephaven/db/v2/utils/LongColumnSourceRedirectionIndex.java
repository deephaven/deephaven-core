package io.deephaven.db.v2.utils;

import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.sources.WritableChunkSink;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
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

        return previous == QueryConstants.NULL_LONG ? Index.NULL_KEY : previous;
    }

    @Override
    public final void putVoid(long key, long index) {
        columnSource.set(key, index);
    }

    @Override
    public final long remove(long key) {
        final long previous = columnSource.getLong(key);
        if (previous == QueryConstants.NULL_LONG) {
            return Index.NULL_KEY;
        }
        columnSource.set(key, QueryConstants.NULL_LONG);
        return previous;
    }

    @Override
    public final void removeVoid(long key) {
        columnSource.set(key, QueryConstants.NULL_LONG);
    }

    @Override
    public void removeAll(final OrderedKeys keys) {
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
    public void fillFromChunk(@NotNull WritableChunkSink.FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull OrderedKeys orderedKeys) {
        columnSource.fillFromChunk(context, src, orderedKeys);
    }

    @Override
    public final void startTrackingPrevValues() {
        columnSource.startTrackingPrevValues();
    }
}
