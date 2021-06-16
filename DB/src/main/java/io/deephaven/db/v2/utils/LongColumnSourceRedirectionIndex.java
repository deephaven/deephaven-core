package io.deephaven.db.v2.utils;

import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableChunkSink;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import org.jetbrains.annotations.NotNull;

public final class LongColumnSourceRedirectionIndex implements RedirectionIndex {
    private final WritableSource<Long> columnSource;

    public LongColumnSourceRedirectionIndex(WritableSource<Long> columnSource) {
        this.columnSource = columnSource;
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
    public final long get(long key) {
        final long innerIndex = columnSource.getLong(key);
        if (innerIndex == QueryConstants.NULL_LONG) {
            return Index.NULL_KEY;
        }
        return innerIndex;
    }

    @Override
    public final long getPrev(long key) {
        final long innerIndex = columnSource.getPrevLong(key);
        if (innerIndex == QueryConstants.NULL_LONG) {
            return Index.NULL_KEY;
        }
        return innerIndex;
    }

    private static final class FillContext implements RedirectionIndex.FillContext {

        private final ColumnSource.FillContext colSrcCtx;

        private FillContext(final LongColumnSourceRedirectionIndex csrc, final int chunkSize) {
            colSrcCtx = csrc.columnSource.makeFillContext(chunkSize);
        }

        @Override
        public final void close() {
            colSrcCtx.close();
        }
    }
    @Override
    public final RedirectionIndex.FillContext makeFillContext(final int chunkSize, final SharedContext sharedContext) {
        return new FillContext(this, chunkSize);
    }

    @Override
    public final void fillChunk(
            @NotNull final RedirectionIndex.FillContext fillContext,
            @NotNull final WritableLongChunk<KeyIndices> mappedKeysOut,
            @NotNull final OrderedKeys keysToMap) {
        final WritableLongChunk<Values> asValuesChunk = WritableLongChunk.upcast(mappedKeysOut);
        final FillContext effectiveContext = (FillContext) fillContext;
        columnSource.fillChunk(effectiveContext.colSrcCtx, asValuesChunk, keysToMap);
        for (int ii = 0; ii < mappedKeysOut.size(); ++ii) {
            if (mappedKeysOut.get(ii) == QueryConstants.NULL_LONG) {
                mappedKeysOut.set(ii, Index.NULL_KEY);
            }
        }
    }

    @Override
    public final void fillPrevChunk(
            @NotNull final RedirectionIndex.FillContext fillContext,
            @NotNull final WritableLongChunk<KeyIndices> mappedKeysOut,
            @NotNull final OrderedKeys keysToMap) {
        final WritableLongChunk<Values> asValuesChunk = WritableLongChunk.downcast(WritableLongChunk.upcast(mappedKeysOut));
        final FillContext effectiveContext = (FillContext) fillContext;
        columnSource.fillPrevChunk(effectiveContext.colSrcCtx, asValuesChunk, keysToMap);
        for (int ii = 0; ii < mappedKeysOut.size(); ++ii) {
            if (mappedKeysOut.get(ii) == QueryConstants.NULL_LONG) {
                mappedKeysOut.set(ii, Index.NULL_KEY);
            }
        }
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
