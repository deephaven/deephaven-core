package io.deephaven.db.v2.utils;

import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.db.v2.sources.chunk.WritableIntChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import org.jetbrains.annotations.NotNull;

public final class IntColumnSourceRedirectionIndex implements RedirectionIndex {

    private final WritableSource<Integer> columnSource;

    public IntColumnSourceRedirectionIndex(WritableSource<Integer> columnSource) {
        this.columnSource = columnSource;
    }

    @Override
    public final long put(long key, long index) {
        final int previous = columnSource.getInt(key);

        columnSource.set(key, (int) index);

        return previous == QueryConstants.NULL_INT ? Index.NULL_KEY : previous;
    }

    @Override
    public final void putVoid(long key, long index) {
        columnSource.set(key, (int) index);
    }

    @Override
    public final long get(long key) {
        final int innerIndex = columnSource.getInt(key);
        if (innerIndex == QueryConstants.NULL_INT) {
            return Index.NULL_KEY;
        }
        return innerIndex;
    }

    @Override
    public final long getPrev(long key) {
        final int innerIndex = columnSource.getPrevInt(key);
        if (innerIndex == QueryConstants.NULL_INT) {
            return Index.NULL_KEY;
        }
        return innerIndex;
    }

    private static final class FillContext implements RedirectionIndex.FillContext {

        private final ColumnSource.FillContext colSrcCtx;
        private final WritableIntChunk<Values> intChunk;

        private FillContext(final IntColumnSourceRedirectionIndex csrc, final int chunkSize) {
            colSrcCtx = csrc.columnSource.makeFillContext(chunkSize);
            intChunk = WritableIntChunk.makeWritableChunk(chunkSize);
        }

        @Override
        public final void close() {
            colSrcCtx.close();
            intChunk.close();
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
        final FillContext effectiveContext = (FillContext) fillContext;
        columnSource.fillChunk(effectiveContext.colSrcCtx, effectiveContext.intChunk, keysToMap);
        final int sz = keysToMap.intSize();
        for (int ii = 0; ii < sz; ++ii) {
            final int innerIndex = effectiveContext.intChunk.get(ii);
            mappedKeysOut.set(ii, innerIndex == QueryConstants.NULL_INT ? Index.NULL_KEY : innerIndex);
        }
        mappedKeysOut.setSize(sz);
    }

    @Override
    public final void fillPrevChunk(
            @NotNull final RedirectionIndex.FillContext fillContext,
            @NotNull final WritableLongChunk<KeyIndices> mappedKeysOut,
            @NotNull final OrderedKeys keysToMap) {
        final FillContext effectiveContext = (FillContext) fillContext;
        columnSource.fillPrevChunk(effectiveContext.colSrcCtx, effectiveContext.intChunk, keysToMap);
        final int sz = keysToMap.intSize();
        for (int ii = 0; ii < sz; ++ii) {
            final int innerIndex = effectiveContext.intChunk.get(ii);
            mappedKeysOut.set(ii, innerIndex == QueryConstants.NULL_INT ? Index.NULL_KEY : innerIndex);
        }
        mappedKeysOut.setSize(sz);
    }

    @Override
    public final long remove(long key) {
        final int previous = columnSource.getInt(key);
        if (previous == QueryConstants.NULL_INT) {
            return Index.NULL_KEY;
        }
        columnSource.set(key, QueryConstants.NULL_INT);
        return previous;
    }

    @Override
    public final void removeVoid(long key) {
        columnSource.set(key, QueryConstants.NULL_INT);
    }

    @Override
    public final void startTrackingPrevValues() {
        columnSource.startTrackingPrevValues();
    }
}
