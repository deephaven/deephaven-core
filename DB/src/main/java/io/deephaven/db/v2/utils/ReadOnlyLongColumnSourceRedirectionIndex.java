package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

/**
 * Read-only {@link RedirectionIndex} implementation that wraps a {@link ColumnSource} of {@code longs}.
 */
public class ReadOnlyLongColumnSourceRedirectionIndex<CST extends ColumnSource<Long>> implements RedirectionIndex {

    protected final CST columnSource;

    public ReadOnlyLongColumnSourceRedirectionIndex(CST columnSource) {
        this.columnSource = columnSource;
    }

    @Override
    public long put(final long key, final long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long get(final long key) {
        final long innerIndex = columnSource.getLong(key);
        if (innerIndex == QueryConstants.NULL_LONG) {
            return Index.NULL_KEY;
        }
        return innerIndex;
    }

    @Override
    public final long getPrev(final long key) {
        final long innerIndex = columnSource.getPrevLong(key);
        if (innerIndex == QueryConstants.NULL_LONG) {
            return Index.NULL_KEY;
        }
        return innerIndex;
    }

    @Override
    public long remove(final long key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void startTrackingPrevValues() {
    }

    @Override
    public final RedirectionIndex.FillContext makeFillContext(final int chunkSize, final SharedContext sharedContext) {
        return new FillContext(this, chunkSize);
    }

    @Override
    public final void fillChunk(
            @NotNull final RedirectionIndex.FillContext fillContext,
            @NotNull final WritableLongChunk<Attributes.KeyIndices> mappedKeysOut,
            @NotNull final OrderedKeys keysToMap) {
        final WritableLongChunk<Attributes.Values> asValuesChunk = WritableLongChunk.upcast(mappedKeysOut);
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
            @NotNull final WritableLongChunk<Attributes.KeyIndices> mappedKeysOut,
            @NotNull final OrderedKeys keysToMap) {
        final WritableLongChunk<Attributes.Values> asValuesChunk = WritableLongChunk.downcast(WritableLongChunk.upcast(mappedKeysOut));
        final FillContext effectiveContext = (FillContext) fillContext;
        columnSource.fillPrevChunk(effectiveContext.colSrcCtx, asValuesChunk, keysToMap);
        for (int ii = 0; ii < mappedKeysOut.size(); ++ii) {
            if (mappedKeysOut.get(ii) == QueryConstants.NULL_LONG) {
                mappedKeysOut.set(ii, Index.NULL_KEY);
            }
        }
    }

    private static final class FillContext implements RedirectionIndex.FillContext {

        private final ColumnSource.FillContext colSrcCtx;

        private FillContext(@NotNull final ReadOnlyLongColumnSourceRedirectionIndex csrc, final int chunkSize) {
            colSrcCtx = csrc.columnSource.makeFillContext(chunkSize);
        }

        @Override
        public final void close() {
            colSrcCtx.close();
        }
    }
}
