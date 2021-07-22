package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.verify.Require;
import javax.annotation.OverridingMethodsMustInvokeSuper;
import io.deephaven.db.v2.locations.parquet.ColumnChunkPageStore;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.page.ChunkPage;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexUtilities;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ShiftedOrderedKeys;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public class ParquetColumnRegionBase<ATTR extends Attributes.Any> implements ParquetColumnRegion<ATTR> {

    @NotNull
    private final ColumnChunkPageStore<ATTR>[] columnChunkPageStores;
    final long[] accumLengths;
    final int nPages;

    ParquetColumnRegionBase(@NotNull ColumnChunkPageStore<ATTR>[] columnChunkPageStores) {
        this.columnChunkPageStores = Require.neqNull(columnChunkPageStores, "columnChunkPageStores");
        Require.gtZero(columnChunkPageStores.length, "columnChunkPageStores.length");
        nPages = columnChunkPageStores.length;
        accumLengths = new long[columnChunkPageStores.length];

        long len = 0;
        int i = 0;
        // We are making the following assumptions, so these basic functions are inlined rather than virtual calls.
        for (ColumnChunkPageStore<ATTR> columnChunkPageStore : columnChunkPageStores) {
            len += columnChunkPageStore.length();
            accumLengths[i++] = len;
            Require.eq(columnChunkPageStore.mask(), "columnChunkPageStore.mask()", mask(), "ColumnRegion.mask()");
            Require.eq(columnChunkPageStore.firstRowOffset(), "columnChunkPageStore.firstRowOffset()", firstRowOffset(), "ColumnRegion.firstrRowOffset()");
        }
    }

    @Override
    final public long length() {
        return accumLengths[nPages - 1];
    }

    @Override
    @NotNull
    final public Class<?> getNativeType() {
        return columnChunkPageStores[0].getNativeType();
    }

    final int pageIndex(final long rowOffset) {
        int idx = Arrays.binarySearch(accumLengths, rowOffset);
        return (idx < 0) ? ~idx : idx;
    }

    @Override
    public Chunk<? extends ATTR> getChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys) {
        if (nPages == 1) {
            return columnChunkPageStores[0].getChunk(context, orderedKeys);
        }
        if (orderedKeys.isEmpty()) {
            return getChunkType().getEmptyChunk();
        }
        final int pageIndexMin = pageIndex(orderedKeys.firstKey());
        final int pageIndexMax = pageIndex(orderedKeys.lastKey());
        if (pageIndexMin == pageIndexMax) {
            if (pageIndexMin == 0) {
                return columnChunkPageStores[0].getChunk(context, orderedKeys);
            }
            return columnChunkPageStores[pageIndexMin].getChunk(
                    context, ShiftedOrderedKeys.wrap(orderedKeys, accumLengths[pageIndexMin - 1]));
        }
        final WritableChunk<ATTR> destination = DefaultGetContext.getWritableChunk(context);
        final FillContext fillContext = DefaultGetContext.getFillContext(context);
        long firstKeyOnPage = 0;
        try (OrderedKeys.Iterator it = orderedKeys.getOrderedKeysIterator()) {
            for (int i = pageIndexMin; i <= pageIndexMax; ++i) {
                final long lastKeyOnPage = accumLengths[i] - 1;
                it.advance(firstKeyOnPage);
                final OrderedKeys oki = it.getNextOrderedKeysThrough(lastKeyOnPage);
                final OrderedKeys shiftedOki = ShiftedOrderedKeys.wrap(oki, firstKeyOnPage);
                columnChunkPageStores[i].fillChunkAppend(fillContext, destination, shiftedOki.getOrderedKeysIterator());
                firstKeyOnPage = lastKeyOnPage + 1;
            }
            return destination;
        }
    }

    @Override
    public Chunk<? extends ATTR> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        if (nPages == 1) {
            return columnChunkPageStores[0].getChunk(context, firstKey, lastKey);
        }
        final int pageIndexMin = pageIndex(firstKey);
        final int pageIndexMax = pageIndex(lastKey);
        if (pageIndexMin == pageIndexMax) {
            if (pageIndexMin == 0) {
                return columnChunkPageStores[0].getChunk(context, firstKey, lastKey);
            }
            final long delta = accumLengths[pageIndexMin - 1];
            return columnChunkPageStores[pageIndexMin].getChunk(context, firstKey - delta, lastKey - delta);
        }
        final WritableChunk<ATTR> destination = DefaultGetContext.getWritableChunk(context);
        final FillContext fillContext = DefaultGetContext.getFillContext(context);
        long firstKeyOnPage = firstKey;
        int pageIndex = pageIndexMin;
        long delta = 0;
        while (true) {
            final long lastKeyOnPage = accumLengths[pageIndex];
            columnChunkPageStores[pageIndex].fillChunkAppend(
                    fillContext,
                    destination,
                    Index.FACTORY.getIndexByRange(firstKeyOnPage - delta, lastKeyOnPage - delta).getOrderedKeysIterator());
            firstKeyOnPage = lastKeyOnPage + 1;
            if (firstKeyOnPage > lastKey) {
                break;
            }
            delta = lastKeyOnPage;
            ++pageIndex;
        }
        return destination;
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination, @NotNull OrderedKeys orderedKeys) {
        if (nPages == 1) {
            columnChunkPageStores[0].fillChunk(context, destination, orderedKeys);
        }
        // TODO MISSING
    }

    @Override
    public void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination, @NotNull OrderedKeys.Iterator orderedKeysIterator) {
        if (nPages == 1) {
            columnChunkPageStores[0].fillChunkAppend(context, destination, orderedKeysIterator);
        }
        // TODO MISSING
    }

    @Override
    final public ChunkPage<ATTR> getChunkPageContaining(long elementIndex) {
        if (nPages == 0) {
            return columnChunkPageStores[0].getPageContaining(elementIndex);
        }
        final int pageIndex = pageIndex(elementIndex);
        final long delta = (pageIndex == 0) ? 0 : accumLengths[pageIndex - 1];
        return columnChunkPageStores[pageIndex].getPageContaining(elementIndex - delta);
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void releaseCachedResources() {
        ParquetColumnRegion.super.releaseCachedResources();
        for (int i = 0; i < nPages; ++i) {
            columnChunkPageStores[i].close();
        }
    }

    @Override
    public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
        return columnChunkPageStores[0].makeFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
        return columnChunkPageStores[0].makeGetContext(chunkCapacity, sharedContext);
    }

}


