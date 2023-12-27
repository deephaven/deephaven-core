/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.region;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.engine.table.impl.sources.regioned.GenericColumnRegionBase;
import io.deephaven.engine.table.impl.sources.regioned.RegionContextHolder;
import io.deephaven.parquet.table.pagestore.ColumnChunkPageStore;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.page.ChunkPage;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;

public abstract class ParquetColumnRegionBase<ATTR extends Any>
        extends GenericColumnRegionBase<ATTR>
        implements ParquetColumnRegion<ATTR> {

    final ColumnChunkPageStore<ATTR> columnChunkPageStore;

    ParquetColumnRegionBase(final long pageMask, @NotNull final ColumnChunkPageStore<ATTR> columnChunkPageStore) {
        super(pageMask);
        this.columnChunkPageStore = Require.neqNull(columnChunkPageStore, "columnChunkPageStore");

        // We are making the following assumptions, so these basic functions are inlined rather than virtual calls.
        Require.eq(columnChunkPageStore.mask(), "columnChunkPageStore.mask()", mask(), "ColumnRegion.mask()");
        Require.eq(columnChunkPageStore.firstRowOffset(), "columnChunkPageStore.firstRowOffset()",
                firstRowOffset(), "ColumnRegion.firstRowOffset()");
    }

    @Override
    public final Chunk<? extends ATTR> getChunk(
            @NotNull final GetContext context,
            @NotNull final RowSequence rowSequence) {
        throwIfInvalidated();
        return columnChunkPageStore.getChunk(innerGetContext(context), rowSequence);
    }

    @Override
    public final Chunk<? extends ATTR> getChunk(
            @NotNull final GetContext context,
            final long firstKey,
            final long lastKey) {
        throwIfInvalidated();
        return columnChunkPageStore.getChunk(innerGetContext(context), firstKey, lastKey);
    }

    @Override
    public final void fillChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence rowSequence) {
        throwIfInvalidated();
        columnChunkPageStore.fillChunk(innerFillContext(context), destination, rowSequence);
    }

    @Override
    public final void fillChunkAppend(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence.Iterator rowSequenceIterator) {
        throwIfInvalidated();
        columnChunkPageStore.fillChunkAppend(innerFillContext(context), destination, rowSequenceIterator);
    }

    @Override
    public final ChunkPage<ATTR> getChunkPageContaining(final long elementIndex) {
        throwIfInvalidated();
        return columnChunkPageStore.getPageContaining(null, elementIndex);
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void releaseCachedResources() {
        ParquetColumnRegion.super.releaseCachedResources();
        columnChunkPageStore.releaseCachedResources();
    }

    private FillContext innerFillContext(@NotNull final FillContext context) {
        return ((RegionContextHolder) context)
                .updateInnerContext(this::fillContextUpdater);
    }

    private <T extends FillContext> T fillContextUpdater(
            int chunkCapacity,
            @Nullable final SharedContext sharedContext,
            @Nullable final Context currentInnerContext) {
        // noinspection unchecked
        return (T) (columnChunkPageStore.isFillContextCompatible(currentInnerContext)
                ? currentInnerContext
                : columnChunkPageStore.makeFillContext(chunkCapacity, sharedContext));
    }

    private GetContext innerGetContext(@NotNull final GetContext context) {
        return ((RegionContextHolder) DefaultGetContext.getFillContext(context))
                .updateInnerContext(this::getContextUpdater);
    }

    private <T extends GetContext> T getContextUpdater(
            int chunkCapacity,
            @Nullable final SharedContext sharedContext,
            @Nullable final Context currentInnerContext) {
        // noinspection unchecked
        return (T) (columnChunkPageStore.isGetContextCompatible(currentInnerContext)
                ? currentInnerContext
                : columnChunkPageStore.makeGetContext(chunkCapacity, sharedContext));
    }
}
