package io.deephaven.parquet.table.region;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.impl.sources.regioned.GenericColumnRegionBase;
import io.deephaven.parquet.table.pagestore.ColumnChunkPageStore;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.Chunk;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.page.ChunkPage;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

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
        Require.eq(columnChunkPageStore.firstRowOffset(), "columnChunkPageStore.firstRowOffset()", firstRowOffset(),
                "ColumnRegion.firstrRowOffset()");
    }

    @Override
    public final Chunk<? extends ATTR> getChunk(@NotNull final GetContext context,
            @NotNull final RowSequence rowSequence) {
        return columnChunkPageStore.getChunk(context, rowSequence);
    }

    @Override
    public final Chunk<? extends ATTR> getChunk(@NotNull final GetContext context, final long firstKey,
            final long lastKey) {
        return columnChunkPageStore.getChunk(context, firstKey, lastKey);
    }

    @Override
    public final void fillChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination, @NotNull final RowSequence rowSequence) {
        columnChunkPageStore.fillChunk(context, destination, rowSequence);
    }

    @Override
    public final void fillChunkAppend(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence.Iterator RowSequenceIterator) {
        columnChunkPageStore.fillChunkAppend(context, destination, RowSequenceIterator);
    }

    @Override
    public final ChunkPage<ATTR> getChunkPageContaining(final long elementIndex) {
        return columnChunkPageStore.getPageContaining(elementIndex);
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void releaseCachedResources() {
        ParquetColumnRegion.super.releaseCachedResources();
        columnChunkPageStore.releaseCachedResources();
    }

    @Override
    public final FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return columnChunkPageStore.makeFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public final GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedContext) {
        return columnChunkPageStore.makeGetContext(chunkCapacity, sharedContext);
    }
}
