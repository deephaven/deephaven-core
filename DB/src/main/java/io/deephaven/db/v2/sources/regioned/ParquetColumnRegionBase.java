package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.verify.Require;
import io.deephaven.db.v2.locations.parquet.ColumnChunkPageStore;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.db.v2.sources.chunk.page.ChunkPage;
import io.deephaven.db.v2.sources.chunk.page.PageStore;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;

public class ParquetColumnRegionBase<ATTR extends Any> implements ParquetColumnRegion<ATTR>, PageStore<ATTR, ATTR, ColumnChunkPageStore<ATTR>> {

    private final ColumnChunkPageStore<ATTR>[] columnChunkPageStores;

    ParquetColumnRegionBase(@NotNull final ColumnChunkPageStore<ATTR>[] columnChunkPageStores) {
        this.columnChunkPageStores = Require.neqNull(columnChunkPageStores, "columnChunkPageStores");

        // TODO-RWC: Finish porting page store change, and update dictionary stuff
        // We are making the following assumptions, so these basic functions are inlined rather than virtual calls.
        Require.eq(columnChunkPageStore.mask(), "columnChunkPageStore.mask()", mask(), "ColumnRegion.mask()");
        Require.eq(columnChunkPageStore.firstRowOffset(), "columnChunkPageStore.firstRowOffset()", firstRowOffset(), "ColumnRegion.firstrRowOffset()");
    }

    @Override
    final public long length() {
        return columnChunkPageStore.length();
    }

    @Override
    @NotNull
    final public Class<?> getNativeType() {
        return columnChunkPageStore.getNativeType();
    }

    @NotNull
    @Override
    public final ColumnChunkPageStore<ATTR> getPageContaining(@NotNull final FillContext fillContext, final long row) {
        return columnChunkPageStore;
    }

    @Override
    final public ChunkPage<ATTR> getChunkPageContaining(final long elementIndex) {
        return columnChunkPageStore.getPageContaining(elementIndex);
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void releaseCachedResources() {
        ParquetColumnRegion.super.releaseCachedResources();
        columnChunkPageStore.close();
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        // TODO-RWC: Check these
        return columnChunkPageStore.makeFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedContext) {
        return columnChunkPageStore.makeGetContext(chunkCapacity, sharedContext);
    }
}
