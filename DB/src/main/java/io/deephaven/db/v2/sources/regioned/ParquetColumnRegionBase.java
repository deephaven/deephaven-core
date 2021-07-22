package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.verify.Require;
import io.deephaven.db.v2.locations.parquet.ColumnChunkPageStore;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.page.ChunkPage;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;

public class ParquetColumnRegionBase<ATTR extends Any> implements ParquetColumnRegion<ATTR> {

    private final ColumnChunkPageStore<ATTR> columnChunkPageStore;

    ParquetColumnRegionBase(@NotNull final ColumnChunkPageStore<ATTR> columnChunkPageStore) {
        this.columnChunkPageStore = Require.neqNull(columnChunkPageStore, "columnChunkPageStore");

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

    @Override
    public Chunk<? extends ATTR> getChunk(@NotNull final GetContext context, @NotNull final OrderedKeys orderedKeys) {
        return columnChunkPageStore.getChunk(context, orderedKeys);
    }

    @Override
    public Chunk<? extends ATTR> getChunk(@NotNull final GetContext context, final long firstKey, final long lastKey) {
        return columnChunkPageStore.getChunk(context, firstKey, lastKey);
    }

    @Override
    public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super ATTR> destination, @NotNull final OrderedKeys orderedKeys) {
        columnChunkPageStore.fillChunk(context, destination, orderedKeys);
    }

    @Override
    public void fillChunkAppend(@NotNull final FillContext context, @NotNull final WritableChunk<? super ATTR> destination, @NotNull final OrderedKeys.Iterator orderedKeysIterator) {
        columnChunkPageStore.fillChunkAppend(context, destination, orderedKeysIterator);
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
        return columnChunkPageStore.makeFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedContext) {
        return columnChunkPageStore.makeGetContext(chunkCapacity, sharedContext);
    }
}
