//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.region;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.sources.regioned.GenericColumnRegionBase;
import io.deephaven.parquet.table.pagestore.ColumnChunkPageStore;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.page.ChunkPage;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.Optional;

public abstract class ParquetColumnRegionBase<ATTR extends Any>
        extends GenericColumnRegionBase<ATTR>
        implements ParquetColumnRegion<ATTR> {

    final ColumnChunkPageStore<ATTR> columnChunkPageStore;

    private final ColumnLocation columnLocation;

    ParquetColumnRegionBase(final long pageMask, @NotNull final ColumnChunkPageStore<ATTR> columnChunkPageStore,
            @NotNull final ColumnLocation columnLocation) {
        super(pageMask);
        this.columnChunkPageStore = Require.neqNull(columnChunkPageStore, "columnChunkPageStore");
        this.columnLocation = Require.neqNull(columnLocation, "columnLocation");

        // We are making the following assumptions, so these basic functions are inlined rather than virtual calls.
        Require.eq(columnChunkPageStore.mask(), "columnChunkPageStore.mask()", mask(), "ColumnRegion.mask()");
        Require.eq(columnChunkPageStore.firstRowOffset(), "columnChunkPageStore.firstRowOffset()",
                firstRowOffset(), "ColumnRegion.firstRowOffset()");
    }

    @Override
    public Optional<ColumnLocation> getColumnLocation() {
        return Optional.of(columnLocation);
    }

    @Override
    public final Chunk<? extends ATTR> getChunk(
            @NotNull final GetContext context,
            @NotNull final RowSequence rowSequence) {
        throwIfInvalidated();
        return columnChunkPageStore.getChunk(context, rowSequence);
    }

    @Override
    public final Chunk<? extends ATTR> getChunk(
            @NotNull final GetContext context,
            final long firstKey,
            final long lastKey) {
        throwIfInvalidated();
        return columnChunkPageStore.getChunk(context, firstKey, lastKey);
    }

    @Override
    public final void fillChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence rowSequence) {
        throwIfInvalidated();
        columnChunkPageStore.fillChunk(context, destination, rowSequence);
    }

    @Override
    public final void fillChunkAppend(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence.Iterator rowSequenceIterator) {
        throwIfInvalidated();
        columnChunkPageStore.fillChunkAppend(context, destination, rowSequenceIterator);
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
}
