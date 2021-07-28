package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.verify.Require;
import io.deephaven.db.v2.locations.parquet.ColumnChunkPageStore;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.db.v2.sources.chunk.page.ChunkPage;
import io.deephaven.db.v2.sources.chunk.page.PageStore;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.Arrays;
import java.util.stream.Stream;

public class ParquetColumnRegionBase<ATTR extends Any> implements ParquetColumnRegion<ATTR>, PageStore<ATTR, ATTR, ColumnChunkPageStore<ATTR>> {

    private final ColumnChunkPageStore<ATTR>[] columnChunkPageStores;
    private final long[] columnChunkLastIndices;
    private final long length;

    ParquetColumnRegionBase(@NotNull final ColumnChunkPageStore<ATTR>[] columnChunkPageStores) {
        this.columnChunkPageStores = Require.neqNull(columnChunkPageStores, "columnChunkPageStores");

        final int columnChunkCount = Require.gtZero(columnChunkPageStores.length, "columnChunkPageStores.length");
        columnChunkLastIndices = new long[columnChunkCount];
        long lastIndex = -1L;
        for (int cci = 0; cci < columnChunkCount; ++cci) {
            final ColumnChunkPageStore<ATTR> columnChunkPageStore = columnChunkPageStores[cci];

            // We are making the following assumptions, so these basic functions are inlined rather than virtual calls.
            Require.eq(columnChunkPageStore.mask(), "columnChunkPageStore.mask()", mask(), "ColumnRegion.mask()");
            Require.eq(columnChunkPageStore.firstRowOffset(), "columnChunkPageStore.firstRowOffset()", firstRowOffset(), "ColumnRegion.firstRowOffset()");
            // TODO-RWC: Clean this up
//            Require.requirement(cci == 0 || columnChunkPageStore.getNativeType() == columnChunkPageStores[cci - 1].getNativeType(), "all column chunk page stores have same native type");

            columnChunkLastIndices[cci] = lastIndex += columnChunkPageStore.length();
        }
        length = lastIndex + 1;
    }

    @Override
    final public long length() {
        return length;
    }

    @NotNull
    @Override
    public final ColumnChunkPageStore<ATTR> getPageContaining(@NotNull final FillContext fillContext, final long elementIndex) {
        final long rowIndex = elementIndex & mask();
        if (rowIndex < 0 || rowIndex >= length()) {
            throw new IllegalArgumentException("Unknown row index " + rowIndex + " (from element index " + elementIndex
                    + "), expected in range [0," + length() + ")");
        }
        final int rawPageIndex = Arrays.binarySearch(columnChunkLastIndices, rowIndex);
        final int pageIndex = rawPageIndex < 0 ? ~rawPageIndex : rawPageIndex;
        return columnChunkPageStores[pageIndex];
    }

    @Override
    public final ChunkPage<ATTR> getChunkPageContaining(final long elementIndex) {
        // NB: No ColumnChunkPageStore implementations use fill contexts for anything, so we can safely use the default
        //     context in this helper.
        return getPageContaining(DEFAULT_FILL_INSTANCE, elementIndex).getPageContaining(DEFAULT_FILL_INSTANCE, elementIndex);
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void releaseCachedResources() {
        ParquetColumnRegion.super.releaseCachedResources();
        Stream.of(columnChunkPageStores).forEach(ColumnChunkPageStore::close);
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        // See note in getChunkPageContaining; we can safely use one page store's context for all
        return columnChunkPageStores[0].makeFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedContext) {
        // See note in getChunkPageContaining; we can safely use one page store's context for all
        return columnChunkPageStores[0].makeGetContext(chunkCapacity, sharedContext);
    }
}
