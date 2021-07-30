package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.parquet.ColumnChunkPageStore;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.page.ChunkPage;
import io.deephaven.db.v2.sources.chunk.page.Page;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnRegionObject} implementation for regions that support fetching objects from
 * {@link ColumnChunkPageStore column chunk page stores}.
 */
public final class ParquetColumnRegionObject<DATA_TYPE, ATTR extends Any> extends ParquetColumnRegionBase<ATTR>
        implements ColumnRegionObject<DATA_TYPE, ATTR>, ParquetColumnRegion<ATTR>, Page<ATTR> {

    public ParquetColumnRegionObject(@NotNull final ColumnChunkPageStore<ATTR> columnChunkPageStore) {
        super(columnChunkPageStore.mask(), columnChunkPageStore);
    }

    public DATA_TYPE getObject(final long elementIndex) {
        final ChunkPage<ATTR> page = getChunkPageContaining(elementIndex);
        try {
            return page.<DATA_TYPE>asObjectChunk().get(page.getChunkOffset(elementIndex));
        } catch (Exception e) {
            throw new TableDataException("Error retrieving object at table object index " + elementIndex
                    + ", from a parquet table", e);
        }
    }

    @Override
    public boolean supportsDictionaryFormat(@NotNull final OrderedKeys.Iterator remainingKeys, final boolean failFast) {
        final boolean result = columnChunkPageStore.usesDictionaryOnEveryPage();
        if (result || !failFast) {
            advanceToNextPage(remainingKeys);
        }
        return result;
    }
}
