package io.deephaven.parquet.table.region;

import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionChar;
import io.deephaven.parquet.table.pagestore.ColumnChunkPageStore;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.page.ChunkPage;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnRegionChar} implementation for regions that support fetching primitive chars from
 * {@link ColumnChunkPageStore column chunk page stores}.
 */
public final class ParquetColumnRegionChar<ATTR extends Any> extends ParquetColumnRegionBase<ATTR>
        implements ColumnRegionChar<ATTR>, ParquetColumnRegion<ATTR> {

    public ParquetColumnRegionChar(@NotNull final ColumnChunkPageStore<ATTR> columnChunkPageStore) {
        super(columnChunkPageStore.mask(), columnChunkPageStore);
    }

    @Override
    public char getChar(final long elementIndex) {
        final ChunkPage<ATTR> page = getChunkPageContaining(elementIndex);
        try {
            return page.asCharChunk().get(page.getChunkOffset(elementIndex));
        } catch (Exception e) {
            throw new TableDataException("Error retrieving char at table char rowSet " + elementIndex
                    + ", from a parquet table", e);
        }
    }
}
