package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.parquet.ColumnChunkPageStore;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.page.ChunkPage;
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
            throw new TableDataException("Error retrieving char at table char index " + elementIndex
                    + ", from a parquet table", e);
        }
    }
}
