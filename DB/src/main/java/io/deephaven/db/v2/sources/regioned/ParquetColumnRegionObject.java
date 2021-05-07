package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.parquet.ColumnChunkPageStore;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.page.ChunkPage;
import io.deephaven.db.v2.sources.chunk.page.Page;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnRegionObject} implementation for regions that support fetching objects from a
 * {@link ColumnChunkPageStore}.
 */
public final class ParquetColumnRegionObject<T, ATTR extends Attributes.Any> extends ParquetColumnRegionBase<ATTR>
    implements ColumnRegionObject<T, ATTR>, ParquetColumnRegion<ATTR>, Page<ATTR> {

    ParquetColumnRegionObject(@NotNull ColumnChunkPageStore<ATTR> columnChunkPageStore) {
        super(columnChunkPageStore);
    }

    public T getObject(final long elementIndex) {
        final ChunkPage<ATTR> page = getChunkPageContaining(elementIndex);

        try {
            return page.<T>asObjectChunk().get(page.getChunkOffset(elementIndex));
        } catch (Exception e) {
            throw new TableDataException("Error retrieving char at table char index " + elementIndex
                    + ", from a parquet table.", e);
        }
    }
}
