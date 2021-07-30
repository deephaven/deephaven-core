/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ParquetColumnRegionChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.parquet.ColumnChunkPageStore;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.page.ChunkPage;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnRegionShort} implementation for regions that support fetching primitive shorts from
 * {@link ColumnChunkPageStore column chunk page stores}.
 */
public final class ParquetColumnRegionShort<ATTR extends Any> extends ParquetColumnRegionBase<ATTR>
        implements ColumnRegionShort<ATTR>, ParquetColumnRegion<ATTR> {

    public ParquetColumnRegionShort(@NotNull final ColumnChunkPageStore<ATTR> columnChunkPageStore) {
        super(columnChunkPageStore.mask(), columnChunkPageStore);
    }

    @Override
    public short getShort(final long elementIndex) {
        final ChunkPage<ATTR> page = getChunkPageContaining(elementIndex);
        try {
            return page.asShortChunk().get(page.getChunkOffset(elementIndex));
        } catch (Exception e) {
            throw new TableDataException("Error retrieving short at table short index " + elementIndex
                    + ", from a parquet table", e);
        }
    }
}
