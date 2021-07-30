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
 * {@link ColumnRegionLong} implementation for regions that support fetching primitive longs from
 * {@link ColumnChunkPageStore column chunk page stores}.
 */
public final class ParquetColumnRegionLong<ATTR extends Any> extends ParquetColumnRegionBase<ATTR>
        implements ColumnRegionLong<ATTR>, ParquetColumnRegion<ATTR> {

    public ParquetColumnRegionLong(@NotNull final ColumnChunkPageStore<ATTR> columnChunkPageStore) {
        super(columnChunkPageStore.mask(), columnChunkPageStore);
    }

    @Override
    public long getLong(final long elementIndex) {
        final ChunkPage<ATTR> page = getChunkPageContaining(elementIndex);
        try {
            return page.asLongChunk().get(page.getChunkOffset(elementIndex));
        } catch (Exception e) {
            throw new TableDataException("Error retrieving long at table long index " + elementIndex
                    + ", from a parquet table", e);
        }
    }
}
