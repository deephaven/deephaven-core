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
 * {@link ColumnRegionFloat} implementation for regions that support fetching primitive floats from
 * {@link ColumnChunkPageStore column chunk page stores}.
 */
public final class ParquetColumnRegionFloat<ATTR extends Any> extends ParquetColumnRegionBase<ATTR>
        implements ColumnRegionFloat<ATTR>, ParquetColumnRegion<ATTR> {

    public ParquetColumnRegionFloat(@NotNull final ColumnChunkPageStore<ATTR> columnChunkPageStore) {
        super(columnChunkPageStore.mask(), columnChunkPageStore);
    }

    @Override
    public float getFloat(final long elementIndex) {
        final ChunkPage<ATTR> page = getChunkPageContaining(elementIndex);
        try {
            return page.asFloatChunk().get(page.getChunkOffset(elementIndex));
        } catch (Exception e) {
            throw new TableDataException("Error retrieving float at table float index " + elementIndex
                    + ", from a parquet table", e);
        }
    }
}
