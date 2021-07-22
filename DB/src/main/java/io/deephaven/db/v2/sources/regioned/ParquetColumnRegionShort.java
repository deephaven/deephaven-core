/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ParquetColumnRegionChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.parquet.ColumnChunkPageStore;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.page.ChunkPage;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnRegionShort} implementation for regions that support fetching primitive shorts from a
 * {@link ColumnChunkPageStore}.
 */
public final class ParquetColumnRegionShort<ATTR extends Attributes.Any> extends ParquetColumnRegionBase<ATTR>
    implements ColumnRegionShort<ATTR>, ParquetColumnRegion<ATTR> {

    public ParquetColumnRegionShort(@NotNull ColumnChunkPageStore<ATTR>[] columnChunkPageStores) {
        super(columnChunkPageStores);
    }

    @Override
    public short getShort(final long elementIndex) {
        final ChunkPage<ATTR> page = getChunkPageContaining(elementIndex);

        try {
            return page.asShortChunk().get(page.getChunkOffset(elementIndex));
        } catch (Exception e) {
            throw new TableDataException("Error retrieving short at table short index " + elementIndex
                    + ", from a parquet table.", e);
        }
    }

    @Override
    public short getShort(@NotNull final FillContext context, final long elementIndex) {
        final ChunkPage<ATTR> page = getChunkPageContaining(elementIndex);

        try {
            return page.asShortChunk().get(page.getChunkOffset(elementIndex));
        } catch (Exception e) {
            throw new TableDataException("Error retrieving short at table short index " + elementIndex
                    + ", from a parquet table.", e);
        }
    }
}
