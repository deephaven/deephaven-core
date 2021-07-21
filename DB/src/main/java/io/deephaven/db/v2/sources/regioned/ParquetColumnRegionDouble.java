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
 * {@link ColumnRegionDouble} implementation for regions that support fetching primitive doubles from a
 * {@link ColumnChunkPageStore}.
 */
public final class ParquetColumnRegionDouble<ATTR extends Any> extends ParquetColumnRegionBase<ATTR>
    implements ColumnRegionDouble<ATTR>, ParquetColumnRegion<ATTR> {

    public ParquetColumnRegionDouble(@NotNull final ColumnChunkPageStore<ATTR> columnChunkPageStore) {
        super(columnChunkPageStore);
    }

    @Override
    public double getDouble(final long elementIndex) {
        final ChunkPage<ATTR> page = getChunkPageContaining(elementIndex);

        try {
            return page.asDoubleChunk().get(page.getChunkOffset(elementIndex));
        } catch (Exception e) {
            throw new TableDataException("Error retrieving double at table double index " + elementIndex
                    + ", from a parquet table", e);
        }
    }

    @Override
    public double getDouble(@NotNull final FillContext context, final long elementIndex) {
        final ChunkPage<ATTR> page = getChunkPageContaining(elementIndex);

        try {
            return page.asDoubleChunk().get(page.getChunkOffset(elementIndex));
        } catch (Exception e) {
            throw new TableDataException("Error retrieving double at table double index " + elementIndex
                    + ", from a parquet table", e);
        }
    }
}
