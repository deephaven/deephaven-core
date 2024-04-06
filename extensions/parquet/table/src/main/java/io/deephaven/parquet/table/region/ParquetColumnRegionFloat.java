//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ParquetColumnRegionChar and run "./gradlew replicateRegionsAndRegionedSources" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.region;

import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionFloat;
import io.deephaven.parquet.table.pagestore.ColumnChunkPageStore;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.page.ChunkPage;
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

    // region getBytes
    // endregion getBytes

    @Override
    public float getFloat(final long rowKey) {
        final ChunkPage<ATTR> page = getChunkPageContaining(rowKey);
        try {
            return page.asFloatChunk().get(page.getChunkOffset(rowKey));
        } catch (Exception e) {
            throw new TableDataException("Error retrieving float at row key " + rowKey + " from a parquet table", e);
        }
    }
}
