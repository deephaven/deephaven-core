//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ParquetColumnRegionChar and run "./gradlew replicateRegionsAndRegionedSources" to regenerate
//
// @formatter:off
package io.deephaven.parquet.table.region;

import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionLong;
import io.deephaven.parquet.table.pagestore.ColumnChunkPageStore;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.page.ChunkPage;
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

    // region getBytes
    // endregion getBytes

    @Override
    public long getLong(final long rowKey) {
        final ChunkPage<ATTR> page = getChunkPageContaining(rowKey);
        try {
            return page.asLongChunk().get(page.getChunkOffset(rowKey));
        } catch (Exception e) {
            throw new TableDataException("Error retrieving long at row key " + rowKey + " from a parquet table", e);
        }
    }
}
