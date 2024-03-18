//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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

    // region getBytes
    // endregion getBytes

    @Override
    public char getChar(final long rowKey) {
        final ChunkPage<ATTR> page = getChunkPageContaining(rowKey);
        try {
            return page.asCharChunk().get(page.getChunkOffset(rowKey));
        } catch (Exception e) {
            throw new TableDataException("Error retrieving char at row key " + rowKey + " from a parquet table", e);
        }
    }
}
