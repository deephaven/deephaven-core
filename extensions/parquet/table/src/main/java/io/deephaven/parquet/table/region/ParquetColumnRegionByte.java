package io.deephaven.parquet.table.region;

import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionByte;
import io.deephaven.parquet.table.pagestore.ColumnChunkPageStore;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.page.ChunkPage;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnRegionByte} implementation for regions that support fetching primitive bytes from
 * {@link ColumnChunkPageStore column chunk page stores}.
 */
public final class ParquetColumnRegionByte<ATTR extends Any> extends ParquetColumnRegionBase<ATTR>
        implements ColumnRegionByte<ATTR> {

    public ParquetColumnRegionByte(@NotNull final ColumnChunkPageStore<ATTR> columnChunkPageStore) {
        super(columnChunkPageStore.mask(), columnChunkPageStore);
    }

    public byte[] getBytes(
            final long firstElementIndex,
            @NotNull final byte[] destination,
            final int destinationOffset,
            final int length
    ) {
        final WritableChunk<ATTR> byteChunk = WritableByteChunk.writableChunkWrap(destination, destinationOffset, length);
        try (RowSequence rowSequence = RowSequenceFactory.forRange(firstElementIndex, firstElementIndex + length - 1)) {
            fillChunk(DEFAULT_FILL_INSTANCE, byteChunk, rowSequence);
        }
        return destination;
    }

    @Override
    public byte getByte(final long elementIndex) {
        final ChunkPage<ATTR> page = getChunkPageContaining(elementIndex);
        try {
            return page.asByteChunk().get(page.getChunkOffset(elementIndex));
        } catch (Exception e) {
            throw new TableDataException("Error retrieving byte at table byte rowSet " + elementIndex
                    + ", from a parquet table", e);
        }
    }
}
