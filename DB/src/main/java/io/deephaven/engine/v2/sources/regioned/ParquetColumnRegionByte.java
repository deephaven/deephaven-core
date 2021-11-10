package io.deephaven.engine.v2.sources.regioned;

import io.deephaven.engine.v2.locations.TableDataException;
import io.deephaven.engine.v2.locations.parquet.ColumnChunkPageStore;
import io.deephaven.engine.v2.sources.chunk.Attributes.Any;
import io.deephaven.engine.v2.sources.chunk.WritableByteChunk;
import io.deephaven.engine.v2.sources.chunk.WritableChunk;
import io.deephaven.engine.page.ChunkPage;
import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.structures.rowsequence.RowSequenceUtil;
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
        try (RowSequence rowSequence = RowSequenceUtil.forRange(firstElementIndex, firstElementIndex + length - 1)) {
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
