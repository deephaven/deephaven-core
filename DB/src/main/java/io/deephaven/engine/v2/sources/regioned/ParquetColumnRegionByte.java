package io.deephaven.engine.v2.sources.regioned;

import io.deephaven.engine.v2.locations.TableDataException;
import io.deephaven.engine.v2.locations.parquet.ColumnChunkPageStore;
import io.deephaven.engine.structures.chunk.Attributes.Any;
import io.deephaven.engine.structures.chunk.WritableByteChunk;
import io.deephaven.engine.structures.chunk.WritableChunk;
import io.deephaven.engine.structures.chunk.page.ChunkPage;
import io.deephaven.engine.v2.utils.OrderedKeys;
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
        try (OrderedKeys orderedKeys = OrderedKeys.forRange(firstElementIndex, firstElementIndex + length - 1)) {
            fillChunk(DEFAULT_FILL_INSTANCE, byteChunk, orderedKeys);
        }
        return destination;
    }

    @Override
    public byte getByte(final long elementIndex) {
        final ChunkPage<ATTR> page = getChunkPageContaining(elementIndex);
        try {
            return page.asByteChunk().get(page.getChunkOffset(elementIndex));
        } catch (Exception e) {
            throw new TableDataException("Error retrieving byte at table byte index " + elementIndex
                    + ", from a parquet table", e);
        }
    }
}
