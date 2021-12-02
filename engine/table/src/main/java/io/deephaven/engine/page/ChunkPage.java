package io.deephaven.engine.page;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.impl.DefaultChunkSource;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

public interface ChunkPage<ATTR extends Any> extends Page.WithDefaults<ATTR>, Chunk<ATTR>,
        DefaultChunkSource.SupportsContiguousGet<ATTR> {

    @Override
    ChunkType getChunkType();

    /**
     * @param row Any row contained on this page.
     * @return the last row of this page, located in the same way as row.
     */
    default long lastRow(final long row) {
        return (row & ~mask()) | (firstRowOffset() + size() - 1);
    }

    @Override
    @FinalDefault
    default long maxRow(final long row) {
        return lastRow(row);
    }

    /**
     * @return The offset into the chunk for this row.
     * @apiNote This function is for convenience over {@link #getRowOffset(long)}, so the caller doesn't have to cast to
     *          an int.
     * @implNote This page is known to be a chunk, so {@link #size()} is an int, and so is the offset.
     */
    @FinalDefault
    default int getChunkOffset(final long row) {
        return (int) getRowOffset(row);
    }

    @FinalDefault
    @Override
    default Chunk<? extends ATTR> getChunk(@NotNull final GetContext context, final long firstKey, final long lastKey) {
        return slice(getChunkOffset(firstKey), Math.toIntExact(lastKey - firstKey + 1));
    }
}
