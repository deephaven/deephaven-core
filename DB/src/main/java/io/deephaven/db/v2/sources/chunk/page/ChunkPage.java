package io.deephaven.db.v2.sources.chunk.page;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.DefaultChunkSource;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

public interface ChunkPage<ATTR extends Attributes.Any> extends Page.WithDefaults<ATTR>, Chunk<ATTR>,
        DefaultChunkSource.SupportsContiguousGet<ATTR> {

    @Override
    ChunkType getChunkType();

    @FinalDefault
    @Override
    default long length() {
        return size();
    }

    @FinalDefault
    @Override
    default long maxRow(long row) {
        return lastRow(row);
    }

    /**
     * @return The offset into the chunk for this row.
     * @apiNote This function is for convenience over {@link #getRowOffset(long)}, so the caller doesn't have
     *          to cast to an int.
     * @implNote This page is known to be a chunk, so {@link #size()} is an int, and so is the offset.
     */
    @FinalDefault
    default int getChunkOffset(long row) {
        return (int) getRowOffset(row);
    }

    @FinalDefault
    @Override
    default Chunk<? extends ATTR> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        return slice(getChunkOffset(firstKey), Math.toIntExact(lastKey - firstKey + 1));
    }
}
