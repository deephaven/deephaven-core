/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.jetbrains.annotations.NotNull;

public interface ChunkSink<ATTR extends Any> extends ChunkSource<ATTR> {

    FillFromContext DEFAULT_FILL_FROM_INSTANCE = new FillFromContext() {};

    interface FillFromContext extends Context {
    }

    /**
     * Make a context suitable for the {@link ChunkSink#fillFromChunk} method.
     */
    default FillFromContext makeFillFromContext(int chunkCapacity) {
        return DEFAULT_FILL_FROM_INSTANCE;
    }

    /**
     * Fills the ChunkSink with data from the source, with data corresponding to the keys from the given
     * {@link RowSequence}.
     * 
     * @param context A context containing all mutable/state related data used in writing the Chunk.
     * @param src The source of the data {@code rowSequence}
     * @param rowSequence An {@link RowSequence} representing the keys to be written
     */
    void fillFromChunk(
            @NotNull FillFromContext context,
            @NotNull Chunk<? extends ATTR> src,
            @NotNull RowSequence rowSequence);

    /**
     * Fills the ChunkSink with data from the source, with data corresponding to the keys from the given key chunk.
     * 
     * @param context A context containing all mutable/state related data used in writing the Chunk.
     * @param src The source of the data {@code RowSequence}
     * @param keys A {@link LongChunk} representing the keys to be written
     */
    void fillFromChunkUnordered(
            @NotNull FillFromContext context,
            @NotNull Chunk<? extends ATTR> src,
            @NotNull LongChunk<RowKeys> keys);
}
