package io.deephaven.engine.table.impl;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.SharedContext;
import org.jetbrains.annotations.NotNull;

public class DefaultGetContext<ATTR extends Any> extends ContextWithChunk<ATTR, ChunkSource.FillContext>
        implements ChunkSource.GetContext {

    public DefaultGetContext(ChunkSource.FillContext fillContext, ChunkType chunkType, int chunkCapacity) {
        super(fillContext, chunkType, chunkCapacity);
    }

    public DefaultGetContext(ChunkSource<ATTR> chunkSource, int chunkCapacity, SharedContext sharedContext) {
        super(chunkSource.makeFillContext(chunkCapacity, sharedContext), chunkSource.getChunkType(), chunkCapacity);
    }

    public ChunkSource.FillContext getFillContext() {
        return super.getContext();
    }

    public static ChunkSource.FillContext getFillContext(@NotNull ChunkSource.GetContext context) {
        return ContextWithChunk.getContext(context);
    }
}
