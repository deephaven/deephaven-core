package io.deephaven.db.v2.sources.chunk;

import org.jetbrains.annotations.NotNull;

public class DefaultGetContext<ATTR extends Attributes.Any> extends ContextWithChunk<ATTR, ChunkSource.FillContext>
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
