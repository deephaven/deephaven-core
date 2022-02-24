package io.deephaven.engine.table.impl.by.alternatingcolumnsource;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import org.jetbrains.annotations.Nullable;

final class AlternatingGetContext extends BaseAlternatingFillContext implements ChunkSource.GetContext {

    final ChunkSource.GetContext mainGetContext;
    final ChunkSource.GetContext alternateGetContext;
    final WritableChunk<Values> mergeChunk;

    AlternatingGetContext(@Nullable final ColumnSource<?> mainSource,
            @Nullable final ColumnSource<?> alternateSource,
            final int chunkCapacity,
            final SharedContext sharedContext) {
        super(mainSource, alternateSource, chunkCapacity, sharedContext);
        if (mainSource != null) {
            mainGetContext = mainSource.makeGetContext(chunkCapacity, sharedContext);
        } else {
            mainGetContext = null;
        }
        if (alternateSource != null) {
            alternateGetContext = alternateSource.makeGetContext(chunkCapacity, sharedContext);
        } else {
            alternateGetContext = null;
        }
        if (mainGetContext != null && alternateGetContext != null) {
            mergeChunk = mainSource.getChunkType().makeWritableChunk(chunkCapacity);
        } else {
            mergeChunk = null;
        }
    }

    @Override
    public void close() {
        super.close();
        if (mainGetContext != null) {
            mainGetContext.close();
        }
        if (alternateGetContext != null) {
            alternateGetContext.close();
        }
        if (mergeChunk != null) {
            mergeChunk.close();
        }
    }
}
