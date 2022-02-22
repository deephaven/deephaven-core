package io.deephaven.engine.table.impl.by.alternatingcolumnsource;

import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.rowset.impl.ShiftedRowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import org.jetbrains.annotations.Nullable;

abstract class BaseAlternatingFillContext implements ChunkSource.FillContext {

    final ChunkSource.FillContext mainFillContext;
    final ChunkSource.FillContext alternateFillContext;
    final ShiftedRowSequence alternateShiftedRowSequence;
    final ResettableWritableChunk<Any> alternateDestinationSlice;

    BaseAlternatingFillContext(@Nullable final ColumnSource<?> mainSource,
            @Nullable final ColumnSource<?> alternateSource,
            final int chunkCapacity,
            final SharedContext sharedContext) {
        if (mainSource != null) {
            mainFillContext = mainSource.makeFillContext(chunkCapacity, sharedContext);
        } else {
            mainFillContext = null;
        }
        if (alternateSource != null) {
            alternateFillContext = alternateSource.makeFillContext(chunkCapacity, sharedContext);
            alternateShiftedRowSequence = new ShiftedRowSequence();
            alternateDestinationSlice = alternateSource.getChunkType().makeResettableWritableChunk();
        } else {
            alternateFillContext = null;
            alternateShiftedRowSequence = null;
            alternateDestinationSlice = null;
        }
    }

    @Override
    public void close() {
        if (mainFillContext != null) {
            mainFillContext.close();
        }
        if (alternateFillContext != null) {
            alternateFillContext.close();
            alternateShiftedRowSequence.close();
            alternateDestinationSlice.close();
        }
    }
}
