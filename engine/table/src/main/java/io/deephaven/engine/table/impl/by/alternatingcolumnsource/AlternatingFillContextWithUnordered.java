package io.deephaven.engine.table.impl.by.alternatingcolumnsource;

import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import org.jetbrains.annotations.Nullable;

class AlternatingFillContextWithUnordered extends BaseAlternatingFillContext {
    final WritableLongChunk<RowKeys> innerKeys;
    final WritableChunk<? super Values> innerValues;
    final ResettableWritableChunk<? super Values> innerSlice;
    final AlternatingColumnSourceUnorderedMergeKernel mergeKernel;

    AlternatingFillContextWithUnordered(@Nullable final ColumnSource<?> mainSource,
            @Nullable final ColumnSource<?> alternateSource,
            final int chunkCapacity,
            final SharedContext sharedContext) {
        super(mainSource, alternateSource, chunkCapacity, sharedContext);
        if ((mainSource == null || FillUnordered.providesFillUnordered(mainSource))
                && (alternateSource == null || FillUnordered.providesFillUnordered(alternateSource))) {
            innerKeys = alternateSource != null ? WritableLongChunk.makeWritableChunk(chunkCapacity) : null;
            if (mainSource != null && alternateSource != null) {
                innerSlice = mainSource.getChunkType().makeResettableWritableChunk();
                innerValues = mainSource.getChunkType().makeWritableChunk(chunkCapacity);
                mergeKernel = AlternatingColumnSourceUnorderedMergeKernel.getInstance(mainSource.getChunkType());
            } else {
                innerSlice = null;
                innerValues = null;
                mergeKernel = null;
            }
        } else {
            innerKeys = null;
            innerSlice = null;
            innerValues = null;
            mergeKernel = null;
        }
    }

    @Override
    public void close() {
        super.close();
        if (innerKeys != null) {
            innerKeys.close();
        }
        if (innerSlice != null) {
            innerSlice.close();
            innerValues.close();
        }
    }
}
