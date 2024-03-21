//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.DefaultChunkSource;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ChunkSource} that produces data index {@link DataIndex.RowKeyLookup row set lookup} keys from a single
 * {@link ColumnSource source}. This can be used to extract keys from a data index table, or from a table of probe
 * values.
 */
final class DataIndexBoxedKeySourceSingle implements DefaultChunkSource.WithPrev<Values> {

    private final ColumnSource<?> keySource;

    /**
     * Construct a new DataIndexBoxedKeySourceSingle backed by the supplied {@link ColumnSource column source}.
     *
     * @param keySource Source corresponding to the key column
     */
    DataIndexBoxedKeySourceSingle(@NotNull final ColumnSource<?> keySource) {
        this.keySource = ReinterpretUtils.maybeConvertToPrimitive(keySource);
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @Override
    public void fillChunk(
            @NotNull final ChunkSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        fillChunkInternal(context, destination, rowSequence, false);
    }

    public void fillPrevChunk(
            @NotNull final ChunkSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        fillChunkInternal(context, destination, rowSequence, true);
    }

    private void fillChunkInternal(
            @NotNull final ChunkSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence,
            final boolean usePrev) {
        if (rowSequence.isEmpty()) {
            destination.setSize(0);
            return;
        }
        final FillContext fc = (FillContext) context;
        final ObjectChunk<?, ? extends Values> boxedKeys = fc.keyBoxer.box(usePrev
                ? keySource.getPrevChunk(fc.keyContext, rowSequence)
                : keySource.getChunk(fc.keyContext, rowSequence));
        destination.setSize(boxedKeys.size());
        boxedKeys.copyToChunk(0, destination, 0, boxedKeys.size());
    }

    private static class FillContext implements ChunkSource.FillContext {

        private final GetContext keyContext;
        private final ChunkBoxer.BoxerKernel keyBoxer;

        private FillContext(
                final int chunkCapacity,
                @NotNull final ColumnSource<?> keySource,
                final SharedContext sharedContext) {
            keyContext = keySource.makeGetContext(chunkCapacity, sharedContext);
            keyBoxer = ChunkBoxer.getBoxer(keySource.getChunkType(), chunkCapacity);
        }

        @Override
        public void close() {
            SafeCloseable.closeAll(keyContext, keyBoxer);
        }
    }

    @Override
    public ChunkSource.FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new FillContext(chunkCapacity, keySource, sharedContext);
    }
}
