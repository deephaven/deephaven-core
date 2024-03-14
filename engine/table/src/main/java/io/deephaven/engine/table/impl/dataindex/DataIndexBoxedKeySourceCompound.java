//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.chunk.*;
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

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * {@link ChunkSource} that produces data index {@link DataIndex.RowKeyLookup row set lookup} keys from multiple
 * {@link ColumnSource sources}. This can be used to extract keys from a data index table, or from a table of probe
 * values.
 */
final class DataIndexBoxedKeySourceCompound implements DefaultChunkSource.WithPrev<Values> {

    private final ColumnSource<?>[] keySources;
    private final int keyWidth;

    /**
     * Construct a new DataIndexBoxedKeySourceCompound backed by the supplied {@link ColumnSource column sources}.
     *
     * @param keySources Sources corresponding to the key columns
     */
    DataIndexBoxedKeySourceCompound(@NotNull final ColumnSource<?>... keySources) {
        this.keySources = Arrays.stream(keySources)
                .map(ReinterpretUtils::maybeConvertToPrimitive).toArray(ColumnSource[]::new);
        keyWidth = keySources.length;
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
        for (int ci = 0; ci < keyWidth; ++ci) {
            fc.boxedKeys[ci] = fc.keyBoxers[ci].box(usePrev
                    ? keySources[ci].getPrevChunk(fc.keyContexts[ci], rowSequence)
                    : keySources[ci].getChunk(fc.keyContexts[ci], rowSequence));
        }
        final int size = rowSequence.intSize();
        destination.setSize(size);
        final WritableObjectChunk<Object, ? super Values> typedDestination = destination.asWritableObjectChunk();
        for (int ri = 0; ri < size; ++ri) {
            final Object[] columnValues = new Object[keyWidth];
            for (int ci = 0; ci < keyWidth; ++ci) {
                columnValues[ci] = fc.boxedKeys[ci].get(ri);
            }
            typedDestination.set(ri, columnValues);
        }
    }

    private static class FillContext implements ChunkSource.FillContext {

        private final GetContext[] keyContexts;
        private final ChunkBoxer.BoxerKernel[] keyBoxers;
        private final ObjectChunk<?, ? extends Values>[] boxedKeys;

        private FillContext(
                final int chunkCapacity,
                @NotNull final ColumnSource<?>[] keySources,
                final SharedContext sharedContext) {
            keyContexts = Stream.of(keySources)
                    .map(cs -> cs.makeGetContext(chunkCapacity, sharedContext))
                    .toArray(GetContext[]::new);
            keyBoxers = Stream.of(keySources)
                    .map(cs -> ChunkBoxer.getBoxer(cs.getChunkType(), chunkCapacity))
                    .toArray(ChunkBoxer.BoxerKernel[]::new);
            // noinspection unchecked
            boxedKeys = new ObjectChunk[keySources.length];
        }

        @Override
        public void close() {
            SafeCloseable.closeAll(keyContexts);
            SafeCloseable.closeAll(keyBoxers);
        }
    }

    @Override
    public ChunkSource.FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new FillContext(chunkCapacity, keySources, sharedContext);
    }
}
