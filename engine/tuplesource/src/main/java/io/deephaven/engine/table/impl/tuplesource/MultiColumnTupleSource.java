/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.tuplesource;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.DefaultChunkSource;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.tuple.ArrayTuple;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;

/**
 * <p>
 * {@link TupleSource} that produces key column values as {@link ArrayTuple}s from multiple {@link ColumnSource}s.
 */
final class MultiColumnTupleSource implements TupleSource<ArrayTuple>, DefaultChunkSource.WithPrev<Values> {

    private final ColumnSource<?>[] columnSources;

    private final List<ColumnSource<?>> columnSourceList;

    /**
     * Construct a new tuple source backed by the supplied column sources. The column sources array should not be
     * changed after this call.
     *
     * @param columnSources The column sources to produce tuples from
     */
    MultiColumnTupleSource(@NotNull final ColumnSource<?>... columnSources) {
        this.columnSources = columnSources;
        columnSourceList = List.of(columnSources);
    }

    @Override
    public List<ColumnSource<?>> getColumnSources() {
        return columnSourceList;
    }

    @Override
    public final ArrayTuple createTuple(final long rowKey) {
        final int length = columnSources.length;
        final Object[] columnValues = new Object[length];
        for (int csi = 0; csi < length; ++csi) {
            columnValues[csi] = columnSources[csi].get(rowKey);
        }
        return new ArrayTuple(columnValues);
    }

    @Override
    public final ArrayTuple createPreviousTuple(final long rowKey) {
        final int length = columnSources.length;
        final Object[] columnValues = new Object[length];
        for (int csi = 0; csi < length; ++csi) {
            columnValues[csi] = columnSources[csi].getPrev(rowKey);
        }
        return new ArrayTuple(columnValues);
    }

    @Override
    public final ArrayTuple createTupleFromValues(@NotNull final Object... values) {
        final int length = columnSources.length;
        final Object[] valuesCopy = new Object[length];
        System.arraycopy(values, 0, valuesCopy, 0, length);
        return new ArrayTuple(valuesCopy);
    }

    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ArrayTuple tuple, final int elementIndex,
            @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        writableSource.set(destinationIndexKey, tuple.getElement(elementIndex));
    }

    @Override
    public Object exportElement(ArrayTuple tuple, int elementIndex) {
        return tuple.getElement(elementIndex);
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @Override
    public Chunk<Values> getChunk(@NotNull ChunkSource.GetContext context, @NotNull RowSequence rowSequence) {
        return getChunk(context, rowSequence, false);
    }

    public Chunk<Values> getPrevChunk(@NotNull ChunkSource.GetContext context, @NotNull RowSequence rowSequence) {
        return getChunk(context, rowSequence, true);
    }

    private Chunk<Values> getChunk(@NotNull ChunkSource.GetContext context, @NotNull RowSequence rowSequence,
            boolean usePrev) {
        final GetContext gc = (GetContext) context;
        final ObjectChunk<?, ? extends Values>[] underlyingValues = getUnderlyingChunks(rowSequence, usePrev, gc);
        fillFromUnderlying(rowSequence, underlyingValues, gc.values);
        return gc.values;
    }

    private void fillFromUnderlying(@NotNull RowSequence rowSequence,
            ObjectChunk<?, ? extends Values>[] underlyingValues,
            WritableObjectChunk<ArrayTuple, ? super Values> destination) {
        final int length = columnSources.length;
        final int size = rowSequence.intSize();
        destination.setSize(size);
        for (int ii = 0; ii < size; ++ii) {
            final Object[] columnValues = new Object[length];
            for (int csi = 0; csi < length; ++csi) {
                columnValues[csi] = underlyingValues[csi].get(ii);
            }
            destination.set(ii, new ArrayTuple(columnValues));
        }
    }

    @NotNull
    private ObjectChunk<?, ? extends Values>[] getUnderlyingChunks(@NotNull RowSequence rowSequence, boolean usePrev,
            FillContext fc) {
        final int length = columnSources.length;

        // noinspection unchecked
        final ObjectChunk<?, ? extends Values>[] underlyingValues = new ObjectChunk[length];
        for (int csi = 0; csi < length; ++csi) {
            final Chunk<? extends Values> underlyingChunk;
            if (usePrev) {
                underlyingChunk = columnSources[csi].getPrevChunk(fc.underlyingContexts[csi], rowSequence);
            } else {
                underlyingChunk = columnSources[csi].getChunk(fc.underlyingContexts[csi], rowSequence);
            }
            underlyingValues[csi] = fc.boxers[csi].box(underlyingChunk);
        }
        return underlyingValues;
    }

    @Override
    public void fillChunk(@NotNull ChunkSource.FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        final FillContext fc = (FillContext) context;
        final ObjectChunk<?, ? extends Values>[] underlyingValues = getUnderlyingChunks(rowSequence, false, fc);
        fillFromUnderlying(rowSequence, underlyingValues, destination.asWritableObjectChunk());
    }

    public void fillPrevChunk(@NotNull ChunkSource.FillContext context,
            @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        final FillContext fc = (FillContext) context;
        final ObjectChunk<?, ? extends Values>[] underlyingValues = getUnderlyingChunks(rowSequence, true, fc);
        fillFromUnderlying(rowSequence, underlyingValues, destination.asWritableObjectChunk());
    }

    private static class FillContext implements ChunkSource.FillContext {
        final ChunkSource.GetContext[] underlyingContexts;
        final ChunkBoxer.BoxerKernel[] boxers;

        private FillContext(int chunkCapacity, ColumnSource<?>[] columnSources) {
            underlyingContexts = Arrays.stream(columnSources).map(cs -> cs.makeGetContext(chunkCapacity))
                    .toArray(ChunkSource.GetContext[]::new);
            boxers = Arrays.stream(columnSources).map(cs -> ChunkBoxer.getBoxer(cs.getChunkType(), chunkCapacity))
                    .toArray(ChunkBoxer.BoxerKernel[]::new);
        }

        @Override
        public void close() {
            SafeCloseable.closeAll(underlyingContexts);
            SafeCloseable.closeAll(boxers);
        }
    }

    private static class GetContext extends FillContext implements ChunkSource.GetContext {
        final WritableObjectChunk<ArrayTuple, Values> values;

        private GetContext(int chunkCapacity, ColumnSource<?>[] columnSources) {
            super(chunkCapacity, columnSources);
            values = WritableObjectChunk.makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            super.close();
            values.close();
        }
    }

    @Override
    public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
        return new GetContext(chunkCapacity, columnSources);
    }

    @Override
    public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
        return new FillContext(chunkCapacity, columnSources);
    }
}
