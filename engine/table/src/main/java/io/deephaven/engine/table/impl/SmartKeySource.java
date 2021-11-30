package io.deephaven.engine.table.impl;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * <p>
 * {@link ColumnSource} that produces key column values as {@link SmartKey}s from multiple {@link ColumnSource}s.
 */
public final class SmartKeySource extends AbstractColumnSource<SmartKey>
        implements TupleSource<SmartKey>, MutableColumnSourceGetDefaults.ForObject<SmartKey> {

    private final ColumnSource[] columnSources;
    private final List<ColumnSource> columnSourceList;

    /**
     * Construct a new tuple source backed by the supplied column sources. The column sources array should not be
     * changed after this call.
     *
     * @param columnSources The column sources to produce tuples from
     */
    public SmartKeySource(@NotNull final ColumnSource... columnSources) {
        super(SmartKey.class, null);
        this.columnSources = columnSources;
        columnSourceList = Collections.unmodifiableList(Arrays.asList(columnSources));
    }

    @Override
    public final void startTrackingPrevValues() {}

    @Override
    public final SmartKey get(final long indexKey) {
        return createTuple(indexKey);
    }

    @Override
    public final SmartKey getPrev(final long indexKey) {
        return createPreviousTuple(indexKey);
    }

    @Override
    public final List<ColumnSource> getColumnSources() {
        return columnSourceList;
    }

    @Override
    public final SmartKey createTuple(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        final int length = columnSources.length;
        final Object[] columnValues = new Object[length];
        for (int csi = 0; csi < length; ++csi) {
            columnValues[csi] = columnSources[csi].get(rowKey);
        }
        return new SmartKey(columnValues);
    }

    @Override
    public final SmartKey createPreviousTuple(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        final int length = columnSources.length;
        final Object[] columnValues = new Object[length];
        for (int csi = 0; csi < length; ++csi) {
            columnValues[csi] = columnSources[csi].getPrev(rowKey);
        }
        return new SmartKey(columnValues);
    }

    @Override
    public final SmartKey createTupleFromValues(@NotNull final Object... values) {
        final int length = columnSources.length;
        final Object[] valuesCopy = new Object[length];
        System.arraycopy(values, 0, valuesCopy, 0, length);
        return new SmartKey(valuesCopy);
    }

    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final SmartKey smartKey, final int elementIndex,
            @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        // noinspection unchecked
        writableSource.set(destinationIndexKey, (ELEMENT_TYPE) smartKey.get(elementIndex));
    }

    @Override
    public final Object exportElement(@NotNull final SmartKey smartKey, final int elementIndex) {
        return smartKey.get(elementIndex);
    }

    @Override
    public final SmartKey exportToExternalKey(@NotNull final SmartKey smartKey) {
        return smartKey;
    }

    @Override
    public final Chunk<Values> getChunk(@NotNull final ChunkSource.GetContext context,
            @NotNull final RowSequence rowSequence) {
        return getChunk(context, rowSequence, false);
    }

    public final Chunk<Values> getPrevChunk(@NotNull final ChunkSource.GetContext context,
            @NotNull final RowSequence rowSequence) {
        return getChunk(context, rowSequence, true);
    }

    private Chunk<Values> getChunk(@NotNull final ChunkSource.GetContext context,
            @NotNull final RowSequence rowSequence, final boolean usePrev) {
        final GetContext gc = (GetContext) context;
        final ObjectChunk<?, ? extends Values>[] underlyingValues = getUnderlyingChunks(rowSequence, usePrev, gc);
        fillFromUnderlying(rowSequence, underlyingValues, gc.values);
        return gc.values;
    }

    private void fillFromUnderlying(@NotNull final RowSequence rowSequence,
            @NotNull final ObjectChunk<?, ? extends Values>[] underlyingValues,
            @NotNull final WritableObjectChunk<SmartKey, ? super Values> destination) {
        final int length = columnSources.length;
        final int size = rowSequence.intSize();
        destination.setSize(size);
        for (int ii = 0; ii < size; ++ii) {
            final Object[] columnValues = new Object[length];
            for (int csi = 0; csi < length; ++csi) {
                columnValues[csi] = underlyingValues[csi].get(ii);
            }
            destination.set(ii, new SmartKey(columnValues));
        }
    }

    @NotNull
    private ObjectChunk<?, ? extends Values>[] getUnderlyingChunks(@NotNull final RowSequence rowSequence,
            final boolean usePrev, @NotNull final FillContext fillContext) {
        final int length = columnSources.length;

        // noinspection unchecked
        final ObjectChunk<?, ? extends Values>[] underlyingValues = new ObjectChunk[length];
        for (int csi = 0; csi < length; ++csi) {
            final Chunk<Values> underlyingChunk;
            if (usePrev) {
                // noinspection unchecked
                underlyingChunk = columnSources[csi].getPrevChunk(fillContext.underlyingContexts[csi], rowSequence);
            } else {
                // noinspection unchecked
                underlyingChunk = columnSources[csi].getChunk(fillContext.underlyingContexts[csi], rowSequence);
            }
            underlyingValues[csi] = fillContext.boxers[csi].box(underlyingChunk);
        }
        return underlyingValues;
    }

    @Override
    public final void fillChunk(@NotNull final ChunkSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final FillContext fc = (FillContext) context;
        final ObjectChunk<?, ? extends Values>[] underlyingValues = getUnderlyingChunks(rowSequence, false, fc);
        fillFromUnderlying(rowSequence, underlyingValues, destination.asWritableObjectChunk());
    }

    public final void fillPrevChunk(@NotNull final ChunkSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final FillContext fc = (FillContext) context;
        final ObjectChunk<?, ? extends Values>[] underlyingValues = getUnderlyingChunks(rowSequence, true, fc);
        fillFromUnderlying(rowSequence, underlyingValues, destination.asWritableObjectChunk());
    }

    private static class FillContext implements ChunkSource.FillContext {

        private final ChunkSource.GetContext[] underlyingContexts;
        private final ChunkBoxer.BoxerKernel[] boxers;

        private FillContext(final int chunkCapacity, @NotNull final ColumnSource[] columnSources) {
            underlyingContexts = Arrays.stream(columnSources).map(cs -> cs.makeGetContext(chunkCapacity))
                    .toArray(ChunkSource.GetContext[]::new);
            boxers = Arrays.stream(columnSources).map(cs -> ChunkBoxer.getBoxer(cs.getChunkType(), chunkCapacity))
                    .toArray(ChunkBoxer.BoxerKernel[]::new);
        }

        @Override
        public void close() {
            SafeCloseable.closeArray(underlyingContexts);
            SafeCloseable.closeArray(boxers);
        }
    }

    private static final class GetContext extends FillContext implements ChunkSource.GetContext {

        private final WritableObjectChunk<SmartKey, Values> values;

        private GetContext(final int chunkCapacity, @NotNull final ColumnSource[] columnSources) {
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
    public final GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new GetContext(chunkCapacity, columnSources);
    }

    @Override
    public final FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new FillContext(chunkCapacity, columnSources);
    }
}
