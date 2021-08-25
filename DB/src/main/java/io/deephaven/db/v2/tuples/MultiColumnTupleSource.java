package io.deephaven.db.v2.tuples;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.util.tuples.ArrayTuple;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.utils.ChunkBoxer;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * <p>
 * {@link TupleSource} that produces key column values as {@link ArrayTuple}s from multiple
 * {@link ColumnSource}s.
 */
final class MultiColumnTupleSource
    implements TupleSource<ArrayTuple>, DefaultChunkSource.WithPrev<Values> {

    private final ColumnSource[] columnSources;

    private final List<ColumnSource> columnSourceList;

    /**
     * Construct a new tuple source backed by the supplied column sources. The column sources array
     * should not be changed after this call.
     *
     * @param columnSources The column sources to produce tuples from
     */
    MultiColumnTupleSource(@NotNull final ColumnSource... columnSources) {
        this.columnSources = columnSources;
        columnSourceList = Collections.unmodifiableList(Arrays.asList(columnSources));
    }

    @Override
    public List<ColumnSource> getColumnSources() {
        return columnSourceList;
    }

    @Override
    public final ArrayTuple createTuple(final long indexKey) {
        final int length = columnSources.length;
        final Object columnValues[] = new Object[length];
        for (int csi = 0; csi < length; ++csi) {
            columnValues[csi] = columnSources[csi].get(indexKey);
        }
        return new ArrayTuple(columnValues);
    }

    @Override
    public final ArrayTuple createPreviousTuple(final long indexKey) {
        final int length = columnSources.length;
        final Object columnValues[] = new Object[length];
        for (int csi = 0; csi < length; ++csi) {
            columnValues[csi] = columnSources[csi].getPrev(indexKey);
        }
        return new ArrayTuple(columnValues);
    }

    @Override
    public final ArrayTuple createTupleFromValues(@NotNull final Object... values) {
        final int length = columnSources.length;
        final Object valuesCopy[] = new Object[length];
        System.arraycopy(values, 0, valuesCopy, 0, length);
        return new ArrayTuple(valuesCopy);
    }

    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ArrayTuple tuple,
        final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource,
        final long destinationIndexKey) {
        writableSource.set(destinationIndexKey, tuple.getElement(elementIndex));
    }

    @Override
    public Object exportElement(ArrayTuple tuple, int elementIndex) {
        return tuple.getElement(elementIndex);
    }

    @Override
    public final SmartKey exportToExternalKey(@NotNull final ArrayTuple tuple) {
        return new SmartKey(tuple.getElements());
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @Override
    public Chunk<Values> getChunk(@NotNull ChunkSource.GetContext context,
        @NotNull OrderedKeys orderedKeys) {
        return getChunk(context, orderedKeys, false);
    }

    public Chunk<Values> getPrevChunk(@NotNull ChunkSource.GetContext context,
        @NotNull OrderedKeys orderedKeys) {
        return getChunk(context, orderedKeys, true);
    }

    private Chunk<Values> getChunk(@NotNull ChunkSource.GetContext context,
        @NotNull OrderedKeys orderedKeys, boolean usePrev) {
        final GetContext gc = (GetContext) context;
        final ObjectChunk<?, ? extends Values>[] underlyingValues =
            getUnderlyingChunks(orderedKeys, usePrev, gc);
        fillFromUnderlying(orderedKeys, underlyingValues, gc.values);
        return gc.values;
    }

    private void fillFromUnderlying(@NotNull OrderedKeys orderedKeys,
        ObjectChunk<?, ? extends Values>[] underlyingValues,
        WritableObjectChunk<ArrayTuple, ? super Values> destination) {
        final int length = columnSources.length;
        final int size = orderedKeys.intSize();
        destination.setSize(size);
        for (int ii = 0; ii < size; ++ii) {
            final Object columnValues[] = new Object[length];
            for (int csi = 0; csi < length; ++csi) {
                columnValues[csi] = underlyingValues[csi].get(ii);
            }
            destination.set(ii, new ArrayTuple(columnValues));
        }
    }

    @NotNull
    private ObjectChunk<?, ? extends Values>[] getUnderlyingChunks(@NotNull OrderedKeys orderedKeys,
        boolean usePrev, FillContext fc) {
        final int length = columnSources.length;

        // noinspection unchecked
        final ObjectChunk<?, ? extends Values>[] underlyingValues = new ObjectChunk[length];
        for (int csi = 0; csi < length; ++csi) {
            final Chunk<? extends Values> underlyingChunk;
            if (usePrev) {
                // noinspection unchecked
                underlyingChunk =
                    columnSources[csi].getPrevChunk(fc.underlyingContexts[csi], orderedKeys);
            } else {
                underlyingChunk =
                    columnSources[csi].getChunk(fc.underlyingContexts[csi], orderedKeys);
            }
            underlyingValues[csi] = fc.boxers[csi].box(underlyingChunk);
        }
        return underlyingValues;
    }

    @Override
    public void fillChunk(@NotNull ChunkSource.FillContext context,
        @NotNull WritableChunk<? super Values> destination, @NotNull OrderedKeys orderedKeys) {
        final FillContext fc = (FillContext) context;
        final ObjectChunk<?, ? extends Values>[] underlyingValues =
            getUnderlyingChunks(orderedKeys, false, fc);
        fillFromUnderlying(orderedKeys, underlyingValues, destination.asWritableObjectChunk());
    }

    public void fillPrevChunk(@NotNull ChunkSource.FillContext context,
        @NotNull WritableChunk<? super Values> destination, @NotNull OrderedKeys orderedKeys) {
        final FillContext fc = (FillContext) context;
        final ObjectChunk<?, ? extends Values>[] underlyingValues =
            getUnderlyingChunks(orderedKeys, true, fc);
        fillFromUnderlying(orderedKeys, underlyingValues, destination.asWritableObjectChunk());
    }

    private static class FillContext implements ChunkSource.FillContext {
        final ChunkSource.GetContext[] underlyingContexts;
        final ChunkBoxer.BoxerKernel[] boxers;

        private FillContext(int chunkCapacity, ColumnSource[] columnSources) {
            underlyingContexts = Arrays.stream(columnSources)
                .map(cs -> cs.makeGetContext(chunkCapacity)).toArray(ChunkSource.GetContext[]::new);
            boxers = Arrays.stream(columnSources)
                .map(cs -> ChunkBoxer.getBoxer(cs.getChunkType(), chunkCapacity))
                .toArray(ChunkBoxer.BoxerKernel[]::new);
        }

        @Override
        public void close() {
            SafeCloseable.closeArray(underlyingContexts);
            SafeCloseable.closeArray(boxers);
        }
    }

    private static class GetContext extends FillContext implements ChunkSource.GetContext {
        final WritableObjectChunk<ArrayTuple, Values> values;

        private GetContext(int chunkCapacity, ColumnSource[] columnSources) {
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
