package io.deephaven.db.v2.tuples;

import io.deephaven.base.verify.Require;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;

/**
 * <p>{@link TupleSource} that produces key column values from a single {@link ColumnSource}.
 */
@SuppressWarnings("unused")
class SingleColumnTupleSource<TUPLE_TYPE> implements TupleSource<TUPLE_TYPE>, DefaultChunkSource.WithPrev<Attributes.Values>
{

    private final ColumnSource<TUPLE_TYPE> columnSource;

    private final List<ColumnSource> columnSourceList;

    SingleColumnTupleSource(@NotNull final ColumnSource<TUPLE_TYPE> columnSource) {
        this.columnSource = columnSource;
        columnSourceList = Collections.unmodifiableList(Collections.singletonList(columnSource));
    }

    @Override
    public List<ColumnSource> getColumnSources() {
        return columnSourceList;
    }

    @Override
    public final TUPLE_TYPE createTuple(final long indexKey) {
        return columnSource.get(indexKey);
    }

    @Override
    public final TUPLE_TYPE createPreviousTuple(final long indexKey) {
        return columnSource.getPrev(indexKey);
    }

    @Override
    public final TUPLE_TYPE createTupleFromValues(@NotNull final Object... values) {
        //noinspection unchecked
        return (TUPLE_TYPE) values[0];
    }

    @Override
    public <ELEMENT_TYPE> void exportElement(@NotNull final TUPLE_TYPE tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        //noinspection unchecked
        writableSource.set(destinationIndexKey, (ELEMENT_TYPE) tuple);
    }

    @Override
    public TUPLE_TYPE exportElement(TUPLE_TYPE tuple, int elementIndex) {
        Require.eqZero(elementIndex, "elementIndex");
        return tuple;
    }

    @Override
    public Object exportToExternalKey(@NotNull TUPLE_TYPE tuple) {
        return tuple;
    }

    @Override
    public ChunkType getChunkType() {
        return columnSource.getChunkType();
    }

    @Override
    public Chunk<? extends Attributes.Values> getChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys) {
        return columnSource.getChunk(context, orderedKeys);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Attributes.Values> destination, @NotNull OrderedKeys orderedKeys) {
        columnSource.fillChunk(context, destination, orderedKeys);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Attributes.Values> destination, @NotNull OrderedKeys orderedKeys) {
        columnSource.fillPrevChunk(context, destination, orderedKeys);
    }

    @Override
    public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
        return columnSource.makeGetContext(chunkCapacity, sharedContext);
    }

    @Override
    public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
        return columnSource.makeFillContext(chunkCapacity, sharedContext);
    }

}
