package io.deephaven.db.v2.tuples;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.db.util.tuples.EmptyTuple;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;

/**
 * <p>
 * {@link TupleSource} that produces only the {@link EmptyTuple}.
 */
enum EmptyTupleSource
    implements TupleSource<EmptyTuple>, DefaultChunkSource.WithPrev<Attributes.Values> {

    INSTANCE;

    @Override
    public List<ColumnSource> getColumnSources() {
        return Collections.emptyList();
    }

    @Override
    public EmptyTuple createTuple(final long indexKey) {
        return EmptyTuple.INSTANCE;
    }

    @Override
    public EmptyTuple createPreviousTuple(final long indexKey) {
        return EmptyTuple.INSTANCE;
    }

    @Override
    public EmptyTuple createTupleFromValues(@NotNull final Object... values) {
        return EmptyTuple.INSTANCE;
    }

    @Override
    public void exportElement(@NotNull final EmptyTuple tuple, final int elementIndex,
        @NotNull final WritableSource writableSource, final long destinationIndexKey) {
        throw new UnsupportedOperationException(
            "EmptyTuple does not contain any elements to export");
    }

    @Override
    public Object exportElement(EmptyTuple tuple, int elementIndex) {
        throw new UnsupportedOperationException(
            "EmptyTuple does not contain any elements to export");
    }

    @Override
    public Object exportToExternalKey(@NotNull final EmptyTuple tuple) {
        return SmartKey.EMPTY;
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @Override
    public void fillChunk(@NotNull FillContext context,
        @NotNull WritableChunk<? super Attributes.Values> destination,
        @NotNull OrderedKeys orderedKeys) {
        destination.asWritableObjectChunk().fillWithValue(0, orderedKeys.intSize(),
            EmptyTuple.INSTANCE);
        destination.setSize(orderedKeys.intSize());
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context,
        @NotNull WritableChunk<? super Attributes.Values> destination,
        @NotNull OrderedKeys orderedKeys) {
        fillChunk(context, destination, orderedKeys);
    }
}
