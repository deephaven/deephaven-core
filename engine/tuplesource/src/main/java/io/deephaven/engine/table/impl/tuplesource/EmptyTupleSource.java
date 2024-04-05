//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.tuplesource;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.DefaultChunkSource;
import io.deephaven.tuple.EmptyTuple;
import org.jetbrains.annotations.NotNull;

/**
 * <p>
 * {@link TupleSource} that produces only the {@link EmptyTuple}.
 */
enum EmptyTupleSource implements TupleSource<EmptyTuple>, DefaultChunkSource.WithPrev<Values> {

    INSTANCE;

    @Override
    public EmptyTuple createTuple(final long rowKey) {
        return EmptyTuple.INSTANCE;
    }

    @Override
    public EmptyTuple createPreviousTuple(final long rowKey) {
        return EmptyTuple.INSTANCE;
    }

    @Override
    public EmptyTuple createTupleFromValues(@NotNull final Object... values) {
        return EmptyTuple.INSTANCE;
    }

    @Override
    public <ELEMENT_TYPE> void exportElement(@NotNull final EmptyTuple tuple, final int elementIndex,
            @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        throw new UnsupportedOperationException("EmptyTuple does not contain any elements to export");
    }

    @Override
    public int tupleLength() {
        return 1;
    }

    @Override
    public Object exportElement(@NotNull EmptyTuple tuple, int elementIndex) {
        throw new UnsupportedOperationException("EmptyTuple does not contain any elements to export");
    }

    @Override
    public void exportAllTo(Object @NotNull [] dest, @NotNull EmptyTuple tuple) {
        throw new UnsupportedOperationException("EmptyTuple does not contain any elements to export");
    }

    @Override
    public void exportAllTo(Object @NotNull [] dest, @NotNull EmptyTuple tuple, int @NotNull [] map) {
        throw new UnsupportedOperationException("EmptyTuple does not contain any elements to export");
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        destination.asWritableObjectChunk().fillWithValue(0, rowSequence.intSize(), EmptyTuple.INSTANCE);
        destination.setSize(rowSequence.intSize());
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context,
            @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        fillChunk(context, destination, rowSequence);
    }
}
