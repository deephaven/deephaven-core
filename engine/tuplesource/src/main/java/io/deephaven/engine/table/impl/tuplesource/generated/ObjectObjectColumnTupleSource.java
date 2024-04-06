//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TupleSourceCodeGenerator and run "./gradlew replicateTupleSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.TwoColumnTupleSourceFactory;
import io.deephaven.tuple.generated.ObjectObjectTuple;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Object and Object.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ObjectObjectColumnTupleSource extends AbstractTupleSource<ObjectObjectTuple> {

    /** {@link TwoColumnTupleSourceFactory} instance to create instances of {@link ObjectObjectColumnTupleSource}. **/
    public static final TwoColumnTupleSourceFactory<ObjectObjectTuple, Object, Object> FACTORY = new Factory();

    private final ColumnSource<Object> columnSource1;
    private final ColumnSource<Object> columnSource2;

    public ObjectObjectColumnTupleSource(
            @NotNull final ColumnSource<Object> columnSource1,
            @NotNull final ColumnSource<Object> columnSource2
    ) {
        super(columnSource1, columnSource2);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
    }

    @Override
    public final ObjectObjectTuple createTuple(final long rowKey) {
        return new ObjectObjectTuple(
                columnSource1.get(rowKey),
                columnSource2.get(rowKey)
        );
    }

    @Override
    public final ObjectObjectTuple createPreviousTuple(final long rowKey) {
        return new ObjectObjectTuple(
                columnSource1.getPrev(rowKey),
                columnSource2.getPrev(rowKey)
        );
    }

    @Override
    public final ObjectObjectTuple createTupleFromValues(@NotNull final Object... values) {
        return new ObjectObjectTuple(
                values[0],
                values[1]
        );
    }

    @Override
    public final ObjectObjectTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ObjectObjectTuple(
                values[0],
                values[1]
        );
    }

    @Override
    public final int tupleLength() {
        return 2;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ObjectObjectTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) tuple.getSecondElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final ObjectObjectTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
        }
        if (elementIndex == 1) {
            return tuple.getSecondElement();
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final ObjectObjectTuple tuple) {
        dest[0] = tuple.getFirstElement();
        dest[1] = tuple.getSecondElement();
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final ObjectObjectTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = tuple.getFirstElement();
        dest[map[1]] = tuple.getSecondElement();
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ObjectObjectTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
        }
        if (elementIndex == 1) {
            return tuple.getSecondElement();
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<ObjectObjectTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<Object, ? extends Values> chunk1 = chunks[0].asObjectChunk();
        ObjectChunk<Object, ? extends Values> chunk2 = chunks[1].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ObjectObjectTuple(chunk1.get(ii), chunk2.get(ii)));
        }
        destination.setSize(chunkSize);
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final ObjectObjectTuple tuple) {
        dest[0] = tuple.getFirstElement();
        dest[1] = tuple.getSecondElement();
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final ObjectObjectTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = tuple.getFirstElement();
        dest[map[1]] = tuple.getSecondElement();
    }

    /** {@link TwoColumnTupleSourceFactory} for instances of {@link ObjectObjectColumnTupleSource}. **/
    private static final class Factory implements TwoColumnTupleSourceFactory<ObjectObjectTuple, Object, Object> {

        private Factory() {
        }

        @Override
        public TupleSource<ObjectObjectTuple> create(
                @NotNull final ColumnSource<Object> columnSource1,
                @NotNull final ColumnSource<Object> columnSource2
        ) {
            return new ObjectObjectColumnTupleSource(
                    columnSource1,
                    columnSource2
            );
        }
    }
}
