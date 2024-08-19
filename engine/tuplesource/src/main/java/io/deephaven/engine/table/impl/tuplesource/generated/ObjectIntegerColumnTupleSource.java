//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TupleSourceCodeGenerator and run "./gradlew replicateTupleSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.TwoColumnTupleSourceFactory;
import io.deephaven.tuple.generated.ObjectIntTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Object and Integer.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ObjectIntegerColumnTupleSource extends AbstractTupleSource<ObjectIntTuple> {

    /** {@link TwoColumnTupleSourceFactory} instance to create instances of {@link ObjectIntegerColumnTupleSource}. **/
    public static final TwoColumnTupleSourceFactory<ObjectIntTuple, Object, Integer> FACTORY = new Factory();

    private final ColumnSource<Object> columnSource1;
    private final ColumnSource<Integer> columnSource2;

    public ObjectIntegerColumnTupleSource(
            @NotNull final ColumnSource<Object> columnSource1,
            @NotNull final ColumnSource<Integer> columnSource2
    ) {
        super(columnSource1, columnSource2);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
    }

    @Override
    public final ObjectIntTuple createTuple(final long rowKey) {
        return new ObjectIntTuple(
                columnSource1.get(rowKey),
                columnSource2.getInt(rowKey)
        );
    }

    @Override
    public final ObjectIntTuple createPreviousTuple(final long rowKey) {
        return new ObjectIntTuple(
                columnSource1.getPrev(rowKey),
                columnSource2.getPrevInt(rowKey)
        );
    }

    @Override
    public final ObjectIntTuple createTupleFromValues(@NotNull final Object... values) {
        return new ObjectIntTuple(
                values[0],
                TypeUtils.unbox((Integer)values[1])
        );
    }

    @Override
    public final ObjectIntTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ObjectIntTuple(
                values[0],
                TypeUtils.unbox((Integer)values[1])
        );
    }

    @Override
    public final int tupleLength() {
        return 2;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ObjectIntTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, tuple.getSecondElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final ObjectIntTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final ObjectIntTuple tuple) {
        dest[0] = tuple.getFirstElement();
        dest[1] = TypeUtils.box(tuple.getSecondElement());
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final ObjectIntTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = tuple.getFirstElement();
        dest[map[1]] = TypeUtils.box(tuple.getSecondElement());
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ObjectIntTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<ObjectIntTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<Object, ? extends Values> chunk1 = chunks[0].asObjectChunk();
        IntChunk<? extends Values> chunk2 = chunks[1].asIntChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ObjectIntTuple(chunk1.get(ii), chunk2.get(ii)));
        }
        destination.setSize(chunkSize);
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final ObjectIntTuple tuple) {
        dest[0] = tuple.getFirstElement();
        dest[1] = TypeUtils.box(tuple.getSecondElement());
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final ObjectIntTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = tuple.getFirstElement();
        dest[map[1]] = TypeUtils.box(tuple.getSecondElement());
    }

    /** {@link TwoColumnTupleSourceFactory} for instances of {@link ObjectIntegerColumnTupleSource}. **/
    private static final class Factory implements TwoColumnTupleSourceFactory<ObjectIntTuple, Object, Integer> {

        private Factory() {
        }

        @Override
        public TupleSource<ObjectIntTuple> create(
                @NotNull final ColumnSource<Object> columnSource1,
                @NotNull final ColumnSource<Integer> columnSource2
        ) {
            return new ObjectIntegerColumnTupleSource(
                    columnSource1,
                    columnSource2
            );
        }
    }
}
