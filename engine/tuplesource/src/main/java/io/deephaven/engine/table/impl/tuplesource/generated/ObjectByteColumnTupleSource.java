//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TupleSourceCodeGenerator and run "./gradlew replicateTupleSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.ByteChunk;
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
import io.deephaven.tuple.generated.ObjectByteTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Object and Byte.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ObjectByteColumnTupleSource extends AbstractTupleSource<ObjectByteTuple> {

    /** {@link TwoColumnTupleSourceFactory} instance to create instances of {@link ObjectByteColumnTupleSource}. **/
    public static final TwoColumnTupleSourceFactory<ObjectByteTuple, Object, Byte> FACTORY = new Factory();

    private final ColumnSource<Object> columnSource1;
    private final ColumnSource<Byte> columnSource2;

    public ObjectByteColumnTupleSource(
            @NotNull final ColumnSource<Object> columnSource1,
            @NotNull final ColumnSource<Byte> columnSource2
    ) {
        super(columnSource1, columnSource2);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
    }

    @Override
    public final ObjectByteTuple createTuple(final long rowKey) {
        return new ObjectByteTuple(
                columnSource1.get(rowKey),
                columnSource2.getByte(rowKey)
        );
    }

    @Override
    public final ObjectByteTuple createPreviousTuple(final long rowKey) {
        return new ObjectByteTuple(
                columnSource1.getPrev(rowKey),
                columnSource2.getPrevByte(rowKey)
        );
    }

    @Override
    public final ObjectByteTuple createTupleFromValues(@NotNull final Object... values) {
        return new ObjectByteTuple(
                values[0],
                TypeUtils.unbox((Byte)values[1])
        );
    }

    @Override
    public final ObjectByteTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ObjectByteTuple(
                values[0],
                TypeUtils.unbox((Byte)values[1])
        );
    }

    @Override
    public final int tupleLength() {
        return 2;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ObjectByteTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
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
    public final Object exportElement(@NotNull final ObjectByteTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final ObjectByteTuple tuple) {
        dest[0] = tuple.getFirstElement();
        dest[1] = TypeUtils.box(tuple.getSecondElement());
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final ObjectByteTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = tuple.getFirstElement();
        dest[map[1]] = TypeUtils.box(tuple.getSecondElement());
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ObjectByteTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<ObjectByteTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<Object, ? extends Values> chunk1 = chunks[0].asObjectChunk();
        ByteChunk<? extends Values> chunk2 = chunks[1].asByteChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ObjectByteTuple(chunk1.get(ii), chunk2.get(ii)));
        }
        destination.setSize(chunkSize);
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final ObjectByteTuple tuple) {
        dest[0] = tuple.getFirstElement();
        dest[1] = TypeUtils.box(tuple.getSecondElement());
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final ObjectByteTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = tuple.getFirstElement();
        dest[map[1]] = TypeUtils.box(tuple.getSecondElement());
    }

    /** {@link TwoColumnTupleSourceFactory} for instances of {@link ObjectByteColumnTupleSource}. **/
    private static final class Factory implements TwoColumnTupleSourceFactory<ObjectByteTuple, Object, Byte> {

        private Factory() {
        }

        @Override
        public TupleSource<ObjectByteTuple> create(
                @NotNull final ColumnSource<Object> columnSource1,
                @NotNull final ColumnSource<Byte> columnSource2
        ) {
            return new ObjectByteColumnTupleSource(
                    columnSource1,
                    columnSource2
            );
        }
    }
}
