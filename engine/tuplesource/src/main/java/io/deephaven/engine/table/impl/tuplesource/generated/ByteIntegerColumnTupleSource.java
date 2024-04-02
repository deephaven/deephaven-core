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
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.TwoColumnTupleSourceFactory;
import io.deephaven.tuple.generated.ByteIntTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Byte and Integer.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ByteIntegerColumnTupleSource extends AbstractTupleSource<ByteIntTuple> {

    /** {@link TwoColumnTupleSourceFactory} instance to create instances of {@link ByteIntegerColumnTupleSource}. **/
    public static final TwoColumnTupleSourceFactory<ByteIntTuple, Byte, Integer> FACTORY = new Factory();

    private final ColumnSource<Byte> columnSource1;
    private final ColumnSource<Integer> columnSource2;

    public ByteIntegerColumnTupleSource(
            @NotNull final ColumnSource<Byte> columnSource1,
            @NotNull final ColumnSource<Integer> columnSource2
    ) {
        super(columnSource1, columnSource2);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
    }

    @Override
    public final ByteIntTuple createTuple(final long rowKey) {
        return new ByteIntTuple(
                columnSource1.getByte(rowKey),
                columnSource2.getInt(rowKey)
        );
    }

    @Override
    public final ByteIntTuple createPreviousTuple(final long rowKey) {
        return new ByteIntTuple(
                columnSource1.getPrevByte(rowKey),
                columnSource2.getPrevInt(rowKey)
        );
    }

    @Override
    public final ByteIntTuple createTupleFromValues(@NotNull final Object... values) {
        return new ByteIntTuple(
                TypeUtils.unbox((Byte)values[0]),
                TypeUtils.unbox((Integer)values[1])
        );
    }

    @Override
    public final ByteIntTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ByteIntTuple(
                TypeUtils.unbox((Byte)values[0]),
                TypeUtils.unbox((Integer)values[1])
        );
    }

    @Override
    public final int tupleLength() {
        return 2;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ByteIntTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, tuple.getSecondElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final ByteIntTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final ByteIntTuple tuple) {
        dest[0] = TypeUtils.box(tuple.getFirstElement());
        dest[1] = TypeUtils.box(tuple.getSecondElement());
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final ByteIntTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = TypeUtils.box(tuple.getFirstElement());
        dest[map[1]] = TypeUtils.box(tuple.getSecondElement());
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ByteIntTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<ByteIntTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ByteChunk<? extends Values> chunk1 = chunks[0].asByteChunk();
        IntChunk<? extends Values> chunk2 = chunks[1].asIntChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ByteIntTuple(chunk1.get(ii), chunk2.get(ii)));
        }
        destination.setSize(chunkSize);
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final ByteIntTuple tuple) {
        dest[0] = TypeUtils.box(tuple.getFirstElement());
        dest[1] = TypeUtils.box(tuple.getSecondElement());
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final ByteIntTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = TypeUtils.box(tuple.getFirstElement());
        dest[map[1]] = TypeUtils.box(tuple.getSecondElement());
    }

    /** {@link TwoColumnTupleSourceFactory} for instances of {@link ByteIntegerColumnTupleSource}. **/
    private static final class Factory implements TwoColumnTupleSourceFactory<ByteIntTuple, Byte, Integer> {

        private Factory() {
        }

        @Override
        public TupleSource<ByteIntTuple> create(
                @NotNull final ColumnSource<Byte> columnSource1,
                @NotNull final ColumnSource<Integer> columnSource2
        ) {
            return new ByteIntegerColumnTupleSource(
                    columnSource1,
                    columnSource2
            );
        }
    }
}
