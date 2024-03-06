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
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.TwoColumnTupleSourceFactory;
import io.deephaven.tuple.generated.IntShortTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Integer and Short.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class IntegerShortColumnTupleSource extends AbstractTupleSource<IntShortTuple> {

    /** {@link TwoColumnTupleSourceFactory} instance to create instances of {@link IntegerShortColumnTupleSource}. **/
    public static final TwoColumnTupleSourceFactory<IntShortTuple, Integer, Short> FACTORY = new Factory();

    private final ColumnSource<Integer> columnSource1;
    private final ColumnSource<Short> columnSource2;

    public IntegerShortColumnTupleSource(
            @NotNull final ColumnSource<Integer> columnSource1,
            @NotNull final ColumnSource<Short> columnSource2
    ) {
        super(columnSource1, columnSource2);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
    }

    @Override
    public final IntShortTuple createTuple(final long rowKey) {
        return new IntShortTuple(
                columnSource1.getInt(rowKey),
                columnSource2.getShort(rowKey)
        );
    }

    @Override
    public final IntShortTuple createPreviousTuple(final long rowKey) {
        return new IntShortTuple(
                columnSource1.getPrevInt(rowKey),
                columnSource2.getPrevShort(rowKey)
        );
    }

    @Override
    public final IntShortTuple createTupleFromValues(@NotNull final Object... values) {
        return new IntShortTuple(
                TypeUtils.unbox((Integer)values[0]),
                TypeUtils.unbox((Short)values[1])
        );
    }

    @Override
    public final IntShortTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new IntShortTuple(
                TypeUtils.unbox((Integer)values[0]),
                TypeUtils.unbox((Short)values[1])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final IntShortTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
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
    public final Object exportElement(@NotNull final IntShortTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final IntShortTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<IntShortTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        IntChunk<? extends Values> chunk1 = chunks[0].asIntChunk();
        ShortChunk<? extends Values> chunk2 = chunks[1].asShortChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new IntShortTuple(chunk1.get(ii), chunk2.get(ii)));
        }
        destination.setSize(chunkSize);
    }

    /** {@link TwoColumnTupleSourceFactory} for instances of {@link IntegerShortColumnTupleSource}. **/
    private static final class Factory implements TwoColumnTupleSourceFactory<IntShortTuple, Integer, Short> {

        private Factory() {
        }

        @Override
        public TupleSource<IntShortTuple> create(
                @NotNull final ColumnSource<Integer> columnSource1,
                @NotNull final ColumnSource<Short> columnSource2
        ) {
            return new IntegerShortColumnTupleSource(
                    columnSource1,
                    columnSource2
            );
        }
    }
}
