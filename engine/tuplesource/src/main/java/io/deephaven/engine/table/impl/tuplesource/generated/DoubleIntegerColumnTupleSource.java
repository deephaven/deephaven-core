//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TupleSourceCodeGenerator and run "./gradlew replicateTupleSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.TwoColumnTupleSourceFactory;
import io.deephaven.tuple.generated.DoubleIntTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Double and Integer.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class DoubleIntegerColumnTupleSource extends AbstractTupleSource<DoubleIntTuple> {

    /** {@link TwoColumnTupleSourceFactory} instance to create instances of {@link DoubleIntegerColumnTupleSource}. **/
    public static final TwoColumnTupleSourceFactory<DoubleIntTuple, Double, Integer> FACTORY = new Factory();

    private final ColumnSource<Double> columnSource1;
    private final ColumnSource<Integer> columnSource2;

    public DoubleIntegerColumnTupleSource(
            @NotNull final ColumnSource<Double> columnSource1,
            @NotNull final ColumnSource<Integer> columnSource2
    ) {
        super(columnSource1, columnSource2);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
    }

    @Override
    public final DoubleIntTuple createTuple(final long rowKey) {
        return new DoubleIntTuple(
                columnSource1.getDouble(rowKey),
                columnSource2.getInt(rowKey)
        );
    }

    @Override
    public final DoubleIntTuple createPreviousTuple(final long rowKey) {
        return new DoubleIntTuple(
                columnSource1.getPrevDouble(rowKey),
                columnSource2.getPrevInt(rowKey)
        );
    }

    @Override
    public final DoubleIntTuple createTupleFromValues(@NotNull final Object... values) {
        return new DoubleIntTuple(
                TypeUtils.unbox((Double)values[0]),
                TypeUtils.unbox((Integer)values[1])
        );
    }

    @Override
    public final DoubleIntTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new DoubleIntTuple(
                TypeUtils.unbox((Double)values[0]),
                TypeUtils.unbox((Integer)values[1])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final DoubleIntTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
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
    public final Object exportElement(@NotNull final DoubleIntTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final DoubleIntTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<DoubleIntTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        DoubleChunk<? extends Values> chunk1 = chunks[0].asDoubleChunk();
        IntChunk<? extends Values> chunk2 = chunks[1].asIntChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new DoubleIntTuple(chunk1.get(ii), chunk2.get(ii)));
        }
        destination.setSize(chunkSize);
    }

    /** {@link TwoColumnTupleSourceFactory} for instances of {@link DoubleIntegerColumnTupleSource}. **/
    private static final class Factory implements TwoColumnTupleSourceFactory<DoubleIntTuple, Double, Integer> {

        private Factory() {
        }

        @Override
        public TupleSource<DoubleIntTuple> create(
                @NotNull final ColumnSource<Double> columnSource1,
                @NotNull final ColumnSource<Integer> columnSource2
        ) {
            return new DoubleIntegerColumnTupleSource(
                    columnSource1,
                    columnSource2
            );
        }
    }
}
