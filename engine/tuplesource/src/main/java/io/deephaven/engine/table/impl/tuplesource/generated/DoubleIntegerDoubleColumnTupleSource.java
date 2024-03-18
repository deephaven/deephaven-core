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
import io.deephaven.engine.table.impl.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.tuple.generated.DoubleIntDoubleTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Double, Integer, and Double.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class DoubleIntegerDoubleColumnTupleSource extends AbstractTupleSource<DoubleIntDoubleTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link DoubleIntegerDoubleColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<DoubleIntDoubleTuple, Double, Integer, Double> FACTORY = new Factory();

    private final ColumnSource<Double> columnSource1;
    private final ColumnSource<Integer> columnSource2;
    private final ColumnSource<Double> columnSource3;

    public DoubleIntegerDoubleColumnTupleSource(
            @NotNull final ColumnSource<Double> columnSource1,
            @NotNull final ColumnSource<Integer> columnSource2,
            @NotNull final ColumnSource<Double> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final DoubleIntDoubleTuple createTuple(final long rowKey) {
        return new DoubleIntDoubleTuple(
                columnSource1.getDouble(rowKey),
                columnSource2.getInt(rowKey),
                columnSource3.getDouble(rowKey)
        );
    }

    @Override
    public final DoubleIntDoubleTuple createPreviousTuple(final long rowKey) {
        return new DoubleIntDoubleTuple(
                columnSource1.getPrevDouble(rowKey),
                columnSource2.getPrevInt(rowKey),
                columnSource3.getPrevDouble(rowKey)
        );
    }

    @Override
    public final DoubleIntDoubleTuple createTupleFromValues(@NotNull final Object... values) {
        return new DoubleIntDoubleTuple(
                TypeUtils.unbox((Double)values[0]),
                TypeUtils.unbox((Integer)values[1]),
                TypeUtils.unbox((Double)values[2])
        );
    }

    @Override
    public final DoubleIntDoubleTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new DoubleIntDoubleTuple(
                TypeUtils.unbox((Double)values[0]),
                TypeUtils.unbox((Integer)values[1]),
                TypeUtils.unbox((Double)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final DoubleIntDoubleTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationRowKey, tuple.getThirdElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final DoubleIntDoubleTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final DoubleIntDoubleTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<DoubleIntDoubleTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        DoubleChunk<? extends Values> chunk1 = chunks[0].asDoubleChunk();
        IntChunk<? extends Values> chunk2 = chunks[1].asIntChunk();
        DoubleChunk<? extends Values> chunk3 = chunks[2].asDoubleChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new DoubleIntDoubleTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link DoubleIntegerDoubleColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<DoubleIntDoubleTuple, Double, Integer, Double> {

        private Factory() {
        }

        @Override
        public TupleSource<DoubleIntDoubleTuple> create(
                @NotNull final ColumnSource<Double> columnSource1,
                @NotNull final ColumnSource<Integer> columnSource2,
                @NotNull final ColumnSource<Double> columnSource3
        ) {
            return new DoubleIntegerDoubleColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
