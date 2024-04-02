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
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.tuple.generated.ShortDoubleObjectTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Short, Double, and Object.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ShortDoubleObjectColumnTupleSource extends AbstractTupleSource<ShortDoubleObjectTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link ShortDoubleObjectColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<ShortDoubleObjectTuple, Short, Double, Object> FACTORY = new Factory();

    private final ColumnSource<Short> columnSource1;
    private final ColumnSource<Double> columnSource2;
    private final ColumnSource<Object> columnSource3;

    public ShortDoubleObjectColumnTupleSource(
            @NotNull final ColumnSource<Short> columnSource1,
            @NotNull final ColumnSource<Double> columnSource2,
            @NotNull final ColumnSource<Object> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final ShortDoubleObjectTuple createTuple(final long rowKey) {
        return new ShortDoubleObjectTuple(
                columnSource1.getShort(rowKey),
                columnSource2.getDouble(rowKey),
                columnSource3.get(rowKey)
        );
    }

    @Override
    public final ShortDoubleObjectTuple createPreviousTuple(final long rowKey) {
        return new ShortDoubleObjectTuple(
                columnSource1.getPrevShort(rowKey),
                columnSource2.getPrevDouble(rowKey),
                columnSource3.getPrev(rowKey)
        );
    }

    @Override
    public final ShortDoubleObjectTuple createTupleFromValues(@NotNull final Object... values) {
        return new ShortDoubleObjectTuple(
                TypeUtils.unbox((Short)values[0]),
                TypeUtils.unbox((Double)values[1]),
                values[2]
        );
    }

    @Override
    public final ShortDoubleObjectTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ShortDoubleObjectTuple(
                TypeUtils.unbox((Short)values[0]),
                TypeUtils.unbox((Double)values[1]),
                values[2]
        );
    }

    @Override
    public final int tupleLength() {
        return 3;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ShortDoubleObjectTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) tuple.getThirdElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final ShortDoubleObjectTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return tuple.getThirdElement();
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final ShortDoubleObjectTuple tuple) {
        dest[0] = TypeUtils.box(tuple.getFirstElement());
        dest[1] = TypeUtils.box(tuple.getSecondElement());
        dest[2] = tuple.getThirdElement();
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final ShortDoubleObjectTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = TypeUtils.box(tuple.getFirstElement());
        dest[map[1]] = TypeUtils.box(tuple.getSecondElement());
        dest[map[2]] = tuple.getThirdElement();
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ShortDoubleObjectTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return tuple.getThirdElement();
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }
    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final ShortDoubleObjectTuple tuple) {
        dest[0] = TypeUtils.box(tuple.getFirstElement());
        dest[1] = TypeUtils.box(tuple.getSecondElement());
        dest[2] = tuple.getThirdElement();
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final ShortDoubleObjectTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = TypeUtils.box(tuple.getFirstElement());
        dest[map[1]] = TypeUtils.box(tuple.getSecondElement());
        dest[map[2]] = tuple.getThirdElement();
    }


    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<ShortDoubleObjectTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ShortChunk<? extends Values> chunk1 = chunks[0].asShortChunk();
        DoubleChunk<? extends Values> chunk2 = chunks[1].asDoubleChunk();
        ObjectChunk<Object, ? extends Values> chunk3 = chunks[2].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ShortDoubleObjectTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link ShortDoubleObjectColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<ShortDoubleObjectTuple, Short, Double, Object> {

        private Factory() {
        }

        @Override
        public TupleSource<ShortDoubleObjectTuple> create(
                @NotNull final ColumnSource<Short> columnSource1,
                @NotNull final ColumnSource<Double> columnSource2,
                @NotNull final ColumnSource<Object> columnSource3
        ) {
            return new ShortDoubleObjectColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
