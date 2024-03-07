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
import io.deephaven.tuple.generated.ObjectShortDoubleTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Object, Short, and Double.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ObjectShortDoubleColumnTupleSource extends AbstractTupleSource<ObjectShortDoubleTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link ObjectShortDoubleColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<ObjectShortDoubleTuple, Object, Short, Double> FACTORY = new Factory();

    private final ColumnSource<Object> columnSource1;
    private final ColumnSource<Short> columnSource2;
    private final ColumnSource<Double> columnSource3;

    public ObjectShortDoubleColumnTupleSource(
            @NotNull final ColumnSource<Object> columnSource1,
            @NotNull final ColumnSource<Short> columnSource2,
            @NotNull final ColumnSource<Double> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final ObjectShortDoubleTuple createTuple(final long rowKey) {
        return new ObjectShortDoubleTuple(
                columnSource1.get(rowKey),
                columnSource2.getShort(rowKey),
                columnSource3.getDouble(rowKey)
        );
    }

    @Override
    public final ObjectShortDoubleTuple createPreviousTuple(final long rowKey) {
        return new ObjectShortDoubleTuple(
                columnSource1.getPrev(rowKey),
                columnSource2.getPrevShort(rowKey),
                columnSource3.getPrevDouble(rowKey)
        );
    }

    @Override
    public final ObjectShortDoubleTuple createTupleFromValues(@NotNull final Object... values) {
        return new ObjectShortDoubleTuple(
                values[0],
                TypeUtils.unbox((Short)values[1]),
                TypeUtils.unbox((Double)values[2])
        );
    }

    @Override
    public final ObjectShortDoubleTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ObjectShortDoubleTuple(
                values[0],
                TypeUtils.unbox((Short)values[1]),
                TypeUtils.unbox((Double)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ObjectShortDoubleTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) tuple.getFirstElement());
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
    public final Object exportElement(@NotNull final ObjectShortDoubleTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
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
    public final Object exportElementReinterpreted(@NotNull final ObjectShortDoubleTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
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
        WritableObjectChunk<ObjectShortDoubleTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<Object, ? extends Values> chunk1 = chunks[0].asObjectChunk();
        ShortChunk<? extends Values> chunk2 = chunks[1].asShortChunk();
        DoubleChunk<? extends Values> chunk3 = chunks[2].asDoubleChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ObjectShortDoubleTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link ObjectShortDoubleColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<ObjectShortDoubleTuple, Object, Short, Double> {

        private Factory() {
        }

        @Override
        public TupleSource<ObjectShortDoubleTuple> create(
                @NotNull final ColumnSource<Object> columnSource1,
                @NotNull final ColumnSource<Short> columnSource2,
                @NotNull final ColumnSource<Double> columnSource3
        ) {
            return new ObjectShortDoubleColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
