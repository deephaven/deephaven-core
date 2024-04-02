//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TupleSourceCodeGenerator and run "./gradlew replicateTupleSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.TwoColumnTupleSourceFactory;
import io.deephaven.tuple.generated.FloatObjectTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Float and Object.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class FloatObjectColumnTupleSource extends AbstractTupleSource<FloatObjectTuple> {

    /** {@link TwoColumnTupleSourceFactory} instance to create instances of {@link FloatObjectColumnTupleSource}. **/
    public static final TwoColumnTupleSourceFactory<FloatObjectTuple, Float, Object> FACTORY = new Factory();

    private final ColumnSource<Float> columnSource1;
    private final ColumnSource<Object> columnSource2;

    public FloatObjectColumnTupleSource(
            @NotNull final ColumnSource<Float> columnSource1,
            @NotNull final ColumnSource<Object> columnSource2
    ) {
        super(columnSource1, columnSource2);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
    }

    @Override
    public final FloatObjectTuple createTuple(final long rowKey) {
        return new FloatObjectTuple(
                columnSource1.getFloat(rowKey),
                columnSource2.get(rowKey)
        );
    }

    @Override
    public final FloatObjectTuple createPreviousTuple(final long rowKey) {
        return new FloatObjectTuple(
                columnSource1.getPrevFloat(rowKey),
                columnSource2.getPrev(rowKey)
        );
    }

    @Override
    public final FloatObjectTuple createTupleFromValues(@NotNull final Object... values) {
        return new FloatObjectTuple(
                TypeUtils.unbox((Float)values[0]),
                values[1]
        );
    }

    @Override
    public final FloatObjectTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new FloatObjectTuple(
                TypeUtils.unbox((Float)values[0]),
                values[1]
        );
    }

    @Override
    public final int tupleLength() {
        return 2;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final FloatObjectTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) tuple.getSecondElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final FloatObjectTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return tuple.getSecondElement();
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final FloatObjectTuple tuple) {
        dest[0] = TypeUtils.box(tuple.getFirstElement());
        dest[1] = tuple.getSecondElement();
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final FloatObjectTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = TypeUtils.box(tuple.getFirstElement());
        dest[map[1]] = tuple.getSecondElement();
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final FloatObjectTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return tuple.getSecondElement();
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<FloatObjectTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        FloatChunk<? extends Values> chunk1 = chunks[0].asFloatChunk();
        ObjectChunk<Object, ? extends Values> chunk2 = chunks[1].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new FloatObjectTuple(chunk1.get(ii), chunk2.get(ii)));
        }
        destination.setSize(chunkSize);
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final FloatObjectTuple tuple) {
        dest[0] = TypeUtils.box(tuple.getFirstElement());
        dest[1] = tuple.getSecondElement();
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final FloatObjectTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = TypeUtils.box(tuple.getFirstElement());
        dest[map[1]] = tuple.getSecondElement();
    }

    /** {@link TwoColumnTupleSourceFactory} for instances of {@link FloatObjectColumnTupleSource}. **/
    private static final class Factory implements TwoColumnTupleSourceFactory<FloatObjectTuple, Float, Object> {

        private Factory() {
        }

        @Override
        public TupleSource<FloatObjectTuple> create(
                @NotNull final ColumnSource<Float> columnSource1,
                @NotNull final ColumnSource<Object> columnSource2
        ) {
            return new FloatObjectColumnTupleSource(
                    columnSource1,
                    columnSource2
            );
        }
    }
}
