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
import io.deephaven.time.DateTimeUtils;
import io.deephaven.tuple.generated.DoubleLongShortTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Double, Instant, and Short.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class DoubleInstantShortColumnTupleSource extends AbstractTupleSource<DoubleLongShortTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link DoubleInstantShortColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<DoubleLongShortTuple, Double, Instant, Short> FACTORY = new Factory();

    private final ColumnSource<Double> columnSource1;
    private final ColumnSource<Instant> columnSource2;
    private final ColumnSource<Short> columnSource3;

    public DoubleInstantShortColumnTupleSource(
            @NotNull final ColumnSource<Double> columnSource1,
            @NotNull final ColumnSource<Instant> columnSource2,
            @NotNull final ColumnSource<Short> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final DoubleLongShortTuple createTuple(final long rowKey) {
        return new DoubleLongShortTuple(
                columnSource1.getDouble(rowKey),
                DateTimeUtils.epochNanos(columnSource2.get(rowKey)),
                columnSource3.getShort(rowKey)
        );
    }

    @Override
    public final DoubleLongShortTuple createPreviousTuple(final long rowKey) {
        return new DoubleLongShortTuple(
                columnSource1.getPrevDouble(rowKey),
                DateTimeUtils.epochNanos(columnSource2.getPrev(rowKey)),
                columnSource3.getPrevShort(rowKey)
        );
    }

    @Override
    public final DoubleLongShortTuple createTupleFromValues(@NotNull final Object... values) {
        return new DoubleLongShortTuple(
                TypeUtils.unbox((Double)values[0]),
                DateTimeUtils.epochNanos((Instant)values[1]),
                TypeUtils.unbox((Short)values[2])
        );
    }

    @Override
    public final DoubleLongShortTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new DoubleLongShortTuple(
                TypeUtils.unbox((Double)values[0]),
                DateTimeUtils.epochNanos((Instant)values[1]),
                TypeUtils.unbox((Short)values[2])
        );
    }

    @Override
    public final int tupleLength() {
        return 3;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final DoubleLongShortTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) DateTimeUtils.epochNanosToInstant(tuple.getSecondElement()));
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationRowKey, tuple.getThirdElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final DoubleLongShortTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final DoubleLongShortTuple tuple) {
        dest[0] = TypeUtils.box(tuple.getFirstElement());
        dest[1] = DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
        dest[2] = TypeUtils.box(tuple.getThirdElement());
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final DoubleLongShortTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = TypeUtils.box(tuple.getFirstElement());
        dest[map[1]] = DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
        dest[map[2]] = TypeUtils.box(tuple.getThirdElement());
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final DoubleLongShortTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }
    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final DoubleLongShortTuple tuple) {
        dest[0] = TypeUtils.box(tuple.getFirstElement());
        dest[1] = DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
        dest[2] = TypeUtils.box(tuple.getThirdElement());
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final DoubleLongShortTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = TypeUtils.box(tuple.getFirstElement());
        dest[map[1]] = DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
        dest[map[2]] = TypeUtils.box(tuple.getThirdElement());
    }


    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<DoubleLongShortTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        DoubleChunk<? extends Values> chunk1 = chunks[0].asDoubleChunk();
        ObjectChunk<Instant, ? extends Values> chunk2 = chunks[1].asObjectChunk();
        ShortChunk<? extends Values> chunk3 = chunks[2].asShortChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new DoubleLongShortTuple(chunk1.get(ii), DateTimeUtils.epochNanos(chunk2.get(ii)), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link DoubleInstantShortColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<DoubleLongShortTuple, Double, Instant, Short> {

        private Factory() {
        }

        @Override
        public TupleSource<DoubleLongShortTuple> create(
                @NotNull final ColumnSource<Double> columnSource1,
                @NotNull final ColumnSource<Instant> columnSource2,
                @NotNull final ColumnSource<Short> columnSource3
        ) {
            return new DoubleInstantShortColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
