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
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.tuple.generated.FloatIntLongTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Float, Integer, and Instant.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class FloatIntegerInstantColumnTupleSource extends AbstractTupleSource<FloatIntLongTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link FloatIntegerInstantColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<FloatIntLongTuple, Float, Integer, Instant> FACTORY = new Factory();

    private final ColumnSource<Float> columnSource1;
    private final ColumnSource<Integer> columnSource2;
    private final ColumnSource<Instant> columnSource3;

    public FloatIntegerInstantColumnTupleSource(
            @NotNull final ColumnSource<Float> columnSource1,
            @NotNull final ColumnSource<Integer> columnSource2,
            @NotNull final ColumnSource<Instant> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final FloatIntLongTuple createTuple(final long rowKey) {
        return new FloatIntLongTuple(
                columnSource1.getFloat(rowKey),
                columnSource2.getInt(rowKey),
                DateTimeUtils.epochNanos(columnSource3.get(rowKey))
        );
    }

    @Override
    public final FloatIntLongTuple createPreviousTuple(final long rowKey) {
        return new FloatIntLongTuple(
                columnSource1.getPrevFloat(rowKey),
                columnSource2.getPrevInt(rowKey),
                DateTimeUtils.epochNanos(columnSource3.getPrev(rowKey))
        );
    }

    @Override
    public final FloatIntLongTuple createTupleFromValues(@NotNull final Object... values) {
        return new FloatIntLongTuple(
                TypeUtils.unbox((Float)values[0]),
                TypeUtils.unbox((Integer)values[1]),
                DateTimeUtils.epochNanos((Instant)values[2])
        );
    }

    @Override
    public final FloatIntLongTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new FloatIntLongTuple(
                TypeUtils.unbox((Float)values[0]),
                TypeUtils.unbox((Integer)values[1]),
                DateTimeUtils.epochNanos((Instant)values[2])
        );
    }

    @Override
    public final int tupleLength() {
        return 3;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final FloatIntLongTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) DateTimeUtils.epochNanosToInstant(tuple.getThirdElement()));
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final FloatIntLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return DateTimeUtils.epochNanosToInstant(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final FloatIntLongTuple tuple) {
        dest[0] = TypeUtils.box(tuple.getFirstElement());
        dest[1] = TypeUtils.box(tuple.getSecondElement());
        dest[2] = DateTimeUtils.epochNanosToInstant(tuple.getThirdElement());
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final FloatIntLongTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = TypeUtils.box(tuple.getFirstElement());
        dest[map[1]] = TypeUtils.box(tuple.getSecondElement());
        dest[map[2]] = DateTimeUtils.epochNanosToInstant(tuple.getThirdElement());
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final FloatIntLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return DateTimeUtils.epochNanosToInstant(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }
    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final FloatIntLongTuple tuple) {
        dest[0] = TypeUtils.box(tuple.getFirstElement());
        dest[1] = TypeUtils.box(tuple.getSecondElement());
        dest[2] = DateTimeUtils.epochNanosToInstant(tuple.getThirdElement());
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final FloatIntLongTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = TypeUtils.box(tuple.getFirstElement());
        dest[map[1]] = TypeUtils.box(tuple.getSecondElement());
        dest[map[2]] = DateTimeUtils.epochNanosToInstant(tuple.getThirdElement());
    }


    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<FloatIntLongTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        FloatChunk<? extends Values> chunk1 = chunks[0].asFloatChunk();
        IntChunk<? extends Values> chunk2 = chunks[1].asIntChunk();
        ObjectChunk<Instant, ? extends Values> chunk3 = chunks[2].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new FloatIntLongTuple(chunk1.get(ii), chunk2.get(ii), DateTimeUtils.epochNanos(chunk3.get(ii))));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link FloatIntegerInstantColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<FloatIntLongTuple, Float, Integer, Instant> {

        private Factory() {
        }

        @Override
        public TupleSource<FloatIntLongTuple> create(
                @NotNull final ColumnSource<Float> columnSource1,
                @NotNull final ColumnSource<Integer> columnSource2,
                @NotNull final ColumnSource<Instant> columnSource3
        ) {
            return new FloatIntegerInstantColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
