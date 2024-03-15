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
import io.deephaven.tuple.generated.LongIntShortTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Instant, Integer, and Short.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class InstantIntegerShortColumnTupleSource extends AbstractTupleSource<LongIntShortTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link InstantIntegerShortColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<LongIntShortTuple, Instant, Integer, Short> FACTORY = new Factory();

    private final ColumnSource<Instant> columnSource1;
    private final ColumnSource<Integer> columnSource2;
    private final ColumnSource<Short> columnSource3;

    public InstantIntegerShortColumnTupleSource(
            @NotNull final ColumnSource<Instant> columnSource1,
            @NotNull final ColumnSource<Integer> columnSource2,
            @NotNull final ColumnSource<Short> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final LongIntShortTuple createTuple(final long rowKey) {
        return new LongIntShortTuple(
                DateTimeUtils.epochNanos(columnSource1.get(rowKey)),
                columnSource2.getInt(rowKey),
                columnSource3.getShort(rowKey)
        );
    }

    @Override
    public final LongIntShortTuple createPreviousTuple(final long rowKey) {
        return new LongIntShortTuple(
                DateTimeUtils.epochNanos(columnSource1.getPrev(rowKey)),
                columnSource2.getPrevInt(rowKey),
                columnSource3.getPrevShort(rowKey)
        );
    }

    @Override
    public final LongIntShortTuple createTupleFromValues(@NotNull final Object... values) {
        return new LongIntShortTuple(
                DateTimeUtils.epochNanos((Instant)values[0]),
                TypeUtils.unbox((Integer)values[1]),
                TypeUtils.unbox((Short)values[2])
        );
    }

    @Override
    public final LongIntShortTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new LongIntShortTuple(
                DateTimeUtils.epochNanos((Instant)values[0]),
                TypeUtils.unbox((Integer)values[1]),
                TypeUtils.unbox((Short)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final LongIntShortTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) DateTimeUtils.epochNanosToInstant(tuple.getFirstElement()));
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
    public final Object exportElement(@NotNull final LongIntShortTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return DateTimeUtils.epochNanosToInstant(tuple.getFirstElement());
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
    public final Object exportElementReinterpreted(@NotNull final LongIntShortTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return DateTimeUtils.epochNanosToInstant(tuple.getFirstElement());
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
        WritableObjectChunk<LongIntShortTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<Instant, ? extends Values> chunk1 = chunks[0].asObjectChunk();
        IntChunk<? extends Values> chunk2 = chunks[1].asIntChunk();
        ShortChunk<? extends Values> chunk3 = chunks[2].asShortChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new LongIntShortTuple(DateTimeUtils.epochNanos(chunk1.get(ii)), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link InstantIntegerShortColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<LongIntShortTuple, Instant, Integer, Short> {

        private Factory() {
        }

        @Override
        public TupleSource<LongIntShortTuple> create(
                @NotNull final ColumnSource<Instant> columnSource1,
                @NotNull final ColumnSource<Integer> columnSource2,
                @NotNull final ColumnSource<Short> columnSource3
        ) {
            return new InstantIntegerShortColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
