//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TupleSourceCodeGenerator and run "./gradlew replicateTupleSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
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
import io.deephaven.tuple.generated.ByteLongLongTuple;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Boolean, Instant, and Long.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class BooleanInstantReinterpretedInstantColumnTupleSource extends AbstractTupleSource<ByteLongLongTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link BooleanInstantReinterpretedInstantColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<ByteLongLongTuple, Boolean, Instant, Long> FACTORY = new Factory();

    private final ColumnSource<Boolean> columnSource1;
    private final ColumnSource<Instant> columnSource2;
    private final ColumnSource<Long> columnSource3;

    public BooleanInstantReinterpretedInstantColumnTupleSource(
            @NotNull final ColumnSource<Boolean> columnSource1,
            @NotNull final ColumnSource<Instant> columnSource2,
            @NotNull final ColumnSource<Long> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final ByteLongLongTuple createTuple(final long rowKey) {
        return new ByteLongLongTuple(
                BooleanUtils.booleanAsByte(columnSource1.getBoolean(rowKey)),
                DateTimeUtils.epochNanos(columnSource2.get(rowKey)),
                columnSource3.getLong(rowKey)
        );
    }

    @Override
    public final ByteLongLongTuple createPreviousTuple(final long rowKey) {
        return new ByteLongLongTuple(
                BooleanUtils.booleanAsByte(columnSource1.getPrevBoolean(rowKey)),
                DateTimeUtils.epochNanos(columnSource2.getPrev(rowKey)),
                columnSource3.getPrevLong(rowKey)
        );
    }

    @Override
    public final ByteLongLongTuple createTupleFromValues(@NotNull final Object... values) {
        return new ByteLongLongTuple(
                BooleanUtils.booleanAsByte((Boolean)values[0]),
                DateTimeUtils.epochNanos((Instant)values[1]),
                DateTimeUtils.epochNanos((Instant)values[2])
        );
    }

    @Override
    public final ByteLongLongTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ByteLongLongTuple(
                BooleanUtils.booleanAsByte((Boolean)values[0]),
                DateTimeUtils.epochNanos((Instant)values[1]),
                TypeUtils.unbox((Long)values[2])
        );
    }

    @Override
    public final int tupleLength() {
        return 3;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ByteLongLongTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) BooleanUtils.byteAsBoolean(tuple.getFirstElement()));
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) DateTimeUtils.epochNanosToInstant(tuple.getSecondElement()));
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) DateTimeUtils.epochNanosToInstant(tuple.getThirdElement()));
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final ByteLongLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return BooleanUtils.byteAsBoolean(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return DateTimeUtils.epochNanosToInstant(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final ByteLongLongTuple tuple) {
        dest[0] = BooleanUtils.byteAsBoolean(tuple.getFirstElement());
        dest[1] = DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
        dest[2] = DateTimeUtils.epochNanosToInstant(tuple.getThirdElement());
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final ByteLongLongTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = BooleanUtils.byteAsBoolean(tuple.getFirstElement());
        dest[map[1]] = DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
        dest[map[2]] = DateTimeUtils.epochNanosToInstant(tuple.getThirdElement());
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ByteLongLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return BooleanUtils.byteAsBoolean(tuple.getFirstElement());
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
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final ByteLongLongTuple tuple) {
        dest[0] = BooleanUtils.byteAsBoolean(tuple.getFirstElement());
        dest[1] = DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
        dest[2] = TypeUtils.box(tuple.getThirdElement());
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final ByteLongLongTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = BooleanUtils.byteAsBoolean(tuple.getFirstElement());
        dest[map[1]] = DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
        dest[map[2]] = TypeUtils.box(tuple.getThirdElement());
    }


    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<ByteLongLongTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<Boolean, ? extends Values> chunk1 = chunks[0].asObjectChunk();
        ObjectChunk<Instant, ? extends Values> chunk2 = chunks[1].asObjectChunk();
        LongChunk<? extends Values> chunk3 = chunks[2].asLongChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ByteLongLongTuple(BooleanUtils.booleanAsByte(chunk1.get(ii)), DateTimeUtils.epochNanos(chunk2.get(ii)), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link BooleanInstantReinterpretedInstantColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<ByteLongLongTuple, Boolean, Instant, Long> {

        private Factory() {
        }

        @Override
        public TupleSource<ByteLongLongTuple> create(
                @NotNull final ColumnSource<Boolean> columnSource1,
                @NotNull final ColumnSource<Instant> columnSource2,
                @NotNull final ColumnSource<Long> columnSource3
        ) {
            return new BooleanInstantReinterpretedInstantColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
