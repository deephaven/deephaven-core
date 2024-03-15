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
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.tuple.generated.LongLongDoubleTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Instant, Instant, and Double.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class InstantInstantDoubleColumnTupleSource extends AbstractTupleSource<LongLongDoubleTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link InstantInstantDoubleColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<LongLongDoubleTuple, Instant, Instant, Double> FACTORY = new Factory();

    private final ColumnSource<Instant> columnSource1;
    private final ColumnSource<Instant> columnSource2;
    private final ColumnSource<Double> columnSource3;

    public InstantInstantDoubleColumnTupleSource(
            @NotNull final ColumnSource<Instant> columnSource1,
            @NotNull final ColumnSource<Instant> columnSource2,
            @NotNull final ColumnSource<Double> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final LongLongDoubleTuple createTuple(final long rowKey) {
        return new LongLongDoubleTuple(
                DateTimeUtils.epochNanos(columnSource1.get(rowKey)),
                DateTimeUtils.epochNanos(columnSource2.get(rowKey)),
                columnSource3.getDouble(rowKey)
        );
    }

    @Override
    public final LongLongDoubleTuple createPreviousTuple(final long rowKey) {
        return new LongLongDoubleTuple(
                DateTimeUtils.epochNanos(columnSource1.getPrev(rowKey)),
                DateTimeUtils.epochNanos(columnSource2.getPrev(rowKey)),
                columnSource3.getPrevDouble(rowKey)
        );
    }

    @Override
    public final LongLongDoubleTuple createTupleFromValues(@NotNull final Object... values) {
        return new LongLongDoubleTuple(
                DateTimeUtils.epochNanos((Instant)values[0]),
                DateTimeUtils.epochNanos((Instant)values[1]),
                TypeUtils.unbox((Double)values[2])
        );
    }

    @Override
    public final LongLongDoubleTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new LongLongDoubleTuple(
                DateTimeUtils.epochNanos((Instant)values[0]),
                DateTimeUtils.epochNanos((Instant)values[1]),
                TypeUtils.unbox((Double)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final LongLongDoubleTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) DateTimeUtils.epochNanosToInstant(tuple.getFirstElement()));
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
    public final Object exportElement(@NotNull final LongLongDoubleTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return DateTimeUtils.epochNanosToInstant(tuple.getFirstElement());
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
    public final Object exportElementReinterpreted(@NotNull final LongLongDoubleTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return DateTimeUtils.epochNanosToInstant(tuple.getFirstElement());
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
    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<LongLongDoubleTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<Instant, ? extends Values> chunk1 = chunks[0].asObjectChunk();
        ObjectChunk<Instant, ? extends Values> chunk2 = chunks[1].asObjectChunk();
        DoubleChunk<? extends Values> chunk3 = chunks[2].asDoubleChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new LongLongDoubleTuple(DateTimeUtils.epochNanos(chunk1.get(ii)), DateTimeUtils.epochNanos(chunk2.get(ii)), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link InstantInstantDoubleColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<LongLongDoubleTuple, Instant, Instant, Double> {

        private Factory() {
        }

        @Override
        public TupleSource<LongLongDoubleTuple> create(
                @NotNull final ColumnSource<Instant> columnSource1,
                @NotNull final ColumnSource<Instant> columnSource2,
                @NotNull final ColumnSource<Double> columnSource3
        ) {
            return new InstantInstantDoubleColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
