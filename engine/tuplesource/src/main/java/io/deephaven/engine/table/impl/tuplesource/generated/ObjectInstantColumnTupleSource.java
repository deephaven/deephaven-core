//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TupleSourceCodeGenerator and run "./gradlew replicateTupleSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.TwoColumnTupleSourceFactory;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.tuple.generated.ObjectLongTuple;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Object and Instant.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ObjectInstantColumnTupleSource extends AbstractTupleSource<ObjectLongTuple> {

    /** {@link TwoColumnTupleSourceFactory} instance to create instances of {@link ObjectInstantColumnTupleSource}. **/
    public static final TwoColumnTupleSourceFactory<ObjectLongTuple, Object, Instant> FACTORY = new Factory();

    private final ColumnSource<Object> columnSource1;
    private final ColumnSource<Instant> columnSource2;

    public ObjectInstantColumnTupleSource(
            @NotNull final ColumnSource<Object> columnSource1,
            @NotNull final ColumnSource<Instant> columnSource2
    ) {
        super(columnSource1, columnSource2);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
    }

    @Override
    public final ObjectLongTuple createTuple(final long rowKey) {
        return new ObjectLongTuple(
                columnSource1.get(rowKey),
                DateTimeUtils.epochNanos(columnSource2.get(rowKey))
        );
    }

    @Override
    public final ObjectLongTuple createPreviousTuple(final long rowKey) {
        return new ObjectLongTuple(
                columnSource1.getPrev(rowKey),
                DateTimeUtils.epochNanos(columnSource2.getPrev(rowKey))
        );
    }

    @Override
    public final ObjectLongTuple createTupleFromValues(@NotNull final Object... values) {
        return new ObjectLongTuple(
                values[0],
                DateTimeUtils.epochNanos((Instant)values[1])
        );
    }

    @Override
    public final ObjectLongTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ObjectLongTuple(
                values[0],
                DateTimeUtils.epochNanos((Instant)values[1])
        );
    }

    @Override
    public final int tupleLength() {
        return 2;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ObjectLongTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) DateTimeUtils.epochNanosToInstant(tuple.getSecondElement()));
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final ObjectLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
        }
        if (elementIndex == 1) {
            return DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final ObjectLongTuple tuple) {
        dest[0] = tuple.getFirstElement();
        dest[1] = DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final ObjectLongTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = tuple.getFirstElement();
        dest[map[1]] = DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ObjectLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
        }
        if (elementIndex == 1) {
            return DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<ObjectLongTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<Object, ? extends Values> chunk1 = chunks[0].asObjectChunk();
        ObjectChunk<Instant, ? extends Values> chunk2 = chunks[1].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ObjectLongTuple(chunk1.get(ii), DateTimeUtils.epochNanos(chunk2.get(ii))));
        }
        destination.setSize(chunkSize);
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final ObjectLongTuple tuple) {
        dest[0] = tuple.getFirstElement();
        dest[1] = DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final ObjectLongTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = tuple.getFirstElement();
        dest[map[1]] = DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
    }

    /** {@link TwoColumnTupleSourceFactory} for instances of {@link ObjectInstantColumnTupleSource}. **/
    private static final class Factory implements TwoColumnTupleSourceFactory<ObjectLongTuple, Object, Instant> {

        private Factory() {
        }

        @Override
        public TupleSource<ObjectLongTuple> create(
                @NotNull final ColumnSource<Object> columnSource1,
                @NotNull final ColumnSource<Instant> columnSource2
        ) {
            return new ObjectInstantColumnTupleSource(
                    columnSource1,
                    columnSource2
            );
        }
    }
}
