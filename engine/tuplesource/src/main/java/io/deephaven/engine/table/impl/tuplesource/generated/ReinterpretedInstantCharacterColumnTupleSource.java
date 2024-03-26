//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TupleSourceCodeGenerator and run "./gradlew replicateTupleSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.TwoColumnTupleSourceFactory;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.tuple.generated.LongCharTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Long and Character.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ReinterpretedInstantCharacterColumnTupleSource extends AbstractTupleSource<LongCharTuple> {

    /** {@link TwoColumnTupleSourceFactory} instance to create instances of {@link ReinterpretedInstantCharacterColumnTupleSource}. **/
    public static final TwoColumnTupleSourceFactory<LongCharTuple, Long, Character> FACTORY = new Factory();

    private final ColumnSource<Long> columnSource1;
    private final ColumnSource<Character> columnSource2;

    public ReinterpretedInstantCharacterColumnTupleSource(
            @NotNull final ColumnSource<Long> columnSource1,
            @NotNull final ColumnSource<Character> columnSource2
    ) {
        super(columnSource1, columnSource2);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
    }

    @Override
    public final LongCharTuple createTuple(final long rowKey) {
        return new LongCharTuple(
                columnSource1.getLong(rowKey),
                columnSource2.getChar(rowKey)
        );
    }

    @Override
    public final LongCharTuple createPreviousTuple(final long rowKey) {
        return new LongCharTuple(
                columnSource1.getPrevLong(rowKey),
                columnSource2.getPrevChar(rowKey)
        );
    }

    @Override
    public final LongCharTuple createTupleFromValues(@NotNull final Object... values) {
        return new LongCharTuple(
                DateTimeUtils.epochNanos((Instant)values[0]),
                TypeUtils.unbox((Character)values[1])
        );
    }

    @Override
    public final LongCharTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new LongCharTuple(
                TypeUtils.unbox((Long)values[0]),
                TypeUtils.unbox((Character)values[1])
        );
    }

    @Override
    public final int tupleLength() {
        return 2;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final LongCharTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) DateTimeUtils.epochNanosToInstant(tuple.getFirstElement()));
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, tuple.getSecondElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final LongCharTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return DateTimeUtils.epochNanosToInstant(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final LongCharTuple tuple) {
        dest[0] = DateTimeUtils.epochNanosToInstant(tuple.getFirstElement());
        dest[1] = TypeUtils.box(tuple.getSecondElement());
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final LongCharTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = DateTimeUtils.epochNanosToInstant(tuple.getFirstElement());
        dest[map[1]] = TypeUtils.box(tuple.getSecondElement());
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final LongCharTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<LongCharTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        LongChunk<? extends Values> chunk1 = chunks[0].asLongChunk();
        CharChunk<? extends Values> chunk2 = chunks[1].asCharChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new LongCharTuple(chunk1.get(ii), chunk2.get(ii)));
        }
        destination.setSize(chunkSize);
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final LongCharTuple tuple) {
        dest[0] = TypeUtils.box(tuple.getFirstElement());
        dest[1] = TypeUtils.box(tuple.getSecondElement());
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final LongCharTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = TypeUtils.box(tuple.getFirstElement());
        dest[map[1]] = TypeUtils.box(tuple.getSecondElement());
    }

    /** {@link TwoColumnTupleSourceFactory} for instances of {@link ReinterpretedInstantCharacterColumnTupleSource}. **/
    private static final class Factory implements TwoColumnTupleSourceFactory<LongCharTuple, Long, Character> {

        private Factory() {
        }

        @Override
        public TupleSource<LongCharTuple> create(
                @NotNull final ColumnSource<Long> columnSource1,
                @NotNull final ColumnSource<Character> columnSource2
        ) {
            return new ReinterpretedInstantCharacterColumnTupleSource(
                    columnSource1,
                    columnSource2
            );
        }
    }
}
