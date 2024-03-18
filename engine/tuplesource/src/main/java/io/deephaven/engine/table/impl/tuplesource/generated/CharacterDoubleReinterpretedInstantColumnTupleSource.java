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
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.tuple.generated.CharDoubleLongTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Character, Double, and Long.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class CharacterDoubleReinterpretedInstantColumnTupleSource extends AbstractTupleSource<CharDoubleLongTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link CharacterDoubleReinterpretedInstantColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<CharDoubleLongTuple, Character, Double, Long> FACTORY = new Factory();

    private final ColumnSource<Character> columnSource1;
    private final ColumnSource<Double> columnSource2;
    private final ColumnSource<Long> columnSource3;

    public CharacterDoubleReinterpretedInstantColumnTupleSource(
            @NotNull final ColumnSource<Character> columnSource1,
            @NotNull final ColumnSource<Double> columnSource2,
            @NotNull final ColumnSource<Long> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final CharDoubleLongTuple createTuple(final long rowKey) {
        return new CharDoubleLongTuple(
                columnSource1.getChar(rowKey),
                columnSource2.getDouble(rowKey),
                columnSource3.getLong(rowKey)
        );
    }

    @Override
    public final CharDoubleLongTuple createPreviousTuple(final long rowKey) {
        return new CharDoubleLongTuple(
                columnSource1.getPrevChar(rowKey),
                columnSource2.getPrevDouble(rowKey),
                columnSource3.getPrevLong(rowKey)
        );
    }

    @Override
    public final CharDoubleLongTuple createTupleFromValues(@NotNull final Object... values) {
        return new CharDoubleLongTuple(
                TypeUtils.unbox((Character)values[0]),
                TypeUtils.unbox((Double)values[1]),
                DateTimeUtils.epochNanos((Instant)values[2])
        );
    }

    @Override
    public final CharDoubleLongTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new CharDoubleLongTuple(
                TypeUtils.unbox((Character)values[0]),
                TypeUtils.unbox((Double)values[1]),
                TypeUtils.unbox((Long)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final CharDoubleLongTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
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
    public final Object exportElement(@NotNull final CharDoubleLongTuple tuple, int elementIndex) {
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
    public final Object exportElementReinterpreted(@NotNull final CharDoubleLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
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
        WritableObjectChunk<CharDoubleLongTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        CharChunk<? extends Values> chunk1 = chunks[0].asCharChunk();
        DoubleChunk<? extends Values> chunk2 = chunks[1].asDoubleChunk();
        LongChunk<? extends Values> chunk3 = chunks[2].asLongChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new CharDoubleLongTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link CharacterDoubleReinterpretedInstantColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<CharDoubleLongTuple, Character, Double, Long> {

        private Factory() {
        }

        @Override
        public TupleSource<CharDoubleLongTuple> create(
                @NotNull final ColumnSource<Character> columnSource1,
                @NotNull final ColumnSource<Double> columnSource2,
                @NotNull final ColumnSource<Long> columnSource3
        ) {
            return new CharacterDoubleReinterpretedInstantColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
