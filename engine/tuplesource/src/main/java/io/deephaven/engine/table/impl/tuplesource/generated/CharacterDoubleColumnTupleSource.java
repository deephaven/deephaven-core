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
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.TwoColumnTupleSourceFactory;
import io.deephaven.tuple.generated.CharDoubleTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Character and Double.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class CharacterDoubleColumnTupleSource extends AbstractTupleSource<CharDoubleTuple> {

    /** {@link TwoColumnTupleSourceFactory} instance to create instances of {@link CharacterDoubleColumnTupleSource}. **/
    public static final TwoColumnTupleSourceFactory<CharDoubleTuple, Character, Double> FACTORY = new Factory();

    private final ColumnSource<Character> columnSource1;
    private final ColumnSource<Double> columnSource2;

    public CharacterDoubleColumnTupleSource(
            @NotNull final ColumnSource<Character> columnSource1,
            @NotNull final ColumnSource<Double> columnSource2
    ) {
        super(columnSource1, columnSource2);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
    }

    @Override
    public final CharDoubleTuple createTuple(final long rowKey) {
        return new CharDoubleTuple(
                columnSource1.getChar(rowKey),
                columnSource2.getDouble(rowKey)
        );
    }

    @Override
    public final CharDoubleTuple createPreviousTuple(final long rowKey) {
        return new CharDoubleTuple(
                columnSource1.getPrevChar(rowKey),
                columnSource2.getPrevDouble(rowKey)
        );
    }

    @Override
    public final CharDoubleTuple createTupleFromValues(@NotNull final Object... values) {
        return new CharDoubleTuple(
                TypeUtils.unbox((Character)values[0]),
                TypeUtils.unbox((Double)values[1])
        );
    }

    @Override
    public final CharDoubleTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new CharDoubleTuple(
                TypeUtils.unbox((Character)values[0]),
                TypeUtils.unbox((Double)values[1])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final CharDoubleTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, tuple.getSecondElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final CharDoubleTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final CharDoubleTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<CharDoubleTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        CharChunk<? extends Values> chunk1 = chunks[0].asCharChunk();
        DoubleChunk<? extends Values> chunk2 = chunks[1].asDoubleChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new CharDoubleTuple(chunk1.get(ii), chunk2.get(ii)));
        }
        destination.setSize(chunkSize);
    }

    /** {@link TwoColumnTupleSourceFactory} for instances of {@link CharacterDoubleColumnTupleSource}. **/
    private static final class Factory implements TwoColumnTupleSourceFactory<CharDoubleTuple, Character, Double> {

        private Factory() {
        }

        @Override
        public TupleSource<CharDoubleTuple> create(
                @NotNull final ColumnSource<Character> columnSource1,
                @NotNull final ColumnSource<Double> columnSource2
        ) {
            return new CharacterDoubleColumnTupleSource(
                    columnSource1,
                    columnSource2
            );
        }
    }
}
