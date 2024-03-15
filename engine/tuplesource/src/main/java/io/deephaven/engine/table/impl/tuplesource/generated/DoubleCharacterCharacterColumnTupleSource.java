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
import io.deephaven.engine.table.impl.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.tuple.generated.DoubleCharCharTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Double, Character, and Character.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class DoubleCharacterCharacterColumnTupleSource extends AbstractTupleSource<DoubleCharCharTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link DoubleCharacterCharacterColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<DoubleCharCharTuple, Double, Character, Character> FACTORY = new Factory();

    private final ColumnSource<Double> columnSource1;
    private final ColumnSource<Character> columnSource2;
    private final ColumnSource<Character> columnSource3;

    public DoubleCharacterCharacterColumnTupleSource(
            @NotNull final ColumnSource<Double> columnSource1,
            @NotNull final ColumnSource<Character> columnSource2,
            @NotNull final ColumnSource<Character> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final DoubleCharCharTuple createTuple(final long rowKey) {
        return new DoubleCharCharTuple(
                columnSource1.getDouble(rowKey),
                columnSource2.getChar(rowKey),
                columnSource3.getChar(rowKey)
        );
    }

    @Override
    public final DoubleCharCharTuple createPreviousTuple(final long rowKey) {
        return new DoubleCharCharTuple(
                columnSource1.getPrevDouble(rowKey),
                columnSource2.getPrevChar(rowKey),
                columnSource3.getPrevChar(rowKey)
        );
    }

    @Override
    public final DoubleCharCharTuple createTupleFromValues(@NotNull final Object... values) {
        return new DoubleCharCharTuple(
                TypeUtils.unbox((Double)values[0]),
                TypeUtils.unbox((Character)values[1]),
                TypeUtils.unbox((Character)values[2])
        );
    }

    @Override
    public final DoubleCharCharTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new DoubleCharCharTuple(
                TypeUtils.unbox((Double)values[0]),
                TypeUtils.unbox((Character)values[1]),
                TypeUtils.unbox((Character)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final DoubleCharCharTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, tuple.getFirstElement());
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
    public final Object exportElement(@NotNull final DoubleCharCharTuple tuple, int elementIndex) {
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
    public final Object exportElementReinterpreted(@NotNull final DoubleCharCharTuple tuple, int elementIndex) {
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
        WritableObjectChunk<DoubleCharCharTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        DoubleChunk<? extends Values> chunk1 = chunks[0].asDoubleChunk();
        CharChunk<? extends Values> chunk2 = chunks[1].asCharChunk();
        CharChunk<? extends Values> chunk3 = chunks[2].asCharChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new DoubleCharCharTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link DoubleCharacterCharacterColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<DoubleCharCharTuple, Double, Character, Character> {

        private Factory() {
        }

        @Override
        public TupleSource<DoubleCharCharTuple> create(
                @NotNull final ColumnSource<Double> columnSource1,
                @NotNull final ColumnSource<Character> columnSource2,
                @NotNull final ColumnSource<Character> columnSource3
        ) {
            return new DoubleCharacterCharacterColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
