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
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.tuple.generated.CharIntFloatTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Character, Integer, and Float.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class CharacterIntegerFloatColumnTupleSource extends AbstractTupleSource<CharIntFloatTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link CharacterIntegerFloatColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<CharIntFloatTuple, Character, Integer, Float> FACTORY = new Factory();

    private final ColumnSource<Character> columnSource1;
    private final ColumnSource<Integer> columnSource2;
    private final ColumnSource<Float> columnSource3;

    public CharacterIntegerFloatColumnTupleSource(
            @NotNull final ColumnSource<Character> columnSource1,
            @NotNull final ColumnSource<Integer> columnSource2,
            @NotNull final ColumnSource<Float> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final CharIntFloatTuple createTuple(final long rowKey) {
        return new CharIntFloatTuple(
                columnSource1.getChar(rowKey),
                columnSource2.getInt(rowKey),
                columnSource3.getFloat(rowKey)
        );
    }

    @Override
    public final CharIntFloatTuple createPreviousTuple(final long rowKey) {
        return new CharIntFloatTuple(
                columnSource1.getPrevChar(rowKey),
                columnSource2.getPrevInt(rowKey),
                columnSource3.getPrevFloat(rowKey)
        );
    }

    @Override
    public final CharIntFloatTuple createTupleFromValues(@NotNull final Object... values) {
        return new CharIntFloatTuple(
                TypeUtils.unbox((Character)values[0]),
                TypeUtils.unbox((Integer)values[1]),
                TypeUtils.unbox((Float)values[2])
        );
    }

    @Override
    public final CharIntFloatTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new CharIntFloatTuple(
                TypeUtils.unbox((Character)values[0]),
                TypeUtils.unbox((Integer)values[1]),
                TypeUtils.unbox((Float)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final CharIntFloatTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
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
    public final Object exportElement(@NotNull final CharIntFloatTuple tuple, int elementIndex) {
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
    public final Object exportElementReinterpreted(@NotNull final CharIntFloatTuple tuple, int elementIndex) {
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
        WritableObjectChunk<CharIntFloatTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        CharChunk<? extends Values> chunk1 = chunks[0].asCharChunk();
        IntChunk<? extends Values> chunk2 = chunks[1].asIntChunk();
        FloatChunk<? extends Values> chunk3 = chunks[2].asFloatChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new CharIntFloatTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link CharacterIntegerFloatColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<CharIntFloatTuple, Character, Integer, Float> {

        private Factory() {
        }

        @Override
        public TupleSource<CharIntFloatTuple> create(
                @NotNull final ColumnSource<Character> columnSource1,
                @NotNull final ColumnSource<Integer> columnSource2,
                @NotNull final ColumnSource<Float> columnSource3
        ) {
            return new CharacterIntegerFloatColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
