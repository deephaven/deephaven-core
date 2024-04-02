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
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.tuple.generated.CharFloatFloatTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Character, Float, and Float.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class CharacterFloatFloatColumnTupleSource extends AbstractTupleSource<CharFloatFloatTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link CharacterFloatFloatColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<CharFloatFloatTuple, Character, Float, Float> FACTORY = new Factory();

    private final ColumnSource<Character> columnSource1;
    private final ColumnSource<Float> columnSource2;
    private final ColumnSource<Float> columnSource3;

    public CharacterFloatFloatColumnTupleSource(
            @NotNull final ColumnSource<Character> columnSource1,
            @NotNull final ColumnSource<Float> columnSource2,
            @NotNull final ColumnSource<Float> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final CharFloatFloatTuple createTuple(final long rowKey) {
        return new CharFloatFloatTuple(
                columnSource1.getChar(rowKey),
                columnSource2.getFloat(rowKey),
                columnSource3.getFloat(rowKey)
        );
    }

    @Override
    public final CharFloatFloatTuple createPreviousTuple(final long rowKey) {
        return new CharFloatFloatTuple(
                columnSource1.getPrevChar(rowKey),
                columnSource2.getPrevFloat(rowKey),
                columnSource3.getPrevFloat(rowKey)
        );
    }

    @Override
    public final CharFloatFloatTuple createTupleFromValues(@NotNull final Object... values) {
        return new CharFloatFloatTuple(
                TypeUtils.unbox((Character)values[0]),
                TypeUtils.unbox((Float)values[1]),
                TypeUtils.unbox((Float)values[2])
        );
    }

    @Override
    public final CharFloatFloatTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new CharFloatFloatTuple(
                TypeUtils.unbox((Character)values[0]),
                TypeUtils.unbox((Float)values[1]),
                TypeUtils.unbox((Float)values[2])
        );
    }

    @Override
    public final int tupleLength() {
        return 3;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final CharFloatFloatTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
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
    public final Object exportElement(@NotNull final CharFloatFloatTuple tuple, int elementIndex) {
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
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final CharFloatFloatTuple tuple) {
        dest[0] = TypeUtils.box(tuple.getFirstElement());
        dest[1] = TypeUtils.box(tuple.getSecondElement());
        dest[2] = TypeUtils.box(tuple.getThirdElement());
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final CharFloatFloatTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = TypeUtils.box(tuple.getFirstElement());
        dest[map[1]] = TypeUtils.box(tuple.getSecondElement());
        dest[map[2]] = TypeUtils.box(tuple.getThirdElement());
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final CharFloatFloatTuple tuple, int elementIndex) {
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
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final CharFloatFloatTuple tuple) {
        dest[0] = TypeUtils.box(tuple.getFirstElement());
        dest[1] = TypeUtils.box(tuple.getSecondElement());
        dest[2] = TypeUtils.box(tuple.getThirdElement());
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final CharFloatFloatTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = TypeUtils.box(tuple.getFirstElement());
        dest[map[1]] = TypeUtils.box(tuple.getSecondElement());
        dest[map[2]] = TypeUtils.box(tuple.getThirdElement());
    }


    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<CharFloatFloatTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        CharChunk<? extends Values> chunk1 = chunks[0].asCharChunk();
        FloatChunk<? extends Values> chunk2 = chunks[1].asFloatChunk();
        FloatChunk<? extends Values> chunk3 = chunks[2].asFloatChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new CharFloatFloatTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link CharacterFloatFloatColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<CharFloatFloatTuple, Character, Float, Float> {

        private Factory() {
        }

        @Override
        public TupleSource<CharFloatFloatTuple> create(
                @NotNull final ColumnSource<Character> columnSource1,
                @NotNull final ColumnSource<Float> columnSource2,
                @NotNull final ColumnSource<Float> columnSource3
        ) {
            return new CharacterFloatFloatColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
