package io.deephaven.engine.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.util.tuples.generated.FloatCharLongTuple;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.WritableSource;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.CharChunk;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.FloatChunk;
import io.deephaven.engine.chunk.LongChunk;
import io.deephaven.engine.chunk.WritableChunk;
import io.deephaven.engine.chunk.WritableObjectChunk;
import io.deephaven.engine.v2.tuples.AbstractTupleSource;
import io.deephaven.engine.v2.tuples.ThreeColumnTupleSourceFactory;
import io.deephaven.engine.v2.tuples.TupleSource;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Float, Character, and Long.
 * <p>Generated by {@link io.deephaven.engine.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class FloatCharacterLongColumnTupleSource extends AbstractTupleSource<FloatCharLongTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link FloatCharacterLongColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<FloatCharLongTuple, Float, Character, Long> FACTORY = new Factory();

    private final ColumnSource<Float> columnSource1;
    private final ColumnSource<Character> columnSource2;
    private final ColumnSource<Long> columnSource3;

    public FloatCharacterLongColumnTupleSource(
            @NotNull final ColumnSource<Float> columnSource1,
            @NotNull final ColumnSource<Character> columnSource2,
            @NotNull final ColumnSource<Long> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final FloatCharLongTuple createTuple(final long indexKey) {
        return new FloatCharLongTuple(
                columnSource1.getFloat(indexKey),
                columnSource2.getChar(indexKey),
                columnSource3.getLong(indexKey)
        );
    }

    @Override
    public final FloatCharLongTuple createPreviousTuple(final long indexKey) {
        return new FloatCharLongTuple(
                columnSource1.getPrevFloat(indexKey),
                columnSource2.getPrevChar(indexKey),
                columnSource3.getPrevLong(indexKey)
        );
    }

    @Override
    public final FloatCharLongTuple createTupleFromValues(@NotNull final Object... values) {
        return new FloatCharLongTuple(
                TypeUtils.unbox((Float)values[0]),
                TypeUtils.unbox((Character)values[1]),
                TypeUtils.unbox((Long)values[2])
        );
    }

    @Override
    public final FloatCharLongTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new FloatCharLongTuple(
                TypeUtils.unbox((Float)values[0]),
                TypeUtils.unbox((Character)values[1]),
                TypeUtils.unbox((Long)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final FloatCharLongTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationIndexKey, tuple.getThirdElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element rowSet " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final FloatCharLongTuple tuple) {
        return new SmartKey(
                TypeUtils.box(tuple.getFirstElement()),
                TypeUtils.box(tuple.getSecondElement()),
                TypeUtils.box(tuple.getThirdElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final FloatCharLongTuple tuple, int elementIndex) {
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
    public final Object exportElementReinterpreted(@NotNull final FloatCharLongTuple tuple, int elementIndex) {
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
    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<FloatCharLongTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        FloatChunk<Attributes.Values> chunk1 = chunks[0].asFloatChunk();
        CharChunk<Attributes.Values> chunk2 = chunks[1].asCharChunk();
        LongChunk<Attributes.Values> chunk3 = chunks[2].asLongChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new FloatCharLongTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link FloatCharacterLongColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<FloatCharLongTuple, Float, Character, Long> {

        private Factory() {
        }

        @Override
        public TupleSource<FloatCharLongTuple> create(
                @NotNull final ColumnSource<Float> columnSource1,
                @NotNull final ColumnSource<Character> columnSource2,
                @NotNull final ColumnSource<Long> columnSource3
        ) {
            return new FloatCharacterLongColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
