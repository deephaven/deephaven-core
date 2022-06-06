package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.tuple.generated.ShortCharLongTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Short, Character, and Long.
 * <p>Generated by io.deephaven.replicators.TupleSourceCodeGenerator.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ShortCharacterLongColumnTupleSource extends AbstractTupleSource<ShortCharLongTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link ShortCharacterLongColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<ShortCharLongTuple, Short, Character, Long> FACTORY = new Factory();

    private final ColumnSource<Short> columnSource1;
    private final ColumnSource<Character> columnSource2;
    private final ColumnSource<Long> columnSource3;

    public ShortCharacterLongColumnTupleSource(
            @NotNull final ColumnSource<Short> columnSource1,
            @NotNull final ColumnSource<Character> columnSource2,
            @NotNull final ColumnSource<Long> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final ShortCharLongTuple createTuple(final long rowKey) {
        return new ShortCharLongTuple(
                columnSource1.getShort(rowKey),
                columnSource2.getChar(rowKey),
                columnSource3.getLong(rowKey)
        );
    }

    @Override
    public final ShortCharLongTuple createPreviousTuple(final long rowKey) {
        return new ShortCharLongTuple(
                columnSource1.getPrevShort(rowKey),
                columnSource2.getPrevChar(rowKey),
                columnSource3.getPrevLong(rowKey)
        );
    }

    @Override
    public final ShortCharLongTuple createTupleFromValues(@NotNull final Object... values) {
        return new ShortCharLongTuple(
                TypeUtils.unbox((Short)values[0]),
                TypeUtils.unbox((Character)values[1]),
                TypeUtils.unbox((Long)values[2])
        );
    }

    @Override
    public final ShortCharLongTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ShortCharLongTuple(
                TypeUtils.unbox((Short)values[0]),
                TypeUtils.unbox((Character)values[1]),
                TypeUtils.unbox((Long)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ShortCharLongTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
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
    public final Object exportElement(@NotNull final ShortCharLongTuple tuple, int elementIndex) {
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
    public final Object exportElementReinterpreted(@NotNull final ShortCharLongTuple tuple, int elementIndex) {
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
    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<Values> [] chunks) {
        WritableObjectChunk<ShortCharLongTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ShortChunk<Values> chunk1 = chunks[0].asShortChunk();
        CharChunk<Values> chunk2 = chunks[1].asCharChunk();
        LongChunk<Values> chunk3 = chunks[2].asLongChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ShortCharLongTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link ShortCharacterLongColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<ShortCharLongTuple, Short, Character, Long> {

        private Factory() {
        }

        @Override
        public TupleSource<ShortCharLongTuple> create(
                @NotNull final ColumnSource<Short> columnSource1,
                @NotNull final ColumnSource<Character> columnSource2,
                @NotNull final ColumnSource<Long> columnSource3
        ) {
            return new ShortCharacterLongColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
