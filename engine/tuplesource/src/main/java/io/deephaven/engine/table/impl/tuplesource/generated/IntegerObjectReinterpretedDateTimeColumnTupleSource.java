package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.tuple.generated.IntObjectLongTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Integer, Object, and Long.
 * <p>Generated by io.deephaven.replicators.TupleSourceCodeGenerator.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class IntegerObjectReinterpretedDateTimeColumnTupleSource extends AbstractTupleSource<IntObjectLongTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link IntegerObjectReinterpretedDateTimeColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<IntObjectLongTuple, Integer, Object, Long> FACTORY = new Factory();

    private final ColumnSource<Integer> columnSource1;
    private final ColumnSource<Object> columnSource2;
    private final ColumnSource<Long> columnSource3;

    public IntegerObjectReinterpretedDateTimeColumnTupleSource(
            @NotNull final ColumnSource<Integer> columnSource1,
            @NotNull final ColumnSource<Object> columnSource2,
            @NotNull final ColumnSource<Long> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final IntObjectLongTuple createTuple(final long rowKey) {
        return new IntObjectLongTuple(
                columnSource1.getInt(rowKey),
                columnSource2.get(rowKey),
                columnSource3.getLong(rowKey)
        );
    }

    @Override
    public final IntObjectLongTuple createPreviousTuple(final long rowKey) {
        return new IntObjectLongTuple(
                columnSource1.getPrevInt(rowKey),
                columnSource2.getPrev(rowKey),
                columnSource3.getPrevLong(rowKey)
        );
    }

    @Override
    public final IntObjectLongTuple createTupleFromValues(@NotNull final Object... values) {
        return new IntObjectLongTuple(
                TypeUtils.unbox((Integer)values[0]),
                values[1],
                DateTimeUtils.nanos((DateTime)values[2])
        );
    }

    @Override
    public final IntObjectLongTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new IntObjectLongTuple(
                TypeUtils.unbox((Integer)values[0]),
                values[1],
                TypeUtils.unbox((Long)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final IntObjectLongTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) DateTimeUtils.nanosToTime(tuple.getThirdElement()));
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final IntObjectLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return tuple.getSecondElement();
        }
        if (elementIndex == 2) {
            return DateTimeUtils.nanosToTime(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final IntObjectLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return tuple.getSecondElement();
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<Values> [] chunks) {
        WritableObjectChunk<IntObjectLongTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        IntChunk<Values> chunk1 = chunks[0].asIntChunk();
        ObjectChunk<Object, Values> chunk2 = chunks[1].asObjectChunk();
        LongChunk<Values> chunk3 = chunks[2].asLongChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new IntObjectLongTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link IntegerObjectReinterpretedDateTimeColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<IntObjectLongTuple, Integer, Object, Long> {

        private Factory() {
        }

        @Override
        public TupleSource<IntObjectLongTuple> create(
                @NotNull final ColumnSource<Integer> columnSource1,
                @NotNull final ColumnSource<Object> columnSource2,
                @NotNull final ColumnSource<Long> columnSource3
        ) {
            return new IntegerObjectReinterpretedDateTimeColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
