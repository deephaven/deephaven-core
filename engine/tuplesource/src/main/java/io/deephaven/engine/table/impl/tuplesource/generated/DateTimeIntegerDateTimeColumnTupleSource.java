package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
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
import io.deephaven.tuple.generated.LongIntLongTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types DateTime, Integer, and DateTime.
 * <p>Generated by io.deephaven.replicators.TupleSourceCodeGenerator.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class DateTimeIntegerDateTimeColumnTupleSource extends AbstractTupleSource<LongIntLongTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link DateTimeIntegerDateTimeColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<LongIntLongTuple, DateTime, Integer, DateTime> FACTORY = new Factory();

    private final ColumnSource<DateTime> columnSource1;
    private final ColumnSource<Integer> columnSource2;
    private final ColumnSource<DateTime> columnSource3;

    public DateTimeIntegerDateTimeColumnTupleSource(
            @NotNull final ColumnSource<DateTime> columnSource1,
            @NotNull final ColumnSource<Integer> columnSource2,
            @NotNull final ColumnSource<DateTime> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final LongIntLongTuple createTuple(final long rowKey) {
        return new LongIntLongTuple(
                DateTimeUtils.nanos(columnSource1.get(rowKey)),
                columnSource2.getInt(rowKey),
                DateTimeUtils.nanos(columnSource3.get(rowKey))
        );
    }

    @Override
    public final LongIntLongTuple createPreviousTuple(final long rowKey) {
        return new LongIntLongTuple(
                DateTimeUtils.nanos(columnSource1.getPrev(rowKey)),
                columnSource2.getPrevInt(rowKey),
                DateTimeUtils.nanos(columnSource3.getPrev(rowKey))
        );
    }

    @Override
    public final LongIntLongTuple createTupleFromValues(@NotNull final Object... values) {
        return new LongIntLongTuple(
                DateTimeUtils.nanos((DateTime)values[0]),
                TypeUtils.unbox((Integer)values[1]),
                DateTimeUtils.nanos((DateTime)values[2])
        );
    }

    @Override
    public final LongIntLongTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new LongIntLongTuple(
                DateTimeUtils.nanos((DateTime)values[0]),
                TypeUtils.unbox((Integer)values[1]),
                DateTimeUtils.nanos((DateTime)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final LongIntLongTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) DateTimeUtils.nanosToTime(tuple.getFirstElement()));
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) DateTimeUtils.nanosToTime(tuple.getThirdElement()));
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final LongIntLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return DateTimeUtils.nanosToTime(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return DateTimeUtils.nanosToTime(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final LongIntLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return DateTimeUtils.nanosToTime(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return DateTimeUtils.nanosToTime(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<Values> [] chunks) {
        WritableObjectChunk<LongIntLongTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<DateTime, Values> chunk1 = chunks[0].asObjectChunk();
        IntChunk<Values> chunk2 = chunks[1].asIntChunk();
        ObjectChunk<DateTime, Values> chunk3 = chunks[2].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new LongIntLongTuple(DateTimeUtils.nanos(chunk1.get(ii)), chunk2.get(ii), DateTimeUtils.nanos(chunk3.get(ii))));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link DateTimeIntegerDateTimeColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<LongIntLongTuple, DateTime, Integer, DateTime> {

        private Factory() {
        }

        @Override
        public TupleSource<LongIntLongTuple> create(
                @NotNull final ColumnSource<DateTime> columnSource1,
                @NotNull final ColumnSource<Integer> columnSource2,
                @NotNull final ColumnSource<DateTime> columnSource3
        ) {
            return new DateTimeIntegerDateTimeColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
