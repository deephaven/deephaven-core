package io.deephaven.db.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.util.tuples.generated.LongDoubleObjectTuple;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.DoubleChunk;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.db.v2.tuples.AbstractTupleSource;
import io.deephaven.db.v2.tuples.ThreeColumnTupleSourceFactory;
import io.deephaven.db.v2.tuples.TupleSource;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types DBDateTime, Double, and Object.
 * <p>Generated by {@link io.deephaven.db.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class DateTimeDoubleObjectColumnTupleSource extends AbstractTupleSource<LongDoubleObjectTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link DateTimeDoubleObjectColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<LongDoubleObjectTuple, DBDateTime, Double, Object> FACTORY = new Factory();

    private final ColumnSource<DBDateTime> columnSource1;
    private final ColumnSource<Double> columnSource2;
    private final ColumnSource<Object> columnSource3;

    public DateTimeDoubleObjectColumnTupleSource(
            @NotNull final ColumnSource<DBDateTime> columnSource1,
            @NotNull final ColumnSource<Double> columnSource2,
            @NotNull final ColumnSource<Object> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final LongDoubleObjectTuple createTuple(final long indexKey) {
        return new LongDoubleObjectTuple(
                DBTimeUtils.nanos(columnSource1.get(indexKey)),
                columnSource2.getDouble(indexKey),
                columnSource3.get(indexKey)
        );
    }

    @Override
    public final LongDoubleObjectTuple createPreviousTuple(final long indexKey) {
        return new LongDoubleObjectTuple(
                DBTimeUtils.nanos(columnSource1.getPrev(indexKey)),
                columnSource2.getPrevDouble(indexKey),
                columnSource3.getPrev(indexKey)
        );
    }

    @Override
    public final LongDoubleObjectTuple createTupleFromValues(@NotNull final Object... values) {
        return new LongDoubleObjectTuple(
                DBTimeUtils.nanos((DBDateTime)values[0]),
                TypeUtils.unbox((Double)values[1]),
                values[2]
        );
    }

    @Override
    public final LongDoubleObjectTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new LongDoubleObjectTuple(
                DBTimeUtils.nanos((DBDateTime)values[0]),
                TypeUtils.unbox((Double)values[1]),
                values[2]
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final LongDoubleObjectTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) DBTimeUtils.nanosToTime(tuple.getFirstElement()));
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) tuple.getThirdElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final LongDoubleObjectTuple tuple) {
        return new SmartKey(
                DBTimeUtils.nanosToTime(tuple.getFirstElement()),
                TypeUtils.box(tuple.getSecondElement()),
                tuple.getThirdElement()
        );
    }

    @Override
    public final Object exportElement(@NotNull final LongDoubleObjectTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return DBTimeUtils.nanosToTime(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return tuple.getThirdElement();
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final LongDoubleObjectTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return DBTimeUtils.nanosToTime(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return tuple.getThirdElement();
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<LongDoubleObjectTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<DBDateTime, Attributes.Values> chunk1 = chunks[0].asObjectChunk();
        DoubleChunk<Attributes.Values> chunk2 = chunks[1].asDoubleChunk();
        ObjectChunk<Object, Attributes.Values> chunk3 = chunks[2].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new LongDoubleObjectTuple(DBTimeUtils.nanos(chunk1.get(ii)), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link DateTimeDoubleObjectColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<LongDoubleObjectTuple, DBDateTime, Double, Object> {

        private Factory() {
        }

        @Override
        public TupleSource<LongDoubleObjectTuple> create(
                @NotNull final ColumnSource<DBDateTime> columnSource1,
                @NotNull final ColumnSource<Double> columnSource2,
                @NotNull final ColumnSource<Object> columnSource3
        ) {
            return new DateTimeDoubleObjectColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
