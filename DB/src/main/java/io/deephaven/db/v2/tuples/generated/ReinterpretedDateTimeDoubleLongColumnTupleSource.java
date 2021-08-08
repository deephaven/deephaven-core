package io.deephaven.db.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.util.tuples.generated.LongDoubleLongTuple;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.DoubleChunk;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.db.v2.tuples.AbstractTupleSource;
import io.deephaven.db.v2.tuples.ThreeColumnTupleSourceFactory;
import io.deephaven.db.v2.tuples.TupleSource;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Long, Double, and Long.
 * <p>Generated by {@link io.deephaven.db.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ReinterpretedDateTimeDoubleLongColumnTupleSource extends AbstractTupleSource<LongDoubleLongTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link ReinterpretedDateTimeDoubleLongColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<LongDoubleLongTuple, Long, Double, Long> FACTORY = new Factory();

    private final ColumnSource<Long> columnSource1;
    private final ColumnSource<Double> columnSource2;
    private final ColumnSource<Long> columnSource3;

    public ReinterpretedDateTimeDoubleLongColumnTupleSource(
            @NotNull final ColumnSource<Long> columnSource1,
            @NotNull final ColumnSource<Double> columnSource2,
            @NotNull final ColumnSource<Long> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final LongDoubleLongTuple createTuple(final long indexKey) {
        return new LongDoubleLongTuple(
                columnSource1.getLong(indexKey),
                columnSource2.getDouble(indexKey),
                columnSource3.getLong(indexKey)
        );
    }

    @Override
    public final LongDoubleLongTuple createPreviousTuple(final long indexKey) {
        return new LongDoubleLongTuple(
                columnSource1.getPrevLong(indexKey),
                columnSource2.getPrevDouble(indexKey),
                columnSource3.getPrevLong(indexKey)
        );
    }

    @Override
    public final LongDoubleLongTuple createTupleFromValues(@NotNull final Object... values) {
        return new LongDoubleLongTuple(
                DBTimeUtils.nanos((DBDateTime)values[0]),
                TypeUtils.unbox((Double)values[1]),
                TypeUtils.unbox((Long)values[2])
        );
    }

    @Override
    public final LongDoubleLongTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new LongDoubleLongTuple(
                TypeUtils.unbox((Long)values[0]),
                TypeUtils.unbox((Double)values[1]),
                TypeUtils.unbox((Long)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final LongDoubleLongTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) DBTimeUtils.nanosToTime(tuple.getFirstElement()));
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
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final LongDoubleLongTuple tuple) {
        return new SmartKey(
                DBTimeUtils.nanosToTime(tuple.getFirstElement()),
                TypeUtils.box(tuple.getSecondElement()),
                TypeUtils.box(tuple.getThirdElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final LongDoubleLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return DBTimeUtils.nanosToTime(tuple.getFirstElement());
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
    public final Object exportElementReinterpreted(@NotNull final LongDoubleLongTuple tuple, int elementIndex) {
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
        WritableObjectChunk<LongDoubleLongTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        LongChunk<Attributes.Values> chunk1 = chunks[0].asLongChunk();
        DoubleChunk<Attributes.Values> chunk2 = chunks[1].asDoubleChunk();
        LongChunk<Attributes.Values> chunk3 = chunks[2].asLongChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new LongDoubleLongTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link ReinterpretedDateTimeDoubleLongColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<LongDoubleLongTuple, Long, Double, Long> {

        private Factory() {
        }

        @Override
        public TupleSource<LongDoubleLongTuple> create(
                @NotNull final ColumnSource<Long> columnSource1,
                @NotNull final ColumnSource<Double> columnSource2,
                @NotNull final ColumnSource<Long> columnSource3
        ) {
            return new ReinterpretedDateTimeDoubleLongColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
