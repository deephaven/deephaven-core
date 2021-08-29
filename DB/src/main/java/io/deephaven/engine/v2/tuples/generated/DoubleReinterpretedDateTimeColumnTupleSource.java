package io.deephaven.engine.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.tables.utils.DBTimeUtils;
import io.deephaven.engine.util.tuples.generated.DoubleLongTuple;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.WritableSource;
import io.deephaven.engine.structures.chunk.Attributes;
import io.deephaven.engine.structures.chunk.Chunk;
import io.deephaven.engine.structures.chunk.DoubleChunk;
import io.deephaven.engine.structures.chunk.LongChunk;
import io.deephaven.engine.structures.chunk.WritableChunk;
import io.deephaven.engine.structures.chunk.WritableObjectChunk;
import io.deephaven.engine.v2.tuples.AbstractTupleSource;
import io.deephaven.engine.v2.tuples.TupleSource;
import io.deephaven.engine.v2.tuples.TwoColumnTupleSourceFactory;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Double and Long.
 * <p>Generated by {@link io.deephaven.engine.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class DoubleReinterpretedDateTimeColumnTupleSource extends AbstractTupleSource<DoubleLongTuple> {

    /** {@link TwoColumnTupleSourceFactory} instance to create instances of {@link DoubleReinterpretedDateTimeColumnTupleSource}. **/
    public static final TwoColumnTupleSourceFactory<DoubleLongTuple, Double, Long> FACTORY = new Factory();

    private final ColumnSource<Double> columnSource1;
    private final ColumnSource<Long> columnSource2;

    public DoubleReinterpretedDateTimeColumnTupleSource(
            @NotNull final ColumnSource<Double> columnSource1,
            @NotNull final ColumnSource<Long> columnSource2
    ) {
        super(columnSource1, columnSource2);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
    }

    @Override
    public final DoubleLongTuple createTuple(final long indexKey) {
        return new DoubleLongTuple(
                columnSource1.getDouble(indexKey),
                columnSource2.getLong(indexKey)
        );
    }

    @Override
    public final DoubleLongTuple createPreviousTuple(final long indexKey) {
        return new DoubleLongTuple(
                columnSource1.getPrevDouble(indexKey),
                columnSource2.getPrevLong(indexKey)
        );
    }

    @Override
    public final DoubleLongTuple createTupleFromValues(@NotNull final Object... values) {
        return new DoubleLongTuple(
                TypeUtils.unbox((Double)values[0]),
                DBTimeUtils.nanos((DBDateTime)values[1])
        );
    }

    @Override
    public final DoubleLongTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new DoubleLongTuple(
                TypeUtils.unbox((Double)values[0]),
                TypeUtils.unbox((Long)values[1])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final DoubleLongTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) DBTimeUtils.nanosToTime(tuple.getSecondElement()));
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final DoubleLongTuple tuple) {
        return new SmartKey(
                TypeUtils.box(tuple.getFirstElement()),
                DBTimeUtils.nanosToTime(tuple.getSecondElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final DoubleLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return DBTimeUtils.nanosToTime(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final DoubleLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<DoubleLongTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        DoubleChunk<Attributes.Values> chunk1 = chunks[0].asDoubleChunk();
        LongChunk<Attributes.Values> chunk2 = chunks[1].asLongChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new DoubleLongTuple(chunk1.get(ii), chunk2.get(ii)));
        }
        destination.setSize(chunkSize);
    }

    /** {@link TwoColumnTupleSourceFactory} for instances of {@link DoubleReinterpretedDateTimeColumnTupleSource}. **/
    private static final class Factory implements TwoColumnTupleSourceFactory<DoubleLongTuple, Double, Long> {

        private Factory() {
        }

        @Override
        public TupleSource<DoubleLongTuple> create(
                @NotNull final ColumnSource<Double> columnSource1,
                @NotNull final ColumnSource<Long> columnSource2
        ) {
            return new DoubleReinterpretedDateTimeColumnTupleSource(
                    columnSource1,
                    columnSource2
            );
        }
    }
}
