package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.tuple.generated.ShortDoubleByteTuple;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Short, Double, and Boolean.
 * <p>Generated by io.deephaven.replicators.TupleSourceCodeGenerator.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ShortDoubleBooleanColumnTupleSource extends AbstractTupleSource<ShortDoubleByteTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link ShortDoubleBooleanColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<ShortDoubleByteTuple, Short, Double, Boolean> FACTORY = new Factory();

    private final ColumnSource<Short> columnSource1;
    private final ColumnSource<Double> columnSource2;
    private final ColumnSource<Boolean> columnSource3;

    public ShortDoubleBooleanColumnTupleSource(
            @NotNull final ColumnSource<Short> columnSource1,
            @NotNull final ColumnSource<Double> columnSource2,
            @NotNull final ColumnSource<Boolean> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final ShortDoubleByteTuple createTuple(final long rowKey) {
        return new ShortDoubleByteTuple(
                columnSource1.getShort(rowKey),
                columnSource2.getDouble(rowKey),
                BooleanUtils.booleanAsByte(columnSource3.getBoolean(rowKey))
        );
    }

    @Override
    public final ShortDoubleByteTuple createPreviousTuple(final long rowKey) {
        return new ShortDoubleByteTuple(
                columnSource1.getPrevShort(rowKey),
                columnSource2.getPrevDouble(rowKey),
                BooleanUtils.booleanAsByte(columnSource3.getPrevBoolean(rowKey))
        );
    }

    @Override
    public final ShortDoubleByteTuple createTupleFromValues(@NotNull final Object... values) {
        return new ShortDoubleByteTuple(
                TypeUtils.unbox((Short)values[0]),
                TypeUtils.unbox((Double)values[1]),
                BooleanUtils.booleanAsByte((Boolean)values[2])
        );
    }

    @Override
    public final ShortDoubleByteTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ShortDoubleByteTuple(
                TypeUtils.unbox((Short)values[0]),
                TypeUtils.unbox((Double)values[1]),
                BooleanUtils.booleanAsByte((Boolean)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ShortDoubleByteTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) BooleanUtils.byteAsBoolean(tuple.getThirdElement()));
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final ShortDoubleByteTuple tuple) {
        return new SmartKey(
                TypeUtils.box(tuple.getFirstElement()),
                TypeUtils.box(tuple.getSecondElement()),
                BooleanUtils.byteAsBoolean(tuple.getThirdElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final ShortDoubleByteTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return BooleanUtils.byteAsBoolean(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ShortDoubleByteTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return BooleanUtils.byteAsBoolean(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<Values> [] chunks) {
        WritableObjectChunk<ShortDoubleByteTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ShortChunk<Values> chunk1 = chunks[0].asShortChunk();
        DoubleChunk<Values> chunk2 = chunks[1].asDoubleChunk();
        ObjectChunk<Boolean, Values> chunk3 = chunks[2].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ShortDoubleByteTuple(chunk1.get(ii), chunk2.get(ii), BooleanUtils.booleanAsByte(chunk3.get(ii))));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link ShortDoubleBooleanColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<ShortDoubleByteTuple, Short, Double, Boolean> {

        private Factory() {
        }

        @Override
        public TupleSource<ShortDoubleByteTuple> create(
                @NotNull final ColumnSource<Short> columnSource1,
                @NotNull final ColumnSource<Double> columnSource2,
                @NotNull final ColumnSource<Boolean> columnSource3
        ) {
            return new ShortDoubleBooleanColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
