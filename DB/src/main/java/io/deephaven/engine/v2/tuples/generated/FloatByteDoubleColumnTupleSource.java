package io.deephaven.engine.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.util.tuples.generated.FloatByteDoubleTuple;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.WritableSource;
import io.deephaven.engine.structures.chunk.Attributes;
import io.deephaven.engine.structures.chunk.ByteChunk;
import io.deephaven.engine.structures.chunk.Chunk;
import io.deephaven.engine.structures.chunk.DoubleChunk;
import io.deephaven.engine.structures.chunk.FloatChunk;
import io.deephaven.engine.structures.chunk.WritableChunk;
import io.deephaven.engine.structures.chunk.WritableObjectChunk;
import io.deephaven.engine.v2.tuples.AbstractTupleSource;
import io.deephaven.engine.v2.tuples.ThreeColumnTupleSourceFactory;
import io.deephaven.engine.v2.tuples.TupleSource;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Float, Byte, and Double.
 * <p>Generated by {@link io.deephaven.engine.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class FloatByteDoubleColumnTupleSource extends AbstractTupleSource<FloatByteDoubleTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link FloatByteDoubleColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<FloatByteDoubleTuple, Float, Byte, Double> FACTORY = new Factory();

    private final ColumnSource<Float> columnSource1;
    private final ColumnSource<Byte> columnSource2;
    private final ColumnSource<Double> columnSource3;

    public FloatByteDoubleColumnTupleSource(
            @NotNull final ColumnSource<Float> columnSource1,
            @NotNull final ColumnSource<Byte> columnSource2,
            @NotNull final ColumnSource<Double> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final FloatByteDoubleTuple createTuple(final long indexKey) {
        return new FloatByteDoubleTuple(
                columnSource1.getFloat(indexKey),
                columnSource2.getByte(indexKey),
                columnSource3.getDouble(indexKey)
        );
    }

    @Override
    public final FloatByteDoubleTuple createPreviousTuple(final long indexKey) {
        return new FloatByteDoubleTuple(
                columnSource1.getPrevFloat(indexKey),
                columnSource2.getPrevByte(indexKey),
                columnSource3.getPrevDouble(indexKey)
        );
    }

    @Override
    public final FloatByteDoubleTuple createTupleFromValues(@NotNull final Object... values) {
        return new FloatByteDoubleTuple(
                TypeUtils.unbox((Float)values[0]),
                TypeUtils.unbox((Byte)values[1]),
                TypeUtils.unbox((Double)values[2])
        );
    }

    @Override
    public final FloatByteDoubleTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new FloatByteDoubleTuple(
                TypeUtils.unbox((Float)values[0]),
                TypeUtils.unbox((Byte)values[1]),
                TypeUtils.unbox((Double)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final FloatByteDoubleTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
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
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final FloatByteDoubleTuple tuple) {
        return new SmartKey(
                TypeUtils.box(tuple.getFirstElement()),
                TypeUtils.box(tuple.getSecondElement()),
                TypeUtils.box(tuple.getThirdElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final FloatByteDoubleTuple tuple, int elementIndex) {
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
    public final Object exportElementReinterpreted(@NotNull final FloatByteDoubleTuple tuple, int elementIndex) {
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
        WritableObjectChunk<FloatByteDoubleTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        FloatChunk<Attributes.Values> chunk1 = chunks[0].asFloatChunk();
        ByteChunk<Attributes.Values> chunk2 = chunks[1].asByteChunk();
        DoubleChunk<Attributes.Values> chunk3 = chunks[2].asDoubleChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new FloatByteDoubleTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link FloatByteDoubleColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<FloatByteDoubleTuple, Float, Byte, Double> {

        private Factory() {
        }

        @Override
        public TupleSource<FloatByteDoubleTuple> create(
                @NotNull final ColumnSource<Float> columnSource1,
                @NotNull final ColumnSource<Byte> columnSource2,
                @NotNull final ColumnSource<Double> columnSource3
        ) {
            return new FloatByteDoubleColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
