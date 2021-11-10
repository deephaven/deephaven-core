package io.deephaven.engine.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.util.tuples.generated.FloatShortShortTuple;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.WritableSource;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.FloatChunk;
import io.deephaven.engine.chunk.ShortChunk;
import io.deephaven.engine.chunk.WritableChunk;
import io.deephaven.engine.chunk.WritableObjectChunk;
import io.deephaven.engine.v2.tuples.AbstractTupleSource;
import io.deephaven.engine.v2.tuples.ThreeColumnTupleSourceFactory;
import io.deephaven.engine.v2.tuples.TupleSource;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Float, Short, and Short.
 * <p>Generated by {@link io.deephaven.engine.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class FloatShortShortColumnTupleSource extends AbstractTupleSource<FloatShortShortTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link FloatShortShortColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<FloatShortShortTuple, Float, Short, Short> FACTORY = new Factory();

    private final ColumnSource<Float> columnSource1;
    private final ColumnSource<Short> columnSource2;
    private final ColumnSource<Short> columnSource3;

    public FloatShortShortColumnTupleSource(
            @NotNull final ColumnSource<Float> columnSource1,
            @NotNull final ColumnSource<Short> columnSource2,
            @NotNull final ColumnSource<Short> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final FloatShortShortTuple createTuple(final long indexKey) {
        return new FloatShortShortTuple(
                columnSource1.getFloat(indexKey),
                columnSource2.getShort(indexKey),
                columnSource3.getShort(indexKey)
        );
    }

    @Override
    public final FloatShortShortTuple createPreviousTuple(final long indexKey) {
        return new FloatShortShortTuple(
                columnSource1.getPrevFloat(indexKey),
                columnSource2.getPrevShort(indexKey),
                columnSource3.getPrevShort(indexKey)
        );
    }

    @Override
    public final FloatShortShortTuple createTupleFromValues(@NotNull final Object... values) {
        return new FloatShortShortTuple(
                TypeUtils.unbox((Float)values[0]),
                TypeUtils.unbox((Short)values[1]),
                TypeUtils.unbox((Short)values[2])
        );
    }

    @Override
    public final FloatShortShortTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new FloatShortShortTuple(
                TypeUtils.unbox((Float)values[0]),
                TypeUtils.unbox((Short)values[1]),
                TypeUtils.unbox((Short)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final FloatShortShortTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
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
    public final Object exportToExternalKey(@NotNull final FloatShortShortTuple tuple) {
        return new SmartKey(
                TypeUtils.box(tuple.getFirstElement()),
                TypeUtils.box(tuple.getSecondElement()),
                TypeUtils.box(tuple.getThirdElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final FloatShortShortTuple tuple, int elementIndex) {
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
    public final Object exportElementReinterpreted(@NotNull final FloatShortShortTuple tuple, int elementIndex) {
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
        WritableObjectChunk<FloatShortShortTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        FloatChunk<Attributes.Values> chunk1 = chunks[0].asFloatChunk();
        ShortChunk<Attributes.Values> chunk2 = chunks[1].asShortChunk();
        ShortChunk<Attributes.Values> chunk3 = chunks[2].asShortChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new FloatShortShortTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link FloatShortShortColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<FloatShortShortTuple, Float, Short, Short> {

        private Factory() {
        }

        @Override
        public TupleSource<FloatShortShortTuple> create(
                @NotNull final ColumnSource<Float> columnSource1,
                @NotNull final ColumnSource<Short> columnSource2,
                @NotNull final ColumnSource<Short> columnSource3
        ) {
            return new FloatShortShortColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
