package io.deephaven.engine.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.util.tuples.generated.ObjectByteTuple;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.WritableSource;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.ByteChunk;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.ObjectChunk;
import io.deephaven.engine.chunk.WritableChunk;
import io.deephaven.engine.chunk.WritableObjectChunk;
import io.deephaven.engine.v2.tuples.AbstractTupleSource;
import io.deephaven.engine.v2.tuples.TupleSource;
import io.deephaven.engine.v2.tuples.TwoColumnTupleSourceFactory;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Object and Byte.
 * <p>Generated by {@link io.deephaven.engine.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ObjectReinterpretedBooleanColumnTupleSource extends AbstractTupleSource<ObjectByteTuple> {

    /** {@link TwoColumnTupleSourceFactory} instance to create instances of {@link ObjectReinterpretedBooleanColumnTupleSource}. **/
    public static final TwoColumnTupleSourceFactory<ObjectByteTuple, Object, Byte> FACTORY = new Factory();

    private final ColumnSource<Object> columnSource1;
    private final ColumnSource<Byte> columnSource2;

    public ObjectReinterpretedBooleanColumnTupleSource(
            @NotNull final ColumnSource<Object> columnSource1,
            @NotNull final ColumnSource<Byte> columnSource2
    ) {
        super(columnSource1, columnSource2);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
    }

    @Override
    public final ObjectByteTuple createTuple(final long indexKey) {
        return new ObjectByteTuple(
                columnSource1.get(indexKey),
                columnSource2.getByte(indexKey)
        );
    }

    @Override
    public final ObjectByteTuple createPreviousTuple(final long indexKey) {
        return new ObjectByteTuple(
                columnSource1.getPrev(indexKey),
                columnSource2.getPrevByte(indexKey)
        );
    }

    @Override
    public final ObjectByteTuple createTupleFromValues(@NotNull final Object... values) {
        return new ObjectByteTuple(
                values[0],
                BooleanUtils.booleanAsByte((Boolean)values[1])
        );
    }

    @Override
    public final ObjectByteTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ObjectByteTuple(
                values[0],
                TypeUtils.unbox((Byte)values[1])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ObjectByteTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) BooleanUtils.byteAsBoolean(tuple.getSecondElement()));
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element rowSet " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final ObjectByteTuple tuple) {
        return new SmartKey(
                tuple.getFirstElement(),
                BooleanUtils.byteAsBoolean(tuple.getSecondElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final ObjectByteTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
        }
        if (elementIndex == 1) {
            return BooleanUtils.byteAsBoolean(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ObjectByteTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<ObjectByteTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<Object, Attributes.Values> chunk1 = chunks[0].asObjectChunk();
        ByteChunk<Attributes.Values> chunk2 = chunks[1].asByteChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ObjectByteTuple(chunk1.get(ii), chunk2.get(ii)));
        }
        destination.setSize(chunkSize);
    }

    /** {@link TwoColumnTupleSourceFactory} for instances of {@link ObjectReinterpretedBooleanColumnTupleSource}. **/
    private static final class Factory implements TwoColumnTupleSourceFactory<ObjectByteTuple, Object, Byte> {

        private Factory() {
        }

        @Override
        public TupleSource<ObjectByteTuple> create(
                @NotNull final ColumnSource<Object> columnSource1,
                @NotNull final ColumnSource<Byte> columnSource2
        ) {
            return new ObjectReinterpretedBooleanColumnTupleSource(
                    columnSource1,
                    columnSource2
            );
        }
    }
}
