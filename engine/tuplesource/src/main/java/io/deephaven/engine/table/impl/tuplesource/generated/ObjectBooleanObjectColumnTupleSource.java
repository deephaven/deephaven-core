//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TupleSourceCodeGenerator and run "./gradlew replicateTupleSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.tuple.generated.ObjectByteObjectTuple;
import io.deephaven.util.BooleanUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Object, Boolean, and Object.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ObjectBooleanObjectColumnTupleSource extends AbstractTupleSource<ObjectByteObjectTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link ObjectBooleanObjectColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<ObjectByteObjectTuple, Object, Boolean, Object> FACTORY = new Factory();

    private final ColumnSource<Object> columnSource1;
    private final ColumnSource<Boolean> columnSource2;
    private final ColumnSource<Object> columnSource3;

    public ObjectBooleanObjectColumnTupleSource(
            @NotNull final ColumnSource<Object> columnSource1,
            @NotNull final ColumnSource<Boolean> columnSource2,
            @NotNull final ColumnSource<Object> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final ObjectByteObjectTuple createTuple(final long rowKey) {
        return new ObjectByteObjectTuple(
                columnSource1.get(rowKey),
                BooleanUtils.booleanAsByte(columnSource2.getBoolean(rowKey)),
                columnSource3.get(rowKey)
        );
    }

    @Override
    public final ObjectByteObjectTuple createPreviousTuple(final long rowKey) {
        return new ObjectByteObjectTuple(
                columnSource1.getPrev(rowKey),
                BooleanUtils.booleanAsByte(columnSource2.getPrevBoolean(rowKey)),
                columnSource3.getPrev(rowKey)
        );
    }

    @Override
    public final ObjectByteObjectTuple createTupleFromValues(@NotNull final Object... values) {
        return new ObjectByteObjectTuple(
                values[0],
                BooleanUtils.booleanAsByte((Boolean)values[1]),
                values[2]
        );
    }

    @Override
    public final ObjectByteObjectTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ObjectByteObjectTuple(
                values[0],
                BooleanUtils.booleanAsByte((Boolean)values[1]),
                values[2]
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ObjectByteObjectTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) BooleanUtils.byteAsBoolean(tuple.getSecondElement()));
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) tuple.getThirdElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final ObjectByteObjectTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
        }
        if (elementIndex == 1) {
            return BooleanUtils.byteAsBoolean(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return tuple.getThirdElement();
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ObjectByteObjectTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
        }
        if (elementIndex == 1) {
            return BooleanUtils.byteAsBoolean(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return tuple.getThirdElement();
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<ObjectByteObjectTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<Object, ? extends Values> chunk1 = chunks[0].asObjectChunk();
        ObjectChunk<Boolean, ? extends Values> chunk2 = chunks[1].asObjectChunk();
        ObjectChunk<Object, ? extends Values> chunk3 = chunks[2].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ObjectByteObjectTuple(chunk1.get(ii), BooleanUtils.booleanAsByte(chunk2.get(ii)), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link ObjectBooleanObjectColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<ObjectByteObjectTuple, Object, Boolean, Object> {

        private Factory() {
        }

        @Override
        public TupleSource<ObjectByteObjectTuple> create(
                @NotNull final ColumnSource<Object> columnSource1,
                @NotNull final ColumnSource<Boolean> columnSource2,
                @NotNull final ColumnSource<Object> columnSource3
        ) {
            return new ObjectBooleanObjectColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
