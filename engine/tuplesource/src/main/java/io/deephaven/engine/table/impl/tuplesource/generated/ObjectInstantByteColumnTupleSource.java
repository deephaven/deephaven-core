//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TupleSourceCodeGenerator and run "./gradlew replicateTupleSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.ByteChunk;
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
import io.deephaven.time.DateTimeUtils;
import io.deephaven.tuple.generated.ObjectLongByteTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Object, Instant, and Byte.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ObjectInstantByteColumnTupleSource extends AbstractTupleSource<ObjectLongByteTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link ObjectInstantByteColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<ObjectLongByteTuple, Object, Instant, Byte> FACTORY = new Factory();

    private final ColumnSource<Object> columnSource1;
    private final ColumnSource<Instant> columnSource2;
    private final ColumnSource<Byte> columnSource3;

    public ObjectInstantByteColumnTupleSource(
            @NotNull final ColumnSource<Object> columnSource1,
            @NotNull final ColumnSource<Instant> columnSource2,
            @NotNull final ColumnSource<Byte> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final ObjectLongByteTuple createTuple(final long rowKey) {
        return new ObjectLongByteTuple(
                columnSource1.get(rowKey),
                DateTimeUtils.epochNanos(columnSource2.get(rowKey)),
                columnSource3.getByte(rowKey)
        );
    }

    @Override
    public final ObjectLongByteTuple createPreviousTuple(final long rowKey) {
        return new ObjectLongByteTuple(
                columnSource1.getPrev(rowKey),
                DateTimeUtils.epochNanos(columnSource2.getPrev(rowKey)),
                columnSource3.getPrevByte(rowKey)
        );
    }

    @Override
    public final ObjectLongByteTuple createTupleFromValues(@NotNull final Object... values) {
        return new ObjectLongByteTuple(
                values[0],
                DateTimeUtils.epochNanos((Instant)values[1]),
                TypeUtils.unbox((Byte)values[2])
        );
    }

    @Override
    public final ObjectLongByteTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ObjectLongByteTuple(
                values[0],
                DateTimeUtils.epochNanos((Instant)values[1]),
                TypeUtils.unbox((Byte)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ObjectLongByteTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) DateTimeUtils.epochNanosToInstant(tuple.getSecondElement()));
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationRowKey, tuple.getThirdElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final ObjectLongByteTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
        }
        if (elementIndex == 1) {
            return DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ObjectLongByteTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
        }
        if (elementIndex == 1) {
            return DateTimeUtils.epochNanosToInstant(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<ObjectLongByteTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<Object, ? extends Values> chunk1 = chunks[0].asObjectChunk();
        ObjectChunk<Instant, ? extends Values> chunk2 = chunks[1].asObjectChunk();
        ByteChunk<? extends Values> chunk3 = chunks[2].asByteChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ObjectLongByteTuple(chunk1.get(ii), DateTimeUtils.epochNanos(chunk2.get(ii)), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link ObjectInstantByteColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<ObjectLongByteTuple, Object, Instant, Byte> {

        private Factory() {
        }

        @Override
        public TupleSource<ObjectLongByteTuple> create(
                @NotNull final ColumnSource<Object> columnSource1,
                @NotNull final ColumnSource<Instant> columnSource2,
                @NotNull final ColumnSource<Byte> columnSource3
        ) {
            return new ObjectInstantByteColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
