//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TupleSourceCodeGenerator and run "./gradlew replicateTupleSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.tuple.generated.LongObjectShortTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Long, Object, and Short.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class LongObjectShortColumnTupleSource extends AbstractTupleSource<LongObjectShortTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link LongObjectShortColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<LongObjectShortTuple, Long, Object, Short> FACTORY = new Factory();

    private final ColumnSource<Long> columnSource1;
    private final ColumnSource<Object> columnSource2;
    private final ColumnSource<Short> columnSource3;

    public LongObjectShortColumnTupleSource(
            @NotNull final ColumnSource<Long> columnSource1,
            @NotNull final ColumnSource<Object> columnSource2,
            @NotNull final ColumnSource<Short> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final LongObjectShortTuple createTuple(final long rowKey) {
        return new LongObjectShortTuple(
                columnSource1.getLong(rowKey),
                columnSource2.get(rowKey),
                columnSource3.getShort(rowKey)
        );
    }

    @Override
    public final LongObjectShortTuple createPreviousTuple(final long rowKey) {
        return new LongObjectShortTuple(
                columnSource1.getPrevLong(rowKey),
                columnSource2.getPrev(rowKey),
                columnSource3.getPrevShort(rowKey)
        );
    }

    @Override
    public final LongObjectShortTuple createTupleFromValues(@NotNull final Object... values) {
        return new LongObjectShortTuple(
                TypeUtils.unbox((Long)values[0]),
                values[1],
                TypeUtils.unbox((Short)values[2])
        );
    }

    @Override
    public final LongObjectShortTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new LongObjectShortTuple(
                TypeUtils.unbox((Long)values[0]),
                values[1],
                TypeUtils.unbox((Short)values[2])
        );
    }

    @Override
    public final int tupleLength() {
        return 3;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final LongObjectShortTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationRowKey, tuple.getThirdElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final LongObjectShortTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return tuple.getSecondElement();
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final LongObjectShortTuple tuple) {
        dest[0] = TypeUtils.box(tuple.getFirstElement());
        dest[1] = tuple.getSecondElement();
        dest[2] = TypeUtils.box(tuple.getThirdElement());
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final LongObjectShortTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = TypeUtils.box(tuple.getFirstElement());
        dest[map[1]] = tuple.getSecondElement();
        dest[map[2]] = TypeUtils.box(tuple.getThirdElement());
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final LongObjectShortTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return tuple.getSecondElement();
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }
    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final LongObjectShortTuple tuple) {
        dest[0] = TypeUtils.box(tuple.getFirstElement());
        dest[1] = tuple.getSecondElement();
        dest[2] = TypeUtils.box(tuple.getThirdElement());
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final LongObjectShortTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = TypeUtils.box(tuple.getFirstElement());
        dest[map[1]] = tuple.getSecondElement();
        dest[map[2]] = TypeUtils.box(tuple.getThirdElement());
    }


    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<LongObjectShortTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        LongChunk<? extends Values> chunk1 = chunks[0].asLongChunk();
        ObjectChunk<Object, ? extends Values> chunk2 = chunks[1].asObjectChunk();
        ShortChunk<? extends Values> chunk3 = chunks[2].asShortChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new LongObjectShortTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link LongObjectShortColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<LongObjectShortTuple, Long, Object, Short> {

        private Factory() {
        }

        @Override
        public TupleSource<LongObjectShortTuple> create(
                @NotNull final ColumnSource<Long> columnSource1,
                @NotNull final ColumnSource<Object> columnSource2,
                @NotNull final ColumnSource<Short> columnSource3
        ) {
            return new LongObjectShortColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
