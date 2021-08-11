package io.deephaven.db.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.util.tuples.generated.ShortIntLongTuple;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.IntChunk;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.ShortChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.db.v2.tuples.AbstractTupleSource;
import io.deephaven.db.v2.tuples.ThreeColumnTupleSourceFactory;
import io.deephaven.db.v2.tuples.TupleSource;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Short, Integer, and DBDateTime.
 * <p>Generated by {@link io.deephaven.db.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ShortIntegerDateTimeColumnTupleSource extends AbstractTupleSource<ShortIntLongTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link ShortIntegerDateTimeColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<ShortIntLongTuple, Short, Integer, DBDateTime> FACTORY = new Factory();

    private final ColumnSource<Short> columnSource1;
    private final ColumnSource<Integer> columnSource2;
    private final ColumnSource<DBDateTime> columnSource3;

    public ShortIntegerDateTimeColumnTupleSource(
            @NotNull final ColumnSource<Short> columnSource1,
            @NotNull final ColumnSource<Integer> columnSource2,
            @NotNull final ColumnSource<DBDateTime> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final ShortIntLongTuple createTuple(final long indexKey) {
        return new ShortIntLongTuple(
                columnSource1.getShort(indexKey),
                columnSource2.getInt(indexKey),
                DBTimeUtils.nanos(columnSource3.get(indexKey))
        );
    }

    @Override
    public final ShortIntLongTuple createPreviousTuple(final long indexKey) {
        return new ShortIntLongTuple(
                columnSource1.getPrevShort(indexKey),
                columnSource2.getPrevInt(indexKey),
                DBTimeUtils.nanos(columnSource3.getPrev(indexKey))
        );
    }

    @Override
    public final ShortIntLongTuple createTupleFromValues(@NotNull final Object... values) {
        return new ShortIntLongTuple(
                TypeUtils.unbox((Short)values[0]),
                TypeUtils.unbox((Integer)values[1]),
                DBTimeUtils.nanos((DBDateTime)values[2])
        );
    }

    @Override
    public final ShortIntLongTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ShortIntLongTuple(
                TypeUtils.unbox((Short)values[0]),
                TypeUtils.unbox((Integer)values[1]),
                DBTimeUtils.nanos((DBDateTime)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ShortIntLongTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) DBTimeUtils.nanosToTime(tuple.getThirdElement()));
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final ShortIntLongTuple tuple) {
        return new SmartKey(
                TypeUtils.box(tuple.getFirstElement()),
                TypeUtils.box(tuple.getSecondElement()),
                DBTimeUtils.nanosToTime(tuple.getThirdElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final ShortIntLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return DBTimeUtils.nanosToTime(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ShortIntLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return DBTimeUtils.nanosToTime(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<ShortIntLongTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ShortChunk<Attributes.Values> chunk1 = chunks[0].asShortChunk();
        IntChunk<Attributes.Values> chunk2 = chunks[1].asIntChunk();
        ObjectChunk<DBDateTime, Attributes.Values> chunk3 = chunks[2].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ShortIntLongTuple(chunk1.get(ii), chunk2.get(ii), DBTimeUtils.nanos(chunk3.get(ii))));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link ShortIntegerDateTimeColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<ShortIntLongTuple, Short, Integer, DBDateTime> {

        private Factory() {
        }

        @Override
        public TupleSource<ShortIntLongTuple> create(
                @NotNull final ColumnSource<Short> columnSource1,
                @NotNull final ColumnSource<Integer> columnSource2,
                @NotNull final ColumnSource<DBDateTime> columnSource3
        ) {
            return new ShortIntegerDateTimeColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
