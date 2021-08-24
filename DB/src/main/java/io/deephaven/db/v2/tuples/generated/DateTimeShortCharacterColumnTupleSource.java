package io.deephaven.db.v2.tuples.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.util.tuples.generated.LongShortCharTuple;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.CharChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;
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
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types DBDateTime, Short, and Character.
 * <p>Generated by {@link io.deephaven.db.v2.tuples.TupleSourceCodeGenerator}.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class DateTimeShortCharacterColumnTupleSource extends AbstractTupleSource<LongShortCharTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link DateTimeShortCharacterColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<LongShortCharTuple, DBDateTime, Short, Character> FACTORY = new Factory();

    private final ColumnSource<DBDateTime> columnSource1;
    private final ColumnSource<Short> columnSource2;
    private final ColumnSource<Character> columnSource3;

    public DateTimeShortCharacterColumnTupleSource(
            @NotNull final ColumnSource<DBDateTime> columnSource1,
            @NotNull final ColumnSource<Short> columnSource2,
            @NotNull final ColumnSource<Character> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final LongShortCharTuple createTuple(final long indexKey) {
        return new LongShortCharTuple(
                DBTimeUtils.nanos(columnSource1.get(indexKey)),
                columnSource2.getShort(indexKey),
                columnSource3.getChar(indexKey)
        );
    }

    @Override
    public final LongShortCharTuple createPreviousTuple(final long indexKey) {
        return new LongShortCharTuple(
                DBTimeUtils.nanos(columnSource1.getPrev(indexKey)),
                columnSource2.getPrevShort(indexKey),
                columnSource3.getPrevChar(indexKey)
        );
    }

    @Override
    public final LongShortCharTuple createTupleFromValues(@NotNull final Object... values) {
        return new LongShortCharTuple(
                DBTimeUtils.nanos((DBDateTime)values[0]),
                TypeUtils.unbox((Short)values[1]),
                TypeUtils.unbox((Character)values[2])
        );
    }

    @Override
    public final LongShortCharTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new LongShortCharTuple(
                DBTimeUtils.nanos((DBDateTime)values[0]),
                TypeUtils.unbox((Short)values[1]),
                TypeUtils.unbox((Character)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final LongShortCharTuple tuple, final int elementIndex, @NotNull final WritableSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
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
    public final Object exportToExternalKey(@NotNull final LongShortCharTuple tuple) {
        return new SmartKey(
                DBTimeUtils.nanosToTime(tuple.getFirstElement()),
                TypeUtils.box(tuple.getSecondElement()),
                TypeUtils.box(tuple.getThirdElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final LongShortCharTuple tuple, int elementIndex) {
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
    public final Object exportElementReinterpreted(@NotNull final LongShortCharTuple tuple, int elementIndex) {
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
    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<LongShortCharTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<DBDateTime, Attributes.Values> chunk1 = chunks[0].asObjectChunk();
        ShortChunk<Attributes.Values> chunk2 = chunks[1].asShortChunk();
        CharChunk<Attributes.Values> chunk3 = chunks[2].asCharChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new LongShortCharTuple(DBTimeUtils.nanos(chunk1.get(ii)), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link DateTimeShortCharacterColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<LongShortCharTuple, DBDateTime, Short, Character> {

        private Factory() {
        }

        @Override
        public TupleSource<LongShortCharTuple> create(
                @NotNull final ColumnSource<DBDateTime> columnSource1,
                @NotNull final ColumnSource<Short> columnSource2,
                @NotNull final ColumnSource<Character> columnSource3
        ) {
            return new DateTimeShortCharacterColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
