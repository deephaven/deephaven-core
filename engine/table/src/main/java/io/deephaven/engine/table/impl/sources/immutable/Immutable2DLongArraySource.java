/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit Immutable2DCharArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.immutable;

import java.util.function.LongFunction;
import java.util.function.ToLongFunction;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.LocalDate;
import java.time.LocalTime;
import io.deephaven.base.verify.Require;
import java.time.ZoneId;

import io.deephaven.engine.table.ColumnSource;

import io.deephaven.time.DateTime;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.WritableSourceWithPrepareForParallelPopulation;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.*;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import java.util.Arrays;

// region boxing imports
import static io.deephaven.util.QueryConstants.NULL_LONG;
// endregion boxing imports

/**
 * Simple almost flat array source that supports fillFromChunk for initial creation.
 *
 * No previous value tracking is permitted, so this column source is only useful as a flat static source.
 *
 * A two-dimension array single array backs the result, with by default segments of 2^30 elements.  This is so that
 * getChunk calls with contiguous ranges are often able to return a reference to the backing store without an array
 * copy.
 *
 * If your size is smaller than the maximum array size, prefer {@link ImmutableLongArraySource}.
 */
public class Immutable2DLongArraySource extends AbstractDeferredGroupingColumnSource<Long>
        implements ImmutableColumnSourceGetDefaults.ForLong, WritableColumnSource<Long>, FillUnordered<Values>,
        InMemoryColumnSource, ChunkedBackingStoreExposedWritableSource, WritableSourceWithPrepareForParallelPopulation
        , ConvertableTimeSource {
    private static final int DEFAULT_SEGMENT_SHIFT = 30;
    private final long segmentShift;
    private final int segmentMask;

    private long size;
    private long[][] data;


    // region constructor
    public Immutable2DLongArraySource() {
        this(DEFAULT_SEGMENT_SHIFT);
    }

    public Immutable2DLongArraySource(int segmentShift) {
        super(long.class);
        this.segmentShift = segmentShift;
        int segmentSize = 1 << segmentShift;
        segmentMask = segmentSize - 1;
    }
    // endregion constructor

    // region allocateArray
    private static long [][] allocateArray(long size, int segmentSize, boolean nullFilled) {
        final int segments = Math.toIntExact((size + segmentSize - 1) / segmentSize);
        final long [][] data = new long[segments][];
        int segment = 0;
        while (size > 0) {
            final int thisSegmentSize = (int)Math.min(segmentSize, size);
            data[segment] = new long[thisSegmentSize];
            if (nullFilled) {
                Arrays.fill(data[segment], 0, thisSegmentSize, NULL_LONG);
            }
            segment++;
            size -= thisSegmentSize;
        }
        return data;
    }
    // endregion allocateArray

    @Override
    public final long getLong(long rowKey) {
        if (rowKey < 0 || rowKey >= size) {
            return NULL_LONG;
        }

        return getUnsafe(rowKey);
    }

    private int keyToSegment(long rowKey) {
        return (int)(rowKey >> segmentShift);
    }

    private int keyToOffset(long rowKey) {
        return (int)(rowKey & segmentMask);
    }

    public final long getUnsafe(long key) {
        return data[keyToSegment(key)][keyToOffset(key)];
    }

    @Override
    public final void setNull(long key) {
        data[keyToSegment(key)][keyToOffset(key)] = NULL_LONG;
    }

    @Override
    public final void set(long key, long value) {
        data[keyToSegment(key)][keyToOffset(key)] = value;
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFilled) {
        if (data == null) {
            size = capacity;
            data = allocateArray(size, segmentMask + 1, nullFilled);
        }
        if (capacity > size) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public long resetWritableChunkToBackingStore(@NotNull ResettableWritableChunk<?> chunk, long position) {
        final int segment = keyToSegment(position);
        chunk.asResettableWritableLongChunk().resetFromTypedArray((long[])data[segment], 0, data[segment].length);
        return (long)segment << segmentShift;
    }

    @Override
    public long resetWritableChunkToBackingStoreSlice(@NotNull ResettableWritableChunk<?> chunk, long position) {
        final int segment = keyToSegment(position);
        final int offset = keyToOffset(position);
        final int segmentLength = data[segment].length;
        final int capacity = segmentLength - offset;
        chunk.asResettableWritableLongChunk().resetFromTypedArray((long[])data[segment], offset, capacity);
        return capacity;
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() >= ArrayBackedColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillChunkByRanges(destination, rowSequence);
        } else {
            fillChunkByKeys(destination, rowSequence);
        }
    }

    // region fillChunkByRanges
    /* TYPE_MIXIN */ void fillChunkByRanges(
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence
            /* CONVERTER */) {
        // region chunkDecl
        final WritableLongChunk<? super Values> chunk = destination.asWritableLongChunk();
        // endregion chunkDecl
        final MutableInt destPosition = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            while (start < end) {
                final int segment = keyToSegment(start);
                final int offset = keyToOffset(start);
                final long segmentEnd = start | segmentMask;
                final long realEnd = Math.min(segmentEnd, end);
                final int length = (int)(realEnd - start + 1);
                // region copyFromTypedArrayImmutable2D
                chunk.copyFromTypedArray(data[segment], offset, destPosition.getAndAdd(length), length);
                // endregion copyFromTypedArrayImmutable2D
                start += length;
            }
        });
    }
    <R> void fillChunkByRanges(
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence
            , LongFunction<R> converter) {
        // region chunkDecl
        final WritableObjectChunk<R, ? super Values> chunk = destination.asWritableObjectChunk();
        // endregion chunkDecl
        final MutableInt destPosition = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            while (start < end) {
                final int segment = keyToSegment(start);
                final int offset = keyToOffset(start);
                final long segmentEnd = start | segmentMask;
                final long realEnd = Math.min(segmentEnd, end);
                final int length = (int)(realEnd - start + 1);
                // region copyFromTypedArrayImmutable2D
               final int destOffset = destPosition.getAndAdd(length);
               for (int ii = 0; ii < length; ii++) {
                   chunk.set(destOffset + ii, converter.apply(data[segment][offset + ii]));
               }
                // endregion copyFromTypedArrayImmutable2D
                start += length;
            }
        });
    }
    // endregion fillChunkByRanges

    // region fillChunkByKeys
    /* TYPE_MIXIN */ void fillChunkByKeys(
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence
            /* CONVERTER */) {
        // region chunkDecl
        final WritableLongChunk<? super Values> chunk = destination.asWritableLongChunk();
        // endregion chunkDecl
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> {
            // region conversion
            chunk.set(srcPos.getAndIncrement(), getUnsafe(key));
            // endregion conversion
        });
    }
    <R> void fillChunkByKeys(
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence
            , LongFunction<R> converter) {
        // region chunkDecl
        final WritableObjectChunk<R, ? super Values> chunk = destination.asWritableObjectChunk();
        // endregion chunkDecl
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> {
            // region conversion
            chunk.set(srcPos.getAndIncrement(),converter.apply( getUnsafe(key)));
            // endregion conversion
        });
    }
    // endregion fillChunkByKeys

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return LongChunk.getEmptyChunk();
        }
        if (rowSequence.isContiguous()) {
            return getChunk(context, rowSequence.firstRowKey(), rowSequence.lastRowKey());
        }
        return super.getChunk(context, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        final int segment = keyToSegment(firstKey);
        if (segment != keyToSegment(lastKey)) {
            // the super will just go into our getChunk with RowSequence and that can be an infinite loop
            try (final RowSequence rs = RowSequenceFactory.forRange(firstKey, lastKey)) {
                return super.getChunk(context, rs);
            }
        }
        final int len = (int)(lastKey - firstKey + 1);
        final int firstOffset = keyToOffset(firstKey);
        //noinspection unchecked
        DefaultGetContext<? extends Values> context1 = (DefaultGetContext<? extends Values>) context;
        return context1.getResettableChunk().resetFromArray(data[segment], firstOffset, len);
    }

    @Override
    public void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() >= ArrayBackedColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillFromChunkByRanges(src, rowSequence);
        } else {
            fillFromChunkByKeys(src, rowSequence);
        }
    }

    // region fillFromChunkByKeys
    /* TYPE_MIXIN */ void fillFromChunkByKeys(
            @NotNull final Chunk<? extends Values> src,
            @NotNull final RowSequence rowSequence
            /* CONVERTER */) {
        // region chunkDecl
        final LongChunk<? extends Values> chunk = src.asLongChunk();
        // endregion chunkDecl
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> {
            // region conversion
            set(key, chunk.get(srcPos.getAndIncrement()));
            // endregion conversion
        });
    }
    <R> void fillFromChunkByKeys(
            @NotNull final Chunk<? extends Values> src,
            @NotNull final RowSequence rowSequence
            , ToLongFunction<R> converter) {
        // region chunkDecl
        final ObjectChunk<R, ? extends Values> chunk = src.asObjectChunk();
        // endregion chunkDecl
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeys((long key) -> {
            // region conversion
            set(key,converter.applyAsLong( chunk.get(srcPos.getAndIncrement())));
            // endregion conversion
        });
    }
    // endregion fillFromChunkByKeys

    // region fillFromChunkByRanges
    /* TYPE_MIXIN */ void fillFromChunkByRanges(
            @NotNull final Chunk<? extends Values> src,
            @NotNull final RowSequence rowSequence
            /* CONVERTER */) {
        // region chunkDecl
        final LongChunk<? extends Values> chunk = src.asLongChunk();
        // endregion chunkDecl
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            while (start < end) {
                final int segment = keyToSegment(start);
                final int destOffset = keyToOffset(start);
                final long segmentEnd = start | segmentMask;
                final long realEnd = Math.min(segmentEnd, end);
                final int length = (int)(realEnd - start + 1);
                // region copyToTypedArrayImmutable2D
                chunk.copyToTypedArray(srcPos.getAndAdd(length), data[segment], destOffset, length);
                // endregion copyToTypedArrayImmutable2D
                start += length;
            }
        });
    }
    <R> void fillFromChunkByRanges(
            @NotNull final Chunk<? extends Values> src,
            @NotNull final RowSequence rowSequence
            , ToLongFunction<R> converter) {
        // region chunkDecl
        final ObjectChunk<R, ? extends Values> chunk = src.asObjectChunk();
        // endregion chunkDecl
        final MutableInt srcPos = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((long start, long end) -> {
            while (start < end) {
                final int segment = keyToSegment(start);
                final int destOffset = keyToOffset(start);
                final long segmentEnd = start | segmentMask;
                final long realEnd = Math.min(segmentEnd, end);
                final int length = (int)(realEnd - start + 1);
                // region copyToTypedArrayImmutable2D
               final int offset = srcPos.getAndAdd(length);
               for (int jj = 0; jj < length; jj++) {
                   data[segment][destOffset + jj] = converter.applyAsLong(chunk.get(offset + jj));
               }
                // endregion copyToTypedArrayImmutable2D
                start += length;
            }
        });
    }
    // endregion fillFromChunkByRanges

    // region fillFromChunkUnordered
    @Override
    public /* TYPE_MIXIN */ void fillFromChunkUnordered(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src,
            @NotNull final LongChunk<RowKeys> keys
            /* CONVERTER */) {
        // region chunkDecl
        final LongChunk<? extends Values> chunk = src.asLongChunk();
        // endregion chunkDecl
        for (int ii = 0; ii < keys.size(); ++ii) {
            // region conversion
            set(keys.get(ii), chunk.get(ii));
            // endregion conversion
        }
    }
    
    public <R> void fillFromChunkUnordered(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src,
            @NotNull final LongChunk<RowKeys> keys
            , ToLongFunction<R> converter) {
        // region chunkDecl
        final ObjectChunk<R, ? extends Values> chunk = src.asObjectChunk();
        // endregion chunkDecl
        for (int ii = 0; ii < keys.size(); ++ii) {
            // region conversion
            set(keys.get(ii),converter.applyAsLong( chunk.get(ii)));
            // endregion conversion
        }
    }
    // endregion fillFromChunkUnordered

    // region fillChunkUnordered
    @Override
    public /* TYPE_MIXIN */ void fillChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys
            /* CONVERTER */) {
        // region chunkDecl
        final WritableLongChunk<? super Values> chunk = dest.asWritableLongChunk();
        // endregion chunkDecl
        for (int ii = 0; ii < keys.size(); ++ii) {
            final long rowKey = keys.get(ii);
            if (rowKey == RowSequence.NULL_ROW_KEY) {
                chunk.set(ii, NULL_LONG);
            } else {
                // region conversion
                chunk.set(ii, getUnsafe((int)(rowKey)));
                // endregion conversion
            }
        }
    }
    
    public <R> void fillChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys
            , LongFunction<R> converter) {
        // region chunkDecl
        final WritableObjectChunk<R, ? super Values> chunk = dest.asWritableObjectChunk();
        // endregion chunkDecl
        for (int ii = 0; ii < keys.size(); ++ii) {
            final long rowKey = keys.get(ii);
            if (rowKey == RowSequence.NULL_ROW_KEY) {
                chunk.set(ii, null);
            } else {
                // region conversion
                chunk.set(ii,converter.apply( getUnsafe((int)(rowKey))));
                // endregion conversion
            }
        }
    }
    // endregion fillChunkUnordered

    @Override
    public void fillPrevChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest, @NotNull LongChunk<? extends RowKeys> keys) {
        fillChunkUnordered(context, dest, keys);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        fillChunk(context, destination, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return getChunk(context, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        return getChunk(context, firstKey, lastKey);
    }

    @Override
    public boolean providesFillUnordered() {
        return true;
    }

    @Override
    public void prepareForParallelPopulation(RowSequence rowSequence) {
        // nothing to do
    }

    // region reinterpretation
    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == long.class || alternateDataType == Instant.class || alternateDataType == DateTime.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        if (alternateDataType == this.getType()) {
            return (ColumnSource<ALTERNATE_DATA_TYPE>) this;
        } else if(alternateDataType == DateTime.class) {
            return (ColumnSource<ALTERNATE_DATA_TYPE>) toDateTime();
        } else if (alternateDataType == Instant.class) {
            return (ColumnSource<ALTERNATE_DATA_TYPE>) toInstant();
        }

        throw new IllegalArgumentException("Cannot reinterpret `" + getType().getName() + "` to `" + alternateDataType.getName() + "`");
    }

    @Override
    public boolean supportsTimeConversion() {
        return true;
    }

    @Override
    public ColumnSource<ZonedDateTime> toZonedDateTime(final @NotNull ZoneId zone) {
        return new Immutable2DZonedDateTimeArraySource(Require.neqNull(zone, "zone"), this);
    }

    @Override
    public ColumnSource<LocalDate> toLocalDate(final @NotNull ZoneId zone) {
        return new LocalDateWrapperSource(toZonedDateTime(zone), zone);
    }

    @Override
    public ColumnSource<LocalTime> toLocalTime(final @NotNull ZoneId zone) {
        return new LocalTimeWrapperSource(toZonedDateTime(zone), zone);
    }

    @Override
    public ColumnSource<DateTime> toDateTime() {
        return new Immutable2DDateTimeArraySource(this);
    }

    @Override
    public ColumnSource<Instant> toInstant() {
        return new Immutable2DInstantArraySource(this);
    }

    @Override
    public ColumnSource<Long> toEpochNano() {
        return this;
    }
    // endregion reinterpretation
}
