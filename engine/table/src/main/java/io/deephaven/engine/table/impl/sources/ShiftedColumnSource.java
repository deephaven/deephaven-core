//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.engine.table.impl.ShiftedColumnOperation;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.*;

/**
 * A {@link ColumnSource} that provides internal shifted redirectionIndex logic to access into an underlying wrapped
 * {@link ColumnSource}. This is used, in a {@link ShiftedColumnOperation}.
 *
 * @param <T>
 */
public class ShiftedColumnSource<T> extends AbstractColumnSource<T>
        implements UngroupableColumnSource, ConvertibleTimeSource, InMemoryColumnSource {
    protected final ColumnSource<T> innerSource;
    protected final TrackingRowSet rowSet;
    protected final long shift;

    public ShiftedColumnSource(
            @NotNull TrackingRowSet rowSet, @NotNull final ColumnSource<T> innerSource, final long shift) {
        super(innerSource.getType());
        this.innerSource = innerSource;
        this.rowSet = rowSet;
        this.shift = shift;
    }

    @Override
    public boolean isInMemory() {
        return innerSource instanceof InMemoryColumnSource && ((InMemoryColumnSource) innerSource).isInMemory();
    }

    long getColumnIndex(long outerKey) {
        long outerPosition = rowSet.find(outerKey);
        long shiftedPosition = outerPosition + shift;
        if (outerPosition < 0 || outerPosition >= rowSet.size()) {
            return RowSequence.NULL_ROW_KEY;
        }
        return rowSet.get(shiftedPosition);
    }

    long getPrevColumnIndex(long outerKey) {
        long outerPosition = rowSet.findPrev(outerKey);
        long shiftedPosition = outerPosition + shift;
        if (outerPosition < 0 || outerPosition >= rowSet.sizePrev()) {
            return RowSequence.NULL_ROW_KEY;
        }
        return rowSet.getPrev(shiftedPosition);
    }

    /**
     * Returns the appropriate Redirected Index for the passed in ordered keys based on current or prev Index.
     *
     * @param usePrev indicates if redirection is needed in pre-shift or post-shift space
     * @param toRedirect keys to Redirect
     * @param nullAtBeginningOrEnd holds the calculated count of null at beginning or end when processing is complete
     * @return returns the appropriate Redirected Index for the passed in ordered keys
     */
    @NotNull
    public RowSet buildRedirectedKeys(
            boolean usePrev,
            @NotNull final RowSequence toRedirect,
            @NotNull final MutableInt nullAtBeginningOrEnd) {
        final RowSet useRowSet = usePrev ? rowSet.copyPrev() : rowSet;
        try (final RowSet ignored = useRowSet != rowSet ? useRowSet : null;
                final RowSet toRedirectKeys = toRedirect.asRowSet();
                final WritableRowSet posIndex = useRowSet.invert(toRedirectKeys)) {
            final long origSize = posIndex.size();
            if (shift < 0) {
                posIndex.removeRange(0, -shift - 1);
                int sz = (int) (origSize - posIndex.size());
                nullAtBeginningOrEnd.setValue(sz);
            } else if (shift > 0) {
                posIndex.removeRange(useRowSet.size() - shift, useRowSet.size());
                int sz = (int) (origSize - posIndex.size());
                nullAtBeginningOrEnd.setValue(-(sz));
            }
            posIndex.shiftInPlace(shift);
            return useRowSet.subSetForPositions(posIndex);
        }
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Nullable
    @Override
    public T get(long index) {
        if (index < 0) {
            return null;
        }
        return innerSource.get(getColumnIndex(index));
    }

    @Nullable
    @Override
    public Boolean getBoolean(long index) {
        if (index < 0) {
            return null;
        }
        return innerSource.getBoolean(getColumnIndex(index));
    }

    @Override
    public byte getByte(long index) {
        if (index < 0) {
            return QueryConstants.NULL_BYTE;
        }
        return innerSource.getByte(getColumnIndex(index));
    }

    @Override
    public char getChar(long index) {
        if (index < 0) {
            return QueryConstants.NULL_CHAR;
        }
        return innerSource.getChar(getColumnIndex(index));
    }

    @Override
    public double getDouble(long index) {
        if (index < 0) {
            return QueryConstants.NULL_DOUBLE;
        }
        return innerSource.getDouble(getColumnIndex(index));
    }

    @Override
    public float getFloat(long index) {
        if (index < 0) {
            return QueryConstants.NULL_FLOAT;
        }
        return innerSource.getFloat(getColumnIndex(index));
    }

    @Override
    public int getInt(long index) {
        if (index < 0) {
            return QueryConstants.NULL_INT;
        }
        return innerSource.getInt(getColumnIndex(index));
    }

    @Override
    public long getLong(long index) {
        if (index < 0) {
            return QueryConstants.NULL_LONG;
        }
        return innerSource.getLong(getColumnIndex(index));
    }

    @Override
    public short getShort(long index) {
        if (index < 0) {
            return QueryConstants.NULL_SHORT;
        }
        return innerSource.getShort(getColumnIndex(index));
    }

    @Nullable
    @Override
    public T getPrev(long index) {
        if (index < 0) {
            return null;
        }
        return innerSource.getPrev(getPrevColumnIndex(index));
    }

    @Nullable
    @Override
    public Boolean getPrevBoolean(long index) {
        if (index < 0) {
            return null;
        }
        return innerSource.getPrevBoolean(getPrevColumnIndex(index));
    }

    @Override
    public byte getPrevByte(long index) {
        if (index < 0) {
            return QueryConstants.NULL_BYTE;
        }
        return innerSource.getPrevByte(getPrevColumnIndex(index));
    }

    @Override
    public char getPrevChar(long index) {
        if (index < 0) {
            return QueryConstants.NULL_CHAR;
        }
        return innerSource.getPrevChar(getPrevColumnIndex(index));
    }

    @Override
    public double getPrevDouble(long index) {
        if (index < 0) {
            return QueryConstants.NULL_DOUBLE;
        }
        return innerSource.getPrevDouble(getPrevColumnIndex(index));
    }

    @Override
    public float getPrevFloat(long index) {
        if (index < 0) {
            return QueryConstants.NULL_FLOAT;
        }
        return innerSource.getPrevFloat(getPrevColumnIndex(index));
    }

    @Override
    public int getPrevInt(long index) {
        if (index < 0) {
            return QueryConstants.NULL_INT;
        }
        return innerSource.getPrevInt(getPrevColumnIndex(index));
    }

    @Override
    public long getPrevLong(long index) {
        if (index < 0) {
            return QueryConstants.NULL_LONG;
        }
        return innerSource.getPrevLong(getPrevColumnIndex(index));
    }

    @Override
    public short getPrevShort(long index) {
        if (index < 0) {
            return QueryConstants.NULL_SHORT;
        }
        return innerSource.getPrevShort(getPrevColumnIndex(index));
    }

    @Override
    public boolean isUngroupable() {
        return innerSource instanceof UngroupableColumnSource && asUngroupableSource().isUngroupable();
    }

    @Override
    public long getUngroupedSize(long groupRowKey) {
        return asUngroupableSource().getUngroupedSize(getColumnIndex(groupRowKey));
    }

    @Override
    public long getUngroupedPrevSize(long groupRowKey) {
        return asUngroupableSource().getUngroupedPrevSize(getPrevColumnIndex(groupRowKey));
    }

    @Override
    public T getUngrouped(long groupRowKey, int offsetInGroup) {
        // noinspection unchecked
        return (T) asUngroupableSource().getUngrouped(getColumnIndex(groupRowKey), offsetInGroup);
    }

    @Override
    public T getUngroupedPrev(long groupRowKey, int offsetInGroup) {
        // noinspection unchecked
        return (T) asUngroupableSource().getUngroupedPrev(getPrevColumnIndex(groupRowKey), offsetInGroup);
    }

    @Override
    public Boolean getUngroupedBoolean(long groupRowKey, int offsetInGroup) {
        return asUngroupableSource().getUngroupedBoolean(getColumnIndex(groupRowKey), offsetInGroup);
    }

    @Override
    public Boolean getUngroupedPrevBoolean(long groupRowKey, int offsetInGroup) {
        return asUngroupableSource().getUngroupedPrevBoolean(getPrevColumnIndex(groupRowKey), offsetInGroup);
    }

    @Override
    public double getUngroupedDouble(long groupRowKey, int offsetInGroup) {
        return asUngroupableSource().getUngroupedDouble(getColumnIndex(groupRowKey), offsetInGroup);
    }

    @Override
    public double getUngroupedPrevDouble(long groupRowKey, int offsetInGroup) {
        return asUngroupableSource().getUngroupedPrevDouble(getPrevColumnIndex(groupRowKey), offsetInGroup);
    }

    @Override
    public float getUngroupedFloat(long groupRowKey, int offsetInGroup) {
        return asUngroupableSource().getUngroupedFloat(getColumnIndex(groupRowKey), offsetInGroup);
    }

    @Override
    public float getUngroupedPrevFloat(long groupRowKey, int offsetInGroup) {
        return asUngroupableSource().getUngroupedPrevFloat(getPrevColumnIndex(groupRowKey), offsetInGroup);
    }

    @Override
    public byte getUngroupedByte(long groupRowKey, int offsetInGroup) {
        return asUngroupableSource().getUngroupedByte(getColumnIndex(groupRowKey), offsetInGroup);
    }

    @Override
    public byte getUngroupedPrevByte(long groupRowKey, int offsetInGroup) {
        return asUngroupableSource().getUngroupedPrevByte(getPrevColumnIndex(groupRowKey), offsetInGroup);
    }

    @Override
    public char getUngroupedChar(long groupRowKey, int offsetInGroup) {
        return asUngroupableSource().getUngroupedChar(getColumnIndex(groupRowKey), offsetInGroup);
    }

    @Override
    public char getUngroupedPrevChar(long groupRowKey, int offsetInGroup) {
        return asUngroupableSource().getUngroupedPrevChar(getPrevColumnIndex(groupRowKey), offsetInGroup);
    }

    @Override
    public short getUngroupedShort(long groupRowKey, int offsetInGroup) {
        return asUngroupableSource().getUngroupedShort(getColumnIndex(groupRowKey), offsetInGroup);
    }

    @Override
    public short getUngroupedPrevShort(long groupRowKey, int offsetInGroup) {
        return asUngroupableSource().getUngroupedPrevShort(getPrevColumnIndex(groupRowKey), offsetInGroup);
    }

    @Override
    public int getUngroupedInt(long groupRowKey, int offsetInGroup) {
        return asUngroupableSource().getUngroupedInt(getColumnIndex(groupRowKey), offsetInGroup);
    }

    @Override
    public int getUngroupedPrevInt(long groupRowKey, int offsetInGroup) {
        return asUngroupableSource().getUngroupedPrevInt(getPrevColumnIndex(groupRowKey), offsetInGroup);
    }

    @Override
    public long getUngroupedLong(long groupRowKey, int offsetInGroup) {
        return asUngroupableSource().getUngroupedLong(getColumnIndex(groupRowKey), offsetInGroup);
    }

    @Override
    public long getUngroupedPrevLong(long groupRowKey, int offsetInGroup) {
        return asUngroupableSource().getUngroupedPrevLong(getPrevColumnIndex(groupRowKey), offsetInGroup);
    }

    private UngroupableColumnSource asUngroupableSource() {
        return (UngroupableColumnSource) innerSource;
    }

    @Override
    public void releaseCachedResources() {
        super.releaseCachedResources();
        innerSource.releaseCachedResources();
    }

    @Override
    public ColumnSource<ZonedDateTime> toZonedDateTime(ZoneId zone) {
        return new ShiftedColumnSource<>(rowSet, ((ConvertibleTimeSource) innerSource).toZonedDateTime(zone), shift);
    }

    @Override
    public ColumnSource<LocalDate> toLocalDate(ZoneId zone) {
        return new ShiftedColumnSource<>(rowSet, ((ConvertibleTimeSource) innerSource).toLocalDate(zone), shift);
    }

    @Override
    public ColumnSource<LocalTime> toLocalTime(ZoneId zone) {
        return new ShiftedColumnSource<>(rowSet, ((ConvertibleTimeSource) innerSource).toLocalTime(zone), shift);
    }

    @Override
    public ColumnSource<Instant> toInstant() {
        return new ShiftedColumnSource<>(rowSet, ((ConvertibleTimeSource) innerSource).toInstant(), shift);
    }

    @Override
    public ColumnSource<Long> toEpochNano() {
        return new ShiftedColumnSource<>(rowSet, ((ConvertibleTimeSource) innerSource).toEpochNano(), shift);
    }

    @Override
    public boolean supportsTimeConversion() {
        return innerSource instanceof ConvertibleTimeSource
                && ((ConvertibleTimeSource) innerSource).supportsTimeConversion();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        if (TypeUtils.getUnboxedTypeIfBoxed(alternateDataType) == byte.class && getType() == Boolean.class) {
            return new ReinterpretToOriginalForBoolean<>(alternateDataType);
        }

        if (innerSource instanceof ConvertibleTimeSource
                && ((ConvertibleTimeSource) innerSource).supportsTimeConversion()) {
            if (alternateDataType == long.class || alternateDataType == Long.class) {
                return (ColumnSource<ALTERNATE_DATA_TYPE>) toEpochNano();
            } else if (alternateDataType == Instant.class) {
                return (ColumnSource<ALTERNATE_DATA_TYPE>) toInstant();
            }
        }

        // noinspection unchecked,rawtypes
        return new ReinterpretToOriginal(alternateDataType);
    }

    private class ReinterpretToOriginal<ALTERNATE_DATA_TYPE> extends ShiftedColumnSource<ALTERNATE_DATA_TYPE> {
        private ReinterpretToOriginal(Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            super(ShiftedColumnSource.this.rowSet,
                    ShiftedColumnSource.this.innerSource.reinterpret(alternateDataType),
                    ShiftedColumnSource.this.shift);
        }

        @Override
        public boolean allowsReinterpret(@SuppressWarnings("rawtypes") @NotNull Class alternateDataType) {
            return alternateDataType == ShiftedColumnSource.this.getType();
        }

        @Override
        protected <ORIGINAL_TYPE> ColumnSource<ORIGINAL_TYPE> doReinterpret(
                @NotNull Class<ORIGINAL_TYPE> alternateDataType) {
            // noinspection unchecked
            return (ColumnSource<ORIGINAL_TYPE>) ShiftedColumnSource.this;
        }
    }

    private class ReinterpretToOriginalForBoolean<ALTERNATE_DATA_TYPE>
            extends ReinterpretToOriginal<ALTERNATE_DATA_TYPE> {
        private ReinterpretToOriginalForBoolean(Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            super(alternateDataType);
        }

        @Override
        public byte getByte(long index) {
            if (index < 0) {
                return BooleanUtils.NULL_BOOLEAN_AS_BYTE;
            }
            return super.getByte(index);
        }

        @Override
        public byte getPrevByte(long index) {
            if (index < 0) {
                return BooleanUtils.NULL_BOOLEAN_AS_BYTE;
            }
            return super.getPrevByte(index);
        }

        @Override
        public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
            return new FillContext(this, chunkCapacity, sharedContext);
        }
    }

    @Override
    public ShiftedColumnSource.FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new ShiftedColumnSource.FillContext(this, chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(
            @NotNull final ColumnSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        doFillChunk(context, destination, rowSequence, false);
    }

    @Override
    public void fillPrevChunk(
            @NotNull final ColumnSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        doFillChunk(context, destination, rowSequence, true);
    }

    private void doFillChunk(
            @NotNull final ColumnSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence,
            final boolean usePrev) {
        final int size = rowSequence.intSize();
        destination.setSize(size);
        if (size <= 0) {
            return;
        }
        final ShiftedColumnSource.FillContext effectiveContext = (ShiftedColumnSource.FillContext) context;

        effectiveContext.shareable.ensureMappedKeysInitialized(this, usePrev, rowSequence);
        effectiveContext.doOrderedFillAscending(innerSource, usePrev, destination);
        destination.setSize(size);
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull ChunkSource.GetContext context, @NotNull RowSequence rowSequence) {
        // noinspection unchecked
        return getChunk((GetContext) context, false, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull ChunkSource.GetContext context,
            @NotNull RowSequence rowSequence) {
        // noinspection unchecked
        return getChunk((GetContext) context, true, rowSequence);
    }

    /**
     * returns the Chunk filled with redirected keys, passes inner getChunk/getPrevChunk if no nulls are present at the
     * beginning or end.
     *
     * @param context the passed in GetContext
     * @param usePrev boolean to indicate is invoked from getPrevChunk or getChunk
     * @param rowSequence the passed in rowSequence
     * @return returns the Chunk filled with redirected keys
     */
    private Chunk<? extends Values> getChunk(
            @NotNull GetContext context, boolean usePrev, @NotNull RowSequence rowSequence) {
        final int size = rowSequence.intSize();

        final FillContext effectiveContext = (FillContext) DefaultGetContext.getFillContext(context);
        effectiveContext.shareable.ensureMappedKeysInitialized(this, usePrev, rowSequence);
        if (effectiveContext.shareable.beginningNullCount == 0 && effectiveContext.shareable.endingNullCount == 0) {
            if (usePrev) {
                return innerSource.getPrevChunk(context.innerGetContext, effectiveContext.shareable.innerRowSequence);
            } else {
                return innerSource.getChunk(context.innerGetContext, effectiveContext.shareable.innerRowSequence);
            }
        }

        final WritableChunk<Values> destination = DefaultGetContext.getWritableChunk(context);
        destination.setSize(size);
        if (size <= 0) {
            return destination;
        }

        effectiveContext.doOrderedFillAscending(innerSource, usePrev, destination);
        destination.setSize(size);

        return destination;
    }

    @Override
    public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
        return new GetContext(chunkCapacity, sharedContext);
    }

    private class GetContext extends DefaultGetContext<Values> {
        private final ColumnSource.GetContext innerGetContext;

        GetContext(int chunkCapacity, SharedContext sharedContext) {
            super(ShiftedColumnSource.this, chunkCapacity, sharedContext);
            // we cannot share the inner context as this shifted column maps to a different set of rows than others
            this.innerGetContext = innerSource.makeGetContext(chunkCapacity, null);
        }

        @Override
        public void close() {
            super.close();
            innerGetContext.close();
        }
    }

    private static class FillContext implements ChunkSource.FillContext {
        private final ShiftedColumnSource.FillContext.Shareable shareable;
        private final ColumnSource.FillContext innerFillContext;
        private final ResettableWritableChunk<? super Values> destinationSlice;

        FillContext(final ShiftedColumnSource<?> cs, final int chunkCapacity, final SharedContext sharedContext) {
            shareable = sharedContext == null
                    ? new ShiftedColumnSource.FillContext.Shareable(false)
                    : sharedContext.getOrCreate(new ShiftedColumnSource.FillContext.SharingKey(cs.rowSet, cs.shift),
                            () -> new ShiftedColumnSource.FillContext.Shareable(true));
            innerFillContext = cs.innerSource.makeFillContext(chunkCapacity, shareable);
            destinationSlice = cs.innerSource.getChunkType().makeResettableWritableChunk();
        }

        @Override
        public void close() {
            innerFillContext.close();
            if (destinationSlice != null) {
                destinationSlice.close();
            }
            if (!shareable.shared) {
                shareable.close();
            }
        }

        private static final class SharingKey implements SharedContext.Key<ShiftedColumnSource.FillContext.Shareable> {
            final RowSet rowSet;
            final long shift;

            private SharingKey(@NotNull final RowSet rowSet, final long shift) {
                this.rowSet = rowSet;
                this.shift = shift;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o)
                    return true;
                if (o == null || getClass() != o.getClass())
                    return false;
                SharingKey that = (SharingKey) o;
                return shift == that.shift && rowSet == that.rowSet;
            }

            @Override
            public int hashCode() {
                return System.identityHashCode(rowSet) + (int) shift;
            }
        }

        private static final class Shareable extends SharedContext {
            private boolean mappedKeysReusable = false;
            private final boolean shared;
            private int beginningNullCount = 0;
            private int endingNullCount = 0;
            private RowSequence innerRowSequence;

            private Shareable(final boolean shared) {
                this.shared = shared;
            }

            private void ensureMappedKeysInitialized(
                    ShiftedColumnSource<?> cs,
                    final boolean usePrev,
                    @NotNull final RowSequence rowSequence) {
                if (mappedKeysReusable) {
                    return;
                }
                if (!shared) {
                    reset();
                }

                final MutableInt nullCountAtBeginningOrEnd = new MutableInt(0);
                innerRowSequence = cs.buildRedirectedKeys(usePrev, rowSequence, nullCountAtBeginningOrEnd);
                if (nullCountAtBeginningOrEnd.intValue() < 0) {
                    beginningNullCount = 0;
                    endingNullCount = -nullCountAtBeginningOrEnd.intValue();
                } else {
                    beginningNullCount = nullCountAtBeginningOrEnd.intValue();
                    endingNullCount = 0;
                }

                mappedKeysReusable = shared;
            }

            @Override
            public void reset() {
                mappedKeysReusable = false;
                beginningNullCount = 0;
                endingNullCount = 0;
                if (innerRowSequence != null) {
                    innerRowSequence.close();
                    innerRowSequence = null;
                }
                super.reset();
            }

            @Override
            public void close() {
                if (innerRowSequence != null) {
                    innerRowSequence.close();
                    innerRowSequence = null;
                }
                super.close();
            }
        }

        private void doOrderedFillAscending(
                @NotNull final ColumnSource<?> innerSource,
                final boolean usePrev,
                @NotNull final WritableChunk<? super Values> destination) {
            final RowSequence innerOk = shareable.innerRowSequence;
            if (shareable.beginningNullCount > 0) {
                destination.fillWithNullValue(0, shareable.beginningNullCount);
                // noinspection unchecked,rawtypes
                destinationSlice.resetFromChunk((WritableChunk) destination, shareable.beginningNullCount,
                        innerOk.intSize());
                if (usePrev) {
                    // noinspection unchecked
                    innerSource.fillPrevChunk(innerFillContext, destinationSlice, innerOk);
                } else {
                    // noinspection unchecked
                    innerSource.fillChunk(innerFillContext, destinationSlice, innerOk);
                }
            } else {
                if (usePrev) {
                    innerSource.fillPrevChunk(innerFillContext, destination, innerOk);
                } else {
                    innerSource.fillChunk(innerFillContext, destination, innerOk);
                }
            }
            if (shareable.endingNullCount > 0) {
                final int startOff = destination.size();
                final int finalSize = startOff + shareable.endingNullCount;
                destination.setSize(finalSize);
                destination.fillWithNullValue(startOff, shareable.endingNullCount);
            }
        }
    }
}
