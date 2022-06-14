/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.impl.ShiftedRowSequence;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnSource} implementation that delegates to the main and overflow sources for a hash table column.
 */
public class HashTableColumnSource<DATA_TYPE> extends AbstractColumnSource<DATA_TYPE>
        implements ColumnSource<DATA_TYPE> {
    public static final int MINIMUM_OVERFLOW_HASH_SLOT = 1 << 30;

    private final ColumnSource<DATA_TYPE> mainSource;
    private final ColumnSource<DATA_TYPE> overflowSource;

    HashTableColumnSource(@NotNull final Class<DATA_TYPE> dataType,
            @NotNull final ColumnSource<DATA_TYPE> mainSource,
            @NotNull final ColumnSource<DATA_TYPE> overflowSource) {
        super(dataType);
        this.mainSource = mainSource;
        this.overflowSource = overflowSource;
    }

    public HashTableColumnSource(@NotNull final ColumnSource<DATA_TYPE> mainSource,
            @NotNull final ColumnSource<DATA_TYPE> overflowSource) {
        this(mainSource.getType(), mainSource, overflowSource);
    }

    private static class HashTableFillContext implements FillContext {

        final FillContext mainFillContext;
        final FillContext overflowFillContext;
        final ShiftedRowSequence overflowShiftedRowSequence;
        final ResettableWritableChunk<Any> overflowDestinationSlice;

        private HashTableFillContext(@NotNull final ColumnSource<?> mainSource,
                @NotNull final ColumnSource<?> overflowSource,
                final int chunkCapacity,
                final SharedContext sharedContext) {
            // TODO: Implement a proper shareable context to use when combining fills from the main and overflow
            // sources. Current usage is "safe" because sources are only exposed through this wrapper, and all
            // sources at a given level will split their keys the same, but this is not ideal.
            mainFillContext = mainSource.makeFillContext(chunkCapacity, sharedContext);
            overflowFillContext = overflowSource.makeFillContext(chunkCapacity, sharedContext);
            overflowShiftedRowSequence = new ShiftedRowSequence();
            overflowDestinationSlice = overflowSource.getChunkType().makeResettableWritableChunk();
        }

        @Override
        public void close() {
            mainFillContext.close();
            overflowFillContext.close();
            overflowShiftedRowSequence.close();
            overflowDestinationSlice.close();
        }
    }

    private static final class HashTableGetContext extends HashTableFillContext implements GetContext {

        private final GetContext mainGetContext;
        private final GetContext overflowGetContext;
        private final WritableChunk<Values> mergeChunk;

        private HashTableGetContext(@NotNull final ColumnSource<?> mainSource,
                @NotNull final ColumnSource<?> overflowSource,
                final int chunkCapacity,
                final SharedContext sharedContext) {
            super(mainSource, overflowSource, chunkCapacity, sharedContext);
            mainGetContext = mainSource.makeGetContext(chunkCapacity, sharedContext);
            overflowGetContext = overflowSource.makeGetContext(chunkCapacity, sharedContext);
            mergeChunk = mainSource.getChunkType().makeWritableChunk(chunkCapacity);
        }

        @Override
        public final void close() {
            super.close();
            mainGetContext.close();
            overflowGetContext.close();
            mergeChunk.close();
        }
    }

    @Override
    public final FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new HashTableFillContext(mainSource, overflowSource, chunkCapacity, sharedContext);
    }

    @Override
    public final GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new HashTableGetContext(mainSource, overflowSource, chunkCapacity, sharedContext);
    }

    @Override
    public final void fillChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final HashTableFillContext typedContext = (HashTableFillContext) context;
        if (!isOverflowLocation(rowSequence.lastRowKey())) {
            // Overflow locations are always after main locations, so there are no responsive overflow locations
            mainSource.fillChunk(typedContext.mainFillContext, destination, rowSequence);
            return;
        }
        if (isOverflowLocation(rowSequence.firstRowKey())) {
            // Main locations are always before overflow locations, so there are no responsive main locations
            typedContext.overflowShiftedRowSequence.reset(rowSequence, -MINIMUM_OVERFLOW_HASH_SLOT);
            overflowSource.fillChunk(typedContext.overflowFillContext, destination,
                    typedContext.overflowShiftedRowSequence);
            typedContext.overflowShiftedRowSequence.clear();
            return;
        }
        // We're going to have to mix main and overflow locations in a single destination chunk, so delegate to fill
        mergedFillChunk(typedContext, destination, rowSequence);
    }

    private void mergedFillChunk(@NotNull final HashTableFillContext typedContext,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final int totalSize = rowSequence.intSize();
        final int firstOverflowChunkPosition;
        try (final RowSequence mainRowSequenceSlice =
                rowSequence.getRowSequenceByKeyRange(0, MINIMUM_OVERFLOW_HASH_SLOT - 1)) {
            firstOverflowChunkPosition = mainRowSequenceSlice.intSize();
            mainSource.fillChunk(typedContext.mainFillContext, destination, mainRowSequenceSlice);
        }
        final int sizeFromOverflow = totalSize - firstOverflowChunkPosition;

        // Set destination size ahead of time, so that resetting our overflow destination slice doesn't run into bounds
        // issues.
        destination.setSize(totalSize);

        try (final RowSequence overflowRowSequenceSlice =
                rowSequence.getRowSequenceByPosition(firstOverflowChunkPosition, sizeFromOverflow)) {
            typedContext.overflowShiftedRowSequence.reset(overflowRowSequenceSlice, -MINIMUM_OVERFLOW_HASH_SLOT);
            overflowSource.fillChunk(typedContext.overflowFillContext,
                    typedContext.overflowDestinationSlice.resetFromChunk(destination, firstOverflowChunkPosition,
                            sizeFromOverflow),
                    typedContext.overflowShiftedRowSequence);
        }
        typedContext.overflowDestinationSlice.clear();
        typedContext.overflowShiftedRowSequence.clear();
    }

    @Override
    public final void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final HashTableFillContext typedContext = (HashTableFillContext) context;
        if (!isOverflowLocation(rowSequence.lastRowKey())) {
            // Overflow locations are always after main locations, so there are no responsive overflow locations
            mainSource.fillPrevChunk(typedContext.mainFillContext, destination, rowSequence);
            return;
        }
        if (isOverflowLocation(rowSequence.firstRowKey())) {
            // Main locations are always before overflow locations, so there are no responsive main locations
            typedContext.overflowShiftedRowSequence.reset(rowSequence, -MINIMUM_OVERFLOW_HASH_SLOT);
            overflowSource.fillPrevChunk(typedContext.overflowFillContext, destination,
                    typedContext.overflowShiftedRowSequence);
            typedContext.overflowShiftedRowSequence.clear();
            return;
        }
        // We're going to have to mix main and overflow locations in a single destination chunk, so delegate to fill
        mergedFillPrevChunk(typedContext, destination, rowSequence);
    }

    private void mergedFillPrevChunk(@NotNull final HashTableFillContext typedContext,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final int totalSize = rowSequence.intSize();
        final int firstOverflowChunkPosition;
        try (final RowSequence mainRowSequenceSlice =
                rowSequence.getRowSequenceByKeyRange(0, MINIMUM_OVERFLOW_HASH_SLOT - 1)) {
            firstOverflowChunkPosition = mainRowSequenceSlice.intSize();
            mainSource.fillPrevChunk(typedContext.mainFillContext, destination, mainRowSequenceSlice);
        }
        final int sizeFromOverflow = totalSize - firstOverflowChunkPosition;

        // Set destination size ahead of time, so that resetting our overflow destination slice doesn't run into bounds
        // issues.
        destination.setSize(totalSize);

        try (final RowSequence overflowRowSequenceSlice =
                rowSequence.getRowSequenceByPosition(firstOverflowChunkPosition, sizeFromOverflow)) {
            typedContext.overflowShiftedRowSequence.reset(overflowRowSequenceSlice, -MINIMUM_OVERFLOW_HASH_SLOT);
            overflowSource.fillPrevChunk(typedContext.overflowFillContext,
                    typedContext.overflowDestinationSlice.resetFromChunk(destination, firstOverflowChunkPosition,
                            sizeFromOverflow),
                    typedContext.overflowShiftedRowSequence);
            typedContext.overflowDestinationSlice.clear();
            typedContext.overflowShiftedRowSequence.clear();
        }
    }

    @Override
    public final Chunk<? extends Values> getChunk(@NotNull final GetContext context,
            @NotNull final RowSequence rowSequence) {
        final HashTableGetContext typedContext = (HashTableGetContext) context;
        if (!isOverflowLocation(rowSequence.lastRowKey())) {
            // Overflow locations are always after main locations, so there are no responsive overflow locations
            return mainSource.getChunk(typedContext.mainGetContext, rowSequence);
        }
        if (isOverflowLocation(rowSequence.firstRowKey())) {
            // Main locations are always before overflow locations, so there are no responsive main locations
            typedContext.overflowShiftedRowSequence.reset(rowSequence, -MINIMUM_OVERFLOW_HASH_SLOT);
            return overflowSource.getChunk(typedContext.overflowGetContext, typedContext.overflowShiftedRowSequence);
        }
        // We're going to have to mix main and overflow locations in a single destination chunk, so delegate to fill
        mergedFillChunk(typedContext, typedContext.mergeChunk, rowSequence);
        return typedContext.mergeChunk;
    }

    @Override
    public final Chunk<? extends Values> getPrevChunk(@NotNull final GetContext context,
            @NotNull final RowSequence rowSequence) {
        final HashTableGetContext typedContext = (HashTableGetContext) context;
        if (!isOverflowLocation(rowSequence.lastRowKey())) {
            // Overflow locations are always after main locations, so there are no responsive overflow locations
            return mainSource.getPrevChunk(typedContext.mainGetContext, rowSequence);
        }
        if (isOverflowLocation(rowSequence.firstRowKey())) {
            // Main locations are always before overflow locations, so there are no responsive main locations
            typedContext.overflowShiftedRowSequence.reset(rowSequence, -MINIMUM_OVERFLOW_HASH_SLOT);
            return overflowSource.getPrevChunk(typedContext.overflowGetContext,
                    typedContext.overflowShiftedRowSequence);
        }
        // We're going to have to mix main and overflow locations in a single destination chunk, so delegate to fill
        mergedFillPrevChunk(typedContext, typedContext.mergeChunk, rowSequence);
        return typedContext.mergeChunk;
    }

    @Override
    public final DATA_TYPE get(final long rowKey) {
        return isOverflowLocation(rowKey) ? overflowSource.get(hashLocationToOverflowLocation(rowKey))
                : mainSource.get(rowKey);
    }

    @Override
    public final Boolean getBoolean(final long rowKey) {
        return isOverflowLocation(rowKey) ? overflowSource.getBoolean(hashLocationToOverflowLocation(rowKey))
                : mainSource.getBoolean(rowKey);
    }

    @Override
    public final byte getByte(final long rowKey) {
        return isOverflowLocation(rowKey) ? overflowSource.getByte(hashLocationToOverflowLocation(rowKey))
                : mainSource.getByte(rowKey);
    }

    @Override
    public final char getChar(final long rowKey) {
        return isOverflowLocation(rowKey) ? overflowSource.getChar(hashLocationToOverflowLocation(rowKey))
                : mainSource.getChar(rowKey);
    }

    @Override
    public final double getDouble(final long rowKey) {
        return isOverflowLocation(rowKey) ? overflowSource.getDouble(hashLocationToOverflowLocation(rowKey))
                : mainSource.getDouble(rowKey);
    }

    @Override
    public final float getFloat(final long rowKey) {
        return isOverflowLocation(rowKey) ? overflowSource.getFloat(hashLocationToOverflowLocation(rowKey))
                : mainSource.getFloat(rowKey);
    }

    @Override
    public final int getInt(final long rowKey) {
        return isOverflowLocation(rowKey) ? overflowSource.getInt(hashLocationToOverflowLocation(rowKey))
                : mainSource.getInt(rowKey);
    }

    @Override
    public final long getLong(final long rowKey) {
        return isOverflowLocation(rowKey) ? overflowSource.getLong(hashLocationToOverflowLocation(rowKey))
                : mainSource.getLong(rowKey);
    }

    @Override
    public final short getShort(final long rowKey) {
        return isOverflowLocation(rowKey) ? overflowSource.getShort(hashLocationToOverflowLocation(rowKey))
                : mainSource.getShort(rowKey);
    }

    @Override
    public final DATA_TYPE getPrev(final long rowKey) {
        return isOverflowLocation(rowKey) ? overflowSource.getPrev(hashLocationToOverflowLocation(rowKey))
                : mainSource.getPrev(rowKey);
    }

    @Override
    public final Boolean getPrevBoolean(final long rowKey) {
        return isOverflowLocation(rowKey) ? overflowSource.getPrevBoolean(hashLocationToOverflowLocation(rowKey))
                : mainSource.getPrevBoolean(rowKey);
    }

    @Override
    public final byte getPrevByte(final long rowKey) {
        return isOverflowLocation(rowKey) ? overflowSource.getPrevByte(hashLocationToOverflowLocation(rowKey))
                : mainSource.getPrevByte(rowKey);
    }

    @Override
    public final char getPrevChar(final long rowKey) {
        return isOverflowLocation(rowKey) ? overflowSource.getPrevChar(hashLocationToOverflowLocation(rowKey))
                : mainSource.getPrevChar(rowKey);
    }

    @Override
    public final double getPrevDouble(final long rowKey) {
        return isOverflowLocation(rowKey) ? overflowSource.getPrevDouble(hashLocationToOverflowLocation(rowKey))
                : mainSource.getPrevDouble(rowKey);
    }

    @Override
    public final float getPrevFloat(final long rowKey) {
        return isOverflowLocation(rowKey) ? overflowSource.getPrevFloat(hashLocationToOverflowLocation(rowKey))
                : mainSource.getPrevFloat(rowKey);
    }

    @Override
    public final int getPrevInt(final long rowKey) {
        return isOverflowLocation(rowKey) ? overflowSource.getPrevInt(hashLocationToOverflowLocation(rowKey))
                : mainSource.getPrevInt(rowKey);
    }

    @Override
    public final long getPrevLong(final long rowKey) {
        return isOverflowLocation(rowKey) ? overflowSource.getPrevLong(hashLocationToOverflowLocation(rowKey))
                : mainSource.getPrevLong(rowKey);
    }

    @Override
    public final short getPrevShort(final long rowKey) {
        return isOverflowLocation(rowKey) ? overflowSource.getPrevShort(hashLocationToOverflowLocation(rowKey))
                : mainSource.getPrevShort(rowKey);
    }

    @Override
    public final void startTrackingPrevValues() {
        mainSource.startTrackingPrevValues();
        overflowSource.startTrackingPrevValues();
    }

    @Override
    public final boolean isImmutable() {
        return mainSource.isImmutable() && overflowSource.isImmutable();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return mainSource.allowsReinterpret(alternateDataType) && overflowSource.allowsReinterpret(alternateDataType);
    }

    private static final class Reinterpreted<DATA_TYPE> extends HashTableColumnSource<DATA_TYPE> {

        private final HashTableColumnSource<?> original;

        private Reinterpreted(@NotNull final Class<DATA_TYPE> dataType,
                @NotNull final HashTableColumnSource<?> original) {
            super(dataType, original.mainSource.reinterpret(dataType), original.overflowSource.reinterpret(dataType));
            this.original = original;
        }

        @Override
        public final <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
                @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            return original.getType() == alternateDataType;
        }

        @Override
        protected final <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
                @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            // noinspection unchecked
            return (ColumnSource<ALTERNATE_DATA_TYPE>) original;
        }
    }

    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return new Reinterpreted<>(alternateDataType, this);
    }

    public static boolean isOverflowLocation(final long index) {
        return index >= MINIMUM_OVERFLOW_HASH_SLOT;
    }

    public static int hashLocationToOverflowLocation(final int hashSlot) {
        return hashSlot - MINIMUM_OVERFLOW_HASH_SLOT;
    }

    public static int overflowLocationToHashLocation(final int overflowSlot) {
        return overflowSlot + MINIMUM_OVERFLOW_HASH_SLOT;
    }

    private static int hashLocationToOverflowLocation(final long hashSlot) {
        return (int) (hashSlot - MINIMUM_OVERFLOW_HASH_SLOT);
    }
}
