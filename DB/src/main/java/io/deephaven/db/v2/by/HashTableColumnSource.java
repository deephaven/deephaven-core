package io.deephaven.db.v2.by;

import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ShiftedOrderedKeys;
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
        final ShiftedOrderedKeys overflowShiftedOrderedKeys;
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
            overflowShiftedOrderedKeys = new ShiftedOrderedKeys();
            overflowDestinationSlice = overflowSource.getChunkType().makeResettableWritableChunk();
        }

        @Override
        public void close() {
            mainFillContext.close();
            overflowFillContext.close();
            overflowShiftedOrderedKeys.close();
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
            @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        final HashTableFillContext typedContext = (HashTableFillContext) context;
        if (!isOverflowLocation(orderedKeys.lastKey())) {
            // Overflow locations are always after main locations, so there are no responsive overflow locations
            mainSource.fillChunk(typedContext.mainFillContext, destination, orderedKeys);
            return;
        }
        if (isOverflowLocation(orderedKeys.firstKey())) {
            // Main locations are always before overflow locations, so there are no responsive main locations
            typedContext.overflowShiftedOrderedKeys.reset(orderedKeys, -MINIMUM_OVERFLOW_HASH_SLOT);
            overflowSource.fillChunk(typedContext.overflowFillContext, destination,
                    typedContext.overflowShiftedOrderedKeys);
            typedContext.overflowShiftedOrderedKeys.clear();
            return;
        }
        // We're going to have to mix main and overflow locations in a single destination chunk, so delegate to fill
        mergedFillChunk(typedContext, destination, orderedKeys);
    }

    private void mergedFillChunk(@NotNull final HashTableFillContext typedContext,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        final int totalSize = orderedKeys.intSize();
        final int firstOverflowChunkPosition;
        try (final OrderedKeys mainOrderedKeysSlice =
                orderedKeys.getOrderedKeysByKeyRange(0, MINIMUM_OVERFLOW_HASH_SLOT - 1)) {
            firstOverflowChunkPosition = mainOrderedKeysSlice.intSize();
            mainSource.fillChunk(typedContext.mainFillContext, destination, mainOrderedKeysSlice);
        }
        final int sizeFromOverflow = totalSize - firstOverflowChunkPosition;

        // Set destination size ahead of time, so that resetting our overflow destination slice doesn't run into bounds
        // issues.
        destination.setSize(totalSize);

        try (final OrderedKeys overflowOrderedKeysSlice =
                orderedKeys.getOrderedKeysByPosition(firstOverflowChunkPosition, sizeFromOverflow)) {
            typedContext.overflowShiftedOrderedKeys.reset(overflowOrderedKeysSlice, -MINIMUM_OVERFLOW_HASH_SLOT);
            overflowSource.fillChunk(typedContext.overflowFillContext,
                    typedContext.overflowDestinationSlice.resetFromChunk(destination, firstOverflowChunkPosition,
                            sizeFromOverflow),
                    typedContext.overflowShiftedOrderedKeys);
        }
        typedContext.overflowDestinationSlice.clear();
        typedContext.overflowShiftedOrderedKeys.clear();
    }

    @Override
    public final void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        final HashTableFillContext typedContext = (HashTableFillContext) context;
        if (!isOverflowLocation(orderedKeys.lastKey())) {
            // Overflow locations are always after main locations, so there are no responsive overflow locations
            mainSource.fillPrevChunk(typedContext.mainFillContext, destination, orderedKeys);
            return;
        }
        if (isOverflowLocation(orderedKeys.firstKey())) {
            // Main locations are always before overflow locations, so there are no responsive main locations
            typedContext.overflowShiftedOrderedKeys.reset(orderedKeys, -MINIMUM_OVERFLOW_HASH_SLOT);
            overflowSource.fillPrevChunk(typedContext.overflowFillContext, destination,
                    typedContext.overflowShiftedOrderedKeys);
            typedContext.overflowShiftedOrderedKeys.clear();
            return;
        }
        // We're going to have to mix main and overflow locations in a single destination chunk, so delegate to fill
        mergedFillPrevChunk(typedContext, destination, orderedKeys);
    }

    private void mergedFillPrevChunk(@NotNull final HashTableFillContext typedContext,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        final int totalSize = orderedKeys.intSize();
        final int firstOverflowChunkPosition;
        try (final OrderedKeys mainOrderedKeysSlice =
                orderedKeys.getOrderedKeysByKeyRange(0, MINIMUM_OVERFLOW_HASH_SLOT - 1)) {
            firstOverflowChunkPosition = mainOrderedKeysSlice.intSize();
            mainSource.fillPrevChunk(typedContext.mainFillContext, destination, mainOrderedKeysSlice);
        }
        final int sizeFromOverflow = totalSize - firstOverflowChunkPosition;

        // Set destination size ahead of time, so that resetting our overflow destination slice doesn't run into bounds
        // issues.
        destination.setSize(totalSize);

        try (final OrderedKeys overflowOrderedKeysSlice =
                orderedKeys.getOrderedKeysByPosition(firstOverflowChunkPosition, sizeFromOverflow)) {
            typedContext.overflowShiftedOrderedKeys.reset(overflowOrderedKeysSlice, -MINIMUM_OVERFLOW_HASH_SLOT);
            overflowSource.fillPrevChunk(typedContext.overflowFillContext,
                    typedContext.overflowDestinationSlice.resetFromChunk(destination, firstOverflowChunkPosition,
                            sizeFromOverflow),
                    typedContext.overflowShiftedOrderedKeys);
            typedContext.overflowDestinationSlice.clear();
            typedContext.overflowShiftedOrderedKeys.clear();
        }
    }

    @Override
    public final Chunk<? extends Values> getChunk(@NotNull final GetContext context,
            @NotNull final OrderedKeys orderedKeys) {
        final HashTableGetContext typedContext = (HashTableGetContext) context;
        if (!isOverflowLocation(orderedKeys.lastKey())) {
            // Overflow locations are always after main locations, so there are no responsive overflow locations
            return mainSource.getChunk(typedContext.mainGetContext, orderedKeys);
        }
        if (isOverflowLocation(orderedKeys.firstKey())) {
            // Main locations are always before overflow locations, so there are no responsive main locations
            typedContext.overflowShiftedOrderedKeys.reset(orderedKeys, -MINIMUM_OVERFLOW_HASH_SLOT);
            return overflowSource.getChunk(typedContext.overflowGetContext, typedContext.overflowShiftedOrderedKeys);
        }
        // We're going to have to mix main and overflow locations in a single destination chunk, so delegate to fill
        mergedFillChunk(typedContext, typedContext.mergeChunk, orderedKeys);
        return typedContext.mergeChunk;
    }

    @Override
    public final Chunk<? extends Values> getPrevChunk(@NotNull final GetContext context,
            @NotNull final OrderedKeys orderedKeys) {
        final HashTableGetContext typedContext = (HashTableGetContext) context;
        if (!isOverflowLocation(orderedKeys.lastKey())) {
            // Overflow locations are always after main locations, so there are no responsive overflow locations
            return mainSource.getPrevChunk(typedContext.mainGetContext, orderedKeys);
        }
        if (isOverflowLocation(orderedKeys.firstKey())) {
            // Main locations are always before overflow locations, so there are no responsive main locations
            typedContext.overflowShiftedOrderedKeys.reset(orderedKeys, -MINIMUM_OVERFLOW_HASH_SLOT);
            return overflowSource.getPrevChunk(typedContext.overflowGetContext,
                    typedContext.overflowShiftedOrderedKeys);
        }
        // We're going to have to mix main and overflow locations in a single destination chunk, so delegate to fill
        mergedFillPrevChunk(typedContext, typedContext.mergeChunk, orderedKeys);
        return typedContext.mergeChunk;
    }

    @Override
    public final DATA_TYPE get(final long index) {
        return isOverflowLocation(index) ? overflowSource.get(hashLocationToOverflowLocation(index))
                : mainSource.get(index);
    }

    @Override
    public final Boolean getBoolean(final long index) {
        return isOverflowLocation(index) ? overflowSource.getBoolean(hashLocationToOverflowLocation(index))
                : mainSource.getBoolean(index);
    }

    @Override
    public final byte getByte(final long index) {
        return isOverflowLocation(index) ? overflowSource.getByte(hashLocationToOverflowLocation(index))
                : mainSource.getByte(index);
    }

    @Override
    public final char getChar(final long index) {
        return isOverflowLocation(index) ? overflowSource.getChar(hashLocationToOverflowLocation(index))
                : mainSource.getChar(index);
    }

    @Override
    public final double getDouble(final long index) {
        return isOverflowLocation(index) ? overflowSource.getDouble(hashLocationToOverflowLocation(index))
                : mainSource.getDouble(index);
    }

    @Override
    public final float getFloat(final long index) {
        return isOverflowLocation(index) ? overflowSource.getFloat(hashLocationToOverflowLocation(index))
                : mainSource.getFloat(index);
    }

    @Override
    public final int getInt(final long index) {
        return isOverflowLocation(index) ? overflowSource.getInt(hashLocationToOverflowLocation(index))
                : mainSource.getInt(index);
    }

    @Override
    public final long getLong(final long index) {
        return isOverflowLocation(index) ? overflowSource.getLong(hashLocationToOverflowLocation(index))
                : mainSource.getLong(index);
    }

    @Override
    public final short getShort(final long index) {
        return isOverflowLocation(index) ? overflowSource.getShort(hashLocationToOverflowLocation(index))
                : mainSource.getShort(index);
    }

    @Override
    public final DATA_TYPE getPrev(final long index) {
        return isOverflowLocation(index) ? overflowSource.getPrev(hashLocationToOverflowLocation(index))
                : mainSource.getPrev(index);
    }

    @Override
    public final Boolean getPrevBoolean(final long index) {
        return isOverflowLocation(index) ? overflowSource.getPrevBoolean(hashLocationToOverflowLocation(index))
                : mainSource.getPrevBoolean(index);
    }

    @Override
    public final byte getPrevByte(final long index) {
        return isOverflowLocation(index) ? overflowSource.getPrevByte(hashLocationToOverflowLocation(index))
                : mainSource.getPrevByte(index);
    }

    @Override
    public final char getPrevChar(final long index) {
        return isOverflowLocation(index) ? overflowSource.getPrevChar(hashLocationToOverflowLocation(index))
                : mainSource.getPrevChar(index);
    }

    @Override
    public final double getPrevDouble(final long index) {
        return isOverflowLocation(index) ? overflowSource.getPrevDouble(hashLocationToOverflowLocation(index))
                : mainSource.getPrevDouble(index);
    }

    @Override
    public final float getPrevFloat(final long index) {
        return isOverflowLocation(index) ? overflowSource.getPrevFloat(hashLocationToOverflowLocation(index))
                : mainSource.getPrevFloat(index);
    }

    @Override
    public final int getPrevInt(final long index) {
        return isOverflowLocation(index) ? overflowSource.getPrevInt(hashLocationToOverflowLocation(index))
                : mainSource.getPrevInt(index);
    }

    @Override
    public final long getPrevLong(final long index) {
        return isOverflowLocation(index) ? overflowSource.getPrevLong(hashLocationToOverflowLocation(index))
                : mainSource.getPrevLong(index);
    }

    @Override
    public final short getPrevShort(final long index) {
        return isOverflowLocation(index) ? overflowSource.getPrevShort(hashLocationToOverflowLocation(index))
                : mainSource.getPrevShort(index);
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
