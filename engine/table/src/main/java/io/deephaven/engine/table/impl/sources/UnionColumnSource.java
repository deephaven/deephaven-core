/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.impl.ShiftedRowSequence;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.engine.util.TableTools;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.stream.Stream;

import static io.deephaven.util.QueryConstants.*;

/**
 * ColumnSource implementation for {@link TableTools#merge} results.
 */
public class UnionColumnSource<T> extends AbstractColumnSource<T> {

    /**
     * Lookup interface for constituent {@link ColumnSource sources}.
     */
    interface ConstituentSourceLookup<T> {

        /**
         * Get the {@link ColumnSource} currently in {@code slot}
         *
         * @param slot The slot to lookup
         * @return The source currently in slot
         */
        ColumnSource<T> slotToCurrSource(int slot);

        /**
         * Get the {@link ColumnSource} previously in {@code slot}
         *
         * @param slot The slot to lookup
         * @return The source previously in slot
         */
        ColumnSource<T> slotToPrevSource(int slot);

        /**
         * Get a stream of all current constituent sources.
         *
         * @return A stream of all current constituent sources, which must be closed.
         */
        Stream<ColumnSource<T>> currSources();
    }

    private final UnionSourceManager unionSourceManager;
    private final UnionRedirection unionRedirection;
    private final ConstituentSourceLookup<T> sourceLookup;

    private Map<Class, ReinterpretReference> reinterpretedSources;

    UnionColumnSource(
            @NotNull final Class<T> type,
            @Nullable final Class<?> componentType,
            @NotNull final UnionSourceManager unionSourceManager,
            @NotNull final UnionRedirection unionRedirection,
            @NotNull final ConstituentSourceLookup<T> sourceLookup) {
        super(type, componentType);
        this.unionSourceManager = unionSourceManager;
        this.unionRedirection = unionRedirection;
        this.sourceLookup = sourceLookup;
    }

    private long getCurrOffset(final long rowKey, final int slot) {
        return rowKey - unionRedirection.currFirstRowKeyForSlot(slot);
    }

    private long getPrevOffset(final long rowKey, final int slot) {
        return rowKey - unionRedirection.prevFirstRowKeyForSlot(slot);
    }

    @Override
    public T get(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        final int slot = unionRedirection.currSlotForRowKey(rowKey);
        final long offset = getCurrOffset(rowKey, slot);
        return sourceLookup.slotToCurrSource(slot).get(offset);
    }

    @Override
    public Boolean getBoolean(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        final int slot = unionRedirection.currSlotForRowKey(rowKey);
        final long offset = getCurrOffset(rowKey, slot);
        return sourceLookup.slotToCurrSource(slot).getBoolean(offset);
    }

    @Override
    public byte getByte(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        final int slot = unionRedirection.currSlotForRowKey(rowKey);
        final long offset = getCurrOffset(rowKey, slot);
        return sourceLookup.slotToCurrSource(slot).getByte(offset);
    }

    @Override
    public char getChar(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        final int slot = unionRedirection.currSlotForRowKey(rowKey);
        final long offset = getCurrOffset(rowKey, slot);
        return sourceLookup.slotToCurrSource(slot).getChar(offset);
    }

    @Override
    public double getDouble(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        final int slot = unionRedirection.currSlotForRowKey(rowKey);
        final long offset = getCurrOffset(rowKey, slot);
        return sourceLookup.slotToCurrSource(slot).getDouble(offset);
    }

    @Override
    public float getFloat(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_FLOAT;
        }
        final int slot = unionRedirection.currSlotForRowKey(rowKey);
        final long offset = getCurrOffset(rowKey, slot);
        return sourceLookup.slotToCurrSource(slot).getFloat(offset);
    }

    @Override
    public int getInt(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_INT;
        }
        final int slot = unionRedirection.currSlotForRowKey(rowKey);
        final long offset = getCurrOffset(rowKey, slot);
        return sourceLookup.slotToCurrSource(slot).getInt(offset);
    }

    @Override
    public long getLong(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_LONG;
        }
        final int slot = unionRedirection.currSlotForRowKey(rowKey);
        final long offset = getCurrOffset(rowKey, slot);
        return sourceLookup.slotToCurrSource(slot).getLong(offset);
    }

    @Override
    public short getShort(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        final int slot = unionRedirection.currSlotForRowKey(rowKey);
        final long offset = getCurrOffset(rowKey, slot);
        return sourceLookup.slotToCurrSource(slot).getShort(offset);
    }

    @Override
    public T getPrev(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        final int slot = unionRedirection.prevSlotForRowKey(rowKey);
        final long offset = getPrevOffset(rowKey, slot);
        return sourceLookup.slotToPrevSource(slot).getPrev(offset);
    }

    @Override
    public Boolean getPrevBoolean(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        final int slot = unionRedirection.prevSlotForRowKey(rowKey);
        final long offset = getPrevOffset(rowKey, slot);
        return sourceLookup.slotToPrevSource(slot).getPrevBoolean(offset);
    }

    @Override
    public byte getPrevByte(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        final int slot = unionRedirection.prevSlotForRowKey(rowKey);
        final long offset = getPrevOffset(rowKey, slot);
        return sourceLookup.slotToPrevSource(slot).getPrevByte(offset);
    }

    @Override
    public char getPrevChar(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        final int slot = unionRedirection.prevSlotForRowKey(rowKey);
        final long offset = getPrevOffset(rowKey, slot);
        return sourceLookup.slotToPrevSource(slot).getPrevChar(offset);
    }

    @Override
    public double getPrevDouble(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        final int slot = unionRedirection.prevSlotForRowKey(rowKey);
        final long offset = getPrevOffset(rowKey, slot);
        return sourceLookup.slotToPrevSource(slot).getPrevDouble(offset);
    }

    @Override
    public float getPrevFloat(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_FLOAT;
        }
        final int slot = unionRedirection.prevSlotForRowKey(rowKey);
        final long offset = getPrevOffset(rowKey, slot);
        return sourceLookup.slotToPrevSource(slot).getPrevFloat(offset);
    }

    @Override
    public int getPrevInt(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_INT;
        }
        final int slot = unionRedirection.prevSlotForRowKey(rowKey);
        final long offset = getPrevOffset(rowKey, slot);
        return sourceLookup.slotToPrevSource(slot).getPrevInt(offset);
    }

    @Override
    public long getPrevLong(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_LONG;
        }
        final int slot = unionRedirection.prevSlotForRowKey(rowKey);
        final long offset = getPrevOffset(rowKey, slot);
        return sourceLookup.slotToPrevSource(slot).getPrevLong(offset);
    }

    @Override
    public short getPrevShort(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        final int slot = unionRedirection.prevSlotForRowKey(rowKey);
        final long offset = getPrevOffset(rowKey, slot);
        return sourceLookup.slotToPrevSource(slot).getPrevShort(offset);
    }

    private enum DataVersion {
        CURR, PREV
    }

    private abstract class SlotState<SLOT_CONTEXT_TYPE extends Context> implements SafeCloseable {

        int slot = -1;
        private DataVersion dataVersion;
        long shift;
        ColumnSource<T> source;
        SLOT_CONTEXT_TYPE context;
        int capacity;

        void prepare(final int sliceSlot, final DataVersion sliceDataVersion, final int sliceSize) {
            final boolean updateSource = sliceSlot != slot || dataVersion != sliceDataVersion;
            final boolean updateContext = updateSource || context == null || capacity < sliceSize;
            if (updateSource) {
                slot = sliceSlot;
                dataVersion = sliceDataVersion;
                switch (dataVersion) {
                    case CURR:
                        shift = unionRedirection.currFirstRowKeyForSlot(sliceSlot);
                        source = sourceLookup.slotToCurrSource(sliceSlot);
                        break;
                    case PREV:
                        shift = unionRedirection.prevFirstRowKeyForSlot(sliceSlot);
                        source = sourceLookup.slotToPrevSource(sliceSlot);
                        break;
                }
            }
            if (updateContext) {
                closeContext();
                makeContext(sliceSize);
            }
        }

        abstract void makeContext(int sliceSize);

        private void closeContext() {
            if (context != null) {
                context.close();
                context = null;
            }
        }

        @Override
        public void close() {
            slot = -1;
            dataVersion = null;
            source = null;
            closeContext();
            capacity = 0;
        }
    }

    private class SlotFillState extends SlotState<ChunkSource.FillContext> {

        @Override
        void makeContext(final int sliceSize) {
            context = source.makeFillContext(sliceSize);
            capacity = context.supportsUnboundedFill() ? Integer.MAX_VALUE : sliceSize;
        }
    }

    private class SlotGetState extends SlotState<ChunkSource.GetContext> {

        @Override
        void makeContext(final int sliceSize) {
            context = source.makeGetContext(sliceSize);
            capacity = sliceSize;
        }
    }

    private class FillContext implements ChunkSource.FillContext {

        private final ShiftedRowSequence sourceRowSequence;
        private final ResettableWritableChunk<Any> sliceDestination;
        private final SlotFillState slotState;

        private FillContext() {
            sourceRowSequence = new ShiftedRowSequence();
            sliceDestination = getChunkType().makeResettableWritableChunk();
            slotState = new SlotFillState();
        }

        private int lastSlot() {
            return Math.max(slotState.slot, 0);
        }

        private RowSequence sourceRowSequence(@NotNull final RowSequence rowSequence) {
            return sourceRowSequence.reset(rowSequence, -slotState.shift);
        }

        private void fillChunkAppend(
                final int slot,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence outerRowSequence) {
            final int sliceSize = outerRowSequence.intSize();
            slotState.prepare(slot, DataVersion.CURR, sliceSize);
            final int offset = destination.size();
            destination.setSize(offset + sliceSize);
            slotState.source.fillChunk(slotState.context,
                    sliceDestination.resetFromChunk(destination, offset, sliceSize),
                    sourceRowSequence(outerRowSequence));
        }

        private void fillPrevChunkAppend(
                final int slot,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence outerRowSequence) {
            final int sliceSize = outerRowSequence.intSize();
            slotState.prepare(slot, DataVersion.PREV, sliceSize);
            final int offset = destination.size();
            destination.setSize(offset + sliceSize);
            slotState.source.fillPrevChunk(slotState.context,
                    sliceDestination.resetFromChunk(destination, offset, sliceSize),
                    sourceRowSequence(outerRowSequence));
        }

        @Override
        public void close() {
            sourceRowSequence.close();
            sliceDestination.close();
            slotState.close();
        }
    }

    private class GetContext extends DefaultGetContext<Values> {

        private final SlotGetState slotState;

        private GetContext(final int chunkCapacity) {
            super(UnionColumnSource.this, chunkCapacity, null);
            slotState = new SlotGetState();
        }

        @Override
        public FillContext getFillContext() {
            // noinspection unchecked
            return ((FillContext) super.getFillContext());
        }

        private int lastSlot() {
            return Math.max(slotState.slot, 0);
        }

        private RowSequence sourceRowSequence(@NotNull final RowSequence rowSequence) {
            return getFillContext().sourceRowSequence.reset(rowSequence, -slotState.shift);
        }

        private Chunk<? extends Values> getChunk(final int slot, @NotNull final RowSequence outerRowSequence) {
            slotState.prepare(slot, DataVersion.CURR, outerRowSequence.intSize());
            return slotState.source.getChunk(slotState.context, sourceRowSequence(outerRowSequence));
        }

        private Chunk<? extends Values> getChunk(final int slot, final long firstOuterKey, final long lastOuterKey) {
            slotState.prepare(slot, DataVersion.CURR, Math.toIntExact(lastOuterKey - firstOuterKey + 1));
            return slotState.source.getChunk(slotState.context,
                    firstOuterKey - slotState.shift, lastOuterKey - slotState.shift);
        }

        private Chunk<? extends Values> getPrevChunk(final int slot, @NotNull final RowSequence outerRowSequence) {
            slotState.prepare(slot, DataVersion.PREV, outerRowSequence.intSize());
            return slotState.source.getPrevChunk(slotState.context, sourceRowSequence(outerRowSequence));
        }

        private Chunk<? extends Values> getPrevChunk(final int slot, final long firstOuterKey,
                final long lastOuterKey) {
            slotState.prepare(slot, DataVersion.PREV, Math.toIntExact(lastOuterKey - firstOuterKey + 1));
            return slotState.source.getPrevChunk(slotState.context,
                    firstOuterKey - slotState.shift, lastOuterKey - slotState.shift);
        }

        @Override
        public void close() {
            super.close();
            slotState.close();
        }
    }

    @Override
    public ColumnSource.FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new FillContext();
    }

    @Override
    public ChunkSource.GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new GetContext(chunkCapacity);
    }

    @Override
    public void fillChunk(
            @NotNull final ColumnSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        destination.setSize(0);
        if (rowSequence.isEmpty()) {
            return;
        }
        // noinspection unchecked
        final FillContext fillContext = (FillContext) context;
        final int firstSlot = unionRedirection.currSlotForRowKey(rowSequence.firstRowKey(), fillContext.lastSlot());
        final long lastKeyForFirstSlot = unionRedirection.currLastRowKeyForSlot(firstSlot);
        if (rowSequence.lastRowKey() <= lastKeyForFirstSlot) {
            fillContext.fillChunkAppend(firstSlot, destination, rowSequence);
            return;
        }
        fillChunkFromMultipleSources(fillContext, destination, rowSequence, firstSlot, lastKeyForFirstSlot);
    }

    private void fillChunkFromMultipleSources(
            @NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence,
            final int firstSlot,
            final long lastKeyForFirstSlot) {
        int slot = firstSlot;
        long lastKeyForSlot = lastKeyForFirstSlot;
        try (final RowSequence.Iterator sliceIterator = rowSequence.getRowSequenceIterator()) {
            do {
                final RowSequence sliceRowSequence = sliceIterator.getNextRowSequenceThrough(lastKeyForSlot);
                Assert.eqFalse(sliceRowSequence.isEmpty(), "sliceRowSequence.isEmpty()");
                fillContext.fillChunkAppend(slot, destination, sliceRowSequence);
                if (!sliceIterator.hasMore()) {
                    return;
                }
                slot = unionRedirection.currSlotForRowKey(sliceIterator.peekNextKey(), slot);
                lastKeyForSlot = unionRedirection.currLastRowKeyForSlot(slot);
            } while (true);
        }
    }

    @Override
    public void fillPrevChunk(
            @NotNull final ColumnSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        destination.setSize(0);
        if (rowSequence.isEmpty()) {
            return;
        }
        // noinspection unchecked
        final FillContext fillContext = (FillContext) context;
        final int firstSlot = unionRedirection.prevSlotForRowKey(rowSequence.firstRowKey(), fillContext.lastSlot());
        final long lastKeyForFirstSlot = unionRedirection.prevLastRowKeyForSlot(firstSlot);
        if (rowSequence.lastRowKey() <= lastKeyForFirstSlot) {
            fillContext.fillPrevChunkAppend(firstSlot, destination, rowSequence);
            return;
        }
        fillPrevChunkFromMultipleSources(fillContext, destination, rowSequence, firstSlot, lastKeyForFirstSlot);
    }

    private void fillPrevChunkFromMultipleSources(
            @NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence,
            final int firstSlot,
            final long lastKeyForFirstSlot) {
        int slot = firstSlot;
        long lastKeyForSlot = lastKeyForFirstSlot;
        try (final RowSequence.Iterator sliceIterator = rowSequence.getRowSequenceIterator()) {
            do {
                final RowSequence sliceRowSequence = sliceIterator.getNextRowSequenceThrough(lastKeyForSlot);
                fillContext.fillPrevChunkAppend(slot, destination, sliceRowSequence);
                if (!sliceIterator.hasMore()) {
                    return;
                }
                slot = unionRedirection.prevSlotForRowKey(sliceIterator.peekNextKey(), slot);
                lastKeyForSlot = unionRedirection.prevLastRowKeyForSlot(slot);
            } while (true);
        }
    }

    @Override
    public Chunk<? extends Values> getChunk(
            @NotNull final ChunkSource.GetContext context, @NotNull final RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return getChunkType().getEmptyChunk();
        }
        // noinspection unchecked
        final GetContext getContext = (GetContext) context;
        final int firstSlot = unionRedirection.currSlotForRowKey(rowSequence.firstRowKey(), getContext.lastSlot());
        final long lastKeyForFirstSlot = unionRedirection.currLastRowKeyForSlot(firstSlot);
        if (rowSequence.lastRowKey() <= lastKeyForFirstSlot) {
            return getContext.getChunk(firstSlot, rowSequence);
        }
        final WritableChunk<Values> destination = DefaultGetContext.getWritableChunk(context);
        fillChunkFromMultipleSources(getContext.getFillContext(), destination, rowSequence, firstSlot,
                lastKeyForFirstSlot);
        return destination;
    }

    @Override
    public Chunk<? extends Values> getChunk(
            @NotNull final ChunkSource.GetContext context, final long firstKey, final long lastKey) {
        final long sizeInRows = lastKey - firstKey + 1;
        if (sizeInRows == 0) {
            return getChunkType().getEmptyChunk();
        }
        // noinspection unchecked
        final GetContext getContext = (GetContext) context;
        final int firstSlot = unionRedirection.currSlotForRowKey(firstKey, getContext.lastSlot());
        final long lastKeyForFirstSlot = unionRedirection.currLastRowKeyForSlot(firstSlot);
        if (lastKey <= lastKeyForFirstSlot) {
            return getContext.getChunk(firstSlot, firstKey, lastKey);
        }
        final WritableChunk<Values> destination = DefaultGetContext.getWritableChunk(context);
        try (final RowSequence rowSequence = RowSequenceFactory.forRange(firstKey, lastKey)) {
            fillChunkFromMultipleSources(getContext.getFillContext(), destination, rowSequence, firstSlot,
                    lastKeyForFirstSlot);
        }
        return destination;
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(
            @NotNull final ChunkSource.GetContext context, @NotNull final RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return getChunkType().getEmptyChunk();
        }
        // noinspection unchecked
        final GetContext getContext = (GetContext) context;
        final int firstSlot = unionRedirection.prevSlotForRowKey(rowSequence.firstRowKey(), getContext.lastSlot());
        final long lastKeyForFirstSlot = unionRedirection.prevLastRowKeyForSlot(firstSlot);
        if (rowSequence.lastRowKey() <= lastKeyForFirstSlot) {
            return getContext.getPrevChunk(firstSlot, rowSequence);
        }
        final WritableChunk<Values> destination = DefaultGetContext.getWritableChunk(context);
        fillPrevChunkFromMultipleSources(getContext.getFillContext(), destination, rowSequence, firstSlot,
                lastKeyForFirstSlot);
        return destination;
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(
            @NotNull final ChunkSource.GetContext context, final long firstKey, final long lastKey) {
        final long sizeInRows = lastKey - firstKey + 1;
        if (sizeInRows == 0) {
            return getChunkType().getEmptyChunk();
        }
        // noinspection unchecked
        final GetContext getContext = (GetContext) context;
        final int firstSlot = unionRedirection.prevSlotForRowKey(firstKey, getContext.lastSlot());
        final long lastKeyForFirstSlot = unionRedirection.prevLastRowKeyForSlot(firstSlot);
        if (lastKey <= lastKeyForFirstSlot) {
            return getContext.getPrevChunk(firstSlot, firstKey, lastKey);
        }
        final WritableChunk<Values> destination = DefaultGetContext.getWritableChunk(context);
        try (final RowSequence rowSequence = RowSequenceFactory.forRange(firstKey, lastKey)) {
            fillPrevChunkFromMultipleSources(getContext.getFillContext(), destination, rowSequence, firstSlot,
                    lastKeyForFirstSlot);
        }
        return destination;
    }

    /**
     * Return the Union source manager that was used to create this table.
     */
    public UnionSourceManager getUnionSourceManager() {
        return unionSourceManager;
    }

    public ColumnSource getSubSource(final int slot) {
        if (!unionSourceManager.isUsingComponentsSafe()) {
            throw new UnsupportedOperationException("Cannot get component tables if constituent changes not permitted");
        }
        return sourceLookup.slotToCurrSource(slot);
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        if (!unionSourceManager.isUsingComponentsSafe()) {
            return false;
        }
        try (final Stream<ColumnSource<T>> sources = sourceLookup.currSources()) {
            return sources.allMatch(cs -> cs.allowsReinterpret(alternateDataType));
        }
    }

    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        {
            final WeakReference<ReinterpretToOriginal> reinterpretedSourceWeakReference =
                    reinterpretedSources == null ? null : reinterpretedSources.get(alternateDataType);
            if (reinterpretedSourceWeakReference != null) {
                final UnionColumnSource cachedValue = reinterpretedSourceWeakReference.get();
                if (cachedValue != null) {
                    // noinspection unchecked
                    return cachedValue;
                }
            }
        }

        final ColumnSource<ALTERNATE_DATA_TYPE>[] reinterpretedSubSources;
        try (final Stream<ColumnSource<T>> sources = sourceLookup.currSources()) {
            // noinspection unchecked
            reinterpretedSubSources = sources.map(cs -> cs.reinterpret(alternateDataType)).toArray(ColumnSource[]::new);
        }
        // noinspection unchecked
        final ReinterpretToOriginal<ALTERNATE_DATA_TYPE> reinterpretedSource = new ReinterpretToOriginal(
                alternateDataType, reinterpretedSubSources, this);

        if (reinterpretedSources == null) {
            reinterpretedSources = new KeyedObjectHashMap<>(ReinterpretedClassKey.INSTANCE);
        }
        reinterpretedSources.put(alternateDataType, new ReinterpretReference(reinterpretedSource, alternateDataType));

        return reinterpretedSource;
    }

    private static class ReinterpretReference extends WeakReference<ReinterpretToOriginal> {
        private final Class alternateDataType;

        private ReinterpretReference(ReinterpretToOriginal referent, Class alternateDataType) {
            super(referent);
            this.alternateDataType = alternateDataType;
        }
    }

    private static class ReinterpretToOriginal<ALTERNATE> extends UnionColumnSource<ALTERNATE> {

        private final UnionColumnSource originalSource;

        private ReinterpretToOriginal(
                @NotNull final Class<ALTERNATE> alternateDataType,
                @NotNull final ColumnSource<ALTERNATE>[] reinterpretedSubSources,
                @NotNull final UnionColumnSource originalSource) {
            super(alternateDataType, null, originalSource.unionSourceManager, originalSource.unionRedirection,
                    new ArraySourceLookup<>(reinterpretedSubSources));
            this.originalSource = originalSource;
        }

        @Override
        public boolean allowsReinterpret(@NotNull Class alternateDataType) {
            return alternateDataType == originalSource.getType();
        }

        @Override
        protected <ORIGINAL_TYPE> ColumnSource<ORIGINAL_TYPE> doReinterpret(
                @NotNull Class<ORIGINAL_TYPE> alternateDataType) {
            // noinspection unchecked
            return (ColumnSource<ORIGINAL_TYPE>) originalSource;
        }
    }

    private static class ReinterpretedClassKey extends KeyedObjectKey.Basic<Class, ReinterpretReference> {

        private static final KeyedObjectKey.Basic<Class, ReinterpretReference> INSTANCE = new ReinterpretedClassKey();

        @Override
        public Class getKey(@NotNull final ReinterpretReference reinterpretReference) {
            return reinterpretReference.alternateDataType;
        }
    }

    /**
     * ConstituentSource backed by an array.
     */
    private static final class ArraySourceLookup<T> implements UnionColumnSource.ConstituentSourceLookup<T> {

        private final ColumnSource<T>[] constituentSources;

        private ArraySourceLookup(@NotNull final ColumnSource<T>[] constituentSources) {
            this.constituentSources = constituentSources;
        }

        @Override
        public ColumnSource<T> slotToCurrSource(final int slot) {
            return constituentSources[slot];
        }

        @Override
        public ColumnSource<T> slotToPrevSource(final int slot) {
            return constituentSources[slot];
        }

        @Override
        public Stream<ColumnSource<T>> currSources() {
            return Arrays.stream(constituentSources);
        }
    }

    @Override
    public boolean isImmutable() {
        if (!unionSourceManager.isUsingComponentsSafe()) {
            return false;
        }
        try (final Stream<ColumnSource<T>> sources = sourceLookup.currSources()) {
            return sources.allMatch(ColumnSource::isImmutable);
        }
    }

    @Override
    public boolean isStateless() {
        if (!unionSourceManager.isUsingComponentsSafe()) {
            return false;
        }
        try (final Stream<ColumnSource<T>> sources = sourceLookup.currSources()) {
            return sources.allMatch(ColumnSource::isStateless);
        }
    }
}
