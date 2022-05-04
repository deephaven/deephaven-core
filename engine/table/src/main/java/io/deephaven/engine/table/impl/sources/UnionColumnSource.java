/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.impl.ShiftedRowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.engine.util.TableTools;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.base.verify.Assert;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.util.*;

import static io.deephaven.util.QueryConstants.*;

/**
 * ColumnSource implementation for {@link TableTools#merge} results.
 */
public class UnionColumnSource<T> extends AbstractColumnSource<T> {

    private int numSources;
    private ColumnSource<T>[] subSources;
    private final UnionRedirection unionRedirection;
    private final UnionSourceManager unionSourceManager;

    private Map<Class, ReinterpretReference> reinterpretedSources;

    UnionColumnSource(@NotNull final Class<T> type,
            @Nullable final Class componentType,
            @NotNull final UnionRedirection unionRedirection,
            @NotNull final UnionSourceManager unionSourceManager) {
        // noinspection unchecked
        this(type, componentType, unionRedirection, unionSourceManager, 0, new ColumnSource[8]);
    }

    private UnionColumnSource(@NotNull final Class<T> type,
            @Nullable final Class componentType,
            @NotNull final UnionRedirection unionRedirection,
            @NotNull final UnionSourceManager unionSourceManager,
            final int numSources,
            @NotNull final ColumnSource<T>[] subSources) {
        super(type, componentType);
        this.unionRedirection = unionRedirection;
        this.unionSourceManager = unionSourceManager;
        this.numSources = numSources;
        this.subSources = subSources;
    }

    private long getOffset(final long rowKey, final int slot) {
        return rowKey - unionRedirection.currFirstRowKeyForSlot(slot);
    }

    private long getPrevOffset(final long rowKey, final int slot) {
        return rowKey - unionRedirection.prevFirstRowKeyForSlot(slot);
    }

    @Override
    public Boolean getBoolean(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        final int slot = unionRedirection.currSlotForRowKey(rowKey);
        final long offset = getOffset(rowKey, slot);
        checkPos(rowKey, slot);
        return subSources[slot].getBoolean(offset);
    }

    @Override
    public byte getByte(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        final int slot = unionRedirection.currSlotForRowKey(rowKey);
        final long offset = getOffset(rowKey, slot);
        checkPos(rowKey, slot);
        return subSources[slot].getByte(offset);
    }

    @Override
    public char getChar(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        final int slot = unionRedirection.currSlotForRowKey(rowKey);
        final long offset = getOffset(rowKey, slot);
        checkPos(rowKey, slot);
        return subSources[slot].getChar(offset);
    }

    @Override
    public double getDouble(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        final int slot = unionRedirection.currSlotForRowKey(rowKey);
        final long offset = getOffset(rowKey, slot);
        checkPos(rowKey, slot);
        return subSources[slot].getDouble(offset);
    }

    @Override
    public float getFloat(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_FLOAT;
        }
        final int slot = unionRedirection.currSlotForRowKey(rowKey);
        final long offset = getOffset(rowKey, slot);
        checkPos(rowKey, slot);
        return subSources[slot].getFloat(offset);
    }

    @Override
    public int getInt(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_INT;
        }
        final int slot = unionRedirection.currSlotForRowKey(rowKey);
        final long offset = getOffset(rowKey, slot);
        checkPos(rowKey, slot);
        return subSources[slot].getInt(offset);
    }

    @Override
    public long getLong(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_LONG;
        }
        final int slot = unionRedirection.currSlotForRowKey(rowKey);
        final long offset = getOffset(rowKey, slot);
        checkPos(rowKey, slot);
        return subSources[slot].getLong(offset);
    }

    @Override
    public short getShort(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        final int slot = unionRedirection.currSlotForRowKey(rowKey);
        final long offset = getOffset(rowKey, slot);
        checkPos(rowKey, slot);
        return subSources[slot].getShort(offset);
    }

    @Override
    public T get(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        final int slot = unionRedirection.currSlotForRowKey(rowKey);
        final long offset = getOffset(rowKey, slot);
        checkPos(rowKey, slot);
        return subSources[slot].get(offset);
    }

    private void checkPos(long index, int pos) {
        if (pos >= subSources.length) {
            // noinspection ThrowableNotThrown
            Assert.statementNeverExecuted(
                    "rowSet: " + index + ", pos: " + pos + ", subSources.length: " + subSources.length);
        }
    }

    @Override
    public T getPrev(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        final int slot = unionRedirection.prevSlotForRowKey(rowKey);
        final long offset = getPrevOffset(rowKey, slot);
        checkPos(rowKey, slot);
        return subSources[slot].getPrev(offset);
    }

    @Override
    public Boolean getPrevBoolean(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        final int slot = unionRedirection.prevSlotForRowKey(rowKey);
        final long offset = getPrevOffset(rowKey, slot);
        checkPos(rowKey, slot);
        return subSources[slot].getPrevBoolean(offset);
    }

    @Override
    public byte getPrevByte(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        final int slot = unionRedirection.prevSlotForRowKey(rowKey);
        final long offset = getPrevOffset(rowKey, slot);
        checkPos(rowKey, slot);
        return subSources[slot].getPrevByte(offset);
    }

    @Override
    public char getPrevChar(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        final int slot = unionRedirection.prevSlotForRowKey(rowKey);
        final long offset = getPrevOffset(rowKey, slot);
        checkPos(rowKey, slot);
        return subSources[slot].getPrevChar(offset);
    }

    @Override
    public double getPrevDouble(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        final int slot = unionRedirection.prevSlotForRowKey(rowKey);
        final long offset = getPrevOffset(rowKey, slot);
        checkPos(rowKey, slot);
        return subSources[slot].getPrevDouble(offset);
    }

    @Override
    public float getPrevFloat(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_FLOAT;
        }
        final int slot = unionRedirection.prevSlotForRowKey(rowKey);
        final long offset = getPrevOffset(rowKey, slot);
        checkPos(rowKey, slot);
        return subSources[slot].getPrevFloat(offset);
    }

    @Override
    public int getPrevInt(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_INT;
        }
        final int slot = unionRedirection.prevSlotForRowKey(rowKey);
        final long offset = getPrevOffset(rowKey, slot);
        checkPos(rowKey, slot);
        return subSources[slot].getPrevInt(offset);
    }

    @Override
    public long getPrevLong(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_LONG;
        }
        final int slot = unionRedirection.prevSlotForRowKey(rowKey);
        final long offset = getPrevOffset(rowKey, slot);
        checkPos(rowKey, slot);
        return subSources[slot].getPrevLong(offset);
    }

    @Override
    public short getPrevShort(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        final int slot = unionRedirection.prevSlotForRowKey(rowKey);
        final long offset = getPrevOffset(rowKey, slot);
        checkPos(rowKey, slot);
        return subSources[slot].getPrevShort(offset);
    }

    @Override
    public boolean isImmutable() {
        for (ColumnSource<T> subSource : subSources) {
            if (subSource != null) {
                if (!subSource.isImmutable()) {
                    return false;
                }
            }
        }
        return true;
    }

    private enum DataVersion {
        CURR, PREV
    }

    private abstract class SlotState<SLOT_CONTEXT_TYPE extends Context> implements SafeCloseable {

        private int slot = -1;
        private long slotFirstKeyAllocated;
        private DataVersion dataVersion;
        ColumnSource<T> source;
        SLOT_CONTEXT_TYPE context;
        int capacity;

        void prepare(final int sliceSlot, final DataVersion sliceDataVersion, final int sliceSize) {
            final boolean updateSource = sliceSlot != slot || dataVersion != sliceDataVersion;
            final boolean updateContext = updateSource || context == null || sliceSize < capacity;
            if (updateSource) {
                slot = sliceSlot;
                dataVersion = sliceDataVersion;
                switch (dataVersion) {
                    case CURR:
                        slotFirstKeyAllocated = unionRedirection.currFirstRowKeyForSlot(sliceSlot);
                        break;
                    case PREV:
                        slotFirstKeyAllocated = unionRedirection.prevFirstRowKeyForSlot(sliceSlot);
                        break;
                }
                source = subSources[slot]; // TODO-RWC
            }
            if (updateContext) {
                closeContext();
                makeContext(sliceSize);
            }
        }

        long shift() {
            return slotFirstKeyAllocated;
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

        private RowSequence sourceRowSequence(@NotNull final RowSequence rowSequence) {
            return sourceRowSequence.reset(rowSequence, -slotState.shift());
        }

        private void fillChunkAppend(
                final int slot,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence outerRowSequence) {
            final int sliceSize = outerRowSequence.intSize();
            slotState.prepare(slot, DataVersion.CURR, sliceSize);
            slotState.source.fillChunk(slotState.context,
                    sliceDestination.resetFromChunk(destination, destination.size(), sliceSize),
                    sourceRowSequence(outerRowSequence));
            destination.setSize(destination.size() + sliceSize);
        }

        private void fillPrevChunkAppend(
                final int slot,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence outerRowSequence) {
            final int sliceSize = outerRowSequence.intSize();
            slotState.prepare(slot, DataVersion.PREV, sliceSize);
            slotState.source.fillPrevChunk(slotState.context,
                    sliceDestination.resetFromChunk(destination, destination.size(), sliceSize),
                    sourceRowSequence(outerRowSequence));
            destination.setSize(destination.size() + sliceSize);
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

        private RowSequence sourceRowSequence(@NotNull final RowSequence rowSequence) {
            return getFillContext().sourceRowSequence.reset(rowSequence, -slotState.shift());
        }

        private Chunk<? extends Values> getChunk(final int slot, @NotNull final RowSequence outerRowSequence) {
            slotState.prepare(slot, DataVersion.CURR, outerRowSequence.intSize());
            return slotState.source.getChunk(slotState.context, sourceRowSequence(outerRowSequence));
        }

        private Chunk<? extends Values> getChunk(final int slot, final long firstOuterKey, final long lastOuterKey) {
            slotState.prepare(slot, DataVersion.CURR, Math.toIntExact(lastOuterKey - firstOuterKey + 1));
            return slotState.source.getChunk(slotState.context,
                    firstOuterKey - slotState.shift(), lastOuterKey - slotState.shift());
        }

        private Chunk<? extends Values> getPrevChunk(final int slot, @NotNull final RowSequence outerRowSequence) {
            slotState.prepare(slot, DataVersion.PREV, outerRowSequence.intSize());
            return slotState.source.getPrevChunk(slotState.context, sourceRowSequence(outerRowSequence));
        }

        private Chunk<? extends Values> getPrevChunk(final int slot, final long firstOuterKey,
                final long lastOuterKey) {
            slotState.prepare(slot, DataVersion.PREV, Math.toIntExact(lastOuterKey - firstOuterKey + 1));
            return slotState.source.getPrevChunk(slotState.context,
                    firstOuterKey - slotState.shift(), lastOuterKey - slotState.shift());
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
        if (rowSequence.isEmpty()) {
            destination.setSize(0);
            return;
        }
        // noinspection unchecked
        final FillContext fillContext = (FillContext) context;
        final int firstSlot = unionRedirection.currSlotForRowKey(rowSequence.firstRowKey());
        final long lastKeyForFirstSlot = unionRedirection.currLastRowKeyForSlot(firstSlot);
        doFillChunk(fillContext, destination, rowSequence, firstSlot, lastKeyForFirstSlot);
    }

    private void doFillChunk(
            @NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence,
            final int firstSlot,
            final long lastKeyForFirstSlot) {
        if (rowSequence.lastRowKey() <= lastKeyForFirstSlot) {
            fillContext.fillChunkAppend(firstSlot, destination, rowSequence);
            return;
        }
        try (final RowSequence.Iterator sliceIterator = rowSequence.getRowSequenceIterator()) {
            {
                final RowSequence sliceRowSequence = sliceIterator.getNextRowSequenceThrough(lastKeyForFirstSlot);
                fillContext.fillChunkAppend(firstSlot, destination, sliceRowSequence);
            }
            while (sliceIterator.hasMore()) {
                final int slot = unionRedirection.currSlotForRowKey(sliceIterator.peekNextKey());
                final long lastKeyForSlot = unionRedirection.currLastRowKeyForSlot(slot);
                final RowSequence sliceRowSequence = sliceIterator.getNextRowSequenceThrough(lastKeyForSlot);
                fillContext.fillChunkAppend(slot, destination, sliceRowSequence);
            }
        }
    }

    @Override
    public void fillPrevChunk(
            @NotNull final ColumnSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            destination.setSize(0);
            return;
        }
        // noinspection unchecked
        final FillContext fillContext = (FillContext) context;
        final int firstSlot = unionRedirection.prevSlotForRowKey(rowSequence.firstRowKey());
        final long lastKeyForFirstSlot = unionRedirection.prevLastRowKeyForSlot(firstSlot);
        doFillPrevChunk(fillContext, destination, rowSequence, firstSlot, lastKeyForFirstSlot);
    }

    private void doFillPrevChunk(
            @NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence,
            final int firstSlot,
            final long lastKeyForFirstSlot) {
        if (rowSequence.lastRowKey() <= lastKeyForFirstSlot) {
            fillContext.fillPrevChunkAppend(firstSlot, destination, rowSequence);
            return;
        }
        try (final RowSequence.Iterator sliceIterator = rowSequence.getRowSequenceIterator()) {
            {
                final RowSequence sliceRowSequence = sliceIterator.getNextRowSequenceThrough(lastKeyForFirstSlot);
                fillContext.fillPrevChunkAppend(firstSlot, destination, sliceRowSequence);
            }
            while (sliceIterator.hasMore()) {
                final int slot = unionRedirection.prevSlotForRowKey(sliceIterator.peekNextKey());
                final long lastKeyForSlot = unionRedirection.prevLastRowKeyForSlot(slot);
                final RowSequence sliceRowSequence = sliceIterator.getNextRowSequenceThrough(lastKeyForSlot);
                fillContext.fillPrevChunkAppend(slot, destination, sliceRowSequence);
            }
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
        final int firstSlot = unionRedirection.currSlotForRowKey(rowSequence.firstRowKey());
        final long lastKeyForFirstSlot = unionRedirection.currLastRowKeyForSlot(firstSlot);
        if (rowSequence.lastRowKey() <= lastKeyForFirstSlot) {
            return getContext.getChunk(firstSlot, rowSequence);
        }
        final WritableChunk<Values> destination = DefaultGetContext.getWritableChunk(context);
        doFillChunk(getContext.getFillContext(), destination, rowSequence, firstSlot, lastKeyForFirstSlot);
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
        final int firstSlot = unionRedirection.currSlotForRowKey(firstKey);
        final long lastKeyForFirstSlot = unionRedirection.currLastRowKeyForSlot(firstSlot);
        if (lastKey <= lastKeyForFirstSlot) {
            return getContext.getChunk(firstSlot, firstKey, lastKey);
        }
        final WritableChunk<Values> destination = DefaultGetContext.getWritableChunk(context);
        try (final RowSequence rowSequence = RowSequenceFactory.forRange(firstKey, lastKey)) {
            doFillChunk(getContext.getFillContext(), destination, rowSequence, firstSlot, lastKeyForFirstSlot);
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
        final int firstSlot = unionRedirection.prevSlotForRowKey(rowSequence.firstRowKey());
        final long lastKeyForFirstSlot = unionRedirection.prevLastRowKeyForSlot(firstSlot);
        if (rowSequence.lastRowKey() <= lastKeyForFirstSlot) {
            return getContext.getPrevChunk(firstSlot, rowSequence);
        }
        final WritableChunk<Values> destination = DefaultGetContext.getWritableChunk(context);
        doFillPrevChunk(getContext.getFillContext(), destination, rowSequence, firstSlot, lastKeyForFirstSlot);
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
        final int firstSlot = unionRedirection.prevSlotForRowKey(firstKey);
        final long lastKeyForFirstSlot = unionRedirection.prevLastRowKeyForSlot(firstSlot);
        if (lastKey <= lastKeyForFirstSlot) {
            return getContext.getPrevChunk(firstSlot, firstKey, lastKey);
        }
        final WritableChunk<Values> destination = DefaultGetContext.getWritableChunk(context);
        try (final RowSequence rowSequence = RowSequenceFactory.forRange(firstKey, lastKey)) {
            doFillPrevChunk(getContext.getFillContext(), destination, rowSequence, firstSlot, lastKeyForFirstSlot);
        }
        return destination;
    }

    void appendColumnSource(ColumnSource<T> sourceToAdd) {
        ensureCapacity(numSources);
        subSources[numSources++] = sourceToAdd;

        if (reinterpretedSources == null) {
            return;
        }

        for (final Iterator<Map.Entry<Class, ReinterpretReference>> it = reinterpretedSources.entrySet().iterator(); it
                .hasNext();) {
            final Map.Entry<Class, ReinterpretReference> entry = it.next();
            final WeakReference<ReinterpretToOriginal> weakReference = entry.getValue();
            final ReinterpretToOriginal reinterpretToOriginal = weakReference.get();
            if (reinterpretToOriginal == null) {
                it.remove();
                continue;
            }
            // noinspection unchecked
            reinterpretToOriginal.appendColumnSource(sourceToAdd.reinterpret(entry.getKey()));
        }
    }

    private void ensureCapacity(final int slot) {
        int newLen = subSources.length;
        if (slot < newLen) {
            return;
        }
        do {
            newLen *= 2;
        } while (slot >= newLen);
        subSources = Arrays.copyOf(subSources, newLen);
    }

    /**
     * Return the Union source manager that was used to create this table.
     */
    public UnionSourceManager getUnionSourceManager() {
        return unionSourceManager;
    }

    public ColumnSource getSubSource(int i) {
        return subSources[i];
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return unionSourceManager.allowsReinterpret()
                && Arrays.stream(subSources).filter(Objects::nonNull)
                        .allMatch(cs -> cs.allowsReinterpret(alternateDataType));
    }

    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        final WeakReference<ReinterpretToOriginal> reinterpretedSourceWeakReference =
                reinterpretedSources == null ? null : reinterpretedSources.get(alternateDataType);
        if (reinterpretedSourceWeakReference != null) {
            final UnionColumnSource cachedValue = reinterpretedSourceWeakReference.get();
            if (cachedValue != null) {
                // noinspection unchecked
                return cachedValue;
            }
        }

        // noinspection unchecked
        final ColumnSource<ALTERNATE_DATA_TYPE>[] reinterpretedSubSources = new ColumnSource[subSources.length];
        for (int ii = 0; ii < subSources.length; ++ii) {
            if (subSources[ii] == null) {
                continue;
            }
            reinterpretedSubSources[ii] = subSources[ii].reinterpret(alternateDataType);
        }

        // noinspection unchecked
        final ReinterpretToOriginal<ALTERNATE_DATA_TYPE> reinterpretedSource =
                new ReinterpretToOriginal(alternateDataType, numSources, reinterpretedSubSources, this);

        if (reinterpretedSources == null) {
            reinterpretedSources = new KeyedObjectHashMap<>(REINTERPRETED_CLASS_KEY_INSTANCE);
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

        private ReinterpretToOriginal(Class<ALTERNATE> alternateDataType, int numSources,
                ColumnSource<ALTERNATE>[] reinterpretedSubSources, UnionColumnSource originalSource) {
            super(alternateDataType, null, originalSource.unionRedirection,
                    originalSource.unionSourceManager, numSources, reinterpretedSubSources);
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
        @Override
        public Class getKey(ReinterpretReference reinterpretReference) {
            return reinterpretReference.alternateDataType;
        }
    }

    private static final ReinterpretedClassKey REINTERPRETED_CLASS_KEY_INSTANCE = new ReinterpretedClassKey();

    @Override
    public boolean preventsParallelism() {
        for (int ii = 0; ii < numSources; ++ii) {
            if (subSources[ii].preventsParallelism()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isStateless() {
        for (int ii = 0; ii < numSources; ++ii) {
            if (!subSources[ii].isStateless()) {
                return false;
            }
        }
        return true;
    }
}
