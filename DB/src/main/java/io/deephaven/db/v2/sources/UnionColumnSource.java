/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import io.deephaven.db.v2.sources.chunk.ResettableWritableChunk;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ShiftedOrderedKeys;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.util.*;

import static io.deephaven.util.QueryConstants.*;

/**
 * ColumnSource implementation for {@link io.deephaven.db.tables.utils.TableTools#merge} results.
 */
@AbstractColumnSource.IsSerializable(value = true)
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

    private long getOffset(long index, int tid) {
        return index - unionRedirection.startOfIndices[tid];
    }

    private long getPrevOffset(long index, int tid) {
        return index - unionRedirection.prevStartOfIndices[tid];
    }

    @Override
    public Boolean getBoolean(long index) {
        if (index == Index.NULL_KEY) {
            return null;
        }
        final int tid = unionRedirection.tidForIndex(index);
        final long offset = getOffset(index, tid);
        checkPos(index, tid);
        return subSources[tid].getBoolean(offset);
    }

    @Override
    public byte getByte(long index) {
        if (index == Index.NULL_KEY) {
            return NULL_BYTE;
        }
        final int tid = unionRedirection.tidForIndex(index);
        final long offset = getOffset(index, tid);
        checkPos(index, tid);
        return subSources[tid].getByte(offset);
    }

    @Override
    public char getChar(long index) {
        if (index == Index.NULL_KEY) {
            return NULL_CHAR;
        }
        final int tid = unionRedirection.tidForIndex(index);
        final long offset = getOffset(index, tid);
        checkPos(index, tid);
        return subSources[tid].getChar(offset);
    }

    @Override
    public double getDouble(long index) {
        if (index == Index.NULL_KEY) {
            return NULL_DOUBLE;
        }
        final int tid = unionRedirection.tidForIndex(index);
        final long offset = getOffset(index, tid);
        checkPos(index, tid);
        return subSources[tid].getDouble(offset);
    }

    @Override
    public float getFloat(long index) {
        if (index == Index.NULL_KEY) {
            return NULL_FLOAT;
        }
        final int tid = unionRedirection.tidForIndex(index);
        final long offset = getOffset(index, tid);
        checkPos(index, tid);
        return subSources[tid].getFloat(offset);
    }

    @Override
    public int getInt(long index) {
        if (index == Index.NULL_KEY) {
            return NULL_INT;
        }
        final int tid = unionRedirection.tidForIndex(index);
        final long offset = getOffset(index, tid);
        checkPos(index, tid);
        return subSources[tid].getInt(offset);
    }

    @Override
    public long getLong(long index) {
        if (index == Index.NULL_KEY) {
            return NULL_LONG;
        }
        final int tid = unionRedirection.tidForIndex(index);
        final long offset = getOffset(index, tid);
        checkPos(index, tid);
        return subSources[tid].getLong(offset);
    }

    @Override
    public short getShort(long index) {
        if (index == Index.NULL_KEY) {
            return NULL_SHORT;
        }
        final int tid = unionRedirection.tidForIndex(index);
        final long offset = getOffset(index, tid);
        checkPos(index, tid);
        return subSources[tid].getShort(offset);
    }

    @Override
    public T get(long index) {
        if (index == Index.NULL_KEY) {
            return null;
        }
        final int tid = unionRedirection.tidForIndex(index);
        final long offset = getOffset(index, tid);
        checkPos(index, tid);
        return subSources[tid].get(offset);
    }

    private void checkPos(long index, int pos) {
        if (pos >= subSources.length) {
            throw Assert.statementNeverExecuted(
                    "index: " + index + ", pos: " + pos + ", subSources.length: " + subSources.length);
        }
    }

    @Override
    public T getPrev(long index) {
        if (index == Index.NULL_KEY) {
            return null;
        }
        final int tid = unionRedirection.tidForPrevIndex(index);
        final long offset = getPrevOffset(index, tid);
        checkPos(index, tid);
        return subSources[tid].getPrev(offset);
    }

    @Override
    public Boolean getPrevBoolean(long index) {
        if (index == Index.NULL_KEY) {
            return null;
        }
        final int tid = unionRedirection.tidForPrevIndex(index);
        final long offset = getPrevOffset(index, tid);
        checkPos(index, tid);
        return subSources[tid].getPrevBoolean(offset);
    }

    @Override
    public byte getPrevByte(long index) {
        if (index == Index.NULL_KEY) {
            return NULL_BYTE;
        }
        final int tid = unionRedirection.tidForPrevIndex(index);
        final long offset = getPrevOffset(index, tid);
        checkPos(index, tid);
        return subSources[tid].getPrevByte(offset);
    }

    @Override
    public char getPrevChar(long index) {
        if (index == Index.NULL_KEY) {
            return NULL_CHAR;
        }
        final int tid = unionRedirection.tidForPrevIndex(index);
        final long offset = getPrevOffset(index, tid);
        checkPos(index, tid);
        return subSources[tid].getPrevChar(offset);
    }

    @Override
    public double getPrevDouble(long index) {
        if (index == Index.NULL_KEY) {
            return NULL_DOUBLE;
        }
        final int tid = unionRedirection.tidForPrevIndex(index);
        final long offset = getPrevOffset(index, tid);
        checkPos(index, tid);
        return subSources[tid].getPrevDouble(offset);
    }

    @Override
    public float getPrevFloat(long index) {
        if (index == Index.NULL_KEY) {
            return NULL_FLOAT;
        }
        final int tid = unionRedirection.tidForPrevIndex(index);
        final long offset = getPrevOffset(index, tid);
        checkPos(index, tid);
        return subSources[tid].getPrevFloat(offset);
    }

    @Override
    public int getPrevInt(long index) {
        if (index == Index.NULL_KEY) {
            return NULL_INT;
        }
        final int tid = unionRedirection.tidForPrevIndex(index);
        final long offset = getPrevOffset(index, tid);
        checkPos(index, tid);
        return subSources[tid].getPrevInt(offset);
    }

    @Override
    public long getPrevLong(long index) {
        if (index == Index.NULL_KEY) {
            return NULL_LONG;
        }
        final int tid = unionRedirection.tidForPrevIndex(index);
        final long offset = getPrevOffset(index, tid);
        checkPos(index, tid);
        return subSources[tid].getPrevLong(offset);
    }

    @Override
    public short getPrevShort(long index) {
        if (index == Index.NULL_KEY) {
            return NULL_SHORT;
        }
        final int tid = unionRedirection.tidForPrevIndex(index);
        final long offset = getPrevOffset(index, tid);
        checkPos(index, tid);
        return subSources[tid].getPrevShort(offset);
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

    private static class FillContext implements ColumnSource.FillContext {
        private final int chunkCapacity;

        private int lastTableId = -1;
        private int lastContextCapacity = 0;
        private ColumnSource.FillContext lastTableContext = null;

        FillContext(final int chunkCapacity) {
            this.chunkCapacity = chunkCapacity;
        }

        @Override
        public void close() {
            if (lastTableContext != null) {
                lastTableContext.close();
            }
        }
    }

    @Override
    public ColumnSource.FillContext makeFillContext(int chunkCapacity, final SharedContext sharedContext) {
        return new FillContext(chunkCapacity);
    }

    @Override
    public void fillChunk(@NotNull ColumnSource.FillContext _context,
            @NotNull WritableChunk<? super Values> destination,
            @NotNull OrderedKeys orderedKeys) {
        final FillContext context = (FillContext) _context;
        doFillChunk(context, destination, orderedKeys, false);
    }

    @Override
    public void fillPrevChunk(@NotNull ColumnSource.FillContext _context,
            @NotNull WritableChunk<? super Values> destination,
            @NotNull OrderedKeys orderedKeys) {
        final FillContext context = (FillContext) _context;
        doFillChunk(context, destination, orderedKeys, true);
    }

    private void doFillChunk(@NotNull FillContext context,
            @NotNull WritableChunk<? super Values> destination,
            @NotNull OrderedKeys orderedKeys,
            boolean usePrev) {
        final int okSize = orderedKeys.intSize();

        // Safe to assume destination has sufficient size for the request.
        destination.setSize(okSize);

        // To compute startTid and lastTid we assume at least one element is in the provided orderedKeys.
        if (okSize == 0) {
            return;
        }

        final int startTid = usePrev ? unionRedirection.tidForPrevIndex(orderedKeys.firstKey())
                : unionRedirection.tidForIndex(orderedKeys.firstKey());
        final int lastTid = usePrev ? unionRedirection.tidForPrevIndex(orderedKeys.lastKey())
                : unionRedirection.tidForIndex(orderedKeys.lastKey());
        final long[] startOfIndices = usePrev ? unionRedirection.prevStartOfIndices : unionRedirection.startOfIndices;

        try (final OrderedKeys.Iterator okit = orderedKeys.getOrderedKeysIterator();
                final ResettableWritableChunk<Any> resettableDestination = getChunkType().makeResettableWritableChunk();
                final ShiftedOrderedKeys okHelper = new ShiftedOrderedKeys()) {
            int offset = 0;
            for (int tid = startTid; tid <= lastTid; ++tid) {
                final int capacityRemaining = Math.min(context.chunkCapacity, okSize - offset);
                if (capacityRemaining <= 0) {
                    break;
                }

                okHelper.reset(okit.getNextOrderedKeysThrough(startOfIndices[tid + 1] - 1), -startOfIndices[tid]);
                if (okHelper.intSize() <= 0) {
                    // we do not need to invoke fillChunk on this subSource
                    continue;
                }

                final int minCapacity = Math.min(capacityRemaining, okHelper.intSize());
                if (context.lastTableId != tid || context.lastContextCapacity < minCapacity) {
                    context.lastTableId = tid;
                    context.lastContextCapacity = minCapacity;
                    if (context.lastTableContext != null) {
                        context.lastTableContext.close();
                    }
                    context.lastTableContext = subSources[tid].makeFillContext(minCapacity);
                }

                resettableDestination.resetFromChunk(destination, offset, capacityRemaining);

                if (usePrev) {
                    subSources[tid].fillPrevChunk(context.lastTableContext, resettableDestination, okHelper);
                } else {
                    subSources[tid].fillChunk(context.lastTableContext, resettableDestination, okHelper);
                }
                offset += resettableDestination.size();
            }

            destination.setSize(offset);
        }
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

    private void ensureCapacity(int tid) {
        int newLen = subSources.length;
        if (tid < newLen) {
            return;
        }
        do {
            newLen *= 2;
        } while (tid >= newLen);
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
}
