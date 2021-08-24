/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources.deltaaware;

import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.utils.*;
import org.jetbrains.annotations.NotNull;

import java.util.function.LongConsumer;

// This worked-out example is a sketch of the problem we are trying to solve.
//
// Assume these initial conditions
//  baseline[0] = b0
//  baseline[1] = b1
//  baseline[2] = b2
//  ... and so on up to
//  baseline[999] = b999
//
//  Say the caller does the following puts:
//  put(5, d5)
//  put(6, d6)
//  put(7, d7)
//  put(8, d8)
//  put(9, d9)
//  put(15, d15)
//  put(16, d16)
//  put(17, d17)
//  put(18, d18)
//  put(19, d19)
//
// So we have
//  deltaRows = {5, 6, 7, 8, 9, 15, 16, 17, 18, 19}
//
// And the delta column (densely populated) has:
//  delta[0] = d5
//  delta[1] = d6
//  delta[2] = d7
//  delta[3] = d8
//  delta[4] = d9
//  delta[5] = d15
//  delta[6] = d16
//  delta[7] = d17
//  delta[8] = d18
//  delta[9] = d19
//
// Now someone calls fillChunk with orderedKeys = {0, 4, 5, 9, 10, 14, 15}
//
// We calculate orderedKeys - deltaRows, representing the baseline keys in the baseline space.
// baselineKeysBs = {0, 4, 10, 14}
//
// We also calculate intersection(orderedKeys, deltaRows), representing the delta keys in the _baseline_ space.
// deltaKeysBS = {5, 9, 15}
//
// We translate the above using invert, representing the delta keys in the _delta_ space.
// deltaKeysDS = {0, 4, 5}
//
// Call baseline.getChunk on baselineKeysBS
// Call delta.getChunk on deltaKeysDS
//
// After these calls we will have
// baselineChunk = {b0, b4, b10, b14}
// deltaChunk = {d5, d9, d15}
//
// To get these values back in the right spot, we analyze baselineKeysBs and deltaKeysBS. We take advantage of the
// property that these two sets do not intersect; furthermore, that their union is the original index requested by
// the caller. To do this, we simply count the number of consecutive (not necessarily adjacent) baseline items not
// interrupted by delta; and likewise the number of consecutive (not necessarily adjacent) delta items not
// interrupted by baseline. In our example:
//
// We need the first two items from the baseline chunk: b0, b4
// Then the next two items from the delta chunk: d5, d9
// Then the next two items from the baseline chunk: b10, b14
// Then the final item from the delta chunk: d15

public final class DeltaAwareColumnSource<T> extends AbstractColumnSource<T> implements WritableSource<T>, WritableChunkSink<Attributes.Values> {
    /**
     * The initial size of the delta column source.
     */
    private static final int INITIAL_DELTA_CAPACITY = 256;
    /**
     * In its own coordinate space
     */
    private final WritableChunkSink baseline;
    /**
     * Also in its own coordinate space (i.e. densely packed)
     */
    private WritableChunkSink delta;

    @FunctionalInterface
    private interface CapacityEnsurer {
        void ensureCapacity(long capacity, boolean nullFilled);
    }

    /**
     * A lambda that ensures the capacity of the baseline data structure. (We have this because the WritableChunkSink
     * does not have an 'ensureCapacity', but the underlying data structure we use does).
     */
    private final CapacityEnsurer baselineCapacityEnsurer;
    /**
     * A lambda that ensures the capacity of the delta data structure. (We have this because the WritableChunkSink
     * does not have an 'ensureCapacity', but the underlying data structure we use does).
     */
    private CapacityEnsurer deltaCapacityEnsurer;
    /**
     * The "preferred chunk size" from the underlying SparseArrayColumnSource.
     */
    private final int preferredChunkSize;
    /**
     * The capacity of the delta LightColumnSource
     */
    private int deltaCapacity;
    /**
     * The used delta keys (in the 'baseline' coordinate space). Null until startTrackingPrevValues() is called.
     * This field is volatile because we want concurrent lockfree getters to see correct values from "get()" even
     * though we might be in the middle of commitValues().
     */
    private volatile Index deltaRows;
    /**
     * The maximum key inserted into deltaRows during this phase. We use this to make sure that keys are not inserted
     * out of numerical order.
     */
    private long maxKey;
    /**
     * Adapter (not especially efficient) for turning single-element accesses into Chunk accesses. ThreadLocal because
     * multiple threads might be concurrently calling various get() methods.
     */
    private ThreadLocal<ChunkAdapter<T>> chunkAdapter;
    /**
     * Committing the delta values to the baseline. Null until startTrackingPrevValues() is called.
     */
    private UpdateCommitter<DeltaAwareColumnSource<T>> updateCommitter;

    public DeltaAwareColumnSource(Class<T> type) {
        super(type);
        final SparseArrayColumnSource<T> sparseBaseline = SparseArrayColumnSource.getSparseMemoryColumnSource(getType(), null);
        baseline = sparseBaseline;
        delta = baseline;

        baselineCapacityEnsurer = sparseBaseline::ensureCapacity;
        deltaCapacityEnsurer = baselineCapacityEnsurer;

        preferredChunkSize = sparseBaseline.getPreferredChunkSize();

        deltaCapacity = 0;
        deltaRows = null;
        chunkAdapter = ThreadLocal.withInitial(() -> ChunkAdapter.create(getType(), baseline, delta));
        updateCommitter = null;
    }

    //==================================================================================================================
    // CONTEXT METHODS
    //
    // We have lots of different ways of fetching elements, and therefore lots of different 'fetch' methods. Furthermore,
    // because each type of 'fetch' method needs a getContext method customized to it, we would in principle need one
    // 'getContext' method for each kind of 'fetch' method. In practice, because certain 'fetch' methods share the same
    // Context, we can get away with fewer.
    //
    // Breaking it town, there are twelve kinds of 'fetching' that one might want, represented as points in this
    // three-dimensional space:
    // 1. Will you be doing get or fill?
    // 2. Will you be accessing baseline (aka prev), delta, or current?
    // 3. FUTURE WORK: Will you be specifying all your keys up up front and slurping them sequentially (call this
    //    "sequential access") or will you be specifying OrderedKeys at every get call (call this "random access")
    //
    // Because #3 is future work we only have six types of "fetch" calls we care about, denoted compactly like this:
    // {get, fill} x {prev, delta, current}.
    //
    // These are their names
    // {get, baseline}: getPrevChunk
    // {fill, baseline}: fillPrevChunk
    // {get, delta}: getDeltaChunk
    // {fill, delta}: fillDeltaChunk
    // {get, current}: getChunk
    // {fill, current}: fillChunk
    //
    // To reduce the number of getContext methods, we group the above into triplets so we only need to provide two
    // GetContext methods. The groupings and their names are:
    // {get} x {baseline, delta, current}: makeGetContext(int)
    // {fill} x {baseline, delta, current: makeFillContext(int)
    //==================================================================================================================

    //==================================================================================================================
    // These are the getContext methods for
    // {get} x {baseline, delta, current}: makeGetContext(int)
    // {fill} x {baseline, delta, current: makeFillContext(int)
    //==================================================================================================================

    @Override
    public GetContext makeGetContext(final int chunkSize, final SharedContext sharedContext) {
        return DAContext.createForGet(this.getChunkType(), baseline, delta, chunkSize);
    }

    @Override
    public FillContext makeFillContext(final int chunkSize, final SharedContext sharedContext) {
        return DAContext.createForFill(baseline, delta, chunkSize);
    }

    //==================================================================================================================
    // These are the "get data" methods for
    // {get, current}: getChunk
    // {fill, current}: fillChunk
    //==================================================================================================================

    @Override
    public Chunk<Values> getChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys) {
        //TODO: this can probably use the defaultChunkSource.defaultGetChunk and avoid this cast with a refactoring.
        //noinspection unchecked
        return (Chunk<Values>) getOrFillChunk((DAContext)context, null, orderedKeys);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest, @NotNull OrderedKeys orderedKeys) {
        // Ignore return type.
        getOrFillChunk((DAContext)context, dest, orderedKeys);
    }

    /**
     * This method encapsulates some shared logic for the 'get' and 'fill' paths. If you pass in {@code dest} = null,
     * we assume you are doing a 'get'. Otherwise (if {@code dest} is not null), we assume you are doing a 'fill'.
     * @param context The context.
     * @param optionalDest Null if you are doing a get, or destination chunk if you are doing a fill.
     * @param orderedKeys Keys to get.
     * @return The chunk if you are doing a get, or {@code dest} if you are doing a fill.
     */
    private Chunk<? super Values> getOrFillChunk(@NotNull DAContext context, WritableChunk<? super Values> optionalDest,
                                         @NotNull OrderedKeys orderedKeys) {
        // Do the volatile read once
        final Index dRows = deltaRows;
        // Optimization if we're not tracking prev or if there are no deltas.
        if (dRows == null || dRows.empty()) {
            return getOrFillSimple(baseline, context.baseline, optionalDest, orderedKeys);
        }

        // baselineKeysBS: (orderedKeys - deltaRows): baseline keys in the baseline coordinate space
        // deltaKeysBS: (orderedKeys intersect deltaRows) delta keys, also in the baseline coordinate space
        // deltaKeysDS: the above, translated to the delta coordinate space
        final Index[] splitResult = new Index[2];
        splitKeys(orderedKeys, dRows, splitResult);
        final Index baselineKeysBS = splitResult[1];
        final Index deltaKeysBS = splitResult[0];

        // If one or the other is empty, shortcut here
        if (deltaKeysBS.empty()) {
            // By the way, baselineKeysBS equals orderedKeys, so you could pick either one
            return getOrFillSimple(baseline, context.baseline, optionalDest, baselineKeysBS);
        }

        final Index deltaKeysDS = dRows.invert(deltaKeysBS);
        if (baselineKeysBS.empty()) {
            return getOrFillSimple(delta, context.delta, optionalDest, deltaKeysDS);
        }

        // Always use "get" to pull in the baseline and delta pieces
        final Chunk<Values> bChunk = baseline.getChunk(context.baseline.getContext, baselineKeysBS);
        final Chunk<Values> dChunk = delta.getChunk(context.delta.getContext, deltaKeysDS);
        // Merge them into either the user-provided chunk, or our own preallocated chunk. Note that 'destToUse' will
        // always be non-null. This is because if we arrived here from fillChunk(), then optionalDest will be non-null.
        // Otherwise (if we arrived here from getChunk()), then optionalDest will be null, but context.optionalChunk
        // will be non-null (having been created through makeGetContext()).
        final WritableChunk<? super Values> destToUse = optionalDest != null ? optionalDest : context.optionalChunk;
        ChunkMerger.merge(bChunk, dChunk, baselineKeysBS, deltaKeysBS, destToUse);
        return destToUse;
    }

    private static Chunk<? super Values> getOrFillSimple(ChunkSource src, GetAndFillContexts ctx, WritableChunk<? super Values> optionalDest,
                                                 OrderedKeys orderedKeys) {
        if (optionalDest == null) {
            return src.getChunk(ctx.getContext, orderedKeys);
        }
        src.fillChunk(ctx.optionalFillContext, optionalDest, orderedKeys);
        return optionalDest;
    }

    //==================================================================================================================
    // These are the "get data" methods for
    // {get, baseline}: getPrevChunk
    // {fill, baseline}: fillPrevChunk
    //==================================================================================================================

    @Override
    public Chunk<Values> getPrevChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys) {
        final DAContext dactx = (DAContext)context;
        return baseline.getChunk(dactx.baseline.getContext, orderedKeys);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest, @NotNull OrderedKeys orderedKeys) {
        final DAContext dactx = (DAContext)context;
        baseline.fillChunk(dactx.baseline.optionalFillContext, dest, orderedKeys);
    }

    //==================================================================================================================
    // These are the "get data" methods for
    // {get, delta}: getDeltaChunk
    // {fill, delta}: fillDeltaChunk`
    // TODO(kosak)
    //==================================================================================================================


    //==================================================================================================================
    // Fill from Chunk
    //==================================================================================================================

    @Override
    public void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull OrderedKeys orderedKeys) {
        throw new UnsupportedOperationException("TODO(kosak)");
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src, @NotNull LongChunk<Attributes.KeyIndices> keys) {
        throw new UnsupportedOperationException("TODO");
    }

    //==================================================================================================================
    // These are the elementwise "get" methods. Should DACS even have these?
    //==================================================================================================================

    @Override
    public T get(final long index) {
        final long translatedIndex = lookupIndexInDeltaSpace(index);
        return chunkAdapter.get().get(index, translatedIndex);
    }

    @Override
    public Boolean getBoolean(final long index) {
        final long translatedIndex = lookupIndexInDeltaSpace(index);
        return chunkAdapter.get().getBoolean(index, translatedIndex);
    }

    @Override
    public byte getByte(final long index) {
        final long translatedIndex = lookupIndexInDeltaSpace(index);
        return chunkAdapter.get().getByte(index, translatedIndex);
    }

    @Override
    public char getChar(final long index) {
        final long translatedIndex = lookupIndexInDeltaSpace(index);
        return chunkAdapter.get().getChar(index, translatedIndex);
    }

    @Override
    public double getDouble(final long index) {
        final long translatedIndex = lookupIndexInDeltaSpace(index);
        return chunkAdapter.get().getDouble(index, translatedIndex);
    }

    @Override
    public float getFloat(final long index) {
        final long translatedIndex = lookupIndexInDeltaSpace(index);
        return chunkAdapter.get().getFloat(index, translatedIndex);
    }

    @Override
    public int getInt(final long index) {
        final long translatedIndex = lookupIndexInDeltaSpace(index);
        return chunkAdapter.get().getInt(index, translatedIndex);
    }

    @Override
    public long getLong(final long index) {
        final long translatedIndex = lookupIndexInDeltaSpace(index);
        return chunkAdapter.get().getLong(index, translatedIndex);
    }

    @Override
    public short getShort(final long index) {
        final long translatedIndex = lookupIndexInDeltaSpace(index);
        return chunkAdapter.get().getShort(index, translatedIndex);
    }

    //==================================================================================================================
    // These are the elementwise "get prev" methods. Should DACS even have these?
    //==================================================================================================================

    @Override
    public T getPrev(final long index) {
        return chunkAdapter.get().get(index, -1);
    }

    @Override
    public Boolean getPrevBoolean(final long index) {
        return chunkAdapter.get().getBoolean(index, -1);
    }

    @Override
    public byte getPrevByte(final long index) {
        return chunkAdapter.get().getByte(index, -1);
    }

    @Override
    public char getPrevChar(final long index) {
        return chunkAdapter.get().getChar(index, -1);
    }

    @Override
    public double getPrevDouble(final long index) {
        return chunkAdapter.get().getDouble(index, -1);
    }

    @Override
    public float getPrevFloat(final long index) {
        return chunkAdapter.get().getFloat(index, -1);
    }

    @Override
    public int getPrevInt(final long index) {
        return chunkAdapter.get().getInt(index, -1);
    }

    @Override
    public long getPrevLong(final long index) {
        return chunkAdapter.get().getLong(index, -1);
    }

    @Override
    public short getPrevShort(final long index) {
        return chunkAdapter.get().getShort(index, -1);
    }

    @Override
    public void set(final long key, final T value) {
        final long translatedKey = lookupOrCreateIndexInDeltaSpace(key);
        chunkAdapter.get().set(translatedKey, value);
    }

    @Override
    public void set(final long key, final byte value) {
        final long translatedKey = lookupOrCreateIndexInDeltaSpace(key);
        chunkAdapter.get().set(translatedKey, value);
    }

    @Override
    public void set(final long key, final char value) {
        final long translatedKey = lookupOrCreateIndexInDeltaSpace(key);
        chunkAdapter.get().set(translatedKey, value);
    }

    @Override
    public void set(final long key, final double value) {
        final long translatedKey = lookupOrCreateIndexInDeltaSpace(key);
        chunkAdapter.get().set(translatedKey, value);
    }

    @Override
    public void set(final long key, final float value) {
        final long translatedKey = lookupOrCreateIndexInDeltaSpace(key);
        chunkAdapter.get().set(translatedKey, value);
    }

    @Override
    public void set(final long key, final int value) {
        final long translatedKey = lookupOrCreateIndexInDeltaSpace(key);
        chunkAdapter.get().set(translatedKey, value);
    }

    @Override
    public void set(final long key, final long value) {
        final long translatedKey = lookupOrCreateIndexInDeltaSpace(key);
        chunkAdapter.get().set(translatedKey, value);
    }

    @Override
    public void set(final long key, final short value) {
        final long translatedKey = lookupOrCreateIndexInDeltaSpace(key);
        chunkAdapter.get().set(translatedKey, value);
    }

    /**
     * @param index The key to look up.
     * @return The index, translated into delta space, that the caller should use, or -1 if the caller should use the
     *   original index in baseline space. Will return -1 if either startTrackingPrevValues() has not been called yet,
     *   or if the index does not exist in the deltaRows.
     */
    private long lookupIndexInDeltaSpace(final long index) {
        assertIndexValid(index);
        if (baseline == delta) {
            return -1;
        }
        return deltaRows.find(index);
    }

    /**
     * @param index The key to look up.
     * @return If we're not tracking previous values yet, simply return the key (note 1).
     *   Otherwise, if the key already exists in the 'deltaRows' set, return its index.
     *   Otherwise allocate a new element of the deltaRows set and return that index.
     */
    private long lookupOrCreateIndexInDeltaSpace(final long index) {
        assertIndexValid(index);
        // We're in that special initial state where we're not tracking previous values, so we can just write directly
        // to the column source.
        if (baseline == delta) {
            return index;
        }

        // Do the volatile read once.
        final Index dRows = deltaRows;

        // Otherwise, we need to either update a key or append a key
        final long existing = dRows.find(index);
        if (existing >= 0) {
            return existing;
        }

        if (index < maxKey) {
            throw new UnsupportedOperationException("New keys need to be inserted in ascending order, but " + index +
                    "came after" + maxKey);
        }
        maxKey = index;

        // Here's a good place to activate the update committer.
        updateCommitter.maybeActivate();

        final long newKey = dRows.size();
        if (newKey >= deltaCapacity) {
            deltaCapacity *= 2;
            this.deltaCapacityEnsurer.ensureCapacity(deltaCapacity, false);
        }
        dRows.insert(index);
        return newKey;
    }

    private static void assertIndexValid(final long index) {
        if (index < 0) {
            throw new UnsupportedOperationException("DeltaAwareColumnSource does not accept negative indices: " + index);
        }
    }

    private void commitValues() {
        try (
                final FillFromContext baselineCtx = baseline.makeFillFromContext(preferredChunkSize);
                final WritableLongChunk<OrderedKeyRanges> orderedKeyRanges = WritableLongChunk.makeWritableChunk(2);
                final GetContext deltaCtx = delta.makeGetContext(preferredChunkSize);
                final OrderedKeys.Iterator it = deltaRows.getOrderedKeysIterator()
        ) {
            long startKey = 0;
            while (it.hasMore()) {
                final OrderedKeys baselineOk = it.getNextOrderedKeysWithLength(preferredChunkSize);
                final int baselineOkSize = baselineOk.intSize();
                orderedKeyRanges.set(0, startKey);
                orderedKeyRanges.set(1, startKey + baselineOkSize - 1);
                orderedKeyRanges.setSize(2);
                startKey += baselineOkSize;
                final OrderedKeys deltaOk = OrderedKeys.wrapKeyRangesChunkAsOrderedKeys(orderedKeyRanges);
                final Chunk<Values> data = delta.getChunk(deltaCtx, deltaOk);
                baseline.fillFromChunk(baselineCtx, data, baselineOk);
            }
        }
        deltaRows = Index.FACTORY.getEmptyIndex();
        maxKey = Long.MIN_VALUE;
    }

    @Override
    public void startTrackingPrevValues() {
        if (updateCommitter != null) {
            throw new UnsupportedOperationException("Can't call startTrackingPrevValues() twice");
        }
        deltaCapacity = INITIAL_DELTA_CAPACITY;
        final ArrayBackedColumnSource<T> delta = ArrayBackedColumnSource.getMemoryColumnSource(deltaCapacity, getType(), null);
        this.delta = delta;
        deltaCapacityEnsurer = delta::ensureCapacity;

        deltaRows = Index.FACTORY.getEmptyIndex();
        maxKey = Long.MIN_VALUE;
        /*
         * When 'delta' changes, we need a way to notify all the ChunkAdapters about its new value. We say "all the
         * ChunkAdapters" because chunkAdapter is a ThreadLocal, so there is one lying around for each thread that has
         * happened to have called get* or set*. The reason the ChunkAdapters need to know about this change is because
         * they have Contexts that need to be updated. The simplest way to update them is to just throw them all away
         * and start with a fresh ThreadLocal. This is not that big of a deal because this method is called at most
         * twice during the lifetime of a given DeltaAwareColumnSource: once at construction and once at the time of
         * startTrackingPrevValues().
         */
        chunkAdapter = ThreadLocal.withInitial(() -> ChunkAdapter.create(getType(), baseline, delta));
        updateCommitter = new UpdateCommitter<>(this, DeltaAwareColumnSource::commitValues);
    }

    @Override
    public void copy(ColumnSource<T> sourceColumn, long sourceKey, long destKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFilled) {
        baselineCapacityEnsurer.ensureCapacity(capacity, nullFilled);
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    /**
     * Partitions {@code lhs} into two indices: (lhs intersect rhs) and (lhs minus rhs).
     * @param lhs The {@link OrderedKeys} to partition
     * @param rhs The keys which control the partition operation
     * @param results Allocated by the caller. {@code results[0]} will be set to (lhs intersect rhs). {@code results[1]}
     *                will be set to (lhs minus rhs).
     */
    private static void splitKeys(OrderedKeys lhs, Index rhs, Index[] results) {
        final Index lhsIndex = lhs.asIndex();
        results[0] = lhsIndex.intersect(rhs);
        results[1] = lhsIndex.minus(rhs);
    }

    private static class DAContext implements ChunkSource.GetContext, ChunkSource.FillContext {
        static DAContext createForGet(ChunkType chunkType, ChunkSource baseline, ChunkSource delta, int chunkCapacity) {
            final GetAndFillContexts b = GetAndFillContexts.createForGet(baseline, chunkCapacity);
            final GetAndFillContexts d = GetAndFillContexts.createForGet(delta, chunkCapacity);
            return new DAContext(b, d, chunkType.makeWritableChunk(chunkCapacity));
        }

        static DAContext createForFill(ChunkSource baseline, ChunkSource delta, int chunkCapacity) {
            final GetAndFillContexts b = GetAndFillContexts.createForFill(baseline, chunkCapacity);
            final GetAndFillContexts d = GetAndFillContexts.createForFill(delta, chunkCapacity);
            return new DAContext(b, d, null);
        }

        final GetAndFillContexts baseline;
        final GetAndFillContexts delta;
        /**
         * We only allocate a chunk if the caller is doing a 'get'.
         */
        final WritableChunk<Values> optionalChunk;

        private DAContext(GetAndFillContexts baseline, GetAndFillContexts delta, WritableChunk<Values> optionalChunk) {
            this.baseline = baseline;
            this.delta = delta;
            this.optionalChunk = optionalChunk;
        }
    }

    private static class GetAndFillContexts {
        static GetAndFillContexts createForGet(ChunkSource chunkSource, int chunkCapacity) {
            return new GetAndFillContexts(chunkSource.makeGetContext(chunkCapacity), null);
        }

        static GetAndFillContexts createForFill(ChunkSource chunkSource, int chunkCapacity) {
            return new GetAndFillContexts(chunkSource.makeGetContext(chunkCapacity),
                    chunkSource.makeFillContext(chunkCapacity));
        }

        /**
         * We always have a getContext
         */
        final ChunkSource.GetContext getContext;
        /**
         * We only have a fillContext if the caller is doing a 'fill'.
         */
        final ChunkSource.FillContext optionalFillContext;

        private GetAndFillContexts(ChunkSource.GetContext getContext, ChunkSource.FillContext optionalFillContext) {
            this.getContext = getContext;
            this.optionalFillContext = optionalFillContext;
        }
    }
}
