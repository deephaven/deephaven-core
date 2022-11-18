package io.deephaven.engine.table.impl;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ResettableWritableObjectChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.sources.sparse.SparseConstants;
import io.deephaven.engine.table.impl.updateby.UpdateByWindow;
import io.deephaven.engine.table.impl.util.InverseRowRedirectionImpl;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.impl.util.OperationInitializationPoolJobScheduler;
import io.deephaven.engine.table.impl.util.UpdateGraphProcessorJobScheduler;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.SoftReference;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

/**
 * The core of the {@link Table#updateBy(UpdateByControl, Collection, Collection)} operation.
 */
public abstract class UpdateBy {
    /** When caching a column source, how many rows should we process in each parallel batch? (1M default) */
    public static final int PARALLEL_CACHE_BATCH_SIZE =
            Configuration.getInstance().getIntegerWithDefault("UpdateBy.parallelCacheBatchSize", 1 << 20);
    /** When caching a column source, what size chunks should be used to move data to the cache? (65K default) */
    public static final int PARALLEL_CACHE_CHUNK_SIZE =
            Configuration.getInstance().getIntegerWithDefault("UpdateBy.parallelCacheChunkSize", 1 << 16);

    /** Input sources may be reused by mutiple operators, only store and cache unique ones */
    protected final ColumnSource<?>[] inputSources;
    /** Map operators to input sources, note some operators need more than one input, WAvg e.g. */
    protected final int[][] operatorInputSourceSlots;
    /** All the operators for this UpdateBy manager */
    protected final UpdateByOperator[] operators;
    /** All the windows for this UpdateBy manager */
    protected final UpdateByWindow[] windows;
    /** The source table for the UpdateBy operators */
    protected final QueryTable source;
    /** Helper class for maintaining the RowRedirection when using redirected output sources */
    protected final UpdateByRedirectionContext redirContext;
    /** User control to specify UpdateBy parameters */
    protected final UpdateByControl control;
    /** The single timestamp column used by all time-based operators */
    protected final String timestampColumnName;
    /** Store every bucket in this list for processing */
    protected final LinkedList<UpdateByBucketHelper> buckets;
    /** Whether caching benefits this UpdateBy operation */
    protected final boolean inputCacheNeeded;
    /** Whether caching benefits each input source */
    protected final boolean[] inputSourceCacheNeeded;
    /**
     * References to the dense array sources we are using for the cached sources, it's expected that these will be
     * released and need to be created
     */
    protected final SoftReference<WritableColumnSource<?>>[] inputSourceCaches;
    /** For easy iteration, create a list of the source indices that need to be cached */
    protected final int[] cacheableSourceIndices;

    /** ColumnSet transformer from source to downstream */
    protected ModifiedColumnSet.Transformer transformer;
    /** The output table for this UpdateBy operation */
    protected QueryTable result;

    /** For refreshing sources, maintain a list of each of the bucket listeners */
    protected LinkedList<ListenerRecorder> recorders;
    /** For refreshing sources, need a merged listener to produce downstream updates */
    protected UpdateByListener listener;

    public static class UpdateByRedirectionContext implements Context {
        @Nullable
        protected final WritableRowRedirection rowRedirection;
        protected final WritableRowSet freeRows;
        protected long maxInnerIndex;

        public UpdateByRedirectionContext(@Nullable final WritableRowRedirection rowRedirection) {
            this.rowRedirection = rowRedirection;
            this.freeRows = rowRedirection == null ? null : RowSetFactory.empty();
            this.maxInnerIndex = 0;
        }

        public boolean isRedirected() {
            return rowRedirection != null;
        }

        public long requiredCapacity() {
            return maxInnerIndex + 1;
        }

        @Nullable
        public WritableRowRedirection getRowRedirection() {
            return rowRedirection;
        }

        public void processUpdateForRedirection(@NotNull final TableUpdate upstream, final TrackingRowSet prevRowSet) {
            if (upstream.removed().isNonempty()) {
                final RowSetBuilderRandom freeBuilder = RowSetFactory.builderRandom();
                upstream.removed().forAllRowKeys(key -> freeBuilder.addKey(rowRedirection.remove(key)));
                freeRows.insert(freeBuilder.build());
            }

            if (upstream.shifted().nonempty()) {
                try (final WritableRowSet prevIndexLessRemoves = prevRowSet.copyPrev()) {
                    prevIndexLessRemoves.remove(upstream.removed());
                    final RowSet.SearchIterator fwdIt = prevIndexLessRemoves.searchIterator();

                    upstream.shifted().apply((start, end, delta) -> {
                        if (delta < 0 && fwdIt.advance(start)) {
                            for (long key = fwdIt.currentValue(); fwdIt.currentValue() <= end; key = fwdIt.nextLong()) {
                                if (shiftRedirectedKey(fwdIt, delta, key))
                                    break;
                            }
                        } else {
                            try (final RowSet.SearchIterator revIt = prevIndexLessRemoves.reverseIterator()) {
                                if (revIt.advance(end)) {
                                    for (long key = revIt.currentValue(); revIt.currentValue() >= start; key =
                                            revIt.nextLong()) {
                                        if (shiftRedirectedKey(revIt, delta, key))
                                            break;
                                    }
                                }
                            }
                        }
                    });
                }
            }

            if (upstream.added().isNonempty()) {
                final MutableLong lastAllocated = new MutableLong(0);
                final WritableRowSet.Iterator freeIt = freeRows.iterator();
                upstream.added().forAllRowKeys(outerKey -> {
                    final long innerKey = freeIt.hasNext() ? freeIt.nextLong() : ++maxInnerIndex;
                    lastAllocated.setValue(innerKey);
                    rowRedirection.put(outerKey, innerKey);
                });
                freeRows.removeRange(0, lastAllocated.longValue());
            }
        }

        private boolean shiftRedirectedKey(@NotNull final RowSet.SearchIterator iterator, final long delta,
                final long key) {
            final long inner = rowRedirection.remove(key);
            if (inner != NULL_ROW_KEY) {
                rowRedirection.put(key + delta, inner);
            }
            return !iterator.hasNext();
        }

        public RowSet getInnerKeys(final RowSet outerKeys, final SharedContext sharedContext) {
            if (rowRedirection == null) {
                return null;
            }
            RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            final int chunkSize = Math.min(outerKeys.intSize(), 4096);
            try (final RowSequence.Iterator it = outerKeys.getRowSequenceIterator();
                    ChunkSource.FillContext fillContext = rowRedirection.makeFillContext(chunkSize, sharedContext);
                    WritableLongChunk<? extends RowKeys> chunk = WritableLongChunk.makeWritableChunk(chunkSize)) {
                while (it.hasMore()) {
                    final RowSequence rs = it.getNextRowSequenceWithLength(chunkSize);
                    rowRedirection.fillChunk(fillContext, chunk, rs);
                    builder.addRowKeysChunk(chunk);
                }
            }
            return builder.build();
        }

        @Override
        public void close() {
            try (final WritableRowSet ignored = freeRows) {
            }
        }
    }

    protected UpdateBy(@NotNull final String description,
            @NotNull final QueryTable source,
            @NotNull final UpdateByOperator[] operators,
            @NotNull final UpdateByWindow[] windows,
            @NotNull final ColumnSource<?>[] inputSources,
            @NotNull final int[][] operatorInputSourceSlots,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @Nullable String timestampColumnName,
            @NotNull final UpdateByRedirectionContext redirContext,
            @NotNull final UpdateByControl control) {

        if (operators.length == 0) {
            throw new IllegalArgumentException("At least one operator must be specified");
        }

        this.source = source;
        this.operators = operators;
        this.windows = windows;
        this.inputSources = inputSources;
        this.operatorInputSourceSlots = operatorInputSourceSlots;
        this.timestampColumnName = timestampColumnName;
        this.redirContext = redirContext;
        this.control = control;

        this.inputSourceCacheNeeded = new boolean[inputSources.length];

        boolean cacheNeeded = false;
        TIntArrayList cacheableSourceIndicesList = new TIntArrayList(inputSources.length);
        for (int ii = 0; ii < inputSources.length; ii++) {
            if (!FillUnordered.providesFillUnordered(inputSources[ii])) {
                cacheNeeded = inputSourceCacheNeeded[ii] = true;
                cacheableSourceIndicesList.add(ii);
            }
        }
        // store this list for fast iteration
        cacheableSourceIndices = cacheableSourceIndicesList.toArray();

        this.inputCacheNeeded = cacheNeeded;
        // noinspection unchecked
        inputSourceCaches = new SoftReference[inputSources.length];

        buckets = new LinkedList<>();
    }

    /** Remove all references to Objects for this column source */
    private void fillObjectArraySourceWithNull(ObjectArraySource<?> sourceToNull) {
        Assert.neqNull(sourceToNull, "cached column source was null, must have been GC'd");
        try (final ResettableWritableObjectChunk<?, ?> backingChunk =
                ResettableWritableObjectChunk.makeResettableChunk()) {
            Assert.neqNull(sourceToNull, "cached column source was already GC'd");
            final long targetCapacity = sourceToNull.getCapacity();
            for (long positionToNull = 0; positionToNull < targetCapacity; positionToNull += backingChunk.size()) {
                sourceToNull.resetWritableChunkToBackingStore(backingChunk, positionToNull);
                backingChunk.fillWithNullValue(0, backingChunk.size());
            }
        }
    }

    /** Release the input sources that will not be needed for the rest of this update */
    private void releaseInputSources(int winIdx, ColumnSource<?>[] maybeCachedInputSources,
            TrackingWritableRowSet[] inputSourceRowSets, AtomicInteger[] inputSourceReferenceCounts) {
        final UpdateByWindow win = windows[winIdx];
        final int[] uniqueWindowSources = win.getUniqueSourceIndices();
        for (int srcIdx : uniqueWindowSources) {
            if (inputSourceReferenceCounts[srcIdx] != null) {
                if (inputSourceReferenceCounts[srcIdx].decrementAndGet() == 0) {
                    // do the cleanup immediately
                    inputSourceRowSets[srcIdx].close();
                    inputSourceRowSets[srcIdx] = null;

                    // release any objects we are holding in the cache
                    if (inputSourceCaches[srcIdx].get() instanceof ObjectArraySource) {
                        fillObjectArraySourceWithNull((ObjectArraySource<?>) inputSourceCaches[srcIdx].get());
                    }

                    maybeCachedInputSources[srcIdx] = null;
                }
            }
        }
    }

    /**
     * Overview of work performed by {@link StateManager}:
     * <ol>
     * <li>Create `shiftedRows`, the set of rows for the output sources that are affected by shifts</li>
     * <li>Compute a rowset for each cacheable input source identifying which rows will be needed for processing</li>
     * <li>Process each window serially
     * <ul>
     * <li>Cache the input sources that are needed for this window (this can be done in parallel for each column and
     * parallel again for a subset of the rows)</li>
     * <li>Compute the modified rowset of output column sources and call `prepareForParallelPopulation()', this could be
     * done in parallel with the caching</li>
     * <li>When prepareForParallelPopulation() complete, apply upstream shifts to the output sources</li>
     * <li>When caching and shifts are complete, process the data in this window in parallel by dividing the buckets
     * into sets (N/num_threads) and running a job for each bucket_set 3e) when all buckets processed</li>
     * <li>When all buckets processed, release the input source caches that will not be re-used later</li>
     * </ul>
     * </li>
     * <li>When all windows processed, create the downstream update and notify</li>
     * <li>Release resources</li>
     * </ol>
     */

    protected class StateManager implements LogOutputAppendable {
        final TableUpdate update;
        final boolean initialStep;
        final UpdateByBucketHelper[] dirtyBuckets;

        final ColumnSource<?>[] maybeCachedInputSources;
        final TrackingWritableRowSet[] inputSourceRowSets;
        final AtomicInteger[] inputSourceReferenceCounts;

        final JobScheduler jobScheduler;
        final CompletableFuture<Void> waitForResult;

        final SharedContext sharedContext;

        WritableRowSet shiftedRows;

        public StateManager(TableUpdate update, boolean initialStep) {
            this.update = update;
            this.initialStep = initialStep;

            // determine which buckets we'll examine during this update
            dirtyBuckets = buckets.stream().filter(UpdateByBucketHelper::isDirty).toArray(UpdateByBucketHelper[]::new);

            if (inputCacheNeeded) {
                maybeCachedInputSources = new ColumnSource[inputSources.length];
                inputSourceRowSets = new TrackingWritableRowSet[inputSources.length];
                inputSourceReferenceCounts = new AtomicInteger[inputSources.length];
            } else {
                maybeCachedInputSources = inputSources;
                inputSourceRowSets = null;
                inputSourceReferenceCounts = null;
            }

            if (initialStep) {
                if (OperationInitializationThreadPool.NUM_THREADS > 1) {
                    jobScheduler = new OperationInitializationPoolJobScheduler();
                } else {
                    jobScheduler = ImmediateJobScheduler.INSTANCE;
                }
                waitForResult = new CompletableFuture<>();
            } else {
                if (UpdateGraphProcessor.DEFAULT.getUpdateThreads() > 1) {
                    jobScheduler = new UpdateGraphProcessorJobScheduler();
                } else {
                    jobScheduler = ImmediateJobScheduler.INSTANCE;
                }
                waitForResult = null;
            }

            sharedContext = SharedContext.makeSharedContext();
        }

        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput.append("UpdateBy.StateManager");
        }

        private void onError(Exception error) {
            // signal to the future that an exception has occured
            waitForResult.completeExceptionally(error);
        }

        /**
         * Accumulate in parallel the dirty bucket rowsets for the cacheable input sources. Calls
         * {@code completedAction} when the work is complete
         */
        private void computeCachedColumnRowsets(final Runnable completeAction) {
            if (!inputCacheNeeded) {
                completeAction.run();
                return;
            }

            // initially everything is dirty so cache everything
            if (initialStep) {
                for (int srcIdx : cacheableSourceIndices) {
                    // create a TrackingRowSet to be used by `InverseRowRedirectionImpl`
                    inputSourceRowSets[srcIdx] = source.getRowSet().copy().toTracking();

                    // how many windows require this input source?
                    int refCount = 0;
                    for (UpdateByWindow win : windows) {
                        if (win.isSourceInUse(srcIdx)) {
                            refCount++;
                        }
                    }
                    inputSourceReferenceCounts[srcIdx] = new AtomicInteger(refCount);
                }
                completeAction.run();
                return;
            }

            jobScheduler.iterateParallel(ExecutionContext.getContextToRecord(), this,
                    0, cacheableSourceIndices.length,
                    idx -> {
                        int srcIdx = cacheableSourceIndices[idx];
                        int refCount = 0;
                        for (int winIdx = 0; winIdx < windows.length; winIdx++) {
                            UpdateByWindow win = windows[winIdx];
                            if (win.isSourceInUse(srcIdx)) {
                                for (UpdateByBucketHelper bucket : dirtyBuckets) {
                                    UpdateByWindow.UpdateByWindowContext winCtx = bucket.windowContexts[winIdx];

                                    if (win.isWindowDirty(winCtx)) {
                                        // add this rowset to the running total for this input source
                                        if (inputSourceRowSets[srcIdx] == null) {
                                            inputSourceRowSets[srcIdx] =
                                                    win.getInfluencerRows(winCtx).copy().toTracking();
                                        } else {
                                            inputSourceRowSets[srcIdx].insert(win.getInfluencerRows(winCtx));
                                        }
                                    }
                                }
                                refCount++;
                            }
                        }
                        inputSourceReferenceCounts[srcIdx] = new AtomicInteger(refCount);
                    },
                    completeAction,
                    this::onError);
        }

        /**
         * Create a new input source cache and populate the required rows in parallel. Calls {@code completedAction}
         * when the work is complete
         */
        private void createCachedColumnSource(int srcIdx, final Runnable completeAction) {
            if (maybeCachedInputSources[srcIdx] != null) {
                // already cached from another operator (or caching not needed)
                completeAction.run();
                return;
            }

            final ColumnSource<?> inputSource = inputSources[srcIdx];
            final TrackingWritableRowSet inputRowSet = inputSourceRowSets[srcIdx];

            // re-use the dense column cache if it still exists
            WritableColumnSource<?> innerSource;
            if (inputSourceCaches[srcIdx] == null || (innerSource = inputSourceCaches[srcIdx].get()) == null) {
                // create a new dense cache
                innerSource = ArrayBackedColumnSource.getMemoryColumnSource(inputSource.getType(),
                        inputSource.getComponentType());
                inputSourceCaches[srcIdx] = new SoftReference<>(innerSource);
            }
            innerSource.ensureCapacity(inputRowSet.size());

            // there will be no updates to this cached column source, so use a simple redirection
            final WritableRowRedirection rowRedirection = new InverseRowRedirectionImpl(inputRowSet);
            final WritableColumnSource<?> outputSource =
                    new WritableRedirectedColumnSource(rowRedirection, innerSource, 0);

            // holding this reference should protect `rowDirection` and `innerSource` from GC
            maybeCachedInputSources[srcIdx] = outputSource;

            if (inputRowSet.size() >= PARALLEL_CACHE_BATCH_SIZE) {
                // divide the rowset into reasonable chunks and do the cache population in parallel
                final ArrayList<RowSet> populationRowSets = new ArrayList<>();
                try (final RowSequence.Iterator rsIt = inputRowSet.getRowSequenceIterator()) {
                    while (rsIt.hasMore()) {
                        final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(PARALLEL_CACHE_BATCH_SIZE);
                        populationRowSets.add(chunkOk.asRowSet().copy());
                    }
                }
                jobScheduler.iterateParallel(ExecutionContext.getContextToRecord(), this,
                        0, populationRowSets.size(),
                        idx -> {
                            try (final RowSet chunkRs = populationRowSets.get(idx);
                                    final RowSequence.Iterator rsIt = chunkRs.getRowSequenceIterator();
                                    final ChunkSink.FillFromContext ffc =
                                            outputSource.makeFillFromContext(PARALLEL_CACHE_CHUNK_SIZE);
                                    final ChunkSource.GetContext gc =
                                            inputSource.makeGetContext(PARALLEL_CACHE_CHUNK_SIZE)) {
                                while (rsIt.hasMore()) {
                                    final RowSequence chunkOk =
                                            rsIt.getNextRowSequenceWithLength(PARALLEL_CACHE_CHUNK_SIZE);
                                    final Chunk<? extends Values> values = inputSource.getChunk(gc, chunkOk);
                                    outputSource.fillFromChunk(ffc, values, chunkOk);
                                }
                            }
                        }, () -> {
                            populationRowSets.clear();
                            completeAction.run();
                        },
                        this::onError);
            } else {
                // run this in serial, not worth parallelization
                try (final RowSequence.Iterator rsIt = inputRowSet.getRowSequenceIterator();
                        final ChunkSink.FillFromContext ffc =
                                outputSource.makeFillFromContext(PARALLEL_CACHE_CHUNK_SIZE);
                        final ChunkSource.GetContext gc = inputSource.makeGetContext(PARALLEL_CACHE_CHUNK_SIZE)) {
                    while (rsIt.hasMore()) {
                        final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(PARALLEL_CACHE_CHUNK_SIZE);
                        final Chunk<? extends Values> values = inputSource.getChunk(gc, chunkOk);
                        outputSource.fillFromChunk(ffc, values, chunkOk);
                    }
                }
                completeAction.run();
            }
        }

        /**
         * Create cached input sources for all input needed by {@code windows[winIdx]}. Calls {@code completedAction}
         * when the work is complete
         */
        private void cacheInputSources(final int winIdx, final Runnable completeAction) {
            if (inputCacheNeeded) {
                final UpdateByWindow win = windows[winIdx];
                final int[] uniqueWindowSources = win.getUniqueSourceIndices();

                jobScheduler.iterateParallel(ExecutionContext.getContextToRecord(), this, 0, uniqueWindowSources.length,
                        (idx, sourceComplete) -> {
                            createCachedColumnSource(uniqueWindowSources[idx], sourceComplete);
                        }, completeAction, this::onError);
            } else {
                // no work to do, continue
                completeAction.run();
            }
        }

        /**
         * Divide the buckets for {@code windows[winIdx]} into sets and process each set in parallel. Calls
         * {@code completedAction} when the work is complete
         */
        private void processWindowBuckets(int winIdx, final Runnable completedAction) {
            if (jobScheduler.threadCount() > 1 && dirtyBuckets.length > 1) {
                // process the buckets in parallel
                final int bucketsPerTask = Math.max(1, dirtyBuckets.length / jobScheduler.threadCount());
                final TIntArrayList offsetList = new TIntArrayList();
                final TIntArrayList countList = new TIntArrayList();

                for (int ii = 0; ii < dirtyBuckets.length; ii += bucketsPerTask) {
                    offsetList.add(ii);
                    countList.add(Math.min(bucketsPerTask, dirtyBuckets.length - ii));
                }

                jobScheduler.iterateParallel(ExecutionContext.getContextToRecord(), this, 0, offsetList.size(), idx -> {
                    final int bucketOffset = offsetList.get(idx);
                    final int bucketCount = countList.get(idx);
                    for (int bucketIdx = bucketOffset; bucketIdx < bucketOffset + bucketCount; bucketIdx++) {
                        UpdateByBucketHelper bucket = dirtyBuckets[bucketIdx];
                        bucket.assignInputSources(winIdx, maybeCachedInputSources);
                        bucket.processWindow(winIdx, initialStep);
                    }
                }, completedAction, this::onError);
            } else {
                // minimize overhead when running serially
                for (UpdateByBucketHelper bucket : dirtyBuckets) {
                    bucket.assignInputSources(winIdx, maybeCachedInputSources);
                    bucket.processWindow(winIdx, initialStep);
                }
                completedAction.run();
            }
        }

        /**
         * Process all {@code windows} in a serial manner (to minimize cache memory usage). Will create cached input
         * sources, process the buckets, then release the cached columns before starting the next window. Calls
         * {@code completedAction} when the work is complete
         */
        private void processWindows(final Runnable completeAction) {
            jobScheduler.iterateSerial(ExecutionContext.getContextToRecord(), this, 0, windows.length,
                    (winIdx, windowComplete) -> {
                        UpdateByWindow win = windows[winIdx];

                        // this is a chain of calls: cache, then shift, then process the dirty buckets for this window
                        cacheInputSources(winIdx, () -> {
                            // prepare each operator for the parallel updates to come
                            if (initialStep) {
                                // prepare the entire set of rows on the initial step
                                try (final RowSet changedRows = redirContext.isRedirected()
                                        ? RowSetFactory.flat(redirContext.requiredCapacity())
                                        : source.getRowSet().copy()) {
                                    for (UpdateByOperator op : win.getOperators()) {
                                        op.prepareForParallelPopulation(changedRows);
                                    }
                                }
                            } else {
                                // get the minimal set of rows to be updated for this window
                                try (final WritableRowSet windowRowSet = shiftedRows.copy()) {
                                    for (UpdateByBucketHelper bucket : dirtyBuckets) {
                                        if (win.isWindowDirty(bucket.windowContexts[winIdx])) {
                                            windowRowSet.insert(win.getAffectedRows(bucket.windowContexts[winIdx]));
                                        }
                                    }
                                    try (final RowSet changedRows = redirContext.isRedirected()
                                            ? redirContext.getInnerKeys(windowRowSet, sharedContext)
                                            : windowRowSet.copy()) {
                                        for (UpdateByOperator op : win.getOperators()) {
                                            op.prepareForParallelPopulation(changedRows);
                                        }
                                    }
                                }
                            }

                            if (!redirContext.isRedirected() && update.shifted().nonempty()) {
                                // shift the non-redirected output sources now, after parallelPopulation
                                try (final RowSet prevIdx = source.getRowSet().copyPrev()) {
                                    update.shifted().apply((begin, end, delta) -> {
                                        try (final RowSet subRowSet = prevIdx.subSetByKeyRange(begin, end)) {
                                            for (UpdateByOperator op : win.getOperators()) {
                                                op.applyOutputShift(subRowSet, delta);
                                            }
                                        }
                                    });
                                }
                            }

                            processWindowBuckets(winIdx, () -> {
                                if (inputCacheNeeded) {
                                    // release the cached sources that are no longer needed
                                    releaseInputSources(winIdx, maybeCachedInputSources, inputSourceRowSets,
                                            inputSourceReferenceCounts);
                                }

                                // signal that the work for this window is complete (will iterate to the next window
                                // sequentially)
                                windowComplete.run();
                            });
                        });
                    }, completeAction, this::onError);
        }

        /**
         * Clean up the resources created during this update and notify downstream if applicable. Calls
         * {@code completedAction} when the work is complete
         */
        private void cleanUpAndNotify(final Runnable completeAction) {
            try (final RowSet ignoredRs = shiftedRows;
                    final SharedContext ignoredCtx = sharedContext) {
                // auto close these resources
            }

            // create the downstream before calling finalize() on the buckets (which releases resources)
            final TableUpdate downstream;
            if (!initialStep) {
                downstream = computeDownstreamUpdate();
            } else {
                downstream = null;
            }

            // allow the helpers to release their resources
            for (UpdateByBucketHelper bucket : dirtyBuckets) {
                bucket.finalizeUpdate();
            }

            // pass the result downstream
            if (downstream != null) {
                result.notifyListeners(downstream);
            }

            completeAction.run();
        }

        /**
         * Create the update for downstream listeners. This combines all bucket updates/modifies into a unified update
         */
        private TableUpdate computeDownstreamUpdate() {
            final TableUpdateImpl downstream = new TableUpdateImpl();

            downstream.added = update.added().copy();
            downstream.removed = update.removed().copy();
            downstream.shifted = update.shifted();

            // union the modifies from all the tables (including source)
            downstream.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
            downstream.modifiedColumnSet.clear();

            // get the adds/removes/shifts from upstream, make a copy since TableUpdateImpl#reset will
            // close them with the upstream update

            WritableRowSet modifiedRowSet = RowSetFactory.empty();
            downstream.modified = modifiedRowSet;

            if (update.modified().isNonempty()) {
                modifiedRowSet.insert(update.modified());
                transformer.transform(update.modifiedColumnSet(), downstream.modifiedColumnSet);
            }

            for (UpdateByBucketHelper bucket : dirtyBuckets) {
                // retrieve the modified row and column sets from the windows
                for (int winIdx = 0; winIdx < windows.length; winIdx++) {
                    UpdateByWindow win = windows[winIdx];
                    UpdateByWindow.UpdateByWindowContext winCtx = bucket.windowContexts[winIdx];

                    if (win.isWindowDirty(winCtx)) {
                        // add the window modified rows to this set
                        modifiedRowSet.insert(win.getAffectedRows(winCtx));
                        // add the modified output column sets to the downstream set
                        final UpdateByOperator[] winOps = win.getOperators();
                        for (int winOpIdx : win.getDirtyOperators(winCtx)) {
                            // these were created directly from the result output columns so no transformer needed
                            downstream.modifiedColumnSet.setAll(winOps[winOpIdx].getOutputModifiedColumnSet());
                        }
                    }
                }

            }
            // should not include upstream adds as modifies
            modifiedRowSet.remove(downstream.added);

            return downstream;
        }

        /**
         * Process the {@link TableUpdate update} provided in the constructor. This performs much work in parallel and
         * leverages {@link JobScheduler} extensively
         */
        public void processUpdate() {
            if (redirContext.isRedirected()) {
                // this call does all the work needed for redirected output sources, for sparse output sources
                // we will process shifts only after a call to `prepareForParallelPopulation()` on each source
                redirContext.processUpdateForRedirection(update, source.getRowSet());
                shiftedRows = RowSetFactory.empty();
            } else {
                // for our sparse array output sources, we need to identify which rows will be affected by the upstream
                // shifts and include them in our parallel update preparations
                if (update.shifted().nonempty()) {
                    try (final RowSet prev = source.getRowSet().copyPrev();
                            final RowSequence.Iterator it = prev.getRowSequenceIterator()) {

                        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                        final int size = update.shifted().size();

                        // get these in ascending order and use a sequential builder
                        for (int ii = 0; ii < size; ii++) {
                            final long begin = update.shifted().getBeginRange(ii);
                            final long end = update.shifted().getEndRange(ii);
                            final long delta = update.shifted().getShiftDelta(ii);

                            it.advance(begin);
                            final RowSequence rs = it.getNextRowSequenceThrough(end);
                            builder.appendRowSequenceWithOffset(rs, delta);
                        }
                        shiftedRows = builder.build();
                    }
                } else {
                    shiftedRows = RowSetFactory.empty();
                }
            }

            // this is where we leave single-threaded calls and rely on the scheduler to continue the work. Each
            // call will chain to another until the sequence is complete
            computeCachedColumnRowsets(
                    () -> processWindows(
                            () -> cleanUpAndNotify(
                                    () -> {
                                        // signal to the main task that we have completed our work
                                        if (waitForResult != null) {
                                            waitForResult.complete(null);
                                        }
                                    })));

            if (waitForResult != null) {
                try {
                    // need to wait until this future is complete
                    waitForResult.get();
                } catch (InterruptedException e) {
                    throw new CancellationException("interrupted while processing updateBy");
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof RuntimeException) {
                        throw (RuntimeException) e.getCause();
                    } else {
                        throw new UncheckedDeephavenException("Failure while processing updateBy",
                                e.getCause());
                    }
                }
            }
        }
    }

    /**
     * The Listener for apply to the constituent table updates
     */
    class UpdateByListener extends MergedListener {
        public UpdateByListener(@Nullable String description) {
            super(UpdateBy.this.recorders, List.of(), description, UpdateBy.this.result);
        }

        @Override
        protected void process() {
            final ListenerRecorder sourceRecorder = recorders.peekFirst();
            final TableUpdate upstream = sourceRecorder.getUpdate();

            // we need to keep a reference to TableUpdate during our computation
            final StateManager sm = new StateManager(upstream.acquire(), false);
            sm.processUpdate();
        }

        @Override
        protected boolean canExecute(final long step) {

            synchronized (recorders) {
                return recorders.stream().allMatch(lr -> lr.satisfied(step));
            }
        }
    }

    public UpdateByListener newListener(@NotNull final String description) {
        return new UpdateByListener(description);
    }

    // region UpdateBy implementation

    /**
     * Apply the specified operations to each group of rows in the source table and produce a result table with the same
     * index as the source with each operator applied.
     *
     * @param source the source to apply to.
     * @param clauses the operations to apply.
     * @param byColumns the columns to group by before applying operations
     * @return a new table with the same index as the source with all the operations applied.
     */
    public static Table updateBy(@NotNull final QueryTable source,
            @NotNull final Collection<? extends UpdateByOperation> clauses,
            @NotNull final Collection<? extends ColumnName> byColumns,
            @NotNull final UpdateByControl control) {

        // create the rowRedirection if instructed
        final WritableRowRedirection rowRedirection;
        if (control.useRedirectionOrDefault()) {
            if (!source.isRefreshing()) {
                if (!source.isFlat() && SparseConstants.sparseStructureExceedsOverhead(source.getRowSet(),
                        control.maxStaticSparseMemoryOverheadOrDefault())) {
                    rowRedirection = new InverseRowRedirectionImpl(source.getRowSet());
                } else {
                    rowRedirection = null;
                }
            } else {
                final JoinControl.RedirectionType type = JoinControl.getRedirectionType(source, 4.0, true);
                switch (type) {
                    case Sparse:
                        rowRedirection = new LongColumnSourceWritableRowRedirection(new LongSparseArraySource());
                        break;
                    case Hash:
                        rowRedirection = WritableRowRedirection.FACTORY.createRowRedirection(source.intSize());
                        break;

                    default:
                        throw new IllegalStateException("Unsupported redirection type " + type);
                }
            }
        } else {
            rowRedirection = null;
        }

        // create an UpdateByRedirectionContext for use by the UpdateByBucketHelper objects
        UpdateByRedirectionContext redirContext = new UpdateByRedirectionContext(rowRedirection);

        // TODO(deephaven-core#2693): Improve UpdateByBucketHelper implementation for ColumnName
        // generate a MatchPair array for use by the existing algorithm
        MatchPair[] pairs = MatchPair.fromPairs(byColumns);

        final UpdateByOperatorFactory updateByOperatorFactory =
                new UpdateByOperatorFactory(source, pairs, redirContext, control);
        final Collection<UpdateByOperator> ops = updateByOperatorFactory.getOperators(clauses);

        final UpdateByOperator[] opArr = ops.toArray(UpdateByOperator.ZERO_LENGTH_OP_ARRAY);

        if (opArr.length == 0) {
            throw new IllegalArgumentException("At least one operator must be specified");
        }

        final StringBuilder descriptionBuilder = new StringBuilder("updateBy(ops={")
                .append(updateByOperatorFactory.describe(clauses))
                .append("}");

        String timestampColumnName = null;
        final Set<String> problems = new LinkedHashSet<>();
        // noinspection rawtypes
        final Map<String, ColumnSource<?>> opResultSources = new LinkedHashMap<>();
        for (int opIdx = 0; opIdx < opArr.length; opIdx++) {
            final UpdateByOperator op = opArr[opIdx];
            op.getOutputColumns().forEach((name, col) -> {
                if (opResultSources.putIfAbsent(name, col) != null) {
                    problems.add(name);
                }
            });
            // verify zero or one timestamp column names
            if (op.getTimestampColumnName() != null) {
                if (timestampColumnName == null) {
                    timestampColumnName = op.getTimestampColumnName();
                } else {
                    if (!timestampColumnName.equals(op.getTimestampColumnName())) {
                        throw new UncheckedTableException(
                                "Cannot reference more than one timestamp source on a single UpdateByBucketHelper operation {"
                                        +
                                        timestampColumnName + ", " + op.getTimestampColumnName() + "}");
                    }
                }
            }

        }

        // the next bit is complicated but the goal is simple. We don't want to have duplicate input column sources, so
        // we will store each one only once in inputSources and setup some mapping from the opIdx to the input column.
        final ArrayList<ColumnSource<?>> inputSourceList = new ArrayList<>();
        final int[][] operatorInputSourceSlotArr = new int[opArr.length][];
        final TObjectIntHashMap<ChunkSource<Values>> sourceToSlotMap = new TObjectIntHashMap<>();

        for (int opIdx = 0; opIdx < opArr.length; opIdx++) {
            final String[] inputColumnNames = opArr[opIdx].getInputColumnNames();
            for (int colIdx = 0; colIdx < inputColumnNames.length; colIdx++) {
                final ColumnSource<?> input = source.getColumnSource(inputColumnNames[colIdx]);
                final int maybeExistingSlot = sourceToSlotMap.get(input);
                // add a new entry for this operator
                operatorInputSourceSlotArr[opIdx] = new int[inputColumnNames.length];
                if (maybeExistingSlot == sourceToSlotMap.getNoEntryValue()) {
                    int srcIdx = inputSourceList.size();
                    // create a new input source and map the operator to it
                    inputSourceList.add(ReinterpretUtils.maybeConvertToPrimitive(input));
                    sourceToSlotMap.put(input, srcIdx);
                    operatorInputSourceSlotArr[opIdx][colIdx] = srcIdx;
                } else {
                    operatorInputSourceSlotArr[opIdx][colIdx] = maybeExistingSlot;
                }
            }
        }
        final ColumnSource<?>[] inputSourceArr = inputSourceList.toArray(new ColumnSource<?>[0]);

        // now we want to divide the operators into similar windows for efficient processing
        TIntObjectHashMap<TIntArrayList> windowHashToOperatorIndicesMap = new TIntObjectHashMap<>();

        for (int opIdx = 0; opIdx < opArr.length; opIdx++) {
            int hash = UpdateByWindow.hashCodeFromOperator(opArr[opIdx]);
            boolean added = false;

            // rudimentary collision detection and handling
            while (!added) {
                if (!windowHashToOperatorIndicesMap.containsKey(hash)) {
                    // does not exist, can add immediately
                    windowHashToOperatorIndicesMap.put(hash, new TIntArrayList());
                    windowHashToOperatorIndicesMap.get(hash).add(opIdx);
                    added = true;
                } else {
                    final int existingOpIdx = windowHashToOperatorIndicesMap.get(hash).get(0);
                    if (UpdateByWindow.isEquivalentWindow(opArr[existingOpIdx], opArr[opIdx])) {
                        // no collision, can add immediately
                        windowHashToOperatorIndicesMap.get(hash).add(opIdx);
                        added = true;
                    } else {
                        // there is a collision, increment hash and try again
                        hash++;
                    }
                }
            }
        }
        // store the operators into the windows
        final UpdateByWindow[] windowArr = new UpdateByWindow[windowHashToOperatorIndicesMap.size()];
        final MutableInt winIdx = new MutableInt(0);

        windowHashToOperatorIndicesMap.forEachEntry((final int hash, final TIntArrayList opIndices) -> {
            final UpdateByOperator[] windowOperators = new UpdateByOperator[opIndices.size()];
            final int[][] windowOperatorSourceSlots = new int[opIndices.size()][];

            for (int ii = 0; ii < opIndices.size(); ii++) {
                final int opIdx = opIndices.get(ii);
                windowOperators[ii] = opArr[opIdx];
                windowOperatorSourceSlots[ii] = operatorInputSourceSlotArr[opIdx];
            }
            windowArr[winIdx.getAndIncrement()] =
                    UpdateByWindow.createFromOperatorArray(windowOperators, windowOperatorSourceSlots);
            return true;
        });

        // noinspection rawtypes
        final Map<String, ColumnSource<?>> resultSources = new LinkedHashMap<>(source.getColumnSourceMap());
        resultSources.putAll(opResultSources);

        if (pairs.length == 0) {
            descriptionBuilder.append(")");
            Table ret = ZeroKeyUpdateByManager.compute(
                    descriptionBuilder.toString(),
                    source,
                    opArr,
                    windowArr,
                    inputSourceArr,
                    operatorInputSourceSlotArr,
                    resultSources,
                    timestampColumnName,
                    redirContext,
                    control);

            if (source.isRefreshing()) {
                // start tracking previous values
                if (rowRedirection != null) {
                    rowRedirection.startTrackingPrevValues();
                }
                ops.forEach(UpdateByOperator::startTrackingPrev);
            }
            return ret;
        }

        descriptionBuilder.append(", pairs={").append(MatchPair.matchString(pairs)).append("})");

        for (final MatchPair byColumn : pairs) {
            if (!source.hasColumns(byColumn.rightColumn)) {
                problems.add(byColumn.rightColumn);
                continue;
            }
        }

        if (!problems.isEmpty()) {
            throw new UncheckedTableException(descriptionBuilder + ": Missing byColumns in parent table {" +
                    String.join(", ", problems) + "}");
        }

        Table ret = BucketedPartitionedUpdateByManager.compute(
                descriptionBuilder.toString(),
                source,
                opArr,
                windowArr,
                inputSourceArr,
                operatorInputSourceSlotArr,
                resultSources,
                byColumns,
                timestampColumnName,
                redirContext,
                control);

        if (source.isRefreshing()) {
            // start tracking previous values
            if (rowRedirection != null) {
                rowRedirection.startTrackingPrevValues();
            }
            ops.forEach(UpdateByOperator::startTrackingPrev);
        }
        return ret;
    }
    // endregion
}
