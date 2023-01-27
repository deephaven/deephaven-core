package io.deephaven.engine.table.impl.updateby;

import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ResettableWritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.sources.sparse.SparseConstants;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedQueue;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.SoftReference;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

/**
 * The core of the {@link Table#updateBy(UpdateByControl, Collection, Collection)} operation.
 */
public abstract class UpdateBy {
    /** When caching a column source, how many rows should we process in each parallel batch? (1M default) */
    private static final int PARALLEL_CACHE_BATCH_SIZE =
            Configuration.getInstance().getIntegerWithDefault("UpdateBy.parallelCacheBatchSize", 1 << 20);
    /** When caching a column source, what size chunks should be used to move data to the cache? (64K default) */
    private static final int PARALLEL_CACHE_CHUNK_SIZE =
            Configuration.getInstance().getIntegerWithDefault("UpdateBy.parallelCacheChunkSize", 1 << 16);

    /** When extracting keys from the redirection, what size chunks to use? (2K default) */
    private static final int REDIRECTION_CHUNK_SIZE = 1 << 11;

    /** Input sources may be reused by multiple operators, only store and cache unique ones (post-reinterpret) */
    protected final ColumnSource<?>[] inputSources;
    /** All the operators for this UpdateBy manager */
    protected final UpdateByOperator[] operators;
    /** All the windows for this UpdateBy manager */
    protected final UpdateByWindow[] windows;
    /** The source table for the UpdateBy operators */
    protected final QueryTable source;
    /** Helper class for maintaining the RowRedirection when using redirected output sources */
    protected final UpdateByRedirectionHelper redirHelper;
    /** User control to specify UpdateBy parameters */
    protected final UpdateByControl control;
    /** The single timestamp column used by all time-based operators */
    protected final String timestampColumnName;
    /** Whether caching benefits this UpdateBy operation */
    protected final boolean inputCacheNeeded;
    /** Whether caching benefits each input source */
    protected final boolean[] inputSourceCacheNeeded;
    /**
     * References to the dense array sources we are using for the cached sources. It's expected that these will be
     * released and need to be created.
     */
    protected final SoftReference<WritableColumnSource<?>>[] inputSourceCaches;
    /** For easy iteration, create a list of the source indices that need to be cached */
    protected final int[] cacheableSourceIndices;

    /** ColumnSet transformer from source to downstream */
    protected ModifiedColumnSet.Transformer transformer;

    /** Listener to react to upstream changes to refreshing source tables */
    protected UpdateByListener listener;

    /** Store every bucket in this list for processing */
    protected final IntrusiveDoublyLinkedQueue<UpdateByBucketHelper> buckets;

    static class UpdateByRedirectionHelper {
        @Nullable
        private final WritableRowRedirection rowRedirection;
        private final WritableRowSet freeRows;
        private long maxInnerRowKey;

        private UpdateByRedirectionHelper(@Nullable final WritableRowRedirection rowRedirection) {
            this.rowRedirection = rowRedirection;
            this.freeRows = rowRedirection == null ? null : RowSetFactory.empty().toTracking();
            this.maxInnerRowKey = 0;
        }

        boolean isRedirected() {
            return rowRedirection != null;
        }

        private long requiredCapacity() {
            return maxInnerRowKey;
        }

        /**
         * Process the upstream {@link TableUpdate update} and return the rowset of dense keys that need cleared for
         * Object array sources
         */
        private WritableRowSet processUpdateForRedirection(@NotNull final TableUpdate upstream,
                final TrackingRowSet sourceRowSet) {
            assert rowRedirection != null;

            final WritableRowSet toClear;

            if (upstream.removed().isNonempty()) {
                final RowSetBuilderRandom freeBuilder = RowSetFactory.builderRandom();
                upstream.removed().forAllRowKeys(key -> freeBuilder.addKey(rowRedirection.remove(key)));
                // store all freed rows as the candidate toClear set
                toClear = freeBuilder.build();
                freeRows.insert(toClear);
            } else {
                toClear = RowSetFactory.empty();
            }

            if (upstream.shifted().nonempty()) {
                try (final WritableRowSet prevRowSetLessRemoves = sourceRowSet.copyPrev()) {
                    prevRowSetLessRemoves.remove(upstream.removed());
                    rowRedirection.applyShift(prevRowSetLessRemoves, upstream.shifted());
                }
            }

            if (upstream.added().isNonempty()) {
                final WritableRowSet.Iterator freeIt = freeRows.iterator();
                upstream.added().forAllRowKeys(outerKey -> {
                    final long innerKey = freeIt.hasNext() ? freeIt.nextLong() : maxInnerRowKey++;
                    rowRedirection.put(outerKey, innerKey);
                });
                if (freeIt.hasNext()) {
                    try (final RowSet added = freeRows.subSetByKeyRange(0, freeIt.nextLong() - 1)) {
                        toClear.remove(added);
                        freeRows.remove(added);
                    }
                } else {
                    toClear.clear();
                    freeRows.clear();
                }
            }
            return toClear;
        }

        private RowSet getInnerKeys(final RowSet outerKeys) {
            assert (rowRedirection != null);
            RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            final int chunkSize = Math.min(outerKeys.intSize(), REDIRECTION_CHUNK_SIZE);
            try (final RowSequence.Iterator it = outerKeys.getRowSequenceIterator();
                    ChunkSource.GetContext getContext = rowRedirection.makeGetContext(chunkSize)) {
                while (it.hasMore()) {
                    final RowSequence rs = it.getNextRowSequenceWithLength(chunkSize);
                    Chunk<? extends RowKeys> chunk = rowRedirection.getChunk(getContext, rs);
                    builder.addRowKeysChunk(chunk.asLongChunk());
                }
            }
            return builder.build();
        }
    }

    protected UpdateBy(
            @NotNull final QueryTable source,
            @NotNull final UpdateByOperator[] operators,
            @NotNull final UpdateByWindow[] windows,
            @NotNull final ColumnSource<?>[] inputSources,
            @Nullable String timestampColumnName,
            @Nullable final WritableRowRedirection rowRedirection,
            @NotNull final UpdateByControl control) {

        if (operators.length == 0) {
            throw new IllegalArgumentException("At least one operator must be specified");
        }

        this.source = source;
        this.operators = operators;
        this.windows = windows;
        this.inputSources = inputSources;
        this.timestampColumnName = timestampColumnName;
        this.redirHelper = new UpdateByRedirectionHelper(rowRedirection);
        this.control = control;

        this.inputSourceCacheNeeded = new boolean[inputSources.length];
        cacheableSourceIndices = IntStream.range(0, inputSources.length)
                .filter(ii -> !FillUnordered.providesFillUnordered(inputSources[ii]))
                .peek(ii -> inputSourceCacheNeeded[ii] = true)
                .toArray();
        inputCacheNeeded = cacheableSourceIndices.length > 0;

        inputSourceCaches = new SoftReference[inputSources.length];

        buckets =
                new IntrusiveDoublyLinkedQueue<>(IntrusiveDoublyLinkedNode.Adapter.<UpdateByBucketHelper>getInstance());
    }


    /** Release the input sources that will not be needed for the rest of this update */
    private void releaseInputSources(int winIdx, ColumnSource<?>[] maybeCachedInputSources,
            WritableRowSet[] inputSourceRowSets, int[] inputSourceReferenceCounts) {

        final UpdateByWindow win = windows[winIdx];
        final int[] uniqueWindowSources = win.getUniqueSourceIndices();

        try (final ResettableWritableObjectChunk<?, ?> backingChunk =
                ResettableWritableObjectChunk.makeResettableChunk()) {
            for (int srcIdx : uniqueWindowSources) {
                if (!inputSourceCacheNeeded[srcIdx]) {
                    continue;
                }

                if (--inputSourceReferenceCounts[srcIdx] == 0) {
                    // release any objects we are holding in the cache
                    if (maybeCachedInputSources[srcIdx] instanceof ObjectArraySource) {
                        final long targetCapacity = inputSourceRowSets[srcIdx].size();
                        for (long positionToNull = 0; positionToNull < targetCapacity; positionToNull +=
                                backingChunk.size()) {
                            ((ObjectArraySource<?>) maybeCachedInputSources[srcIdx])
                                    .resetWritableChunkToBackingStore(backingChunk, positionToNull);
                            backingChunk.fillWithNullValue(0, backingChunk.size());
                        }
                    }

                    // release the row set
                    inputSourceRowSets[srcIdx].close();
                    inputSourceRowSets[srcIdx] = null;

                    maybeCachedInputSources[srcIdx] = null;
                }
            }
        }
    }

    /**
     * Overview of work performed by {@link PhasedUpdateProcessor}:
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
     * into sets (N/num_threads) and running a job for each bucket_set</li>
     * <li>When all buckets processed, release the input source caches that will not be re-used later</li>
     * </ul>
     * </li>
     * <li>When all windows processed, create the downstream update and notify</li>
     * <li>Release resources</li>
     * </ol>
     */

    class PhasedUpdateProcessor implements LogOutputAppendable {
        final TableUpdate upstream;
        final boolean initialStep;
        final UpdateByBucketHelper[] dirtyBuckets;
        final boolean[] dirtyWindows;

        /** The active set of sources to use for processing, each source may be cached or original */
        final ColumnSource<?>[] maybeCachedInputSources;
        /** For cacheable sources, the minimal rowset to cache (union of bucket influencer rows) */
        final WritableRowSet[] inputSourceRowSets;
        /** For cacheable sources, track how many windows require this source */
        final int[] inputSourceReferenceCounts;

        final JobScheduler jobScheduler;
        final CompletableFuture<Void> waitForResult;

        /***
         * These rows will be changed because of shifts or removes and will need to be included in
         * {@code prepareForParallelPopulation()} calls
         */
        WritableRowSet changedRows;
        /***
         * These rows will be unused after this cycle and Object columns should NULL these keys
         */
        WritableRowSet toClear;

        PhasedUpdateProcessor(TableUpdate upstream, boolean initialStep) {
            this.upstream = upstream;
            this.initialStep = initialStep;

            // determine which buckets we'll examine during this update
            dirtyBuckets = buckets.stream().filter(UpdateByBucketHelper::isDirty).toArray(UpdateByBucketHelper[]::new);
            // which windows are dirty and need to be computed this cycle
            dirtyWindows = new boolean[windows.length];

            if (inputCacheNeeded) {
                maybeCachedInputSources = new ColumnSource[inputSources.length];
                inputSourceRowSets = new WritableRowSet[inputSources.length];
                inputSourceReferenceCounts = new int[inputSources.length];

                // set the uncacheable columns into the array
                for (int ii = 0; ii < inputSources.length; ii++) {
                    maybeCachedInputSources[ii] = inputSourceCacheNeeded[ii] ? null : inputSources[ii];
                }
            } else {
                maybeCachedInputSources = inputSources;
                inputSourceRowSets = null;
                inputSourceReferenceCounts = null;
            }

            if (initialStep) {
                if (OperationInitializationThreadPool.NUM_THREADS > 1
                        && !OperationInitializationThreadPool.isInitializationThread()) {
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
        }

        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput.append("UpdateBy.PhasedUpdateProcessor");
        }

        private void onError(Exception error) {
            if (waitForResult != null) {
                // signal to the future that an exception has occurred
                waitForResult.completeExceptionally(error);
            } else {
                cleanUpAfterError();
                // this is part of an update, need to notify downstream
                result().notifyListenersOnError(error, null);
            }
        }

        /**
         * Accumulate in parallel the dirty bucket rowsets for the cacheable input sources. Calls
         * {@code completedAction} when the work is complete
         */
        private void computeCachedColumnRowsets(final Runnable resumeAction) {
            if (!inputCacheNeeded) {
                resumeAction.run();
                return;
            }

            // initially everything is dirty so cache everything
            if (initialStep) {
                for (int srcIdx : cacheableSourceIndices) {
                    if (inputSourceCacheNeeded[srcIdx]) {
                        // create a RowSet to be used by `InverseWrappedRowSetWritableRowRedirection`
                        inputSourceRowSets[srcIdx] = source.getRowSet().copy();

                        // record how many windows require this input source
                        inputSourceReferenceCounts[srcIdx] =
                                (int) Arrays.stream(windows).filter(win -> win.isSourceInUse(srcIdx)).count();
                    }
                }
                Arrays.fill(dirtyWindows, true);
                resumeAction.run();
                return;
            }

            jobScheduler.iterateParallel(ExecutionContext.getContextToRecord(), this, JobScheduler.JobContext::new,
                    0, cacheableSourceIndices.length,
                    (context, idx) -> {
                        final int srcIdx = cacheableSourceIndices[idx];
                        for (int winIdx = 0; winIdx < windows.length; winIdx++) {
                            UpdateByWindow win = windows[winIdx];
                            if (win.isSourceInUse(srcIdx)) {
                                boolean srcNeeded = false;
                                for (UpdateByBucketHelper bucket : dirtyBuckets) {
                                    UpdateByWindow.UpdateByWindowBucketContext winCtx = bucket.windowContexts[winIdx];

                                    if (win.isWindowDirty(winCtx)) {
                                        // add this rowset to the running total for this input source
                                        if (inputSourceRowSets[srcIdx] == null) {
                                            inputSourceRowSets[srcIdx] =
                                                    win.getInfluencerRows(winCtx).copy();
                                        } else {
                                            inputSourceRowSets[srcIdx].insert(win.getInfluencerRows(winCtx));
                                        }
                                        // at least one dirty bucket will need this source
                                        srcNeeded = true;
                                        // this window must be computed
                                        dirtyWindows[winIdx] = true;
                                    }
                                }
                                if (srcNeeded) {
                                    inputSourceReferenceCounts[srcIdx]++;
                                }
                            }
                        }
                    },
                    resumeAction,
                    this::onError);
        }

        /**
         * Create a new input source cache and populate the required rows in parallel. Calls {@code completedAction}
         * when the work is complete
         */
        private void createCachedColumnSource(int srcIdx, final Runnable resumeAction) {
            if (maybeCachedInputSources[srcIdx] != null || inputSourceRowSets[srcIdx] == null) {
                // already cached from another operator (or caching not needed)
                resumeAction.run();
                return;
            }

            final ColumnSource<?> inputSource = inputSources[srcIdx];
            final WritableRowSet inputRowSet = inputSourceRowSets[srcIdx];

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
            final WritableRowRedirection rowRedirection = new InverseWrappedRowSetWritableRowRedirection(inputRowSet);
            final WritableColumnSource<?> outputSource =
                    WritableRedirectedColumnSource.maybeRedirect(rowRedirection, innerSource, 0);

            // holding this reference should protect `rowDirection` and `innerSource` from GC
            maybeCachedInputSources[srcIdx] = outputSource;

            // how many batches do we need?
            final int taskCount =
                    Math.toIntExact((inputRowSet.size() + PARALLEL_CACHE_BATCH_SIZE - 1) / PARALLEL_CACHE_BATCH_SIZE);

            final class BatchContext extends JobScheduler.JobContext {
                final RowSequence.Iterator rsIt = inputRowSet.getRowSequenceIterator();
                final ChunkSink.FillFromContext ffc =
                        outputSource.makeFillFromContext(PARALLEL_CACHE_CHUNK_SIZE);
                final ChunkSource.GetContext gc =
                        inputSource.makeGetContext(PARALLEL_CACHE_CHUNK_SIZE);

                @Override
                public void close() {
                    SafeCloseable.closeArray(rsIt, ffc, gc);
                }
            }

            jobScheduler.iterateParallel(ExecutionContext.getContextToRecord(), this, BatchContext::new,
                    0, taskCount,
                    (ctx, idx) -> {
                        // advance to the first key of this block
                        ctx.rsIt.advance(inputRowSet.get(idx * PARALLEL_CACHE_BATCH_SIZE));
                        int remaining = PARALLEL_CACHE_BATCH_SIZE;
                        while (ctx.rsIt.hasMore() && remaining > 0) {
                            final RowSequence chunkOk = ctx.rsIt
                                    .getNextRowSequenceWithLength(Math.min(remaining, PARALLEL_CACHE_CHUNK_SIZE));
                            final Chunk<? extends Values> values = inputSource.getChunk(ctx.gc, chunkOk);
                            outputSource.fillFromChunk(ctx.ffc, values, chunkOk);

                            // reduce by the attempted stride, if this is the final block the iterator will
                            // be exhausted and hasMore() will return false
                            remaining -= PARALLEL_CACHE_CHUNK_SIZE;
                        }
                    }, resumeAction,
                    this::onError);
        }

        /**
         * Create cached input sources for all input needed by {@code windows[winIdx]}. Calls {@code completedAction}
         * when the work is complete
         */
        private void cacheInputSources(final int winIdx, final Runnable resumeAction) {
            if (inputCacheNeeded && dirtyWindows[winIdx]) {
                final UpdateByWindow win = windows[winIdx];
                final int[] uniqueWindowSources = win.getUniqueSourceIndices();

                jobScheduler.iterateParallel(ExecutionContext.getContextToRecord(), this, JobScheduler.JobContext::new,
                        0, uniqueWindowSources.length,
                        (context, idx, sourceComplete) -> createCachedColumnSource(uniqueWindowSources[idx],
                                sourceComplete),
                        resumeAction, this::onError);
            } else {
                // no work to do, continue
                resumeAction.run();
            }
        }

        /**
         * Process each bucket in {@code windows[winIdx]} in parallel. Calls {@code resumeAction} when the work is
         * complete
         */
        private void processWindowBuckets(int winIdx, final Runnable resumeAction) {
            if (jobScheduler.threadCount() > 1 && dirtyBuckets.length > 1) {
                // process the buckets in parallel
                jobScheduler.iterateParallel(ExecutionContext.getContextToRecord(), this, JobScheduler.JobContext::new,
                        0, dirtyBuckets.length,
                        (context, bucketIdx) -> {
                            UpdateByBucketHelper bucket = dirtyBuckets[bucketIdx];
                            bucket.assignInputSources(winIdx, maybeCachedInputSources);
                            bucket.processWindow(winIdx, initialStep);
                        }, resumeAction, this::onError);
            } else {
                // minimize overhead when running serially
                for (UpdateByBucketHelper bucket : dirtyBuckets) {
                    bucket.assignInputSources(winIdx, maybeCachedInputSources);
                    bucket.processWindow(winIdx, initialStep);
                }
                resumeAction.run();
            }
        }

        /**
         * Process all {@code windows} in a serial manner (to minimize cache memory usage and to protect against races
         * to fill the cached input sources). Will create cached input sources, process the buckets, then release the
         * cached columns before starting the next window. Calls {@code completedAction} when the work is complete
         */
        private void processWindows(final Runnable resumeAction) {
            jobScheduler.iterateSerial(ExecutionContext.getContextToRecord(), this, JobScheduler.JobContext::new, 0,
                    windows.length,
                    (context, winIdx, windowComplete) -> {
                        UpdateByWindow win = windows[winIdx];

                        // this is a chain of calls: cache, then shift, then process the dirty buckets for this window
                        cacheInputSources(winIdx, () -> {
                            // prepare each operator for the parallel updates to come
                            if (initialStep) {
                                // prepare the entire set of rows on the initial step
                                try (final RowSet changedRows = redirHelper.isRedirected()
                                        ? RowSetFactory.flat(redirHelper.requiredCapacity())
                                        : source.getRowSet().copy()) {
                                    win.prepareForParallelPopulation(changedRows);
                                }
                            } else {
                                // get the minimal set of rows to be updated for this window (shiftedRows is empty when
                                // using redirection)
                                try (final WritableRowSet windowRowSet = changedRows.copy()) {
                                    for (UpdateByBucketHelper bucket : dirtyBuckets) {
                                        if (win.isWindowDirty(bucket.windowContexts[winIdx])) {
                                            windowRowSet.insert(win.getAffectedRows(bucket.windowContexts[winIdx]));
                                        }
                                    }
                                    try (final RowSet windowChangedRows = redirHelper.isRedirected()
                                            ? redirHelper.getInnerKeys(windowRowSet)
                                            : null) {
                                        final RowSet rowsToUse =
                                                windowChangedRows == null ? windowRowSet : windowChangedRows;
                                        win.prepareForParallelPopulation(rowsToUse);
                                    }
                                }
                            }

                            if (!redirHelper.isRedirected() && upstream.shifted().nonempty()) {
                                // shift the non-redirected output sources now, after parallelPopulation
                                try (final RowSet prevIdx = source.getRowSet().copyPrev()) {
                                    upstream.shifted().apply((begin, end, delta) -> {
                                        try (final RowSet subRowSet = prevIdx.subSetByKeyRange(begin, end)) {
                                            for (UpdateByOperator op : win.getOperators()) {
                                                op.applyOutputShift(subRowSet, delta);
                                            }
                                        }
                                    });
                                }
                            }

                            if (dirtyWindows[winIdx]) {
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
                            } else {
                                windowComplete.run();
                            }
                        });
                    }, resumeAction, this::onError);
        }

        /**
         * Clean up the resources created during this update and notify downstream if applicable. Calls
         * {@code completedAction} when the work is complete
         */
        private void cleanUpAndNotify(final Runnable resumeAction) {
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
                result().notifyListeners(downstream);
            }

            // clear the sparse output columns for rows that no longer exist
            if (!initialStep && !redirHelper.isRedirected() && !toClear.isEmpty()) {
                for (UpdateByOperator op : operators) {
                    op.clearOutputRows(toClear);
                }
            }

            try (final RowSet ignoredRs = changedRows;
                    final RowSet ignoredRs2 = toClear) {
                // auto close these resources
            }
            resumeAction.run();
        }

        /**
         * Clean up the resources created during this update.
         */
        private void cleanUpAfterError() {
            try (final RowSet ignoredRs = changedRows;
                    final RowSet ignoredRs2 = toClear) {
                // auto close these resources
            }

            // allow the helpers to release their resources
            for (UpdateByBucketHelper bucket : dirtyBuckets) {
                bucket.finalizeUpdate();
            }
        }

        /**
         * Create the update for downstream listeners. This combines all bucket updates/modifies into a unified update
         */
        private TableUpdate computeDownstreamUpdate() {
            final TableUpdateImpl downstream = new TableUpdateImpl();

            // get the adds/removes/shifts from upstream, make a copy since TableUpdateImpl#reset will
            // close them with the upstream update
            downstream.added = upstream.added().copy();
            downstream.removed = upstream.removed().copy();
            downstream.shifted = upstream.shifted();

            // union the modifies from all the tables (including source)
            downstream.modifiedColumnSet = result().getModifiedColumnSetForUpdates();
            downstream.modifiedColumnSet.clear();

            WritableRowSet modifiedRowSet = upstream.modified().copy();
            downstream.modified = modifiedRowSet;

            if (upstream.modified().isNonempty()) {
                transformer.transform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);
            }

            for (UpdateByBucketHelper bucket : dirtyBuckets) {
                // retrieve the modified row and column sets from the windows
                for (int winIdx = 0; winIdx < windows.length; winIdx++) {
                    UpdateByWindow win = windows[winIdx];
                    UpdateByWindow.UpdateByWindowBucketContext winCtx = bucket.windowContexts[winIdx];

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
            if (redirHelper.isRedirected()) {
                // this call does all the work needed for redirected output sources, returns the set of rows we need
                // to clear from our Object array output sources
                toClear = redirHelper.processUpdateForRedirection(upstream, source.getRowSet());
                changedRows = RowSetFactory.empty();

                // clear them now and let them set their own prev states
                if (!initialStep && !toClear.isEmpty()) {
                    for (UpdateByOperator op : operators) {
                        op.clearOutputRows(toClear);
                    }
                }
            } else {
                // identify which rows we need to clear in our Object columns (actual clearing will be performed later)
                toClear = source.getRowSet().copyPrev();
                toClear.remove(source.getRowSet());

                // for our sparse array output sources, we need to identify which rows will be affected by the upstream
                // shifts and include them in our parallel update preparations
                if (upstream.shifted().nonempty()) {
                    try (final RowSet prev = source.getRowSet().copyPrev();
                            final RowSequence.Iterator it = prev.getRowSequenceIterator()) {

                        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                        final int size = upstream.shifted().size();

                        // get these in ascending order and use a sequential builder
                        for (int ii = 0; ii < size; ii++) {
                            final long begin = upstream.shifted().getBeginRange(ii);
                            final long end = upstream.shifted().getEndRange(ii);
                            final long delta = upstream.shifted().getShiftDelta(ii);

                            it.advance(begin);
                            final RowSequence rs = it.getNextRowSequenceThrough(end);
                            builder.appendRowSequenceWithOffset(rs, delta);
                        }
                        changedRows = builder.build();
                    }
                } else {
                    changedRows = RowSetFactory.empty();
                }
                // include the cleared rows in the calls to `prepareForParallelPopulation()`
                changedRows.insert(toClear);
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
                    cleanUpAfterError();
                    throw new CancellationException("interrupted while processing updateBy");
                } catch (ExecutionException e) {
                    cleanUpAfterError();
                    if (e.getCause() instanceof RuntimeException) {
                        throw (RuntimeException) e.getCause();
                    } else {
                        // rethrow the error
                        throw new UncheckedDeephavenException("Failure while processing updateBy",
                                e.getCause());
                    }
                }
            }
        }
    }

    /**
     * The Listener that is called when all input tables (source and constituent) are satisfied. This listener will
     * initiate UpdateBy operator processing in parallel by bucket
     */
    class UpdateByListener extends InstrumentedTableUpdateListenerAdapter {
        public UpdateByListener(@Nullable String description) {
            super(description, UpdateBy.this.source, false);
        }

        @Override
        public void onUpdate(final TableUpdate upstream) {
            final PhasedUpdateProcessor sm = new PhasedUpdateProcessor(upstream.acquire(), false);
            sm.processUpdate();
        }

        @Override
        public boolean canExecute(final long step) {
            return upstreamSatisfied(step);
        }
    }

    public UpdateByListener newUpdateByListener(@NotNull final String description) {
        return new UpdateByListener(description);
    }

    protected abstract QueryTable result();

    protected abstract boolean upstreamSatisfied(final long step);

    // region UpdateBy implementation

    /**
     * Apply the specified operations to each group of rows in the source table and produce a result table with the same
     * row set as the source with each operator applied.
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
                    rowRedirection = new InverseWrappedRowSetWritableRowRedirection(source.getRowSet());
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

        // TODO(deephaven-core#2693): Improve UpdateByBucketHelper implementation for ColumnName
        // generate a MatchPair array for use by the existing algorithm
        MatchPair[] pairs = MatchPair.fromPairs(byColumns);

        final UpdateByOperatorFactory updateByOperatorFactory =
                new UpdateByOperatorFactory(source, pairs, rowRedirection, control);
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
        final Map<String, ColumnSource<?>> opResultSources = new LinkedHashMap<>();
        for (final UpdateByOperator op : opArr) {
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
                                "Cannot reference more than one timestamp source on a single UpdateBy call {"
                                        + timestampColumnName + ", " + op.getTimestampColumnName() + "}");
                    }
                }
            }
        }

        if (!problems.isEmpty()) {
            throw new UncheckedTableException(descriptionBuilder + ": resulting column names must be unique {" +
                    String.join(", ", problems) + "}");
        }

        // We will divide the operators into similar windows for efficient processing.
        final KeyedObjectHashMap<UpdateByOperator, List<UpdateByOperator>> windowMap =
                new KeyedObjectHashMap<>(new KeyedObjectKey<>() {
                    @Override
                    public UpdateByOperator getKey(List<UpdateByOperator> updateByOperators) {
                        return updateByOperators.get(0);
                    }

                    @Override
                    public int hashKey(UpdateByOperator updateByOperator) {
                        return UpdateByWindow.hashCodeFromOperator(updateByOperator);
                    }

                    @Override
                    public boolean equalKey(UpdateByOperator updateByOperator,
                            List<UpdateByOperator> updateByOperators) {
                        return UpdateByWindow.isEquivalentWindow(updateByOperator, updateByOperators.get(0));
                    }
                });
        for (UpdateByOperator updateByOperator : opArr) {
            final MutableBoolean created = new MutableBoolean(false);
            final List<UpdateByOperator> opList = windowMap.putIfAbsent(updateByOperator,
                    (newOpListOp) -> {
                        final List<UpdateByOperator> newOpList = new ArrayList<>();
                        newOpList.add(newOpListOp);
                        created.setTrue();
                        return newOpList;
                    });
            if (!created.booleanValue()) {
                opList.add(updateByOperator);
            }
        }

        // make the windows and create unique input sources for all the window operators

        final ArrayList<ColumnSource<?>> inputSourceList = new ArrayList<>();
        final TObjectIntHashMap<ChunkSource<Values>> sourceToSlotMap = new TObjectIntHashMap<>();

        final UpdateByWindow[] windowArr = windowMap.values().stream().map((final List<UpdateByOperator> opList) -> {
            // build an array from the operator indices
            UpdateByOperator[] windowOps = new UpdateByOperator[opList.size()];
            // local map of operators indices to input source indices
            final int[][] windowOpSourceSlots = new int[opList.size()][];

            // do the mapping from operator input sources to unique input sources
            for (int idx = 0; idx < opList.size(); idx++) {
                final UpdateByOperator localOp = opList.get(idx);
                // store this operator into an array for window creation
                windowOps[idx] = localOp;
                // iterate over each input column and map this operator to unique source
                final String[] inputColumnNames = localOp.getInputColumnNames();
                windowOpSourceSlots[idx] = new int[inputColumnNames.length];
                for (int colIdx = 0; colIdx < inputColumnNames.length; colIdx++) {
                    final ColumnSource<?> input = source.getColumnSource(inputColumnNames[colIdx]);
                    final int maybeExistingSlot = sourceToSlotMap.get(input);
                    if (maybeExistingSlot == sourceToSlotMap.getNoEntryValue()) {
                        // create a new input source
                        final int srcIdx = inputSourceList.size();
                        inputSourceList.add(ReinterpretUtils.maybeConvertToPrimitive(input));
                        sourceToSlotMap.put(input, srcIdx);
                        // map the window operator indices to this new source
                        windowOpSourceSlots[idx][colIdx] = srcIdx;
                    } else {
                        // map the window indices to this existing source
                        windowOpSourceSlots[idx][colIdx] = maybeExistingSlot;
                    }
                }
            }
            return UpdateByWindow.createFromOperatorArray(windowOps, windowOpSourceSlots);
        }).toArray(UpdateByWindow[]::new);
        final ColumnSource<?>[] inputSourceArr = inputSourceList.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);

        final Map<String, ColumnSource<?>> resultSources = new LinkedHashMap<>(source.getColumnSourceMap());
        resultSources.putAll(opResultSources);

        if (pairs.length == 0) {
            descriptionBuilder.append(")");
            final ZeroKeyUpdateByManager zkm = new ZeroKeyUpdateByManager(
                    descriptionBuilder.toString(),
                    opArr,
                    windowArr,
                    inputSourceArr,
                    source,
                    resultSources,
                    timestampColumnName,
                    rowRedirection,
                    control);

            if (source.isRefreshing()) {
                // start tracking previous values
                if (rowRedirection != null) {
                    rowRedirection.startTrackingPrevValues();
                }
                ops.forEach(UpdateByOperator::startTrackingPrev);
            }
            return zkm.result;
        }

        descriptionBuilder.append(", pairs={").append(MatchPair.matchString(pairs)).append("})");

        for (final MatchPair byColumn : pairs) {
            if (!source.hasColumns(byColumn.rightColumn)) {
                problems.add(byColumn.rightColumn);
            }
        }

        if (!problems.isEmpty()) {
            throw new UncheckedTableException(descriptionBuilder + ": Missing byColumns in parent table {" +
                    String.join(", ", problems) + "}");
        }

        final BucketedPartitionedUpdateByManager bm = new BucketedPartitionedUpdateByManager(
                descriptionBuilder.toString(),
                opArr,
                windowArr,
                inputSourceArr,
                source,
                resultSources,
                byColumns,
                timestampColumnName,
                rowRedirection,
                control);

        if (source.isRefreshing()) {
            // start tracking previous values
            if (rowRedirection != null) {
                rowRedirection.startTrackingPrevValues();
            }
            ops.forEach(UpdateByOperator::startTrackingPrev);
        }
        return bm.result;
    }
    // endregion
}
