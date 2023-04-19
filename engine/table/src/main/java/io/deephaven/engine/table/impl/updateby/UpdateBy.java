package io.deephaven.engine.table.impl.updateby;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.ColumnUpdateOperation;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ResettableWritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.CancellationException;
import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.perf.BasePerformanceEntry;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.sources.sparse.SparseConstants;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.TerminalNotification;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableArray;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedQueue;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;
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

    /** Store every bucket in this list for processing */
    protected final IntrusiveDoublyLinkedQueue<UpdateByBucketHelper> buckets;

    static class UpdateByRedirectionHelper {
        @Nullable
        private final WritableRowRedirection rowRedirection;
        private final WritableRowSet freeRows;
        private long maxInnerRowKey;

        private UpdateByRedirectionHelper(@Nullable final WritableRowRedirection rowRedirection) {
            this.rowRedirection = rowRedirection;
            // noinspection resource
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
            @NotNull final UpdateByWindow[] windows,
            @NotNull final ColumnSource<?>[] inputSources,
            @Nullable String timestampColumnName,
            @Nullable final WritableRowRedirection rowRedirection,
            @NotNull final UpdateByControl control) {

        this.source = source;
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

        // noinspection unchecked
        inputSourceCaches = new SoftReference[inputSources.length];

        buckets =
                new IntrusiveDoublyLinkedQueue<>(IntrusiveDoublyLinkedNode.Adapter.<UpdateByBucketHelper>getInstance());
    }


    /**
     * Overview of work performed by {@link PhasedUpdateProcessor}:
     * <ol>
     * <li>Create `shiftedRows`, the set of rows for the output sources that are affected by shifts</li>
     * <li>Compute a rowset for each cacheable input source identifying which rows will be needed for processing</li>
     * <li>Compute the modified rowset of output column sources and call `prepareForParallelPopulation()'</li>
     * <li>When prepareForParallelPopulation() complete, apply upstream shifts to the output sources</li>
     * <li>Process each window and operator serially
     * <ul>
     * <li>Pre-create window information for windowed operators (push/pop counts)</li>
     * <li>Cache the input sources that are needed for each window operator (in parallel by chunk of rows)</li>
     * <li>When caching is complete, process the window operator (in parallel by bucket)</li>
     * <li>When all buckets processed, release the input source caches that will not be re-used later by later
     * operators</li>
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
        final BitSet dirtyWindows;
        final BitSet[] dirtyWindowOperators;
        /** The active set of sources to use for processing, each source may be cached or original */
        final ColumnSource<?>[] maybeCachedInputSources;
        /** For cacheable sources, the minimal rowset to cache (union of bucket influencer rows) */
        final AtomicReferenceArray<WritableRowSet> inputSourceRowSets;
        /** For cacheable sources, track how many windows require this source */
        final AtomicIntegerArray inputSourceReferenceCounts;
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

            // What items need to be computed this cycle?
            dirtyBuckets = buckets.stream().filter(UpdateByBucketHelper::isDirty).toArray(UpdateByBucketHelper[]::new);
            dirtyWindows = new BitSet(windows.length);
            dirtyWindowOperators = new BitSet[windows.length];

            if (inputCacheNeeded) {
                maybeCachedInputSources = new ColumnSource[inputSources.length];
                inputSourceRowSets = new AtomicReferenceArray<>(inputSources.length);
                inputSourceReferenceCounts = new AtomicIntegerArray(inputSources.length);

                for (int ii = 0; ii < inputSources.length; ii++) {
                    // Set the uncacheable columns into the array.
                    maybeCachedInputSources[ii] = inputSourceCacheNeeded[ii] ? null : inputSources[ii];
                }
            } else {
                maybeCachedInputSources = inputSources;
                inputSourceRowSets = null;
                inputSourceReferenceCounts = null;
            }

            if (initialStep) {
                // Set all windows as dirty and need computation
                dirtyWindows.set(0, windows.length);
                for (int winIdx = 0; winIdx < windows.length; winIdx++) {
                    dirtyWindowOperators[winIdx] = new BitSet(windows[winIdx].operators.length);
                    dirtyWindowOperators[winIdx].set(0, windows[winIdx].operators.length);
                }
                // Create the proper JobScheduler for the following parallel tasks
                if (OperationInitializationThreadPool.canParallelize()) {
                    jobScheduler = new OperationInitializationPoolJobScheduler();
                } else {
                    jobScheduler = ImmediateJobScheduler.INSTANCE;
                }
                waitForResult = new CompletableFuture<>();
            } else {
                // Determine which windows need to be computed.
                for (int winIdx = 0; winIdx < windows.length; winIdx++) {
                    for (UpdateByBucketHelper bucket : dirtyBuckets) {
                        final UpdateByWindow.UpdateByWindowBucketContext bucketWindowCtx =
                                bucket.windowContexts[winIdx];
                        if (!bucketWindowCtx.isDirty) {
                            continue;
                        }
                        if (dirtyWindowOperators[winIdx] == null) {
                            dirtyWindows.set(winIdx);
                            dirtyWindowOperators[winIdx] = new BitSet(windows[winIdx].operators.length);
                        }
                        final int size = windows[winIdx].operators.length;
                        dirtyWindowOperators[winIdx].or(bucketWindowCtx.dirtyOperators);
                        if (dirtyWindowOperators[winIdx].cardinality() == size) {
                            // all are set, we can stop checking
                            break;
                        }
                    }
                }
                // Create the proper JobScheduler for the following parallel tasks
                if (UpdateGraphProcessor.DEFAULT.getUpdateThreads() > 1) {
                    jobScheduler = new UpdateGraphProcessorJobScheduler();
                } else {
                    jobScheduler = ImmediateJobScheduler.INSTANCE;
                }
                waitForResult = null;
            }
        }

        // region helper-functions
        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput.append("UpdateBy.PhasedUpdateProcessor");
        }

        private LogOutputAppendable stringToAppendable(@NotNull final String toAppend) {
            return logOutput -> logOutput.append(toAppend);
        }

        private LogOutputAppendable stringAndIndexToAppendable(@NotNull final String string, final int index) {
            return logOutput -> logOutput.append(string).append('-').append(index);
        }

        private LogOutputAppendable chainAppendables(
                @NotNull final LogOutputAppendable prefix,
                @NotNull final LogOutputAppendable toAppend) {
            return logOutput -> logOutput.append(prefix).append(toAppend);
        }
        // endregion helper-functions

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
                    forAllOperators(op -> {
                        op.clearOutputRows(toClear);
                    });
                }
            } else {
                // identify which rows we need to clear in our Object columns (actual clearing will be performed later)
                toClear = source.getRowSet().copyPrev();
                toClear.remove(source.getRowSet());

                // for our sparse array output sources, we need to identify which rows will be affected by the upstream
                // shifts and include them in our parallel update preparations
                if (upstream.shifted().nonempty()) {
                    try (final RowSequence.Iterator it = source.getRowSet().prev().getRowSequenceIterator()) {

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
            computeCachedColumnRowSets(
                    () -> prepareForParallelPopulation(
                            () -> processWindows(
                                    () -> cleanUpAndNotify(
                                            () -> {
                                                // signal to the main task that we have completed our work
                                                if (waitForResult != null) {
                                                    waitForResult.complete(null);
                                                }
                                            }))));

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

        /**
         * Accumulate in parallel the dirty bucket RowSets for the cacheable input sources. Calls
         * {@code onComputeComplete} when the work is complete.
         */
        private void computeCachedColumnRowSets(final Runnable onComputeComplete) {
            // We have nothing to cache, so we can exit early.
            if (!inputCacheNeeded || dirtyWindows.isEmpty()) {
                onComputeComplete.run();
                return;
            }

            // Initially everything is dirty so cache everything.
            if (initialStep) {
                for (int srcIdx : cacheableSourceIndices) {
                    if (inputSourceCacheNeeded[srcIdx]) {
                        // create a RowSet to be used by `InverseWrappedRowSetWritableRowRedirection`
                        inputSourceRowSets.set(srcIdx, source.getRowSet().copy());

                        // record how many operators require this input source
                        int useCount = 0;
                        for (UpdateByWindow win : windows) {
                            for (int winOpIdx = 0; winOpIdx < win.operators.length; winOpIdx++) {
                                if (win.operatorUsesSource(winOpIdx, srcIdx)) {
                                    useCount++;
                                }
                            }
                        }
                        inputSourceReferenceCounts.set(srcIdx, useCount);
                    }
                }
                onComputeComplete.run();
                return;
            }

            final int[] dirtyWindowIndices = dirtyWindows.stream().toArray();

            jobScheduler.iterateParallel(ExecutionContext.getContextToRecord(),
                    chainAppendables(this, stringToAppendable("-computeCachedColumnRowSets")),
                    JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, cacheableSourceIndices.length,
                    (context, idx, nec) -> {
                        final int srcIdx = cacheableSourceIndices[idx];

                        int useCount = 0;
                        // If any of the dirty operators use this source, then increment the use count
                        for (int winIdx : dirtyWindowIndices) {
                            UpdateByWindow win = windows[winIdx];
                            // combine the row sets from the dirty windows
                            for (UpdateByBucketHelper bucket : dirtyBuckets) {
                                if (!bucket.windowContexts[winIdx].isDirty) {
                                    continue;
                                }

                                UpdateByWindow.UpdateByWindowBucketContext winBucketCtx = bucket.windowContexts[winIdx];
                                WritableRowSet rows = inputSourceRowSets.get(srcIdx);
                                if (rows == null) {
                                    final WritableRowSet influencerCopy =
                                            win.getInfluencerRows(winBucketCtx).copy();
                                    if (!inputSourceRowSets.compareAndSet(srcIdx, null, influencerCopy)) {
                                        influencerCopy.close();
                                        rows = inputSourceRowSets.get(srcIdx);
                                    }
                                }
                                if (rows != null) {
                                    // if not null, then insert this window's rowset
                                    // noinspection SynchronizationOnLocalVariableOrMethodParameter
                                    synchronized (rows) {
                                        rows.insert(win.getInfluencerRows(winBucketCtx));
                                    }
                                }
                            }

                            for (int winOpIdx = 0; winOpIdx < win.operators.length; winOpIdx++) {
                                if (win.operatorUsesSource(winOpIdx, srcIdx)
                                        && dirtyWindowOperators[winIdx].get(winOpIdx)) {
                                    useCount++;
                                }
                            }
                            inputSourceReferenceCounts.set(srcIdx, useCount);
                        }
                    }, onComputeComplete, this::onError);
        }

        /**
         * Prepare each operator output column for the parallel work to follow. Calls
         * {@code onParallelPopulationComplete} when the work is complete
         */
        private void prepareForParallelPopulation(
                final Runnable onParallelPopulationComplete) {
            jobScheduler.iterateParallel(ExecutionContext.getContextToRecord(),
                    chainAppendables(this, stringToAppendable("-prepareForParallelPopulation")),
                    JobScheduler.DEFAULT_CONTEXT_FACTORY, 0,
                    windows.length,
                    (context, winIdx, nec) -> {
                        UpdateByWindow win = windows[winIdx];
                        // Prepare each operator for the parallel updates to come.
                        if (initialStep) {
                            // Prepare the entire set of rows on the initial step.
                            try (final RowSet changedRows = redirHelper.isRedirected()
                                    ? RowSetFactory.flat(redirHelper.requiredCapacity())
                                    : source.getRowSet().copy()) {
                                win.prepareForParallelPopulation(changedRows);
                            }
                        } else {
                            // Get the minimal set of rows to be updated for this window (shiftedRows is empty when
                            // using redirection).
                            try (final WritableRowSet windowRowSet = changedRows.copy()) {
                                for (UpdateByBucketHelper bucket : dirtyBuckets) {
                                    if (win.isWindowBucketDirty(bucket.windowContexts[winIdx])) {
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
                            // Shift the non-redirected output sources now, after parallelPopulation.
                            upstream.shifted().apply((begin, end, delta) -> {
                                try (final RowSet subRowSet = source.getRowSet().prev().subSetByKeyRange(begin, end)) {
                                    for (UpdateByOperator op : win.getOperators()) {
                                        op.applyOutputShift(subRowSet, delta);
                                    }
                                }
                            });
                        }
                    }, onParallelPopulationComplete, this::onError);
        }

        /**
         * Process all {@code windows} in a serial manner (to minimize cached column memory usage). This function will
         * prepare the shared window resources (e.g. push/pop chunks for Rolling operators) for each dirty bucket in the
         * current window then call {@link #processWindowOperators}. When all operators have been processed then all
         * resources for this window are released before iterating.
         */
        private void processWindows(final Runnable onWindowsComplete) {
            if (dirtyWindows.isEmpty()) {
                onWindowsComplete.run();
                return;
            }

            final int[] dirtyWindowIndices = dirtyWindows.stream().toArray();

            jobScheduler.iterateSerial(ExecutionContext.getContextToRecord(),
                    chainAppendables(this, stringToAppendable("-processWindows")),
                    JobScheduler.DEFAULT_CONTEXT_FACTORY, 0,
                    dirtyWindowIndices.length,
                    (context, idx, nestedErrorConsumer, windowComplete) -> {
                        final int winIdx = dirtyWindowIndices[idx];

                        int maxAffectedChunkSize = 0;
                        int maxInfluencerChunkSize = 0;

                        for (UpdateByBucketHelper bucket : dirtyBuckets) {
                            if (bucket.windowContexts[winIdx].isDirty) {
                                // Assign the (maybe cached) input sources.
                                windows[winIdx].assignInputSources(bucket.windowContexts[winIdx],
                                        maybeCachedInputSources);

                                // Prepare this bucket for processing this window. This allocates window context
                                // resources and rolling ops pre-computes push/pop chunks.
                                windows[winIdx].prepareWindowBucket(bucket.windowContexts[winIdx]);

                                // Determine the largest chunk sizes needed to process the window buckets.
                                maxAffectedChunkSize =
                                        Math.max(maxAffectedChunkSize, bucket.windowContexts[winIdx].workingChunkSize);
                                maxInfluencerChunkSize = Math.max(maxInfluencerChunkSize,
                                        bucket.windowContexts[winIdx] instanceof UpdateByWindowRollingBase.UpdateByWindowRollingBucketContext
                                                ? ((UpdateByWindowRollingBase.UpdateByWindowRollingBucketContext) bucket.windowContexts[winIdx]).maxGetContextSize
                                                : bucket.windowContexts[winIdx].workingChunkSize);
                            }
                        }

                        // Process all the operators in this window
                        processWindowOperators(winIdx, maxAffectedChunkSize, maxInfluencerChunkSize, () -> {
                            // This window has been fully processed, release the resources we allocated
                            for (UpdateByBucketHelper bucket : dirtyBuckets) {
                                if (bucket.windowContexts[winIdx].isDirty) {
                                    windows[winIdx].finalizeWindowBucket(bucket.windowContexts[winIdx]);
                                }
                            }
                            windowComplete.run();
                        }, nestedErrorConsumer);
                    }, onWindowsComplete, this::onError);
        }

        /**
         * Process the operators for a given window in a serial manner. For efficiency, this function organizes the
         * operators into sets of operators that share input sources and that can be computed together efficiently. It
         * also arranges these sets of operators in an order that (hopefully) minimizes the memory footprint of the
         * cached operator input columns.
         * <p>
         * Before each operator set is processed, the sources for the input columns are cached. After the set is
         * processed, the cached sources are released if they will not be used by following operators.
         */
        private void processWindowOperators(
                final int winIdx,
                final int maxAffectedChunkSize,
                final int maxInfluencerChunkSize,
                final Runnable onProcessWindowOperatorsComplete,
                final Consumer<Exception> onProcessWindowOperatorsError) {
            final UpdateByWindow win = windows[winIdx];

            // Organize the dirty operators to increase the chance that the input caches can be released early. This
            // currently must produce sets of operators with identical sets of input sources.
            final Integer[] dirtyOperators = ArrayUtils.toObject(dirtyWindowOperators[winIdx].stream().toArray());
            Arrays.sort(dirtyOperators,
                    Comparator.comparingInt(o -> win.operatorInputSourceSlots[(int) o][0])
                            .thenComparingInt(o -> win.operatorInputSourceSlots[(int) o].length < 2 ? -1
                                    : win.operatorInputSourceSlots[(int) o][1]));

            final List<int[]> operatorSets = new ArrayList<>(dirtyOperators.length);
            final TIntArrayList opList = new TIntArrayList(dirtyOperators.length);

            opList.add(dirtyOperators[0]);
            int lastOpIdx = dirtyOperators[0];
            for (int ii = 1; ii < dirtyOperators.length; ii++) {
                final int opIdx = dirtyOperators[ii];
                if (Arrays.equals(win.operatorInputSourceSlots[opIdx], win.operatorInputSourceSlots[lastOpIdx])) {
                    opList.add(opIdx);
                } else {
                    operatorSets.add(opList.toArray());
                    opList.clear(dirtyOperators.length);
                    opList.add(opIdx);
                }
                lastOpIdx = opIdx;
            }
            operatorSets.add(opList.toArray());

            // Process each set of similar operators in this window serially.
            jobScheduler.iterateSerial(ExecutionContext.getContextToRecord(),
                    chainAppendables(this, stringAndIndexToAppendable("-processWindowOperators", winIdx)),
                    JobScheduler.DEFAULT_CONTEXT_FACTORY, 0,
                    operatorSets.size(),
                    (context, idx, nestedErrorConsumer, opSetComplete) -> {
                        final int[] opIndices = operatorSets.get(idx);

                        // All operators in this bin have identical input source sets
                        final int[] srcIndices = windows[winIdx].operatorInputSourceSlots[opIndices[0]];

                        // Cache the input sources for these operators.
                        cacheOperatorInputSources(winIdx, srcIndices, () -> {
                            // Process the subset of operators for this window.
                            processWindowOperatorSet(winIdx, opIndices, srcIndices, maxAffectedChunkSize,
                                    maxInfluencerChunkSize,
                                    () -> {
                                        // Release the cached sources that are no longer needed.
                                        releaseInputSources(srcIndices);
                                        opSetComplete.run();
                                    }, nestedErrorConsumer);
                        }, nestedErrorConsumer);
                    }, onProcessWindowOperatorsComplete, onProcessWindowOperatorsError);
        }

        /**
         * Create cached input sources for source indices provided. Calls {@code onCachingComplete} when the work is
         * complete.
         */
        private void cacheOperatorInputSources(
                final int winIdx,
                final int[] srcIndices,
                final Runnable onCachingComplete,
                final Consumer<Exception> onCachingError) {
            if (!inputCacheNeeded) {
                // no work to do, continue
                onCachingComplete.run();
                return;
            }

            jobScheduler.iterateParallel(ExecutionContext.getContextToRecord(),
                    chainAppendables(this, stringAndIndexToAppendable("-cacheOperatorInputSources", winIdx)),
                    JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, srcIndices.length,
                    (context, idx, nestedErrorConsumer, sourceComplete) -> createCachedColumnSource(
                            srcIndices[idx], sourceComplete, nestedErrorConsumer),
                    onCachingComplete,
                    onCachingError);
        }

        /**
         * Create a new input source cache and populate the required rows in parallel. Calls {@code onSourceComplete}
         * when the work is complete.
         */
        private void createCachedColumnSource(
                int srcIdx,
                final Runnable onSourceComplete,
                final Consumer<Exception> onSourceError) {
            final WritableRowSet inputRowSet = inputSourceRowSets.get(srcIdx);

            if (maybeCachedInputSources[srcIdx] != null || inputRowSet == null) {
                // already cached from another operator (or caching not needed)
                onSourceComplete.run();
                return;
            }

            final ColumnSource<?> inputSource = inputSources[srcIdx];

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

            // how many batches do we need?
            final int taskCount =
                    Math.toIntExact((inputRowSet.size() + PARALLEL_CACHE_BATCH_SIZE - 1) / PARALLEL_CACHE_BATCH_SIZE);

            final class BatchThreadContext implements JobScheduler.JobThreadContext {
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

            jobScheduler.iterateParallel(ExecutionContext.getContextToRecord(),
                    chainAppendables(this, stringToAppendable("-createCachedColumnSource")),
                    BatchThreadContext::new, 0, taskCount,
                    (ctx, idx, nec) -> {
                        // advance to the first key of this block
                        ctx.rsIt.advance(inputRowSet.get((long) idx * PARALLEL_CACHE_BATCH_SIZE));
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
                    }, () -> {
                        // assign this now
                        maybeCachedInputSources[srcIdx] = outputSource;
                        onSourceComplete.run();
                    }, onSourceError);
        }

        /**
         * Process a subset of operators from {@code windows[winIdx]} in parallel by bucket. Calls
         * {@code onProcessWindowOperatorSetComplete} when the work is complete
         */
        private void processWindowOperatorSet(final int winIdx,
                final int[] opIndices,
                final int[] srcIndices,
                final int maxAffectedChunkSize,
                final int maxInfluencerChunkSize,
                final Runnable onProcessWindowOperatorSetComplete,
                final Consumer<Exception> onProcessWindowOperatorSetError) {
            final class OperatorThreadContext implements JobScheduler.JobThreadContext {
                final Chunk<? extends Values>[] chunkArr;
                final ChunkSource.GetContext[] chunkContexts;
                final UpdateByOperator.Context[] winOpContexts;

                OperatorThreadContext() {
                    winOpContexts = new UpdateByOperator.Context[opIndices.length];

                    for (int ii = 0; ii < opIndices.length; ii++) {
                        final int opIdx = opIndices[ii];
                        winOpContexts[ii] = windows[winIdx].operators[opIdx].makeUpdateContext(maxAffectedChunkSize,
                                maxInfluencerChunkSize);
                    }

                    chunkArr = new Chunk[srcIndices.length];
                    chunkContexts = new ChunkSource.GetContext[srcIndices.length];

                    // All operators in this bin have identical input source sets
                    for (int ii = 0; ii < srcIndices.length; ii++) {
                        int srcIdx = srcIndices[ii];
                        chunkContexts[ii] = maybeCachedInputSources[srcIdx].makeGetContext(maxInfluencerChunkSize);
                    }
                }

                @Override
                public void close() {
                    SafeCloseableArray.close(winOpContexts);
                    SafeCloseableArray.close(chunkContexts);
                }
            }

            jobScheduler.iterateParallel(ExecutionContext.getContextToRecord(),
                    chainAppendables(this, stringAndIndexToAppendable("-processWindowBucketOperators", winIdx)),
                    OperatorThreadContext::new,
                    0, dirtyBuckets.length,
                    (context, bucketIdx, nec) -> {
                        UpdateByBucketHelper bucket = dirtyBuckets[bucketIdx];
                        if (bucket.windowContexts[winIdx].isDirty) {
                            windows[winIdx].processWindowBucketOperatorSet(
                                    bucket.windowContexts[winIdx],
                                    opIndices,
                                    srcIndices,
                                    context.winOpContexts,
                                    context.chunkArr,
                                    context.chunkContexts,
                                    initialStep);
                        }
                    }, onProcessWindowOperatorSetComplete, onProcessWindowOperatorSetError);
        }


        /** Release the input sources that will not be needed for the rest of this update */
        private void releaseInputSources(int[] sources) {
            try (final ResettableWritableObjectChunk<?, ?> backingChunk =
                    ResettableWritableObjectChunk.makeResettableChunk()) {
                for (int srcIdx : sources) {
                    if (!inputSourceCacheNeeded[srcIdx]) {
                        continue;
                    }

                    if (inputSourceReferenceCounts.decrementAndGet(srcIdx) == 0) {
                        // Last use of this set, let's clean up
                        try (final RowSet rows = inputSourceRowSets.get(srcIdx)) {
                            // release any objects we are holding in the cache
                            if (maybeCachedInputSources[srcIdx] instanceof ObjectArraySource) {
                                final long targetCapacity = rows.size();
                                for (long positionToNull = 0; positionToNull < targetCapacity; positionToNull +=
                                        backingChunk.size()) {
                                    ((ObjectArraySource<?>) maybeCachedInputSources[srcIdx])
                                            .resetWritableChunkToBackingStore(backingChunk, positionToNull);
                                    backingChunk.fillWithNullValue(0, backingChunk.size());
                                }
                            }
                            inputSourceRowSets.set(srcIdx, null);
                            maybeCachedInputSources[srcIdx] = null;
                        }
                    }
                }
            }
        }

        /**
         * Clean up the resources created during this update and notify downstream if applicable. Calls
         * {@code onCleanupComplete} when the work is complete
         */
        private void cleanUpAndNotify(final Runnable onCleanupComplete) {
            // create the downstream before calling finalize() on the buckets (which releases resources)
            final TableUpdate downstream = initialStep ? null : computeDownstreamUpdate();

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
                forAllOperators(op -> {
                    op.clearOutputRows(toClear);
                });
            }

            // release remaining resources
            SafeCloseable.closeArray(changedRows, toClear);
            upstream.release();

            // accumulate performance data
            final BasePerformanceEntry accumulated = jobScheduler.getAccumulatedPerformance();
            if (accumulated != null) {
                if (initialStep) {
                    final QueryPerformanceNugget outerNugget = QueryPerformanceRecorder.getInstance().getOuterNugget();
                    if (outerNugget != null) {
                        outerNugget.addBaseEntry(accumulated);
                    }
                } else {
                    UpdateGraphProcessor.DEFAULT.addNotification(new TerminalNotification() {
                        @Override
                        public void run() {
                            synchronized (accumulated) {
                                sourceListener().getEntry().accumulate(accumulated);
                            }
                        }
                    });
                }
            }

            // continue
            onCleanupComplete.run();
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
                mcsTransformer().transform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);
            }

            for (UpdateByBucketHelper bucket : dirtyBuckets) {
                // retrieve the modified row and column sets from the windows
                for (int winIdx = 0; winIdx < windows.length; winIdx++) {
                    UpdateByWindow win = windows[winIdx];
                    UpdateByWindow.UpdateByWindowBucketContext winCtx = bucket.windowContexts[winIdx];

                    if (win.isWindowBucketDirty(winCtx)) {
                        // add the window modified rows to this set
                        modifiedRowSet.insert(win.getAffectedRows(winCtx));
                        // add the modified output column sets to the downstream set
                        final UpdateByOperator[] winOps = win.getOperators();
                        for (int winOpIdx : win.getDirtyOperators(winCtx)) {
                            // these were created directly from the result output columns so no transformer needed
                            win.operators[winOpIdx].extractDownstreamModifiedColumnSet(upstream, downstream);
                        }
                    }
                }

            }
            // should not include upstream adds as modifies
            modifiedRowSet.remove(downstream.added);

            return downstream;
        }

        private void onError(@NotNull final Exception error) {
            if (waitForResult != null) {
                // Use the Future to signal that an exception has occurred. Cleanup will be done by the waiting thread.
                waitForResult.completeExceptionally(error);
            } else {
                // This error was delivered as part of update processing, we need to ensure that cleanup happens and
                // a notification is dispatched downstream.
                cleanUpAfterError();
                deliverUpdateError(error, sourceListener().getEntry(), false);
            }
        }

        /**
         * Clean up the resources created during this update.
         */
        private void cleanUpAfterError() {
            // allow the helpers to release their resources
            final int[] dirtyWindowIndices = dirtyWindows.stream().toArray();

            for (UpdateByBucketHelper bucket : dirtyBuckets) {
                for (int winIdx : dirtyWindowIndices) {
                    if (bucket.windowContexts[winIdx].isDirty) {
                        windows[winIdx].finalizeWindowBucket(bucket.windowContexts[winIdx]);
                    }
                }
                bucket.finalizeUpdate();
            }

            SafeCloseable.closeArray(changedRows, toClear);

            upstream.release();
        }
    }

    /**
     * Disconnect result from the {@link UpdateGraphProcessor}, deliver downstream failure notifications, and cleanup if
     * needed.
     *
     * @param error The {@link Throwable} to deliver, either from upstream or update processing
     * @param sourceEntry The {@link TableListener.Entry} to associate with failure messages
     * @param bucketCleanupNeeded Whether to clean up the buckets; unnecessary if the caller has already done this
     */
    void deliverUpdateError(
            @NotNull final Throwable error,
            @Nullable final TableListener.Entry sourceEntry,
            final boolean bucketCleanupNeeded) {

        final QueryTable result = result();
        if (!result.forceReferenceCountToZero()) {
            // No work to do here, another invocation is responsible for delivering failures.
            return;
        }

        if (bucketCleanupNeeded) {
            buckets.stream().filter(UpdateByBucketHelper::isDirty).forEach(UpdateByBucketHelper::finalizeUpdate);
        }

        result.notifyListenersOnError(error, sourceEntry);

        // Secondary notification to client error monitoring
        try {
            if (SystemicObjectTracker.isSystemic(result)) {
                AsyncClientErrorNotifier.reportError(error);
            }
        } catch (IOException e) {
            throw new UncheckedTableException(
                    "Exception while delivering async client error notification for " + sourceEntry, error);
        }
    }

    void forAllOperators(Consumer<UpdateByOperator> consumer) {
        for (UpdateByWindow win : windows) {
            for (UpdateByOperator op : win.operators) {
                consumer.accept(op);
            }
        }
    }

    /**
     * The Listener that is called when all input tables (source and constituent) are satisfied. This listener will
     * initiate UpdateBy operator processing in parallel by bucket
     */
    protected class UpdateByListener extends InstrumentedTableUpdateListenerAdapter {

        private UpdateByListener() {
            super(UpdateBy.this + "-SourceListener", UpdateBy.this.source, false);
        }

        @Override
        public void onUpdate(@NotNull final TableUpdate upstream) {
            // If we have a bucket update failure to deliver, deliver it
            if (maybeDeliverPendingFailure()) {
                return;
            }

            // If we delivered a failure in bucketing or bucket creation, short-circuit update delivery
            final QueryTable result = result();
            if (result.isFailed()) {
                Assert.eq(result.getLastNotificationStep(), "result.getLastNotificationStep()",
                        LogicalClock.DEFAULT.currentStep(), "LogicalClock.DEFAULT.currentStep()");
                return;
            }

            final PhasedUpdateProcessor sm = new PhasedUpdateProcessor(upstream.acquire(), false);
            sm.processUpdate();
        }

        @Override
        public void onFailureInternal(@NotNull final Throwable originalException, @Nullable final Entry sourceEntry) {
            deliverUpdateError(originalException, sourceEntry, true);
        }

        @Override
        public boolean canExecute(final long step) {
            return upstreamSatisfied(step);
        }
    }

    public UpdateByListener newUpdateByListener() {
        return new UpdateByListener();
    }

    protected abstract QueryTable result();

    protected abstract UpdateByListener sourceListener();

    protected abstract ModifiedColumnSet.Transformer mcsTransformer();

    protected abstract boolean upstreamSatisfied(final long step);

    protected abstract boolean maybeDeliverPendingFailure();

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

        QueryTable.checkInitiateOperation(source);

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

        final Collection<List<ColumnUpdateOperation>> windowSpecs =
                updateByOperatorFactory.getWindowOperatorSpecs(clauses);
        if (windowSpecs.size() == 0) {
            throw new IllegalArgumentException("At least one operator must be specified");
        }

        final StringBuilder descriptionBuilder = new StringBuilder("updateBy(ops={")
                .append(updateByOperatorFactory.describe(clauses))
                .append("}");

        final MutableObject<String> timestampColumnName = new MutableObject<>(null);
        // create an initial set of all source columns
        final Set<String> preservedColumnSet = new LinkedHashSet<>(source.getColumnSourceMap().keySet());

        final Set<String> problems = new LinkedHashSet<>();
        final Map<String, ColumnSource<?>> opResultSources = new LinkedHashMap<>();

        final ArrayList<ColumnSource<?>> inputSourceList = new ArrayList<>();
        final TObjectIntHashMap<ChunkSource<Values>> sourceToSlotMap = new TObjectIntHashMap<>();

        final UpdateByWindow[] windowArr = windowSpecs.stream().map(clauseList -> {
            final UpdateByOperator[] windowOps =
                    updateByOperatorFactory.getOperators(clauseList).toArray(UpdateByOperator[]::new);
            final int[][] windowOpSourceSlots = new int[windowOps.length][];

            for (int opIdx = 0; opIdx < windowOps.length; opIdx++) {
                UpdateByOperator op = windowOps[opIdx];

                op.getOutputColumns().forEach((name, col) -> {
                    if (opResultSources.putIfAbsent(name, col) != null) {
                        problems.add(name);
                    }
                    // remove overridden source columns
                    preservedColumnSet.remove(name);
                });
                // verify zero or one timestamp column names
                if (op.getTimestampColumnName() != null) {
                    if (timestampColumnName.getValue() == null) {
                        timestampColumnName.setValue(op.getTimestampColumnName());
                    } else {
                        if (!timestampColumnName.getValue().equals(op.getTimestampColumnName())) {
                            throw new UncheckedTableException(
                                    "Cannot reference more than one timestamp source on a single UpdateBy call {"
                                            + timestampColumnName + ", " + op.getTimestampColumnName() + "}");
                        }
                    }
                }

                // Iterate over each input column and map this operator to unique source
                final String[] inputColumnNames = op.getInputColumnNames();
                windowOpSourceSlots[opIdx] = new int[inputColumnNames.length];

                for (int colIdx = 0; colIdx < inputColumnNames.length; colIdx++) {
                    final ColumnSource<?> input = source.getColumnSource(inputColumnNames[colIdx]);
                    final int maybeExistingSlot = sourceToSlotMap.get(input);
                    if (maybeExistingSlot == sourceToSlotMap.getNoEntryValue()) {
                        // create a new input source
                        final int srcIdx = inputSourceList.size();
                        inputSourceList.add(ReinterpretUtils.maybeConvertToPrimitive(input));
                        sourceToSlotMap.put(input, srcIdx);
                        // map the window operator indices to this new source
                        windowOpSourceSlots[opIdx][colIdx] = srcIdx;
                    } else {
                        // map the window indices to this existing source
                        windowOpSourceSlots[opIdx][colIdx] = maybeExistingSlot;
                    }
                }
            }

            return UpdateByWindow.createFromOperatorArray(windowOps, windowOpSourceSlots);
        }).toArray(UpdateByWindow[]::new);

        if (!problems.isEmpty()) {
            throw new UncheckedTableException(descriptionBuilder + ": resulting column names must be unique {" +
                    String.join(", ", problems) + "}");
        }

        // These are the source columns that exist unchanged in the result
        final String[] preservedColumns = preservedColumnSet.toArray(String[]::new);

        final ColumnSource<?>[] inputSourceArr = inputSourceList.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);

        final Map<String, ColumnSource<?>> resultSources = new LinkedHashMap<>(source.getColumnSourceMap());
        // Add the output columns in the user-supplied order
        final Collection<String> userOutputColumns = updateByOperatorFactory.getOutputColumns(clauses);
        for (String colName : userOutputColumns) {
            final ColumnSource<?> matchedColumn = opResultSources.get(colName);
            Assert.neqNull(matchedColumn, "matchedColumn");
            resultSources.put(colName, matchedColumn);
        }
        if (pairs.length == 0) {
            descriptionBuilder.append(")");
            return LivenessScopeStack.computeEnclosed(() -> {
                final ZeroKeyUpdateByManager zkm = new ZeroKeyUpdateByManager(
                        windowArr,
                        inputSourceArr,
                        source,
                        preservedColumns,
                        resultSources,
                        timestampColumnName.getValue(),
                        rowRedirection,
                        control);

                if (source.isRefreshing()) {
                    // start tracking previous values
                    if (rowRedirection != null) {
                        rowRedirection.startTrackingPrevValues();
                    }
                    for (UpdateByWindow win : windowArr) {
                        for (UpdateByOperator op : win.operators) {
                            op.startTrackingPrev();
                        }
                    }
                }
                return zkm.result();
            }, source::isRefreshing, DynamicNode::isRefreshing);
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

        // TODO: test whether the source is static and that UpdateBy call uses only cumulative operators. In this
        // case, we can use an optimized manager and a single pass through the column sources (DHC #3393)

        return LivenessScopeStack.computeEnclosed(() -> {
            final BucketedPartitionedUpdateByManager bm = new BucketedPartitionedUpdateByManager(
                    windowArr,
                    inputSourceArr,
                    source,
                    preservedColumns,
                    resultSources,
                    byColumns,
                    timestampColumnName.getValue(),
                    rowRedirection,
                    control);

            if (source.isRefreshing()) {
                // start tracking previous values
                if (rowRedirection != null) {
                    rowRedirection.startTrackingPrevValues();
                }
                for (UpdateByWindow win : windowArr) {
                    for (UpdateByOperator op : win.operators) {
                        op.startTrackingPrev();
                    }
                }
            }
            return bm.result();
        }, source::isRefreshing, DynamicNode::isRefreshing);
    }
    // endregion
}
