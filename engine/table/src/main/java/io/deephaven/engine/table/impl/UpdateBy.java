package io.deephaven.engine.table.impl;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ResettableWritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.sources.sparse.SparseConstants;
import io.deephaven.engine.table.impl.updateby.UpdateByWindow;
import io.deephaven.engine.table.impl.util.InverseRowRedirectionImpl;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.SoftReference;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

/**
 * The core of the {@link Table#updateBy(UpdateByControl, Collection, Collection)} operation.
 */
public abstract class UpdateBy {
    protected final ColumnSource<?>[] inputSources;
    // some columns will have multiple inputs, such as time-based and Weighted computations
    protected final int[][] operatorInputSourceSlots;
    protected final UpdateByOperator[] operators;
    protected final UpdateByWindow[] windows;
    protected final QueryTable source;
    protected final UpdateByRedirectionContext redirContext;
    protected final UpdateByControl control;
    protected final String timestampColumnName;

    protected final LinkedList<UpdateByBucketHelper> buckets;

    // column-caching management

    /**
     * Whether caching benefits this UpdateBy operation
     */
    protected final boolean inputCacheNeeded;
    /**
     * Whether caching benefits this input source
     */
    protected final boolean[] inputSourceCacheNeeded;
    /**
     * References to the dense array sources we are using for the cached sources
     */
    protected final SoftReference<WritableColumnSource<?>>[] inputSourceCaches;

    /**
     * The output table for this UpdateBy operation
     */
    protected QueryTable result;
    protected LinkedList<ListenerRecorder> recorders;
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
        for (int ii = 0; ii < inputSources.length; ii++) {
            inputSourceCacheNeeded[ii] = !FillUnordered.providesFillUnordered(inputSources[ii]);
            cacheNeeded |= inputSourceCacheNeeded[ii];
        }
        this.inputCacheNeeded = cacheNeeded;
        // noinspection unchecked
        inputSourceCaches = new SoftReference[inputSources.length];

        buckets = new LinkedList<>();
    }

    private ColumnSource<?> createCachedColumnSource(int srcIdx, final TrackingWritableRowSet inputRowSet) {
        final ColumnSource<?> inputSource = inputSources[srcIdx];

        // re-use the dense column cache if it still exists
        WritableColumnSource<?> innerSource;
        if (inputSourceCaches[srcIdx] == null || (innerSource = inputSourceCaches[srcIdx].get()) == null) {
            // create a new dense cache
            innerSource = ArrayBackedColumnSource.getMemoryColumnSource(inputSource.getType(), inputSource.getComponentType());
            inputSourceCaches[srcIdx] = new SoftReference<>(innerSource);
        }
        innerSource.ensureCapacity(inputRowSet.size());

        final WritableRowRedirection rowRedirection = new InverseRowRedirectionImpl(inputRowSet);
        final WritableColumnSource<?> outputSource =
                new WritableRedirectedColumnSource(rowRedirection, innerSource, 0);

        final int CHUNK_SIZE = 1 << 16; // copied from SparseSelect

        try (final RowSequence.Iterator rsIt = inputRowSet.getRowSequenceIterator();
             final ChunkSink.FillFromContext ffc =
                     outputSource.makeFillFromContext(CHUNK_SIZE);
             final ChunkSource.GetContext gc = inputSource.makeGetContext(CHUNK_SIZE)) {
            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(CHUNK_SIZE);
                final Chunk<? extends Values> values = inputSource.getChunk(gc, chunkOk);
                outputSource.fillFromChunk(ffc, values, chunkOk);
            }
        }

        // holding this reference will protect `rowDirection` and `innerSource` from GC
        return outputSource;
    }

    /**
     * Examine the buckets and identify the input sources that will benefit from caching. Accumulate the bucket
     * rowsets for each source independently so the caches are as efficient as possible
     */
    private void computeCachedColumnContents(UpdateByBucketHelper[] buckets, boolean initialStep, WritableRowSet[] inputSourceRowSets, AtomicInteger[] inputSourceReferenceCounts) {
        // on the initial step, everything is dirty and we can optimize
        if (initialStep) {
            for (int srcIdx = 0; srcIdx < inputSources.length; srcIdx++) {
                if (inputSourceCacheNeeded[srcIdx]) {
                    // this needs to be a TrackingRowSet to be used by `InverseRowRedirectionImpl`
                    inputSourceRowSets[srcIdx] = source.getRowSet().copy().toTracking();
                }
            }

            // add reference counts for each window
            for (final UpdateByWindow win : windows) {
                final int[] uniqueWindowSources = win.getUniqueSourceIndices();
                for (int srcIdx : uniqueWindowSources) {
                    if (inputSourceCacheNeeded[srcIdx]) {
                        // increment the reference count for this input source
                        if (inputSourceReferenceCounts[srcIdx] == null) {
                            inputSourceReferenceCounts[srcIdx] = new AtomicInteger(1);
                        } else {
                            inputSourceReferenceCounts[srcIdx].incrementAndGet();
                        }
                    }
                }
            }
            return;
        }

        // on update steps, we can be more precise and cache exactly what is needed by the update
        final boolean[] cacheNeeded = new boolean[inputSources.length];

        for (int winIdx = 0; winIdx < windows.length; winIdx++) {
            final UpdateByWindow win = windows[winIdx];
            final int[] uniqueWindowSources = win.getUniqueSourceIndices();

            Arrays.fill(cacheNeeded, false);

            // for each bucket, need to accumulate the rowset if this window is dirty
            for (UpdateByBucketHelper bucket : buckets) {
                UpdateByWindow.UpdateByWindowContext winCtx = bucket.windowContexts[winIdx];
                if (win.isWindowDirty(winCtx)) {
                    //
                    for (int srcIdx : uniqueWindowSources) {
                        if (inputSourceCacheNeeded[srcIdx]) {
                            // record that this window requires this input source
                            cacheNeeded[srcIdx] = true;
                            // add this rowset to the running total
                            if (inputSourceRowSets[srcIdx] == null) {
                                inputSourceRowSets[srcIdx] = win.getInfluencerRows(winCtx).copy().toTracking();
                            } else {
                                inputSourceRowSets[srcIdx].insert(win.getInfluencerRows(winCtx));
                            }
                        }
                    }
                }
            }

            // add one to all the reference counts this windows
            for (int srcIdx : uniqueWindowSources) {
                if (cacheNeeded[srcIdx]) {
                    // increment the reference count for this input source
                    if (inputSourceReferenceCounts[srcIdx] == null) {
                        inputSourceReferenceCounts[srcIdx] = new AtomicInteger(1);
                    } else {
                        inputSourceReferenceCounts[srcIdx].incrementAndGet();
                    }
                }
            }
        }
    }

    private void cacheInputSources(int winIdx, ColumnSource<?>[] maybeCachedInputSources, TrackingWritableRowSet[] inputSourceRowSets) {
        final UpdateByWindow win = windows[winIdx];
        final int[] uniqueWindowSources = win.getUniqueSourceIndices();
        for (int srcIdx : uniqueWindowSources) {
            if (maybeCachedInputSources[srcIdx] == null) {
                maybeCachedInputSources[srcIdx] = createCachedColumnSource(srcIdx, inputSourceRowSets[srcIdx]);
            }
        }
    }

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

    private void releaseInputSources(int winIdx, ColumnSource<?>[] maybeCachedInputSources, TrackingWritableRowSet[] inputSourceRowSets, AtomicInteger[] inputSourceReferenceCounts) {
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

    protected void shiftOutputColumns(TableUpdate upstream) {
        if (redirContext.isRedirected()) {
            redirContext.processUpdateForRedirection(upstream, source.getRowSet());
        } else if (upstream.shifted().nonempty()) {
            try (final RowSet prevIdx = source.getRowSet().copyPrev()) {
                upstream.shifted().apply((begin, end, delta) -> {
                    try (final RowSet subRowSet = prevIdx.subSetByKeyRange(begin, end)) {
                        for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                            operators[opIdx].applyOutputShift(subRowSet, delta);
                        }
                    }
                });
            }
        }
    }

    /***
     * This will handle shifts for the output sources and will also prepare the sources for parallel updates
     * @param upstream the {@link TableUpdate} to process
     */
    protected void shiftWindowOperators(UpdateByWindow win, RowSetShiftData shift) {
        if (!redirContext.isRedirected() && shift.nonempty()) {
            try (final RowSet prevIdx = source.getRowSet().copyPrev()) {
                shift.apply((begin, end, delta) -> {
                    try (final RowSet subRowSet = prevIdx.subSetByKeyRange(begin, end)) {
                        for (UpdateByOperator op : win.getOperators()) {
                            op.applyOutputShift(subRowSet, delta);
                        }
                    }
                });
            }
        }
    }

    protected void processBuckets(UpdateByBucketHelper[] dirtyBuckets, boolean initialStep, RowSetShiftData shifts) {
        // we need to ID which rows may be affected by the upstream shifts so we can include them
        // in our parallel update preparations
        final WritableRowSet shiftedRows;
        if (shifts.nonempty()) {
            try (final RowSet prev = source.getRowSet().copyPrev();
                 final RowSequence.Iterator it = prev.getRowSequenceIterator()) {

                final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                final int size = shifts.size();

                // get these in order so we can use a sequential builder
                for (int ii = 0; ii < size; ii++) {
                    final long begin = shifts.getBeginRange(ii);
                    final long end = shifts.getEndRange(ii);
                    final long delta = shifts.getShiftDelta(ii);

                    it.advance(begin);
                    final RowSequence rs = it.getNextRowSequenceThrough(end);
                    builder.appendRowSequenceWithOffset(rs, delta);
                }
                shiftedRows = builder.build();
            }
        } else {
            shiftedRows = RowSetFactory.empty();
        }

        final ColumnSource<?>[] maybeCachedInputSources;
        final TrackingWritableRowSet[] inputSourceRowSets;
        final AtomicInteger[] inputSourceReferenceCounts;
        if (inputCacheNeeded) {
            maybeCachedInputSources = new ColumnSource[inputSources.length];
            inputSourceRowSets = new TrackingWritableRowSet[inputSources.length];
            inputSourceReferenceCounts = new AtomicInteger[inputSources.length];

            // do the hard work of computing what should be cached for each input source
            computeCachedColumnContents(dirtyBuckets, initialStep, inputSourceRowSets, inputSourceReferenceCounts);

            // assign the non-cached input source or leave null for cached sources
            for (int srcIdx = 0; srcIdx < inputSources.length; srcIdx++) {
                maybeCachedInputSources[srcIdx] = inputSourceCacheNeeded[srcIdx] ? null : inputSources[srcIdx];
            }
        } else {
            maybeCachedInputSources = inputSources;
            inputSourceRowSets = null;
            inputSourceReferenceCounts = null;
        }

        // process the windows
        for (int winIdx = 0; winIdx < windows.length; winIdx++) {
            UpdateByWindow win = windows[winIdx];

            if (inputCacheNeeded) {
                // cache the sources needed for this window
                cacheInputSources(winIdx, maybeCachedInputSources, inputSourceRowSets);
            }

            if (initialStep) {
                // prepare each operator for the parallel updates to come
                for (UpdateByOperator op : win.getOperators()) {
                    op.prepareForParallelPopulation(source.getRowSet());
                }
            } else {
                try (final WritableRowSet windowRowSet = shiftedRows.copy()) {
                    // get the total rowset from this window
                    for (UpdateByBucketHelper bucket : dirtyBuckets) {
                        // append the dirty rows from this window
                        if (win.isWindowDirty(bucket.windowContexts[winIdx])) {
                            windowRowSet.insert(win.getAffectedRows(bucket.windowContexts[winIdx]));
                        }
                    }
                    // prepare each operator for the parallel updates to come
                    for (UpdateByOperator op : win.getOperators()) {
                        op.prepareForParallelPopulation(windowRowSet);
                    }
                }
            }
            // now we can shift (after the parallel prep is complete)
            shiftWindowOperators(win, shifts);

            // PARALLELIZE THIS INTO

            // process the window
            for (UpdateByBucketHelper bucket : dirtyBuckets) {
                bucket.assignInputSources(winIdx, maybeCachedInputSources);
                bucket.processWindow(winIdx, initialStep);
            }

            if (inputCacheNeeded) {
                // release the cached sources that are no longer needed
                releaseInputSources(winIdx, maybeCachedInputSources, inputSourceRowSets, inputSourceReferenceCounts);
            }

        }

        // we are done with this rowset
        shiftedRows.close();
    }

    protected void finalizeBuckets(UpdateByBucketHelper[] dirtyBuckets) {
        for (UpdateByBucketHelper bucket : dirtyBuckets) {
            bucket.finalizeUpdate();
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
            UpdateByBucketHelper[] dirtyBuckets = buckets.stream().filter(b -> b.isDirty())
                    .toArray(UpdateByBucketHelper[]::new);

            final ListenerRecorder sourceRecorder = recorders.peekFirst();
            final TableUpdate upstream = sourceRecorder.getUpdate();

            final TableUpdateImpl downstream = new TableUpdateImpl();

            downstream.added = upstream.added().copy();
            downstream.removed = upstream.removed().copy();
            downstream.shifted = upstream.shifted();

            // union the modifies from all the tables (including source)
            downstream.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
            downstream.modifiedColumnSet.clear();

            if (redirContext.isRedirected()) {
                // this does all the work needed for redirected output sources
                redirContext.processUpdateForRedirection(upstream, source.getRowSet());
            }

            processBuckets(dirtyBuckets, false, upstream.shifted());

            // get the adds/removes/shifts from the first (source) entry, make a copy since TableUpdateImpl#reset will
            // close them with the upstream update

            WritableRowSet modifiedRowSet = RowSetFactory.empty();
            downstream.modified = modifiedRowSet;

            if (upstream.modified().isNonempty()) {
                modifiedRowSet.insert(upstream.modified());
                downstream.modifiedColumnSet.setAll(upstream.modifiedColumnSet());
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

            finalizeBuckets(dirtyBuckets);

            result.notifyListeners(downstream);
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
     * @param source    the source to apply to.
     * @param clauses   the operations to apply.
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
