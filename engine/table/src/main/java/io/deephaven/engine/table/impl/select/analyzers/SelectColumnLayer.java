package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.impl.sources.ChunkedBackingStoreExposedWritableSource;
import io.deephaven.time.DateTime;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.impl.util.ChunkUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.LongToIntFunction;

final public class SelectColumnLayer extends SelectOrViewColumnLayer {
    /**
     * The same reference as super.columnSource, but as a WritableColumnSource
     */
    private final WritableColumnSource writableSource;
    /**
     * Our parent row set, used for ensuring capacity.
     */
    private final RowSet parentRowSet;
    private final boolean isRedirected;
    private final boolean flattenedResult;
    private final BitSet dependencyBitSet;
    private final boolean canUseThreads;
    private final boolean canParallelizeThisColumn;

    /**
     * A memoized copy of selectColumn's data view. Use {@link SelectColumnLayer#getChunkSource()} to access.
     */
    private ChunkSource.WithPrev<Values> chunkSource;

    SelectColumnLayer(RowSet parentRowSet, SelectAndViewAnalyzer inner, String name, SelectColumn sc,
            WritableColumnSource ws, WritableColumnSource underlying,
            String[] deps, ModifiedColumnSet mcsBuilder, boolean isRedirected,
            boolean flattenedResult) {
        super(inner, name, sc, ws, underlying, deps, mcsBuilder);
        this.parentRowSet = parentRowSet;
        this.writableSource = ws;
        this.isRedirected = isRedirected;
        this.dependencyBitSet = new BitSet();
        Arrays.stream(deps).mapToInt(inner::getLayerIndexFor).forEach(dependencyBitSet::set);
        this.flattenedResult = flattenedResult;

        // we can't use threads at all if we have column that uses a Python query scope, because we are likely operating
        // under the GIL which will cause a deadlock
        this.canUseThreads = !sc.getDataView().preventsParallelism();

        // we can only parallelize this column if we are not redirected, our destination provides ensure previous, and
        // the select column is stateless
        this.canParallelizeThisColumn = canUseThreads && !isRedirected
                && WritableSourceWithEnsurePrevious.providesEnsurePrevious(ws) && sc.isStateless();
    }

    private ChunkSource<Values> getChunkSource() {
        if (chunkSource == null) {
            chunkSource = selectColumn.getDataView();
            if (selectColumnHoldsVector) {
                chunkSource = new VectorChunkAdapter<>(chunkSource);
            }
        }
        return chunkSource;
    }

    @Override
    public void applyUpdate(final TableUpdate upstream, final RowSet toClear,
            final UpdateHelper helper, JobScheduler jobScheduler, SelectLayerCompletionHandler onCompletion) {
        if (isRedirected && upstream.removed().isNonempty()) {
            clearObjectsAtThisLevel(upstream.removed());
        }

        // recurse so that dependent intermediate columns are already updated
        inner.applyUpdate(upstream, toClear, helper, jobScheduler,
                new SelectLayerCompletionHandler(dependencyBitSet, onCompletion) {
                    @Override
                    public void onAllRequiredColumnsCompleted() {
                        // we don't want to bother with threads if we are going to process a small update
                        final long totalSize = upstream.added().size() + upstream.modified().size();
                        // if we have shifts, that makes everything nasty; so we do not want to deal with it
                        final boolean hasShifts = upstream.shifted().nonempty();
                        if (canParallelizeThisColumn && jobScheduler.threadCount() > 1
                                && totalSize > QueryTable.MINIMUM_PARALLEL_SELECT_ROWS && !hasShifts) {
                            final long divisionSize = Math.max(QueryTable.MINIMUM_PARALLEL_SELECT_ROWS,
                                    (totalSize + jobScheduler.threadCount() - 1) / jobScheduler.threadCount());
                            final List<TableUpdate> updates = new ArrayList<>();
                            // divide up the additions and modifications
                            try (final RowSequence.Iterator rsAddIt = upstream.added().getRowSequenceIterator();
                                    final RowSequence.Iterator rsModIt = upstream.modified().getRowSequenceIterator()) {
                                while (rsAddIt.hasMore() || rsModIt.hasMore()) {
                                    final TableUpdateImpl update = new TableUpdateImpl();
                                    update.modifiedColumnSet = upstream.modifiedColumnSet();
                                    update.shifted = RowSetShiftData.EMPTY;
                                    update.removed = RowSetFactory.empty();

                                    if (rsAddIt.hasMore()) {
                                        update.added = rsAddIt.getNextRowSequenceWithLength(divisionSize).asRowSet();
                                    } else {
                                        update.added = RowSetFactory.empty();
                                    }

                                    if (update.added.size() < divisionSize && rsModIt.hasMore()) {
                                        update.modified = rsModIt
                                                .getNextRowSequenceWithLength(divisionSize - update.added().size())
                                                .asRowSet();
                                    } else {
                                        update.modified = RowSetFactory.empty();
                                    }

                                    updates.add(update);
                                }
                            }

                            if (updates.isEmpty()) {
                                throw new IllegalStateException();
                            }

                            jobScheduler.submit(() -> prepareParallelUpdate(jobScheduler, upstream, toClear, helper,
                                    onCompletion, this::onError, updates), SelectColumnLayer.this,
                                    this::onError);
                        } else {
                            jobScheduler.submit(
                                    () -> doSerialApplyUpdate(upstream, toClear, helper, onCompletion),
                                    SelectColumnLayer.this, this::onError);
                        }
                    }
                });
    }

    private void prepareParallelUpdate(final JobScheduler jobScheduler, final TableUpdate upstream,
            final RowSet toClear,
            final UpdateHelper helper, SelectLayerCompletionHandler onCompletion, Consumer<Exception> onError,
            List<TableUpdate> splitUpdates) {
        // we have to do removal and previous initialization before we can do any of the actual filling in multiple
        // threads to avoid concurrency problems with our destination column sources
        doEnsureCapacity();

        copyPreviousValues(upstream);

        final AtomicInteger divisions = new AtomicInteger(splitUpdates.size());

        long destinationOffset = 0;
        for (TableUpdate splitUpdate : splitUpdates) {
            final long fdest = destinationOffset;
            jobScheduler.submit(
                    () -> doParallelApplyUpdate(splitUpdate, toClear, helper, onCompletion, divisions, fdest),
                    SelectColumnLayer.this, onError);
            if (flattenedResult) {
                destinationOffset += splitUpdate.added().size();
                Assert.assertion(upstream.removed().isEmpty(), "upstream.removed().isEmpty()");
                Assert.assertion(upstream.modified().isEmpty(), "upstream.modified().isEmpty()");
                Assert.assertion(upstream.shifted().empty(), "upstream.shifted().empty()");
            }
        }
    }

    private void doSerialApplyUpdate(final TableUpdate upstream, final RowSet toClear, final UpdateHelper helper,
            final SelectLayerCompletionHandler onCompletion) {
        doEnsureCapacity();
        doApplyUpdate(upstream, toClear, helper, 0);
        onCompletion.onLayerCompleted(getLayerIndex());
    }

    private void doParallelApplyUpdate(final TableUpdate upstream, final RowSet toClear, final UpdateHelper helper,
            final SelectLayerCompletionHandler onCompletion, AtomicInteger divisions, long startOffset) {
        doApplyUpdate(upstream, toClear, helper, startOffset);
        upstream.release();

        if (divisions.decrementAndGet() == 0) {
            onCompletion.onLayerCompleted(getLayerIndex());
        }
    }

    private void doApplyUpdate(final TableUpdate upstream, final RowSet toClear, final UpdateHelper helper,
            long startOffset) {
        final int PAGE_SIZE = 4096;
        final LongToIntFunction contextSize = (long size) -> size > PAGE_SIZE ? PAGE_SIZE : (int) size;

        final boolean modifiesAffectUs =
                upstream.modified().isNonempty() && upstream.modifiedColumnSet().containsAny(myModifiedColumnSet);

        // We include modifies in our shifted sets if we are not going to process them separately.
        final RowSet preMoveKeys = helper.getPreShifted(!modifiesAffectUs);
        final RowSet postMoveKeys = helper.getPostShifted(!modifiesAffectUs);


        // Note that applyUpdate is called during initialization. If the table begins empty, we still want to force that
        // an initial call to getDataView() (via getChunkSource()) or else the formula will only be computed later when
        // data begins to flow; start-of-day is likely a bad time to find formula errors for our customers.
        final ChunkSource<Values> chunkSource = getChunkSource();

        final boolean needGetContext = upstream.added().isNonempty() || modifiesAffectUs;
        final boolean needDestContext = preMoveKeys.isNonempty() || needGetContext;
        final int chunkSourceContextSize =
                contextSize.applyAsInt(Math.max(upstream.added().size(), upstream.modified().size()));
        final int destContextSize = contextSize.applyAsInt(Math.max(preMoveKeys.size(), chunkSourceContextSize));
        final boolean isBackingChunkExposed =
                ChunkedBackingStoreExposedWritableSource.exposesChunkedBackingStore(writableSource);

        try (final ChunkSink.FillFromContext destContext =
                needDestContext ? writableSource.makeFillFromContext(destContextSize) : null;
                final ChunkSource.GetContext chunkSourceContext =
                        needGetContext ? chunkSource.makeGetContext(chunkSourceContextSize) : null;
                final ChunkSource.FillContext chunkSourceFillContext =
                        needGetContext && isBackingChunkExposed ? chunkSource.makeFillContext(chunkSourceContextSize)
                                : null) {

            // apply shifts!
            if (!isRedirected && preMoveKeys.isNonempty()) {
                assert !flattenedResult;
                assert destContext != null;
                // note: we cannot use a get context here as destination is identical to source
                final int shiftContextSize = contextSize.applyAsInt(preMoveKeys.size());
                try (final ChunkSource.FillContext srcContext = writableSource.makeFillContext(shiftContextSize);
                        final WritableChunk<Values> chunk =
                                writableSource.getChunkType().makeWritableChunk(shiftContextSize);
                        final RowSequence.Iterator srcIter = preMoveKeys.getRowSequenceIterator();
                        final RowSequence.Iterator destIter = postMoveKeys.getRowSequenceIterator()) {

                    while (srcIter.hasMore()) {
                        final RowSequence srcKeys = srcIter.getNextRowSequenceWithLength(PAGE_SIZE);
                        final RowSequence destKeys = destIter.getNextRowSequenceWithLength(PAGE_SIZE);
                        Assert.eq(srcKeys.size(), "srcKeys.size()", destKeys.size(), "destKeys.size()");
                        writableSource.fillPrevChunk(srcContext, chunk, srcKeys);
                        writableSource.fillFromChunk(destContext, chunk, destKeys);
                    }
                }
            }

            // apply adds!
            if (upstream.added().isNonempty()) {
                assert destContext != null;
                assert chunkSourceContext != null;
                if (isBackingChunkExposed) {
                    final ChunkedBackingStoreExposedWritableSource exposedWritableSource =
                            (ChunkedBackingStoreExposedWritableSource) this.writableSource;
                    if (flattenedResult && chunkSourceFillContext.supportsUnboundedFill()) {
                        // drive the fill operation off of the destination rather than the source, because we want to
                        // fill as much as possible as quickly as possible
                        long destinationOffset = startOffset;
                        try (final RowSequence.Iterator keyIter = upstream.added().getRowSequenceIterator();
                                final ResettableWritableChunk<?> backingChunk =
                                        writableSource.getChunkType().makeResettableWritableChunk()) {
                            while (keyIter.hasMore()) {
                                final long destCapacity = exposedWritableSource
                                        .resetWritableChunkToBackingStoreSlice(backingChunk, destinationOffset);
                                Assert.gtZero(destCapacity, "destCapacity");
                                final RowSequence sourceKeys = keyIter.getNextRowSequenceWithLength(destCapacity);
                                chunkSource.fillChunk(chunkSourceFillContext, backingChunk, sourceKeys);
                                destinationOffset += destCapacity;
                            }
                        }
                    } else {
                        try (final RowSequence.Iterator keyIter = upstream.added().getRowSequenceIterator();
                                final RowSequence.Iterator destIter = flattenedResult
                                        ? RowSequenceFactory
                                                .forRange(startOffset, startOffset + upstream.added().size() - 1)
                                                .getRowSequenceIterator()
                                        : null;
                                final ResettableWritableChunk<?> backingChunk =
                                        writableSource.getChunkType().makeResettableWritableChunk()) {
                            while (keyIter.hasMore()) {
                                final RowSequence keys = keyIter.getNextRowSequenceWithLength(PAGE_SIZE);
                                final RowSequence destKeys;
                                if (destIter != null) {
                                    destKeys = destIter.getNextRowSequenceWithLength(PAGE_SIZE);
                                } else {
                                    destKeys = keys;
                                }
                                if (keys.isContiguous() || flattenedResult) {
                                    long firstDest = destKeys.firstRowKey();
                                    final long lastDest = destKeys.lastRowKey();
                                    final long destCapacity = exposedWritableSource
                                            .resetWritableChunkToBackingStoreSlice(backingChunk, firstDest);
                                    if (destCapacity >= (lastDest - firstDest + 1)) {
                                        chunkSource.fillChunk(chunkSourceFillContext, backingChunk, keys);
                                    } else {
                                        long chunkDestCapacity = destCapacity;
                                        try (final RowSequence.Iterator chunkIterator = keys.getRowSequenceIterator()) {
                                            do {
                                                RowSequence chunkSourceKeys =
                                                        chunkIterator.getNextRowSequenceWithLength(chunkDestCapacity);
                                                chunkSource.fillChunk(chunkSourceFillContext, backingChunk,
                                                        chunkSourceKeys);
                                                firstDest += chunkDestCapacity;
                                                if (firstDest <= lastDest) {
                                                    chunkDestCapacity = Math.min(
                                                            exposedWritableSource.resetWritableChunkToBackingStoreSlice(
                                                                    backingChunk, firstDest),
                                                            lastDest - firstDest + 1);
                                                }
                                            } while (firstDest <= lastDest);
                                        }
                                    }
                                } else {
                                    writableSource.fillFromChunk(destContext,
                                            chunkSource.getChunk(chunkSourceContext, keys), destKeys);
                                }
                            }
                        }
                    }
                } else {
                    Assert.eqFalse(flattenedResult, "flattenedResult");
                    try (final RowSequence.Iterator keyIter = upstream.added().getRowSequenceIterator()) {
                        while (keyIter.hasMore()) {
                            final RowSequence keys = keyIter.getNextRowSequenceWithLength(PAGE_SIZE);
                            writableSource.fillFromChunk(destContext, chunkSource.getChunk(chunkSourceContext, keys),
                                    keys);
                        }
                    }
                }
            }

            // apply modifies!
            if (modifiesAffectUs) {
                assert !flattenedResult;
                assert chunkSourceContext != null;
                try (final RowSequence.Iterator keyIter = upstream.modified().getRowSequenceIterator()) {
                    while (keyIter.hasMore()) {
                        final RowSequence keys = keyIter.getNextRowSequenceWithLength(PAGE_SIZE);
                        writableSource.fillFromChunk(destContext, chunkSource.getChunk(chunkSourceContext, keys), keys);
                    }
                }
            }
        }

        if (!isRedirected) {
            clearObjectsAtThisLevel(toClear);
        }
    }

    private void doEnsureCapacity() {
        if (parentRowSet.isEmpty())
            return;
        if (flattenedResult || isRedirected) {
            // This is our "fake" update, the only thing that matters is the size of the addition; because we
            // are going to write the data into the column source flat ignoring the original row set.
            writableSource.ensureCapacity(parentRowSet.size(), false);
        } else {
            writableSource.ensureCapacity(parentRowSet.lastRowKey() + 1, false);
        }
    }

    void copyPreviousValues(TableUpdate upstream) {
        // we do not permit in-column parallelization with redirected results, so do not need to worry about how this
        // interacts with the previous clearing of the redirection index that has occurred at the start of applyUpdate
        try (final WritableRowSet changedRows =
                upstream.added().union(upstream.getModifiedPreShift())) {
            changedRows.insert(upstream.removed());
            ((WritableSourceWithEnsurePrevious) (writableSource)).ensurePrevious(changedRows);
        }
    }

    private void clearObjectsAtThisLevel(RowSet keys) {
        // Only bother doing this if we're holding on to references.
        if (!writableSource.getType().isPrimitive() && (writableSource.getType() != DateTime.class)) {
            ChunkUtils.fillWithNullValue(writableSource, keys);
        }
    }

    @Override
    public boolean flattenedResult() {
        return flattenedResult;
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("{SelectColumnLayer: ").append(selectColumn.toString()).append(", layerIndex=")
                .append(getLayerIndex()).append("}");
    }

    @Override
    public boolean allowCrossColumnParallelization() {
        return canUseThreads && inner.allowCrossColumnParallelization();
    }
}
