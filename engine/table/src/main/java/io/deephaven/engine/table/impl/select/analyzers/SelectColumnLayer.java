/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select.analyzers;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.ObjectChunkIterator;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.VectorChunkAdapter;
import io.deephaven.engine.table.impl.sources.ChunkedBackingStoreExposedWritableSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.updategraph.UpdateCommitterEx;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.time.DateTime;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.LongToIntFunction;
import java.util.stream.StreamSupport;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.LARGEST_POOLED_CHUNK_CAPACITY;

final public class SelectColumnLayer extends SelectOrViewColumnLayer {
    /**
     * The same reference as super.columnSource, but as a WritableColumnSource and maybe reinterpretted
     */
    private final WritableColumnSource<?> writableSource;

    /**
     * The execution context the select column layer was constructed in
     */
    private final ExecutionContext executionContext;

    /**
     * Our parent row set, used for ensuring capacity.
     */
    private final RowSet parentRowSet;
    private final boolean isRedirected;
    private final boolean flattenedResult;
    private final boolean alreadyFlattenedSources;
    private final BitSet dependencyBitSet;
    private final boolean canParallelizeThisColumn;
    private final boolean isSystemic;
    private final boolean resultTypeIsLivenessReferent;
    private final boolean resultTypeIsTable;

    private UpdateCommitterEx<SelectColumnLayer, LivenessNode> prevUnmanager;
    private List<WritableObjectChunk<? extends LivenessReferent, Values>> prevValueChunksToUnmanage;

    /**
     * A memoized copy of selectColumn's data view. Use {@link SelectColumnLayer#getChunkSource()} to access.
     */
    private ChunkSource.WithPrev<Values> chunkSource;

    SelectColumnLayer(RowSet parentRowSet, SelectAndViewAnalyzer inner, String name, SelectColumn sc,
            WritableColumnSource<?> ws, WritableColumnSource<?> underlying,
            String[] deps, ModifiedColumnSet mcsBuilder, boolean isRedirected,
            boolean flattenedResult, boolean alreadyFlattenedSources) {
        super(inner, name, sc, ws, underlying, deps, mcsBuilder);
        this.parentRowSet = parentRowSet;
        this.writableSource = ReinterpretUtils.maybeConvertToWritablePrimitive(ws);
        this.isRedirected = isRedirected;
        this.executionContext = ExecutionContext.getContextToRecord();

        dependencyBitSet = new BitSet();
        Arrays.stream(deps).mapToInt(inner::getLayerIndexFor).forEach(dependencyBitSet::set);

        this.flattenedResult = flattenedResult;
        this.alreadyFlattenedSources = alreadyFlattenedSources;

        // We can only parallelize this column if we are not redirected, our destination provides ensure previous, and
        // the select column is stateless
        canParallelizeThisColumn = !isRedirected
                && WritableSourceWithPrepareForParallelPopulation.supportsParallelPopulation(writableSource)
                && sc.isStateless();

        // If we were created on a systemic thread, we want to be sure to make sure that any updates are also
        // applied systemically.
        isSystemic = SystemicObjectTracker.isSystemicThread();

        // We want to ensure that results are managed appropriately if they are LivenessReferents
        resultTypeIsLivenessReferent = LivenessReferent.class.isAssignableFrom(ws.getType());

        // We assume that formulas producing Tables are likely to
        // 1. be expensive to evaluate and
        // 2. have wildly varying job size,
        // and so we ignore minimum size to parallelize and limit divisionSize to 1 to maximize the
        // effect of our parallelism.
        resultTypeIsTable = Table.class.isAssignableFrom(ws.getType());
    }

    private ChunkSource<Values> getChunkSource() {
        if (chunkSource == null) {
            ColumnSource<?> dataSource = selectColumn.getDataView();
            if (dataSource.getType() != writableSource.getType()) {
                // this should only occur when using primitives internally and the user has requested a non-primitive
                dataSource = ReinterpretUtils.maybeConvertToPrimitive(dataSource);
                Assert.eq(dataSource.getType(), "dataSource.getType()",
                        writableSource.getType(), "writableSource.getType()");
            }
            chunkSource = dataSource;
            if (selectColumnHoldsVector) {
                chunkSource = new VectorChunkAdapter<>(chunkSource);
            }
        }
        return chunkSource;
    }

    @Override
    public void applyUpdate(final TableUpdate upstream, final RowSet toClear,
            final UpdateHelper helper, final JobScheduler jobScheduler, @Nullable final LivenessNode liveResultOwner,
            final SelectLayerCompletionHandler onCompletion) {
        if (upstream.removed().isNonempty()) {
            if (isRedirected) {
                clearObjectsAtThisLevel(upstream.removed());
            }
            if (resultTypeIsLivenessReferent && liveResultOwner != null) {
                addRemovesToPrevUnmanager(upstream.removed(), liveResultOwner);
            }
        }

        // recurse so that dependent intermediate columns are already updated
        inner.applyUpdate(upstream, toClear, helper, jobScheduler, liveResultOwner,
                new SelectLayerCompletionHandler(dependencyBitSet, onCompletion) {
                    @Override
                    public void onAllRequiredColumnsCompleted() {
                        // We don't want to bother with threads if we are going to process a small update
                        final long totalSize = upstream.added().size() + upstream.modified().size();

                        // If we have shifts, that makes everything nasty; so we do not want to deal with it
                        final boolean hasShifts = upstream.shifted().nonempty();

                        final boolean checkTableOperations =
                                UpdateGraphProcessor.DEFAULT.getCheckTableOperations()
                                        && !UpdateGraphProcessor.DEFAULT.sharedLock().isHeldByCurrentThread()
                                        && !UpdateGraphProcessor.DEFAULT.exclusiveLock().isHeldByCurrentThread();

                        if (canParallelizeThisColumn && jobScheduler.threadCount() > 1 && !hasShifts &&
                                ((resultTypeIsTable && totalSize > 0)
                                        || totalSize > QueryTable.MINIMUM_PARALLEL_SELECT_ROWS)) {
                            final long divisionSize = resultTypeIsTable ? 1
                                    : Math.max(QueryTable.MINIMUM_PARALLEL_SELECT_ROWS,
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

                            jobScheduler.submit(
                                    executionContext,
                                    () -> prepareParallelUpdate(jobScheduler, upstream, toClear, helper,
                                            liveResultOwner, onCompletion, this::onError, updates,
                                            checkTableOperations),
                                    SelectColumnLayer.this, this::onError);
                        } else {
                            jobScheduler.submit(
                                    executionContext,
                                    () -> doSerialApplyUpdate(upstream, toClear, helper, liveResultOwner, onCompletion,
                                            checkTableOperations),
                                    SelectColumnLayer.this, this::onError);
                        }
                    }
                });
    }

    private void prepareParallelUpdate(final JobScheduler jobScheduler, final TableUpdate upstream,
            final RowSet toClear, final UpdateHelper helper, @Nullable final LivenessNode liveResultOwner,
            final SelectLayerCompletionHandler onCompletion, final Consumer<Exception> onError,
            final List<TableUpdate> splitUpdates, final boolean checkTableOperations) {
        // we have to do removal and previous initialization before we can do any of the actual filling in multiple
        // threads to avoid concurrency problems with our destination column sources
        doEnsureCapacity();

        prepareSourcesForParallelPopulation(upstream);

        final int numTasks = splitUpdates.size();
        final long[] destinationOffsets = new long[numTasks];
        if (flattenedResult) {
            Assert.assertion(upstream.removed().isEmpty(), "upstream.removed().isEmpty()");
            Assert.assertion(upstream.modified().isEmpty(), "upstream.modified().isEmpty()");
            Assert.assertion(upstream.shifted().empty(), "upstream.shifted().empty()");
            long destinationOffset = 0;
            for (int ti = 0; ti < numTasks; ++ti) {
                final TableUpdate splitUpdate = splitUpdates.get(ti);
                Assert.assertion(splitUpdate.removed().isEmpty(), "splitUpdate.removed().isEmpty()");
                Assert.assertion(splitUpdate.modified().isEmpty(), "splitUpdate.modified().isEmpty()");
                Assert.assertion(splitUpdate.shifted().empty(), "splitUpdate.shifted().empty()");
                destinationOffsets[ti] = destinationOffset;
                destinationOffset += splitUpdate.added().size();
            }
        }
        jobScheduler.iterateParallel(
                executionContext, SelectColumnLayer.this, JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, numTasks,
                (ctx, ti, nec) -> doParallelApplyUpdate(
                        splitUpdates.get(ti), helper, liveResultOwner, checkTableOperations, destinationOffsets[ti]),
                () -> {
                    if (!isRedirected) {
                        clearObjectsAtThisLevel(toClear);
                    }
                    onCompletion.onLayerCompleted(getLayerIndex());
                },
                onError);
    }

    private void doSerialApplyUpdate(final TableUpdate upstream, final RowSet toClear, final UpdateHelper helper,
            @Nullable final LivenessNode liveResultOwner, final SelectLayerCompletionHandler onCompletion,
            final boolean checkTableOperations) {
        doEnsureCapacity();
        final boolean oldCheck = UpdateGraphProcessor.DEFAULT.setCheckTableOperations(checkTableOperations);
        try {
            SystemicObjectTracker.executeSystemically(isSystemic,
                    () -> doApplyUpdate(upstream, helper, liveResultOwner, 0));
        } finally {
            UpdateGraphProcessor.DEFAULT.setCheckTableOperations(oldCheck);
        }
        if (!isRedirected) {
            clearObjectsAtThisLevel(toClear);
        }
        onCompletion.onLayerCompleted(getLayerIndex());
    }

    private void doParallelApplyUpdate(final TableUpdate upstream, final UpdateHelper helper,
            @Nullable final LivenessNode liveResultOwner, final boolean checkTableOperations, final long startOffset) {
        final boolean oldCheck = UpdateGraphProcessor.DEFAULT.setCheckTableOperations(checkTableOperations);
        try {
            SystemicObjectTracker.executeSystemically(isSystemic,
                    () -> doApplyUpdate(upstream, helper, liveResultOwner, startOffset));
        } finally {
            UpdateGraphProcessor.DEFAULT.setCheckTableOperations(oldCheck);
        }
        upstream.release();
    }

    private Boolean doApplyUpdate(final TableUpdate upstream, final UpdateHelper helper,
            @Nullable final LivenessNode liveResultOwner, final long startOffset) {
        final int PAGE_SIZE = 4096;
        final LongToIntFunction contextSize = (long size) -> size > PAGE_SIZE ? PAGE_SIZE : (int) size;

        final boolean modifiesAffectUs =
                upstream.modified().isNonempty() && upstream.modifiedColumnSet().containsAny(myModifiedColumnSet);

        // We include modifies in our shifted sets if we are not going to process them separately.
        final RowSet preMoveKeys = helper.getPreShifted(!modifiesAffectUs);
        final RowSet postMoveKeys = helper.getPostShifted(!modifiesAffectUs);

        final ChunkSource<Values> chunkSource = getChunkSource();

        final boolean needGetContext = upstream.added().isNonempty() || modifiesAffectUs;
        final boolean needDestContext = preMoveKeys.isNonempty() || needGetContext;
        final int chunkSourceContextSize =
                contextSize.applyAsInt(Math.max(upstream.added().size(), upstream.modified().size()));
        final int destContextSize = contextSize.applyAsInt(Math.max(preMoveKeys.size(), chunkSourceContextSize));
        final boolean isBackingChunkExposed =
                ChunkedBackingStoreExposedWritableSource.exposesChunkedBackingStore(writableSource);

        try (final SafeCloseable ignored = LivenessScopeStack.open();
                final ChunkSink.FillFromContext destContext = needDestContext
                        ? writableSource.makeFillFromContext(destContextSize)
                        : null;
                final ChunkSource.GetContext chunkSourceContext = needGetContext
                        ? chunkSource.makeGetContext(chunkSourceContextSize)
                        : null;
                final ChunkSource.FillContext chunkSourceFillContext = needGetContext && isBackingChunkExposed
                        ? chunkSource.makeFillContext(chunkSourceContextSize)
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

            // apply modifies!
            if (modifiesAffectUs) {
                assert !flattenedResult;
                assert chunkSourceContext != null;
                final boolean needToUnmanagePrevValues = resultTypeIsLivenessReferent && liveResultOwner != null;
                try (final RowSequence.Iterator keyIter = upstream.modified().getRowSequenceIterator();
                        final RowSequence.Iterator prevKeyIter = needToUnmanagePrevValues
                                ? upstream.getModifiedPreShift().getRowSequenceIterator()
                                : null;
                        final ChunkSource.FillContext fillContext = needToUnmanagePrevValues
                                ? columnSource.makeFillContext(PAGE_SIZE)
                                : null) {
                    while (keyIter.hasMore()) {
                        final RowSequence keys = keyIter.getNextRowSequenceWithLength(PAGE_SIZE);
                        final Chunk<? extends Values> modifiedResults = chunkSource.getChunk(chunkSourceContext, keys);
                        writableSource.fillFromChunk(destContext, modifiedResults, keys);
                        if (needToUnmanagePrevValues) {
                            handleModifyManagement(liveResultOwner, fillContext, prevKeyIter, modifiedResults);
                        }
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
                                maybeManageAdds(backingChunk, liveResultOwner);
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
                                        maybeManageAdds(backingChunk, liveResultOwner);
                                    } else {
                                        long chunkDestCapacity = destCapacity;
                                        try (final RowSequence.Iterator chunkIterator = keys.getRowSequenceIterator()) {
                                            do {
                                                RowSequence chunkSourceKeys =
                                                        chunkIterator.getNextRowSequenceWithLength(chunkDestCapacity);
                                                chunkSource.fillChunk(chunkSourceFillContext, backingChunk,
                                                        chunkSourceKeys);
                                                maybeManageAdds(backingChunk, liveResultOwner);
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
                                            maybeManageAdds(chunkSource.getChunk(chunkSourceContext, keys),
                                                    liveResultOwner),
                                            destKeys);
                                }
                            }
                        }
                    }
                } else {
                    Assert.eqFalse(flattenedResult, "flattenedResult");
                    try (final RowSequence.Iterator keyIter = upstream.added().getRowSequenceIterator()) {
                        while (keyIter.hasMore()) {
                            final RowSequence keys = keyIter.getNextRowSequenceWithLength(PAGE_SIZE);
                            writableSource.fillFromChunk(destContext,
                                    maybeManageAdds(chunkSource.getChunk(chunkSourceContext, keys), liveResultOwner),
                                    keys);
                        }
                    }
                }
            }
        }
        return null;
    }

    private static void maybeManage(
            @NotNull final LivenessNode liveResultOwner,
            @Nullable final LivenessReferent value) {
        if (value != null && DynamicNode.notDynamicOrIsRefreshing(value)) {
            liveResultOwner.manage(value);
        }
    }

    private <CT extends Chunk<?>> CT maybeManageAdds(
            @NotNull final CT resultChunk,
            @Nullable final LivenessNode liveResultOwner) {
        if (resultTypeIsLivenessReferent && liveResultOwner != null) {
            final ObjectChunk<? extends LivenessReferent, ?> typedChunk =
                    resultChunk.asObjectChunk().asTypedObjectChunk();
            final int chunkSize = typedChunk.size();
            for (int ii = 0; ii < chunkSize; ++ii) {
                maybeManage(liveResultOwner, typedChunk.get(ii));
            }
        }
        return resultChunk;
    }

    private void addRemovesToPrevUnmanager(
            @NotNull final RowSequence removedKeys,
            @NotNull final LivenessNode liveResultOwner) {
        try (final RowSequence.Iterator removedKeysIterator = removedKeys.getRowSequenceIterator();
                final ChunkSource.FillContext fillContext = columnSource.makeFillContext(
                        (int) Math.min(removedKeys.size(), LARGEST_POOLED_CHUNK_CAPACITY))) {
            while (removedKeysIterator.hasMore()) {
                final RowSequence chunkRemovedKeys =
                        removedKeysIterator.getNextRowSequenceWithLength(LARGEST_POOLED_CHUNK_CAPACITY);
                final WritableObjectChunk<? extends LivenessReferent, Values> removedValues =
                        WritableObjectChunk.makeWritableChunk(chunkRemovedKeys.intSize());
                columnSource.fillPrevChunk(fillContext, removedValues, chunkRemovedKeys);
                addToPrevUnmanager(liveResultOwner, removedValues);
            }
        }
    }

    private void handleModifyManagement(
            @NotNull final LivenessNode liveResultOwner,
            @NotNull final ChunkSource.FillContext fillContext,
            @NotNull final RowSequence.Iterator prevKeyIter,
            @NotNull final Chunk<? extends Values> modifiedResults) {
        final RowSequence prevKeys = prevKeyIter.getNextRowSequenceWithLength(modifiedResults.size());
        final ObjectChunk<? extends LivenessReferent, ? extends Values> typedModifiedResults =
                modifiedResults.asObjectChunk().asTypedObjectChunk();
        final WritableObjectChunk<? extends LivenessReferent, Values> prevModifiedResults =
                WritableObjectChunk.makeWritableChunk(prevKeys.intSize());
        columnSource.fillPrevChunk(fillContext, prevModifiedResults, prevKeys);
        final int chunkSize = prevModifiedResults.size();
        int sameCount = 0;
        for (int ci = 0; ci < chunkSize; ++ci) {
            if (typedModifiedResults.get(ci) == prevModifiedResults.get(ci)) {
                prevModifiedResults.set(ci, null);
                ++sameCount;
            } else {
                maybeManage(liveResultOwner, typedModifiedResults.get(ci));
            }
        }
        if (prevModifiedResults.size() == sameCount) {
            prevModifiedResults.close();
        } else {
            addToPrevUnmanager(liveResultOwner, prevModifiedResults);
        }
    }

    private synchronized void addToPrevUnmanager(
            @NotNull final LivenessNode liveResultOwner,
            @NotNull final WritableObjectChunk<? extends LivenessReferent, Values> prevValuesToUnmanage) {
        if (prevUnmanager == null) {
            prevUnmanager = new UpdateCommitterEx<>(this, SelectColumnLayer::unmanagePreviousValues);
        }
        prevUnmanager.maybeActivate(liveResultOwner);
        if (prevValueChunksToUnmanage == null) {
            prevValueChunksToUnmanage = new ArrayList<>();
        }
        prevValueChunksToUnmanage.add(prevValuesToUnmanage);
    }

    private synchronized void unmanagePreviousValues(@NotNull final LivenessNode liveResultOwner) {
        if (prevValueChunksToUnmanage == null || prevValueChunksToUnmanage.isEmpty()) {
            return;
        }
        liveResultOwner.tryUnmanage(prevValueChunksToUnmanage.stream()
                .flatMap(pvc -> StreamSupport.stream(Spliterators.spliterator(
                        new ObjectChunkIterator<>(pvc), pvc.size(), Spliterator.ORDERED), false))
                .filter(Objects::nonNull)
                .filter(DynamicNode::notDynamicOrIsRefreshing));
        prevValueChunksToUnmanage.forEach(SafeCloseable::closeIfNonNull);
        prevValueChunksToUnmanage.clear();
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

    void prepareSourcesForParallelPopulation(TableUpdate upstream) {
        // we do not permit in-column parallelization with redirected results, so do not need to worry about how this
        // interacts with the previous clearing of the redirection index that has occurred at the start of applyUpdate
        try (final WritableRowSet changedRows = upstream.added().union(upstream.getModifiedPreShift())) {
            changedRows.insert(upstream.removed());
            ((WritableSourceWithPrepareForParallelPopulation) (writableSource))
                    .prepareForParallelPopulation(changedRows);
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
    public boolean alreadyFlattenedSources() {
        return alreadyFlattenedSources;
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("{SelectColumnLayer: ").append(selectColumn.toString()).append(", layerIndex=")
                .append(getLayerIndex()).append("}");
    }

    @Override
    public boolean allowCrossColumnParallelization() {
        return selectColumn.isStateless() && inner.allowCrossColumnParallelization();
    }
}
