package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.util.SizedSafeCloseable;
import io.deephaven.engine.table.impl.util.UpdateSizeCalculator;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Map;

/**
 * An implementation of {@link UpdateBy} dedicated to zero key computation.
 */
class ZeroKeyUpdateBy extends UpdateBy {

    /**
     * Perform an updateBy without any key columns.
     *
     * @param description the operation description
     * @param source the source table
     * @param ops the operations to perform
     * @param resultSources the result sources
     * @param rowRedirection the {@link io.deephaven.engine.table.impl.util.RowRedirection}, if one is used.
     * @param control the control object.
     * @return the result table
     */
    public static Table compute(@NotNull final String description,
            @NotNull final QueryTable source,
            @NotNull final UpdateByOperator[] ops,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @Nullable final WritableRowRedirection rowRedirection,
            @NotNull final UpdateByControl control) {
        final QueryTable result = new QueryTable(source.getRowSet(), resultSources);
        final ZeroKeyUpdateBy updateBy = new ZeroKeyUpdateBy(ops, source, rowRedirection, control);
        updateBy.doInitialAdditions();

        if (source.isRefreshing()) {
            if (rowRedirection != null) {
                rowRedirection.startTrackingPrevValues();
            }
            Arrays.stream(ops).forEach(UpdateByOperator::startTrackingPrev);
            final ZeroKeyUpdateByListener listener = updateBy.newListener(description, result);
            source.listenForUpdates(listener);
            result.addParentReference(listener);
        }

        return result;
    }

    protected ZeroKeyUpdateBy(@NotNull final UpdateByOperator[] operators,
            @NotNull final QueryTable source,
            @Nullable final WritableRowRedirection rowRedirection,
            @NotNull final UpdateByControl control) {
        super(operators, source, rowRedirection, control);
    }

    ZeroKeyUpdateByListener newListener(@NotNull final String description, @NotNull final QueryTable result) {
        return new ZeroKeyUpdateByListener(description, source, result);
    }

    void doInitialAdditions() {
        final TableUpdateImpl fakeUpdate = new TableUpdateImpl(source.getRowSet(),
                RowSetFactory.empty(),
                RowSetFactory.empty(),
                RowSetShiftData.EMPTY,
                ModifiedColumnSet.ALL);
        try (final UpdateContext ctx = new UpdateContext(fakeUpdate, null, true)) {
            ctx.setAllAffected();
            if (rowRedirection != null && source.isRefreshing()) {
                processUpdateForRedirection(fakeUpdate);
            }
            ctx.doUpdate(source.getRowSet(), source.getRowSet(), UpdateType.Add);
        }
    }

    /**
     * An object to hold the transient state during a single {@link ShiftAwareListener#onUpdate(TableUpdate)} update
     * cycle.
     */
    private class UpdateContext implements SafeCloseable {
        /** The expected size of chunks to the various update stages */
        int chunkSize;

        /** An indicator of if each slot has been populated with data or not for this phase. */
        boolean[] inputChunkPopulated;

        /** An array of boolean denoting which operators are affected by the current update. */
        final boolean[] opAffected;

        /** true if any operator requested keys */
        boolean anyRequireKeys;

        /** An array of context objects for each underlying operator */
        final UpdateByOperator.UpdateContext[] opContext;

        /** A {@link SharedContext} to be used while creating other contexts */
        SharedContext sharedContext = SharedContext.makeSharedContext();

        /** An array of {@link ChunkSource.FillContext}s for each input column */
        final SizedSafeCloseable<ChunkSource.FillContext>[] fillContexts;

        /** A set of chunks used to store post-shift working values */
        final SizedSafeCloseable<WritableChunk<Values>>[] postWorkingChunks;

        /** A Chunk of longs to store the keys being updated */
        final SizedLongChunk<OrderedRowKeys> keyChunk;

        /** A sharedContext to be used with previous values */
        SharedContext prevSharedContext;

        /** An array of {@link ChunkSource.FillContext}s for previous values */
        ChunkSource.FillContext[] prevFillContexts;

        /** An array of chunks for previous values */
        WritableChunk<Values>[] prevWorkingChunks;

        /** A Long Chunk for previous keys */
        WritableLongChunk<OrderedRowKeys> prevKeyChunk;

        final long smallestModifiedKey;

        @SuppressWarnings("resource")
        UpdateContext(@NotNull final TableUpdate upstream,
                @Nullable final ModifiedColumnSet[] inputModifiedColumnSets,
                final boolean isInitializeStep) {
            final int updateSize = UpdateSizeCalculator.chunkSize(upstream, control.chunkCapacityOrDefault());

            this.chunkSize =
                    UpdateSizeCalculator.chunkSize(updateSize, upstream.shifted(), control.chunkCapacityOrDefault());
            this.opAffected = new boolean[operators.length];
            // noinspection unchecked
            this.fillContexts = new SizedSafeCloseable[operators.length];
            this.opContext = new UpdateByOperator.UpdateContext[operators.length];
            this.keyChunk = new SizedLongChunk<>();
            this.inputChunkPopulated = new boolean[operators.length];

            if (upstream.shifted().nonempty()) {
                this.prevKeyChunk = WritableLongChunk.makeWritableChunk(chunkSize);
            }

            final boolean hasModifies = upstream.modified().isNonempty();
            if (hasModifies) {
                // noinspection unchecked
                this.prevWorkingChunks = new WritableChunk[operators.length];
                this.prevSharedContext = SharedContext.makeSharedContext();
                this.prevFillContexts = new ChunkSource.FillContext[operators.length];
            }

            final boolean upstreamAppendOnly =
                    isInitializeStep || UpdateByOperator.isAppendOnly(upstream, source.getRowSet().lastRowKeyPrev());
            smallestModifiedKey = upstreamAppendOnly ? Long.MAX_VALUE
                    : UpdateByOperator.determineSmallestVisitedKey(upstream, source.getRowSet());

            // noinspection unchecked
            this.postWorkingChunks = new SizedSafeCloseable[operators.length];
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                opAffected[opIdx] = upstream.added().isNonempty() ||
                        upstream.removed().isNonempty() ||
                        upstream.shifted().nonempty() ||
                        (upstream.modifiedColumnSet().nonempty() && (inputModifiedColumnSets == null
                                || upstream.modifiedColumnSet().containsAny(inputModifiedColumnSets[opIdx])));
                if (!opAffected[opIdx]) {
                    continue;
                }

                opContext[opIdx] = operators[opIdx].makeUpdateContext(chunkSize);

                final int slotPosition = inputSourceSlots[opIdx];
                if (fillContexts[slotPosition] == null) {
                    fillContexts[slotPosition] = new SizedSafeCloseable<>(
                            sz -> inputSources[slotPosition].makeFillContext(sz, getSharedContext()));
                    fillContexts[slotPosition].ensureCapacity(chunkSize);
                    postWorkingChunks[slotPosition] = new SizedSafeCloseable<>(
                            sz -> inputSources[slotPosition].getChunkType().makeWritableChunk(sz));
                    postWorkingChunks[slotPosition].ensureCapacity(chunkSize);

                    // Note that these don't participate in setChunkSize() because nothing will use them. If that
                    // changes
                    // then these must also become SizedSafeCloseables.
                    if (hasModifies) {
                        prevFillContexts[slotPosition] =
                                inputSources[opIdx].makeFillContext(chunkSize, prevSharedContext);
                        prevWorkingChunks[slotPosition] =
                                inputSources[opIdx].getChunkType().makeWritableChunk(chunkSize);
                    }
                }

                operators[opIdx].initializeForUpdate(opContext[opIdx], upstream, source.getRowSet(), false,
                        upstreamAppendOnly);
            }
        }

        public SharedContext getSharedContext() {
            return sharedContext;
        }

        void setChunkSize(int newChunkSize) {
            if (newChunkSize <= chunkSize) {
                return;
            }

            // We have to close and recreate the shared context because a .reset() is not enough to ensure that any
            // cached chunks that something stuffed into there are resized.
            this.sharedContext.close();
            this.sharedContext = SharedContext.makeSharedContext();

            if (prevSharedContext != null) {
                this.prevSharedContext.close();
                this.prevSharedContext = null;
            }

            this.chunkSize = newChunkSize;
            this.keyChunk.ensureCapacity(newChunkSize);

            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (!opAffected[opIdx]) {
                    continue;
                }

                operators[opIdx].setChunkSize(opContext[opIdx], newChunkSize);
                if (fillContexts[opIdx] != null) {
                    fillContexts[opIdx].ensureCapacity(newChunkSize);
                    postWorkingChunks[opIdx].ensureCapacity(newChunkSize);

                    // Note that this doesn't include the prevFillContexts or prevWorkingChunks. If they become
                    // needed for an op, they must be added here.
                }
            }
        }

        void initializeFor(@NotNull final RowSet updateRowSet,
                @NotNull final UpdateType type) {
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    operators[opIdx].initializeFor(opContext[opIdx], updateRowSet, type);
                    anyRequireKeys |= operators[opIdx].requiresKeys();
                }
            }

            if (anyRequireKeys) {
                keyChunk.ensureCapacity(chunkSize);
            }
        }

        void finishFor(@NotNull final UpdateType type) {
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    operators[opIdx].finishFor(opContext[opIdx], type);
                }
            }

            anyRequireKeys = false;
            Arrays.fill(inputChunkPopulated, false);
        }

        @Override
        public void close() {
            sharedContext.close();
            keyChunk.close();

            if (prevKeyChunk != null) {
                prevKeyChunk.close();
            }

            if (prevSharedContext != null) {
                prevSharedContext.close();
            }

            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opContext[opIdx] != null) {
                    opContext[opIdx].close();
                }

                if (fillContexts[opIdx] != null) {
                    fillContexts[opIdx].close();
                }

                if (postWorkingChunks[opIdx] != null) {
                    postWorkingChunks[opIdx].close();
                }

                if (prevFillContexts != null && prevFillContexts[opIdx] != null) {
                    prevFillContexts[opIdx].close();
                }

                if (prevWorkingChunks != null && prevWorkingChunks[opIdx] != null) {
                    prevWorkingChunks[opIdx].close();
                }
            }
        }

        /**
         * Mark all columns as affected
         */
        public void setAllAffected() {
            Arrays.fill(opAffected, true);
        }

        /**
         * Check if any of the operators have produced additional modified rows.
         *
         * @return true if any operator produced more modified rows.
         */
        boolean anyModified() {
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx] && operators[opIdx].anyModified(opContext[opIdx])) {
                    return true;
                }
            }
            return false;
        }

        void doUpdate(@NotNull final RowSet updateRowSet,
                @NotNull final RowSet preShiftUpdateRowSet,
                @NotNull final UpdateType type) {
            if (updateRowSet.isEmpty()) {
                return;
            }

            try (final RowSequence.Iterator okIt = updateRowSet.getRowSequenceIterator();
                    final RowSequence.Iterator preShiftOkIt = preShiftUpdateRowSet == updateRowSet ? null
                            : preShiftUpdateRowSet.getRowSequenceIterator()) {
                initializeFor(updateRowSet, type);

                while (okIt.hasMore()) {
                    sharedContext.reset();
                    if (prevSharedContext != null) {
                        prevSharedContext.reset();
                    }
                    Arrays.fill(inputChunkPopulated, false);

                    final RowSequence chunkOk = okIt.getNextRowSequenceWithLength(chunkSize);
                    final RowSequence prevChunkOk = preShiftUpdateRowSet == updateRowSet ? chunkOk
                            : preShiftOkIt.getNextRowSequenceWithLength(chunkSize);

                    if (anyRequireKeys) {
                        chunkOk.fillRowKeyChunk(keyChunk.get());
                        if (prevChunkOk != chunkOk) {
                            prevChunkOk.fillRowKeyChunk(prevKeyChunk);
                        }
                    }

                    for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                        if (!opAffected[opIdx]) {
                            continue;
                        }

                        final UpdateByOperator currentOp = operators[opIdx];
                        final int slotPosition = inputSourceSlots[opIdx];
                        if (type == UpdateType.Add) {
                            prepareValuesChunkFor(opIdx, slotPosition, false, true, chunkOk, prevChunkOk,
                                    null, postWorkingChunks[slotPosition].get(),
                                    null, fillContexts[slotPosition].get());
                            currentOp.addChunk(opContext[opIdx], chunkOk, keyChunk.get(),
                                    postWorkingChunks[slotPosition].get(), 0);
                        } else if (type == UpdateType.Remove) {
                            prepareValuesChunkFor(opIdx, slotPosition, true, false, chunkOk, prevChunkOk,
                                    postWorkingChunks[slotPosition].get(), null,
                                    fillContexts[slotPosition].get(), null);
                            currentOp.removeChunk(opContext[opIdx], keyChunk.get(),
                                    postWorkingChunks[slotPosition].get(), 0);
                        } else if (type == UpdateType.Modify) {
                            prepareValuesChunkFor(opIdx, slotPosition, true, true, chunkOk, prevChunkOk,
                                    prevWorkingChunks[slotPosition], postWorkingChunks[slotPosition].get(),
                                    prevFillContexts[slotPosition], fillContexts[slotPosition].get());
                            currentOp.modifyChunk(opContext[opIdx],
                                    prevKeyChunk == null ? keyChunk.get() : prevKeyChunk,
                                    keyChunk.get(),
                                    prevWorkingChunks[slotPosition],
                                    postWorkingChunks[slotPosition].get(),
                                    0);
                        } else if (type == UpdateType.Reprocess) {
                            // TODO: When we reprocess rows, we are basically re-adding the entire table starting at the
                            // lowest key.
                            // Since every operator might start at a different key, we could try to be efficient and not
                            // replay
                            // chunks of rows to operators that don't actually need them.
                            //
                            // At the time of writing, any op that reprocesses uses the same logic to decide when,
                            // so there is no need for fancyness deciding if we need to push this particular set
                            // of RowSequence through.
                            prepareValuesChunkFor(opIdx, slotPosition, false, true, chunkOk, null,
                                    null, postWorkingChunks[slotPosition].get(),
                                    null, fillContexts[slotPosition].get());
                            currentOp.reprocessChunk(opContext[opIdx],
                                    chunkOk,
                                    keyChunk.get(),
                                    postWorkingChunks[slotPosition].get(),
                                    source.getRowSet());
                        }
                    }
                }

                finishFor(type);
            }
        }

        /**
         * Locate the smallest key that requires reprocessing and then replay the table from that point
         */
        private void reprocessRows(RowSetShiftData shifted) {
            // Get a sub-index of the source from that minimum reprocessing index and make sure we update our
            // contextual chunks and FillContexts to an appropriate size for this step.
            final RowSet sourceRowSet = source.getRowSet();
            try (final RowSet indexToReprocess =
                    sourceRowSet.subSetByKeyRange(smallestModifiedKey, sourceRowSet.lastRowKey())) {
                final int newChunkSize = (int) Math.min(control.chunkCapacityOrDefault(), indexToReprocess.size());
                setChunkSize(newChunkSize);

                final long keyBefore;
                try (final RowSet.SearchIterator sit = sourceRowSet.searchIterator()) {
                    keyBefore = sit.binarySearchValue(
                            (compareTo, ignored) -> Long.compare(smallestModifiedKey - 1, compareTo), 1);
                }

                for (int opRowSet = 0; opRowSet < operators.length; opRowSet++) {
                    if (opAffected[opRowSet]) {
                        operators[opRowSet].resetForReprocess(opContext[opRowSet], sourceRowSet, keyBefore);
                    }
                }

                // We will not mess with shifts if we are using a redirection because we'll have applied the shift
                // to the redirection index already by now.
                if (rowRedirection == null && shifted.nonempty()) {
                    try (final RowSet prevIdx = source.getRowSet().copyPrev()) {
                        shifted.apply((begin, end, delta) -> {
                            try (final RowSet subRowSet = prevIdx.subSetByKeyRange(begin, end)) {
                                for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                                    operators[opIdx].applyOutputShift(opContext[opIdx], subRowSet, delta);
                                }
                            }
                        });
                    }
                }

                // Now iterate index to reprocess.
                doUpdate(indexToReprocess, indexToReprocess, UpdateType.Reprocess);
            }
        }

        /**
         * Prepare the specified chunk for use.
         *
         * @param opRowSet the operator index
         * @param usePrev if previous values should be fetched
         * @param chunkOk the {@link RowSequence} for current values
         * @param prevChunkOk the {@link RowSequence} for previous values.
         */
        private void prepareValuesChunkFor(final int opRowSet,
                final int inputSlot,
                final boolean usePrev,
                final boolean useCurrent,
                final RowSequence chunkOk,
                final RowSequence prevChunkOk,
                final WritableChunk<Values> prevWorkingChunk,
                final WritableChunk<Values> postWorkingChunk,
                final ChunkSource.FillContext prevFillContext,
                final ChunkSource.FillContext postFillContext) {
            if (!operators[opRowSet].requiresValues(opContext[opRowSet])) {
                return;
            }

            if (!inputChunkPopulated[inputSlot]) {
                // Using opRowSet below is OK, because if we are sharing an input slot, we are referring to the same
                // input source. Instead of maintaining another array of sourced by slot, we'll just use the opRowSet
                inputChunkPopulated[inputSlot] = true;
                if (usePrev) {
                    inputSources[opRowSet].fillPrevChunk(prevFillContext, prevWorkingChunk, prevChunkOk);
                }

                if (useCurrent) {
                    inputSources[opRowSet].fillChunk(postFillContext, postWorkingChunk, chunkOk);
                }
            }
        }

        private void onBucketsRemoved(@NotNull final RowSet removedBuckets) {
            for (final UpdateByOperator operator : operators) {
                operator.onBucketsRemoved(removedBuckets);
            }
        }

        public boolean canAnyProcessNormally() {
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx] && operators[opIdx].canProcessNormalUpdate(opContext[opIdx])) {
                    return true;
                }
            }

            return false;
        }
    }

    /**
     * The Listener for apply an upstream {@link ShiftAwareListener#onUpdate(Update) update}
     */
    class ZeroKeyUpdateByListener extends InstrumentedTableUpdateListenerAdapter {
        private final QueryTable result;
        private final ModifiedColumnSet[] inputModifiedColumnSets;
        private final ModifiedColumnSet[] outputModifiedColumnSets;
        private final ModifiedColumnSet.Transformer transformer;

        public ZeroKeyUpdateByListener(@Nullable String description,
                @NotNull final QueryTable source,
                @NotNull final QueryTable result) {
            super(description, source, false);
            this.result = result;
            this.inputModifiedColumnSets = new ModifiedColumnSet[operators.length];
            this.outputModifiedColumnSets = new ModifiedColumnSet[operators.length];

            for (int ii = 0; ii < operators.length; ii++) {
                final String[] outputColumnNames = operators[ii].getOutputColumnNames();
                inputModifiedColumnSets[ii] = source.newModifiedColumnSet(operators[ii].getAffectingColumnNames());
                outputModifiedColumnSets[ii] = result.newModifiedColumnSet(outputColumnNames);
            }

            this.transformer =
                    source.newModifiedColumnSetTransformer(result, source.getDefinition().getColumnNamesArray());
        }

        @Override
        public void onUpdate(TableUpdate upstream) {
            try (final UpdateContext ctx = new UpdateContext(upstream, inputModifiedColumnSets, false)) {
                if (rowRedirection != null) {
                    processUpdateForRedirection(upstream);
                }

                // If anything can process normal operations we have to pass them down, otherwise we can skip this
                // entirely.
                if (ctx.canAnyProcessNormally()) {
                    ctx.doUpdate(upstream.removed(), upstream.removed(), UpdateType.Remove);
                    if (upstream.shifted().nonempty()) {
                        try (final WritableRowSet prevRowSet = source.getRowSet().copyPrev();
                                final RowSet modPreShift = upstream.getModifiedPreShift()) {

                            prevRowSet.remove(upstream.removed());
                            for (int ii = 0; ii < operators.length; ii++) {
                                operators[ii].initializeFor(ctx.opContext[ii], prevRowSet, UpdateType.Shift);
                                operators[ii].applyShift(ctx.opContext[ii], prevRowSet, upstream.shifted());
                                operators[ii].finishFor(ctx.opContext[ii], UpdateType.Shift);
                            }
                            ctx.doUpdate(upstream.modified(), modPreShift, UpdateType.Modify);
                        }
                    } else {
                        ctx.doUpdate(upstream.modified(), upstream.modified(), UpdateType.Modify);
                    }
                    ctx.doUpdate(upstream.added(), upstream.added(), UpdateType.Add);
                }

                if (source.getRowSet().isEmpty()) {
                    ctx.onBucketsRemoved(RowSetFactory.fromKeys(0));
                }

                // Now do the reprocessing phase.
                ctx.reprocessRows(upstream.shifted());

                final TableUpdateImpl downstream = new TableUpdateImpl();
                // copy these rowSets since TableUpdateImpl#reset will close them with the upstream update
                downstream.added = upstream.added().copy();
                downstream.removed = upstream.removed().copy();
                downstream.shifted = upstream.shifted();

                if (upstream.modified().isNonempty() || ctx.anyModified()) {
                    downstream.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
                    downstream.modifiedColumnSet.clear();

                    WritableRowSet modifiedRowSet = RowSetFactory.empty();
                    downstream.modified = modifiedRowSet;
                    if (upstream.modified().isNonempty()) {
                        // Transform any untouched modified columns to the output.
                        transformer.clearAndTransform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);
                        modifiedRowSet.insert(upstream.modified());
                    }

                    for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                        if (ctx.opAffected[opIdx]) {
                            downstream.modifiedColumnSet.setAll(outputModifiedColumnSets[opIdx]);
                            if (operators[opIdx].anyModified(ctx.opContext[opIdx])) {
                                modifiedRowSet
                                        .insert(operators[opIdx].getAdditionalModifications(ctx.opContext[opIdx]));
                            }
                        }
                    }

                    if (ctx.anyModified()) {
                        modifiedRowSet.remove(upstream.added());
                    }
                } else {
                    downstream.modified = RowSetFactory.empty();
                    downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                }
                result.notifyListeners(downstream);
            }
        }
    }
}
