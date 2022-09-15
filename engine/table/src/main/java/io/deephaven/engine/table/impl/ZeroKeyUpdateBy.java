package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.engine.table.impl.util.SizedSafeCloseable;
import io.deephaven.engine.table.impl.util.UpdateSizeCalculator;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Map;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * An implementation of {@link UpdateBy} dedicated to zero key computation.
 */
class ZeroKeyUpdateBy extends UpdateBy {
    /** Apply shifts to operator outputs? */
    final boolean applyShifts;

    /** store timestamp data in an Ssa (if needed) */
    final String timestampColumnName;
    final LongSegmentedSortedArray timestampSsa;
    final ChunkSource.WithPrev<Values> timestampColumn;
    final ModifiedColumnSet timestampColumnSet;

    /**
     * Perform an updateBy without any key columns.
     *
     * @param description the operation description
     * @param source the source table
     * @param ops the operations to perform
     * @param resultSources the result sources
     * @param redirContext the row redirection shared context
     * @param control the control object.
     * @return the result table
     */
    public static Table compute(@NotNull final String description,
            @NotNull final QueryTable source,
            @NotNull final UpdateByOperator[] ops,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @NotNull final UpdateByRedirectionContext redirContext,
            @NotNull final UpdateByControl control,
            final boolean applyShifts) {
        final QueryTable result = new QueryTable(source.getRowSet(), resultSources);
        final ZeroKeyUpdateBy updateBy = new ZeroKeyUpdateBy(ops, source, redirContext, control, applyShifts);
        updateBy.doInitialAdditions();

        if (source.isRefreshing()) {
            final ZeroKeyUpdateByListener listener = updateBy.newListener(description, result);
            source.listenForUpdates(listener);
            result.addParentReference(listener);
        }

        return result;
    }

    protected ZeroKeyUpdateBy(@NotNull final UpdateByOperator[] operators,
            @NotNull final QueryTable source,
            @NotNull final UpdateByRedirectionContext redirContext,
            @NotNull final UpdateByControl control,
            final boolean applyShifts) {
        super(operators, source, redirContext, control);

        // do we need a timestamp SSA?
        this.timestampColumnName = Arrays.stream(operators)
                .filter(op -> op.getTimestampColumnName() != null)
                .map(UpdateByOperator::getTimestampColumnName)
                .findFirst().orElse(null);

        if (timestampColumnName != null) {
            this.timestampSsa = new LongSegmentedSortedArray(4096);
            this.timestampColumn =
                    ReinterpretUtils.maybeConvertToPrimitive(source.getColumnSource(this.timestampColumnName));
            this.timestampColumnSet = source.newModifiedColumnSet(timestampColumnName);
        } else {
            this.timestampSsa = null;
            this.timestampColumn = null;
            this.timestampColumnSet = null;
        }
        this.applyShifts = applyShifts;
    }

    ZeroKeyUpdateByListener newListener(@NotNull final String description, @NotNull final QueryTable result) {
        return new ZeroKeyUpdateByListener(description, source, result);
    }

    private void processUpdateForSsa(TableUpdate upstream) {
        final boolean stampModified = upstream.modifiedColumnSet().containsAny(timestampColumnSet);

        final RowSet restampRemovals;
        final RowSet restampAdditions;

        // modifies are remove + add operations
        if (stampModified) {
            restampAdditions = upstream.added().union(upstream.modified());
            restampRemovals = upstream.removed().union(upstream.getModifiedPreShift());
        } else {
            restampAdditions = upstream.added();
            restampRemovals = upstream.removed();
        }

        // removes
        if (restampRemovals.isNonempty()) {
            final int size = (int) Math.min(restampRemovals.size(), 4096);
            try (final RowSequence.Iterator it = restampRemovals.getRowSequenceIterator();
                    final ChunkSource.GetContext context = timestampColumn.makeGetContext(size);
                    final WritableLongChunk<? extends Values> ssaValues = WritableLongChunk.makeWritableChunk(size);
                    final WritableLongChunk<OrderedRowKeys> ssaKeys = WritableLongChunk.makeWritableChunk(size)) {

                MutableLong lastTimestamp = new MutableLong(NULL_LONG);
                while (it.hasMore()) {
                    RowSequence chunkRs = it.getNextRowSequenceWithLength(4096);

                    // get the chunks for values and keys
                    LongChunk<? extends Values> valuesChunk =
                            timestampColumn.getPrevChunk(context, chunkRs).asLongChunk();
                    LongChunk<OrderedRowKeys> keysChunk = chunkRs.asRowKeyChunk();

                    // push only non-null values/keys into the Ssa
                    fillChunkWithNonNull(keysChunk, valuesChunk, ssaKeys, ssaValues, lastTimestamp);
                    timestampSsa.remove(ssaValues, ssaKeys);
                }
            }
        }

        // shifts
        if (upstream.shifted().nonempty()) {
            final int size = Math.max(
                    upstream.modified().intSize() + Math.max(upstream.added().intSize(), upstream.removed().intSize()),
                    (int) upstream.shifted().getEffectiveSize());
            try (final RowSet prevRowSet = source.getRowSet().copyPrev();
                    final RowSet withoutMods = prevRowSet.minus(upstream.getModifiedPreShift());
                    final ColumnSource.GetContext getContext = timestampColumn.makeGetContext(size)) {

                final RowSetShiftData.Iterator sit = upstream.shifted().applyIterator();
                while (sit.hasNext()) {
                    sit.next();
                    try (final RowSet subRowSet = withoutMods.subSetByKeyRange(sit.beginRange(), sit.endRange());
                            final RowSet rowSetToShift = subRowSet.minus(upstream.removed())) {
                        if (rowSetToShift.isEmpty()) {
                            continue;
                        }

                        final LongChunk<? extends Values> shiftValues =
                                timestampColumn.getPrevChunk(getContext, rowSetToShift).asLongChunk();

                        timestampSsa.applyShift(shiftValues, rowSetToShift.asRowKeyChunk(), sit.shiftDelta());
                    }
                }
            }
        }

        // adds
        if (restampAdditions.isNonempty()) {
            final int size = (int) Math.min(restampAdditions.size(), 4096);
            try (final RowSequence.Iterator it = restampAdditions.getRowSequenceIterator();
                    final ChunkSource.GetContext context = timestampColumn.makeGetContext(size);
                    final WritableLongChunk<? extends Values> ssaValues = WritableLongChunk.makeWritableChunk(size);
                    final WritableLongChunk<OrderedRowKeys> ssaKeys = WritableLongChunk.makeWritableChunk(size)) {
                MutableLong lastTimestamp = new MutableLong(NULL_LONG);

                while (it.hasMore()) {
                    RowSequence chunkRs = it.getNextRowSequenceWithLength(4096);

                    // get the chunks for values and keys
                    LongChunk<? extends Values> valuesChunk = timestampColumn.getChunk(context, chunkRs).asLongChunk();
                    LongChunk<OrderedRowKeys> keysChunk = chunkRs.asRowKeyChunk();

                    // push only non-null values/keys into the Ssa
                    fillChunkWithNonNull(keysChunk, valuesChunk, ssaKeys, ssaValues, lastTimestamp);
                    timestampSsa.insert(ssaValues, ssaKeys);
                }
            }
        }
    }

    private void fillChunkWithNonNull(LongChunk<OrderedRowKeys> keysChunk, LongChunk<? extends Values> valuesChunk,
            WritableLongChunk<OrderedRowKeys> ssaKeys, WritableLongChunk<? extends Values> ssaValues,
            MutableLong lastTimestamp) {
        // reset the insertion chunks
        ssaValues.setSize(0);
        ssaKeys.setSize(0);

        // add only non-null timestamps to this Ssa
        for (int i = 0; i < valuesChunk.size(); i++) {
            long ts = valuesChunk.get(i);
            if (ts < lastTimestamp.longValue()) {
                throw (new IllegalStateException(
                        "updateBy time-based operators require non-descending timestamp values"));
            }
            if (ts != NULL_LONG) {
                ssaValues.add(ts);
                ssaKeys.add(keysChunk.get(i));
            }
        }
    }


    void doInitialAdditions() {
        final TableUpdateImpl fakeUpdate = new TableUpdateImpl(source.getRowSet(),
                RowSetFactory.empty(),
                RowSetFactory.empty(),
                RowSetShiftData.EMPTY,
                ModifiedColumnSet.ALL);
        if (redirContext.isRedirected()) {
            redirContext.processUpdateForRedirection(fakeUpdate, source.getRowSet());
        }

        // add all the SSA data
        if (timestampColumnName != null) {
            processUpdateForSsa(fakeUpdate);
        }

        try (final UpdateContext ctx = new UpdateContext(fakeUpdate, null, true)) {
            ctx.setAllAffected();

            // do a reprocessing phase for operators that can't add directly
            ctx.processRows(RowSetShiftData.EMPTY);
        }
    }

    /**
     * An object to hold the transient state during a single
     * {@link InstrumentedTableUpdateListenerAdapter#onUpdate(TableUpdate)} update cycle.
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

        /** true if any operator requested positions */
        boolean anyRequirePositions;

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

        /** A Chunk of longs to store the posiitions of the keys being updated */
        final SizedLongChunk<OrderedRowKeys> posChunk;

        /** A sharedContext to be used with previous values */
        SharedContext prevSharedContext;

        /** An array of {@link ChunkSource.FillContext}s for previous values */
        ChunkSource.FillContext[] prevFillContexts;

        /** An array of chunks for previous values */
        WritableChunk<Values>[] prevWorkingChunks;

        /** A Long Chunk for previous keys */
        WritableLongChunk<OrderedRowKeys> prevKeyChunk;

        final RowSet rowsToProcess;

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
            this.posChunk = new SizedLongChunk<>();
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

                opContext[opIdx] = operators[opIdx].makeUpdateContext(chunkSize, timestampSsa);

                final int slotPosition = inputSourceSlots[opIdx];
                if (fillContexts[slotPosition] == null) {
                    fillContexts[slotPosition] = new SizedSafeCloseable<>(
                            sz -> inputSources[slotPosition].makeFillContext(sz, getSharedContext()));
                    fillContexts[slotPosition].ensureCapacity(chunkSize);
                    postWorkingChunks[slotPosition] = new SizedSafeCloseable<>(
                            sz -> inputSources[slotPosition].getChunkType().makeWritableChunk(sz));
                    postWorkingChunks[slotPosition].ensureCapacity(chunkSize);

                    // Note that these don't participate in setChunkSize() because nothing will use them. If that
                    // changes then these must also become SizedSafeCloseables.
                    if (hasModifies) {
                        prevFillContexts[slotPosition] =
                                inputSources[opIdx].makeFillContext(chunkSize, prevSharedContext);
                        prevWorkingChunks[slotPosition] =
                                inputSources[opIdx].getChunkType().makeWritableChunk(chunkSize);
                    }
                }
            }

            // retrieve the affected rows from all operator update contexts
            WritableRowSet tmp = RowSetFactory.empty();
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (!opAffected[opIdx]) {
                    continue;
                }
                // trigger the operator to determine its own set of affected rows (window-specific), do not close()
                // since this is managed by the operator context
                final RowSet rs =
                        opContext[opIdx].determineAffectedRows(upstream, source.getRowSet(), isInitializeStep);

                // union the operator rowsets together to get a global set
                tmp.insert(rs);
            }
            rowsToProcess = tmp;
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
            this.posChunk.ensureCapacity(newChunkSize);

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

        void initializeFor(@NotNull final RowSet updateRowSet) {
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    operators[opIdx].initializeFor(opContext[opIdx], updateRowSet);
                    anyRequireKeys |= operators[opIdx].requiresKeys();
                    anyRequirePositions |= operators[opIdx].requiresPositions();
                }
            }

            if (anyRequireKeys) {
                keyChunk.ensureCapacity(chunkSize);
            }
            if (anyRequirePositions) {
                posChunk.ensureCapacity(chunkSize);
            }
        }

        void finishFor() {
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    operators[opIdx].finishFor(opContext[opIdx]);
                }
            }

            anyRequireKeys = false;
            Arrays.fill(inputChunkPopulated, false);
        }

        @Override
        public void close() {
            sharedContext.close();
            keyChunk.close();
            posChunk.close();
            rowsToProcess.close();

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
                @NotNull final RowSet preShiftUpdateRowSet) {

        }

        /**
         * Locate the smallest key that requires reprocessing and then replay the table from that point
         */
        private void processRows(RowSetShiftData shifted) {
            // Get a sub-index of the source from that minimum reprocessing index and make sure we update our
            // contextual chunks and FillContexts to an appropriate size for this step.
            final RowSet sourceRowSet = source.getRowSet();

            final int newChunkSize = (int) Math.min(control.chunkCapacityOrDefault(), rowsToProcess.size());
            setChunkSize(newChunkSize);

            for (int opIndex = 0; opIndex < operators.length; opIndex++) {
                if (opAffected[opIndex]) {
                    final long keyStart = opContext[opIndex].getAffectedRows().firstRowKey();
                    final long keyBefore;
                    try (final RowSet.SearchIterator sit = sourceRowSet.searchIterator()) {
                        keyBefore = sit.binarySearchValue(
                                (compareTo, ignored) -> Long.compare(keyStart - 1, compareTo), 1);
                    }
                    // apply a shift to keyBefore since the output column is still in prev key space-

                    operators[opIndex].resetForProcess(opContext[opIndex], sourceRowSet, keyBefore);
                }
            }

            // Now iterate rowset to reprocess.
            if (rowsToProcess.isEmpty()) {
                return;
            }

            initializeFor(rowsToProcess);

            try (final RowSet positionsToProcess =
                    anyRequirePositions ? source.getRowSet().invert(rowsToProcess) : null;
                    final RowSequence.Iterator keyIt = rowsToProcess.getRowSequenceIterator();
                    final RowSequence.Iterator posIt = positionsToProcess == null ? null
                            : positionsToProcess.getRowSequenceIterator()) {

                while (keyIt.hasMore()) {
                    sharedContext.reset();
                    if (prevSharedContext != null) {
                        prevSharedContext.reset();
                    }
                    Arrays.fill(inputChunkPopulated, false);

                    final RowSequence chunkOk = keyIt.getNextRowSequenceWithLength(chunkSize);
                    if (anyRequireKeys) {
                        chunkOk.fillRowKeyChunk(keyChunk.get());
                    }
                    if (anyRequirePositions) {
                        posIt.getNextRowSequenceWithLength(chunkSize).fillRowKeyChunk(posChunk.get());
                    }

                    for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                        if (!opAffected[opIdx]) {
                            continue;
                        }

                        final UpdateByOperator currentOp = operators[opIdx];
                        final int slotPosition = inputSourceSlots[opIdx];
                        // is this chunk relevant to this operator? If so, then intersect and process only the
                        // relevant rows
                        if (chunkOk.firstRowKey() <= opContext[opIdx].getAffectedRows().lastRowKey()
                                && chunkOk.lastRowKey() >= opContext[opIdx].getAffectedRows().firstRowKey()) {
                            try (final RowSet rs = chunkOk.asRowSet();
                                    final RowSet intersect = rs.intersect(opContext[opIdx].getAffectedRows())) {

                                prepareValuesChunkFor(opIdx, slotPosition, false, true, intersect, intersect,
                                        null, postWorkingChunks[slotPosition].get(),
                                        null, fillContexts[slotPosition].get());
                                currentOp.processChunk(opContext[opIdx],
                                        intersect,
                                        keyChunk.get(),
                                        posChunk.get(),
                                        postWorkingChunks[slotPosition].get(),
                                        source.getRowSet());
                            }
                        }
                    }
                }
                finishFor();
            }
        }

        /**
         * Prepare the specified chunk for use.
         *
         * @param opIdx the operator index
         * @param usePrev if previous values should be fetched
         * @param chunkOk the {@link RowSequence} for current values
         * @param prevChunkOk the {@link RowSequence} for previous values.
         */
        private void prepareValuesChunkFor(final int opIdx,
                final int inputSlot,
                final boolean usePrev,
                final boolean useCurrent,
                final RowSequence chunkOk,
                final RowSequence prevChunkOk,
                final WritableChunk<Values> prevWorkingChunk,
                final WritableChunk<Values> postWorkingChunk,
                final ChunkSource.FillContext prevFillContext,
                final ChunkSource.FillContext postFillContext) {
            if (!operators[opIdx].requiresValues(opContext[opIdx])) {
                return;
            }

            if (!inputChunkPopulated[inputSlot]) {
                // Using opIdx below is OK, because if we are sharing an input slot, we are referring to the same
                // input source. Instead of maintaining another array of sourced by slot, we'll just use the opIdx
                inputChunkPopulated[inputSlot] = true;
                if (usePrev) {
                    inputSources[opIdx].fillPrevChunk(prevFillContext, prevWorkingChunk, prevChunkOk);
                }

                if (useCurrent) {
                    inputSources[opIdx].fillChunk(postFillContext, postWorkingChunk, chunkOk);
                }
            }
        }
    }

    /**
     * The Listener for apply an upstream {@link InstrumentedTableUpdateListenerAdapter#onUpdate(TableUpdate) update}
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
            // update the Ssa
            if (timestampColumnName != null) {
                processUpdateForSsa(upstream);
            }

            try (final UpdateContext ctx = new UpdateContext(upstream, inputModifiedColumnSets, false)) {


                if (applyShifts) {
                    if (redirContext.isRedirected()) {
                        redirContext.processUpdateForRedirection(upstream, source.getRowSet());
                    } else {
                        // We will not mess with shifts if we are using a redirection because we'll have applied the
                        // shift
                        // to the redirection index already by now.
                        if (upstream.shifted().nonempty()) {
                            try (final RowSet prevIdx = source.getRowSet().copyPrev()) {
                                upstream.shifted().apply((begin, end, delta) -> {
                                    try (final RowSet subRowSet = prevIdx.subSetByKeyRange(begin, end)) {
                                        for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                                            operators[opIdx].applyOutputShift(ctx.opContext[opIdx], subRowSet, delta);
                                        }
                                    }
                                });
                            }
                        }
                    }
                }

                // Now do the reprocessing phase.
                ctx.processRows(upstream.shifted());

                final TableUpdateImpl downstream = new TableUpdateImpl();
                // copy these rowSets since TableUpdateImpl#reset will close them with the upstream update
                downstream.added = upstream.added().copy();
                downstream.removed = upstream.removed().copy();
                downstream.shifted = upstream.shifted();

                downstream.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
                downstream.modifiedColumnSet.clear();

                if (upstream.modified().isNonempty() || ctx.anyModified()) {
                    WritableRowSet modifiedRowSet = RowSetFactory.empty();
                    downstream.modified = modifiedRowSet;
                    if (upstream.modified().isNonempty()) {
                        // Transform any untouched modified columns to the output.
                        transformer.clearAndTransform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);
                        modifiedRowSet.insert(upstream.modified());
                    }

                    for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                        if (ctx.opAffected[opIdx]) {
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
                }

                // set the modified columns if any operators made changes (add/rem/modify)
                for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                    if (ctx.opAffected[opIdx]) {
                        downstream.modifiedColumnSet.setAll(outputModifiedColumnSets[opIdx]);
                    }
                }

                result.notifyListeners(downstream);
            }
        }
    }
}
