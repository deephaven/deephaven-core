package io.deephaven.engine.table.impl.updateby;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.UpdateByCumulativeOperator;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.UpdateByWindowedOperator;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class UpdateByWindow {
    protected final boolean windowed;
    @Nullable
    protected final String timestampColumnName;
    protected final long prevUnits;
    protected final long fwdUnits;

    // store the operators for this window
    protected final UpdateByOperator[] operators;
    // store the index in the {@link UpdateBy.inputSources}
    protected final int[] operatorSourceSlots;
    // individual input/output modifiedColumnSets for the operators
    protected final ModifiedColumnSet[] operatorInputModifiedColumnSets;
    protected final ModifiedColumnSet[] operatorOutputModifiedColumnSets;

    protected boolean trackModifications;

    public class UpdateByWindowContext implements SafeCloseable {
        /** store a reference to the source rowset */
        final TrackingRowSet sourceRowSet;

        /** the column source providing the timestamp data for this window */
        @Nullable
        final ColumnSource<?> timestampColumnSource;

        /** the timestamp SSA providing fast lookup for time windows */
        @Nullable
        final LongSegmentedSortedArray timestampSsa;

        /** An array of boolean denoting which operators are affected by the current update. */
        final boolean[] opAffected;

        /** An array of context objects for each underlying operator */
        final UpdateByOperator.UpdateContext[] opContext;

        /** An array of ColumnSources for each underlying operator */
        final ColumnSource<?>[] inputSources;

        /** An array of {@link ChunkSource.FillContext}s for each input column */
        final ChunkSource.FillContext[] inputSourceFillContexts;

        /** A set of chunks used to store working values */
        final WritableChunk<Values>[] inputSourceChunks;

        /** An indicator of if each slot has been populated with data or not for this phase. */
        final boolean[] inputSourceChunkPopulated;

        /** the rows affected by this update */
        RowSet affectedRows;
        /** the rows that contain values used to compute affected row values */
        RowSet influencerRows;

        /** for use with a ticking window */
        RowSet affectedRowPositions;
        RowSet influencerPositions;

        /** keep track of what rows were modified (we'll use a single set for all operators sharing a window) */
        RowSetBuilderSequential modifiedBuilder;
        RowSet newModified;

        final int chunkSize;
        final boolean initialStep;

        public UpdateByWindowContext(final TrackingRowSet sourceRowSet, final ColumnSource<?>[] inputSources,
                @Nullable final ColumnSource<?> timestampColumnSource,
                @Nullable final LongSegmentedSortedArray timestampSsa, final int chunkSize, final boolean initialStep) {
            this.sourceRowSet = sourceRowSet;
            this.inputSources = inputSources;
            this.timestampColumnSource = timestampColumnSource;
            this.timestampSsa = timestampSsa;

            this.opAffected = new boolean[operators.length];
            this.opContext = new UpdateByOperator.UpdateContext[operators.length];
            this.inputSourceFillContexts = new ChunkSource.FillContext[operators.length];
            this.inputSourceChunkPopulated = new boolean[operators.length];
            // noinspection unchecked
            this.inputSourceChunks = new WritableChunk[operators.length];

            this.chunkSize = chunkSize;
            this.initialStep = initialStep;
        }

        public boolean computeAffectedRowsAndOperators(@NotNull final TableUpdate upstream) {
            // all rows are affected on the initial step
            if (initialStep) {
                affectedRows = sourceRowSet.copy();
                influencerRows = affectedRows;

                // no need to invert, just create a flat rowset
                if (windowed && timestampColumnName == null) {
                    affectedRowPositions = RowSetFactory.flat(sourceRowSet.size());
                    influencerPositions = RowSetFactory.flat(sourceRowSet.size());
                }
                // mark all operators as affected by this update
                Arrays.fill(opAffected, true);
                makeOperatorContexts();
                return true;
            }

            // determine which operators are affected by this update
            boolean anyAffected = false;
            boolean allAffected = upstream.added().isNonempty() ||
                    upstream.removed().isNonempty() ||
                    upstream.shifted().nonempty();

            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                opAffected[opIdx] = allAffected
                        || (upstream.modifiedColumnSet().nonempty() && (operatorInputModifiedColumnSets[opIdx] == null
                                || upstream.modifiedColumnSet().containsAny(operatorInputModifiedColumnSets[opIdx])));
                if (opAffected[opIdx]) {
                    anyAffected = true;
                }
            }

            if (!anyAffected) {
                return false;
            }

            if (!windowed) {
                computeCumulativeRowsAffected(upstream);
            } else {
                computeWindowedRowsAffected(upstream);
            }

            makeOperatorContexts();
            return true;
        }

        // cumulative windows is simple, just find the smallest key and return the range from smallest to end
        private void computeCumulativeRowsAffected(@NotNull TableUpdate upstream) {
            long smallestModifiedKey = smallestAffectedKey(upstream.added(), upstream.modified(), upstream.removed(),
                    upstream.shifted(), sourceRowSet);

            affectedRows = smallestModifiedKey == Long.MAX_VALUE
                    ? RowSetFactory.empty()
                    : sourceRowSet.subSetByKeyRange(smallestModifiedKey, sourceRowSet.lastRowKey());
            influencerRows = affectedRows;
        }

        // windowed by time/ticks is more complex to compute: find all the changed rows and the rows that would
        // be affected by the changes (includes newly added rows) and need to be recomputed. Then include all
        // the rows that are affected by deletions (if any). After the affected rows have been identified,
        // determine which rows will be needed to compute new values for the affected rows (influencer rows)
        private void computeWindowedRowsAffected(@NotNull TableUpdate upstream) {
            // changed rows are all mods+adds
            WritableRowSet changed = upstream.added().copy();
            changed.insert(upstream.modified());

            // need a writable rowset
            WritableRowSet tmpAffected;

            // compute the rows affected from these changes
            if (timestampColumnName == null) {
                try (final WritableRowSet changedInverted = sourceRowSet.invert(changed)) {
                    tmpAffected = computeAffectedRowsTicks(sourceRowSet, changed, changedInverted, prevUnits, fwdUnits);
                }
            } else {
                tmpAffected = computeAffectedRowsTime(sourceRowSet, changed, prevUnits, fwdUnits, timestampColumnSource,
                        timestampSsa);
            }

            // other rows can be affected by removes
            if (upstream.removed().isNonempty()) {
                try (final RowSet prev = sourceRowSet.copyPrev();
                        final RowSet removedPositions = timestampColumnName == null
                                ? null
                                : prev.invert(upstream.removed());
                        final WritableRowSet affectedByRemoves = timestampColumnName == null
                                ? computeAffectedRowsTicks(prev, upstream.removed(), removedPositions, prevUnits,
                                        fwdUnits)
                                : computeAffectedRowsTime(prev, upstream.removed(), prevUnits, fwdUnits,
                                        timestampColumnSource, timestampSsa)) {
                    // apply shifts to get back to pos-shift space
                    upstream.shifted().apply(affectedByRemoves);
                    // retain only the rows that still exist in the sourceRowSet
                    affectedByRemoves.retain(sourceRowSet);
                    tmpAffected.insert(affectedByRemoves);
                }
            }

            affectedRows = tmpAffected;

            // now get influencer rows for the affected rows
            if (timestampColumnName == null) {
                // generate position data rowsets for efficiently computed position offsets
                affectedRowPositions = sourceRowSet.invert(affectedRows);

                influencerRows = computeInfluencerRowsTicks(sourceRowSet, affectedRows, affectedRowPositions, prevUnits,
                        fwdUnits);
                influencerPositions = sourceRowSet.invert(influencerRows);
            } else {
                influencerRows = computeInfluencerRowsTime(sourceRowSet, affectedRows, prevUnits, fwdUnits,
                        timestampColumnSource, timestampSsa);
            }
        }

        private void makeOperatorContexts() {
            // use this to make which input sources are initialized
            Arrays.fill(inputSourceChunkPopulated, false);

            // create contexts for the affected operators
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    // create the fill contexts for the input sources
                    int sourceSlot = operatorSourceSlots[opIdx];
                    if (!inputSourceChunkPopulated[sourceSlot]) {
                        inputSourceChunks[sourceSlot] =
                                inputSources[sourceSlot].getChunkType().makeWritableChunk(chunkSize);
                        inputSourceFillContexts[sourceSlot] = inputSources[sourceSlot].makeFillContext(chunkSize);
                        inputSourceChunkPopulated[sourceSlot] = true;
                    }
                    opContext[opIdx] = operators[opIdx].makeUpdateContext(chunkSize, inputSources[sourceSlot]);

                }
            }

        }

        public boolean anyModified() {
            return newModified != null && newModified.isNonempty();
        }

        public RowSet getModifiedRows() {
            return newModified;
        }

        public void updateOutputModifiedColumnSet(ModifiedColumnSet outputModifiedColumnSet) {
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    outputModifiedColumnSet.setAll(operatorOutputModifiedColumnSets[opIdx]);
                }
            }
        }

        public RowSet getAffectedRows() {
            return affectedRows;
        }

        public RowSet getInfluencerRows() {
            return influencerRows;
        }

        public void processRows() {
            if (trackModifications) {
                modifiedBuilder = RowSetFactory.builderSequential();
            }

            if (!windowed) {
                processRowsCumulative();
            } else if (timestampColumnName == null) {
                processRowsWindowedTicks();
            } else {
                processRowsWindowedTime();
            }

            if (trackModifications) {
                newModified = modifiedBuilder.build();
            }
        }

        private void prepareValuesChunkForSource(final int srcIdx, final RowSequence rs) {
            if (!inputSourceChunkPopulated[srcIdx]) {
                inputSources[srcIdx].fillChunk(
                        inputSourceFillContexts[srcIdx],
                        inputSourceChunks[srcIdx],
                        rs);
                inputSourceChunkPopulated[srcIdx] = true;
            }
        }

        private void processRowsCumulative() {
            // find the key before the first affected row and preload that data for these operators
            final long keyBefore;
            try (final RowSet.SearchIterator sit = sourceRowSet.searchIterator()) {
                keyBefore = sit.binarySearchValue(
                        (compareTo, ignored) -> Long.compare(affectedRows.firstRowKey() - 1, compareTo), 1);
            }

            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    UpdateByCumulativeOperator cumOp = (UpdateByCumulativeOperator) operators[opIdx];
                    if (cumOp.getTimestampColumnName() == null || keyBefore == -1) {
                        cumOp.initializeUpdate(opContext[opIdx], keyBefore, NULL_LONG);
                    } else {
                        UpdateByCumulativeOperator.Context cumOpContext =
                                (UpdateByCumulativeOperator.Context) opContext[opIdx];
                        // make sure the time-based cumulative operators are starting from a valid value and timestamp
                        long potentialResetTimestamp = timestampColumnSource.getLong(keyBefore);

                        if (potentialResetTimestamp == NULL_LONG ||
                                !cumOpContext.isValueValid(keyBefore)) {
                            try (final RowSet.SearchIterator rIt = sourceRowSet.reverseIterator()) {
                                if (rIt.advance(keyBefore)) {
                                    while (rIt.hasNext()) {
                                        final long nextKey = rIt.nextLong();
                                        potentialResetTimestamp = timestampColumnSource.getLong(nextKey);
                                        if (potentialResetTimestamp != NULL_LONG &&
                                                cumOpContext.isValueValid(nextKey)) {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        // call the specialized version of `intializeUpdate()` for these operators
                        cumOp.initializeUpdate(opContext[opIdx], keyBefore, potentialResetTimestamp);
                    }
                }
            }

            try (final RowSequence.Iterator it = affectedRows.getRowSequenceIterator();
                    ChunkSource.GetContext tsGetCtx =
                            timestampColumnSource == null ? null : timestampColumnSource.makeGetContext(chunkSize)) {
                while (it.hasMore()) {
                    final RowSequence rs = it.getNextRowSequenceWithLength(chunkSize);
                    Arrays.fill(inputSourceChunkPopulated, false);

                    // create the timestamp chunk if needed
                    LongChunk<? extends Values> tsChunk = timestampColumnSource == null ? null
                            : timestampColumnSource.getChunk(tsGetCtx, rs).asLongChunk();

                    for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                        if (opAffected[opIdx]) {
                            final int srcIdx = operatorSourceSlots[opIdx];

                            // get the values this operator needs
                            prepareValuesChunkForSource(srcIdx, rs);

                            // process the chunk
                            operators[opIdx].processChunk(
                                    opContext[opIdx],
                                    rs,
                                    null,
                                    null,
                                    inputSourceChunks[srcIdx],
                                    tsChunk);
                        }
                    }
                    // all these rows were modified
                    if (modifiedBuilder != null) {
                        modifiedBuilder.appendRowSequence(rs);
                    }
                }
            }

            // call the generic `finishUpdate()` function for each operator
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    operators[opIdx].finishUpdate(opContext[opIdx]);
                }
            }
        }

        private void processRowsWindowedTicks() {
            // start loading the window for these operators using position data
            // find the key before the first affected row and preload that data for these operators
            final long keyBefore;
            try (final RowSet.SearchIterator sit = sourceRowSet.searchIterator()) {
                keyBefore = sit.binarySearchValue(
                        (compareTo, ignored) -> Long.compare(affectedRows.firstRowKey() - 1, compareTo), 1);
            }

            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    UpdateByCumulativeOperator cumOp = (UpdateByCumulativeOperator) operators[opIdx];
                    if (cumOp.getTimestampColumnName() == null || keyBefore == -1) {
                        cumOp.initializeUpdate(opContext[opIdx], keyBefore, NULL_LONG);
                    } else {
                        UpdateByCumulativeOperator.Context cumOpContext =
                                (UpdateByCumulativeOperator.Context) opContext[opIdx];
                        // make sure the time-based cumulative operators are starting from a valid value and timestamp
                        long potentialResetTimestamp = timestampColumnSource.getLong(keyBefore);

                        if (potentialResetTimestamp == NULL_LONG ||
                                !cumOpContext.isValueValid(keyBefore)) {
                            try (final RowSet.SearchIterator rIt = sourceRowSet.reverseIterator()) {
                                if (rIt.advance(keyBefore)) {
                                    while (rIt.hasNext()) {
                                        final long nextKey = rIt.nextLong();
                                        potentialResetTimestamp = timestampColumnSource.getLong(nextKey);
                                        if (potentialResetTimestamp != NULL_LONG &&
                                                cumOpContext.isValueValid(nextKey)) {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        // call the specialized version of `intializeUpdate()` for these operators
                        cumOp.initializeUpdate(opContext[opIdx], keyBefore, potentialResetTimestamp);
                    }
                }
            }

            try (final RowSequence.Iterator it = affectedRows.getRowSequenceIterator();
                    ChunkSource.GetContext tsGetCtx =
                            timestampColumnSource == null ? null : timestampColumnSource.makeGetContext(chunkSize)) {
                while (it.hasMore()) {
                    final RowSequence rs = it.getNextRowSequenceWithLength(chunkSize);
                    Arrays.fill(inputSourceChunkPopulated, false);

                    // create the timestamp chunk if needed
                    LongChunk<? extends Values> tsChunk = timestampColumnSource == null ? null
                            : timestampColumnSource.getChunk(tsGetCtx, rs).asLongChunk();

                    for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                        if (opAffected[opIdx]) {
                            final int srcIdx = operatorSourceSlots[opIdx];

                            // get the values this operator needs
                            prepareValuesChunkForSource(srcIdx, rs);

                            // process the chunk
                            operators[opIdx].processChunk(
                                    opContext[opIdx],
                                    rs,
                                    null,
                                    null,
                                    inputSourceChunks[srcIdx],
                                    tsChunk);
                        }
                    }
                    // all these rows were modified
                    if (modifiedBuilder != null) {
                        modifiedBuilder.appendRowSequence(rs);
                    }
                }
            }

            // call the generic `finishUpdate()` function for each operator
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    operators[opIdx].finishUpdate(opContext[opIdx]);
                }
            }
        }

        private void processRowsWindowedTime() {
            // start loading the window for these operators using timestamp data
        }

        @Override
        public void close() {
            if (influencerRows != null && influencerRows != affectedRows) {
                influencerRows.close();
            }
            if (influencerPositions != null && influencerPositions != affectedRowPositions) {
                influencerPositions.close();
            }
            try (final RowSet ignoredRs1 = affectedRows;
                    final RowSet ignoredRs2 = affectedRowPositions;
                    final RowSet ignoredRs3 = newModified) {
            }
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    final int srcIdx = operatorSourceSlots[opIdx];
                    if (inputSourceChunks[srcIdx] != null) {

                        inputSourceChunks[srcIdx].close();
                        inputSourceChunks[srcIdx] = null;

                        inputSourceFillContexts[srcIdx].close();
                        inputSourceFillContexts[srcIdx] = null;
                    }
                    opContext[opIdx].close();
                }
            }
        }
    }

    public UpdateByWindowContext makeWindowContext(final TrackingRowSet sourceRowSet,
            final ColumnSource<?>[] inputSources,
            final ColumnSource<?> timestampColumnSource,
            final LongSegmentedSortedArray timestampSsa,
            final int chunkSize,
            final boolean isInitializeStep) {
        return new UpdateByWindowContext(sourceRowSet, inputSources, timestampColumnSource, timestampSsa, chunkSize,
                isInitializeStep);
    }

    public void startTrackingModifications(@NotNull final QueryTable source, @NotNull final QueryTable result) {
        trackModifications = true;
        for (int opIdx = 0; opIdx < operators.length; opIdx++) {
            operatorInputModifiedColumnSets[opIdx] =
                    source.newModifiedColumnSet(operators[opIdx].getAffectingColumnNames());
            operatorOutputModifiedColumnSets[opIdx] =
                    result.newModifiedColumnSet(operators[opIdx].getOutputColumnNames());
        }
    }

    /**
     * Find the smallest valued key that participated in the upstream {@link TableUpdate}.
     *
     * @param added the added rows
     * @param modified the modified rows
     * @param removed the removed rows
     * @param shifted the shifted rows
     *
     * @return the smallest key that participated in any part of the update.
     */
    private static long smallestAffectedKey(@NotNull final RowSet added,
            @NotNull final RowSet modified,
            @NotNull final RowSet removed,
            @NotNull final RowSetShiftData shifted,
            @NotNull final RowSet affectedIndex) {

        long smallestModifiedKey = Long.MAX_VALUE;
        if (removed.isNonempty()) {
            smallestModifiedKey = removed.firstRowKey();
        }

        if (added.isNonempty()) {
            smallestModifiedKey = Math.min(smallestModifiedKey, added.firstRowKey());
        }

        if (modified.isNonempty()) {
            smallestModifiedKey = Math.min(smallestModifiedKey, modified.firstRowKey());
        }

        if (shifted.nonempty()) {
            final long firstModKey = modified.isEmpty() ? Long.MAX_VALUE : modified.firstRowKey();
            boolean modShiftFound = !modified.isEmpty();
            boolean affectedFound = false;
            try (final RowSequence.Iterator it = affectedIndex.getRowSequenceIterator()) {
                for (int shiftIdx = 0; shiftIdx < shifted.size()
                        && (!modShiftFound || !affectedFound); shiftIdx++) {
                    final long shiftStart = shifted.getBeginRange(shiftIdx);
                    final long shiftEnd = shifted.getEndRange(shiftIdx);
                    final long shiftDelta = shifted.getShiftDelta(shiftIdx);

                    if (!affectedFound) {
                        if (it.advance(shiftStart + shiftDelta)) {
                            final long maybeAffectedKey = it.peekNextKey();
                            if (maybeAffectedKey <= shiftEnd + shiftDelta) {
                                affectedFound = true;
                                final long keyToCompare =
                                        shiftDelta > 0 ? maybeAffectedKey - shiftDelta : maybeAffectedKey;
                                smallestModifiedKey = Math.min(smallestModifiedKey, keyToCompare);
                            }
                        } else {
                            affectedFound = true;
                        }
                    }

                    if (!modShiftFound) {
                        if (firstModKey <= (shiftEnd + shiftDelta)) {
                            modShiftFound = true;
                            // If the first modified key is in the range we should include it
                            if (firstModKey >= (shiftStart + shiftDelta)) {
                                smallestModifiedKey = Math.min(smallestModifiedKey, firstModKey - shiftDelta);
                            } else {
                                // Otherwise it's not included in any shifts, and since shifts can't reorder rows
                                // it is the smallest possible value and we've already accounted for it above.
                                break;
                            }
                        }
                    }
                }
            }
        }

        return smallestModifiedKey;
    }

    private static WritableRowSet computeAffectedRowsTime(final RowSet sourceSet, final RowSet subset, long revNanos,
            long fwdNanos, final ColumnSource<?> timestampColumnSource, final LongSegmentedSortedArray timestampSsa) {
        // swap fwd/rev to get the affected windows
        return computeInfluencerRowsTime(sourceSet, subset, fwdNanos, revNanos, timestampColumnSource, timestampSsa);
    }

    private static WritableRowSet computeInfluencerRowsTime(final RowSet sourceSet, final RowSet subset, long revNanos,
            long fwdNanos, final ColumnSource<?> timestampColumnSource, final LongSegmentedSortedArray timestampSsa) {
        if (sourceSet.size() == subset.size()) {
            return sourceSet.copy();
        }

        int chunkSize = (int) Math.min(subset.size(), 4096);
        try (final RowSequence.Iterator it = subset.getRowSequenceIterator();
                final ChunkSource.GetContext context = timestampColumnSource.makeGetContext(chunkSize)) {
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            LongSegmentedSortedArray.Iterator ssaIt = timestampSsa.iterator(false, false);
            while (it.hasMore() && ssaIt.hasNext()) {
                final RowSequence rs = it.getNextRowSequenceWithLength(chunkSize);
                LongChunk<? extends Values> timestamps = timestampColumnSource.getChunk(context, rs).asLongChunk();

                for (int ii = 0; ii < rs.intSize(); ii++) {
                    // if the timestamp of the row is null, it won't belong to any set and we can ignore it
                    // completely
                    final long ts = timestamps.get(ii);
                    if (ts != NULL_LONG) {
                        // look at every row timestamp, compute the head and tail in nanos
                        final long head = ts - revNanos;
                        final long tail = ts + fwdNanos;

                        // advance the iterator to the beginning of the window
                        if (ssaIt.nextValue() < head) {
                            ssaIt.advanceToBeforeFirst(head);
                            if (!ssaIt.hasNext()) {
                                // SSA is exhausted
                                break;
                            }
                        }

                        Assert.assertion(ssaIt.hasNext() && ssaIt.nextValue() >= head,
                                "SSA Iterator outside of window");

                        // step through the SSA and collect keys until outside of the window
                        while (ssaIt.hasNext() && ssaIt.nextValue() <= tail) {
                            builder.appendKey(ssaIt.nextKey());
                            ssaIt.next();
                        }

                        if (!ssaIt.hasNext()) {
                            // SSA is exhausted
                            break;
                        }
                    }
                }
            }
            return builder.build();
        }
    }

    private static WritableRowSet computeAffectedRowsTicks(final RowSet sourceSet, final RowSet subset,
            final RowSet invertedSubSet, long revTicks, long fwdTicks) {
        // swap fwd/rev to get the influencer windows
        return computeInfluencerRowsTicks(sourceSet, subset, invertedSubSet, fwdTicks, revTicks);
    }

    private static WritableRowSet computeInfluencerRowsTicks(final RowSet sourceSet, final RowSet subset,
            final RowSet invertedSubSet, long revTicks, long fwdTicks) {
        if (sourceSet.size() == subset.size()) {
            return sourceSet.copy();
        }

        long maxPos = sourceSet.size() - 1;

        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        final MutableLong minPos = new MutableLong(0L);

        invertedSubSet.forAllRowKeyRanges((s, e) -> {
            long sPos = Math.max(s - revTicks, minPos.longValue());
            long ePos = Math.min(e + fwdTicks, maxPos);
            builder.appendRange(sPos, ePos);
            minPos.setValue(ePos + 1);
        });

        try (final RowSet positions = builder.build()) {
            return sourceSet.subSetForPositions(positions);
        }
    }

    private static long locatePreviousTimestamp(final RowSet sourceSet, final ColumnSource<?> timestampColumnSource,
            final long firstUnmodifiedKey) {
        long potentialResetTimestamp = timestampColumnSource.getLong(firstUnmodifiedKey);
        if (potentialResetTimestamp != NULL_LONG) {
            return potentialResetTimestamp;
        }

        try (final RowSet.SearchIterator rIt = sourceSet.reverseIterator()) {
            if (rIt.advance(firstUnmodifiedKey)) {
                while (rIt.hasNext()) {
                    final long nextKey = rIt.nextLong();
                    potentialResetTimestamp = timestampColumnSource.getLong(nextKey);
                    if (potentialResetTimestamp != NULL_LONG) {
                        return potentialResetTimestamp;
                    }
                }
            }
        }

        return NULL_LONG;
    }

    private UpdateByWindow(UpdateByOperator[] operators, int[] operatorSourceSlots, boolean windowed,
            @Nullable String timestampColumnName, long prevUnits, long fwdUnits) {
        this.operators = operators;
        this.operatorSourceSlots = operatorSourceSlots;
        this.windowed = windowed;
        this.timestampColumnName = timestampColumnName;
        this.prevUnits = prevUnits;
        this.fwdUnits = fwdUnits;

        operatorInputModifiedColumnSets = new ModifiedColumnSet[operators.length];
        operatorOutputModifiedColumnSets = new ModifiedColumnSet[operators.length];
        trackModifications = false;
    }

    public static UpdateByWindow createFromOperatorArray(final UpdateByOperator[] operators,
            final int[] operatorSourceSlots) {
        // review operators to extract timestamp column (if one exists)
        String timestampColumnName = null;
        for (UpdateByOperator operator : operators) {
            if (operator.getTimestampColumnName() != null) {
                timestampColumnName = operator.getTimestampColumnName();
                break;
            }
        }

        return new UpdateByWindow(operators,
                operatorSourceSlots,
                operators[0] instanceof UpdateByWindowedOperator,
                timestampColumnName,
                operators[0].getPrevWindowUnits(),
                operators[0].getPrevWindowUnits());
    }

    @Nullable
    public String getTimestampColumnName() {
        return timestampColumnName;
    }

    private static int hashCode(boolean windowed, @Nullable String timestampColumnName, long prevUnits, long fwdUnits) {
        // treat all cumulative as identical, even if they rely on timestamps
        if (!windowed) {
            return Boolean.hashCode(windowed);
        }

        // windowed are unique per timestamp column and window-size
        int hash = Boolean.hashCode(windowed);
        if (timestampColumnName != null) {
            hash = 31 * hash + timestampColumnName.hashCode();
        }
        hash = 31 * hash + Long.hashCode(prevUnits);
        hash = 31 * hash + Long.hashCode(fwdUnits);
        return hash;
    }

    public static int hashCodeFromOperator(final UpdateByOperator op) {
        return hashCode(op instanceof UpdateByWindowedOperator,
                op.getTimestampColumnName(),
                op.getPrevWindowUnits(),
                op.getPrevWindowUnits());
    }

    @Override
    public int hashCode() {
        return hashCode(windowed,
                timestampColumnName,
                prevUnits,
                fwdUnits);
    }
}
