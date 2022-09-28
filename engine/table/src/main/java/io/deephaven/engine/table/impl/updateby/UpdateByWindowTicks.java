package io.deephaven.engine.table.impl.updateby;

import io.deephaven.base.ringbuffer.IntRingBuffer;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

// this class is currently too big, should specialize into CumWindow, TickWindow, TimeWindow to simplify implementation
public class UpdateByWindowTicks extends UpdateByWindow {
    protected final long prevUnits;
    protected final long fwdUnits;

    public class UpdateByWindowTicksContext extends UpdateByWindow.UpdateByWindowContext {
        protected final IntRingBuffer currentWindowPositions;

        protected RowSet affectedRowPositions;
        protected RowSet influencerPositions;

        protected int nextInfluencerIndex;
        protected int nextInfluencerPos;
        protected long nextInfluencerKey;

        protected RowSequence.Iterator influencerIt;
        protected RowSequence.Iterator influencerPosIt;
        protected LongChunk<OrderedRowKeys> influencerPosChunk;
        protected LongChunk<OrderedRowKeys> influencerKeyChunk;

        public UpdateByWindowTicksContext(final TrackingRowSet sourceRowSet, final ColumnSource<?>[] inputSources,
                @Nullable final ColumnSource<?> timestampColumnSource,
                @Nullable final LongSegmentedSortedArray timestampSsa, final int chunkSize, final boolean initialStep) {
            super(sourceRowSet, inputSources, null, null, chunkSize, initialStep);

            currentWindowPositions = new IntRingBuffer(512, true);
        }


        @Override
        public void close() {
            super.close();
            // these might be identical, don't close both!
            if (influencerPositions != null && influencerPositions != affectedRowPositions) {
                influencerPositions.close();
            }
            try (final RowSet ignoredRs1 = affectedRowPositions;
                    final RowSequence.Iterator ignoreIt1 = influencerIt;
                    final RowSequence.Iterator ignoreIt2 = influencerPosIt) {
                // leveraging try with resources to auto-close
            }
        }

        // windowed by time/ticks is more complex to compute: find all the changed rows and the rows that would
        // be affected by the changes (includes newly added rows) and need to be recomputed. Then include all
        // the rows that are affected by deletions (if any). After the affected rows have been identified,
        // determine which rows will be needed to compute new values for the affected rows (influencer rows)
        @Override
        public boolean computeAffectedRowsAndOperators(@NotNull final TableUpdate upstream) {
            // all rows are affected on the initial step
            if (initialStep) {
                affectedRows = sourceRowSet.copy();
                influencerRows = affectedRows;

                // no need to invert, just create a flat rowset
                affectedRowPositions = RowSetFactory.flat(sourceRowSet.size());
                influencerPositions = RowSetFactory.flat(sourceRowSet.size());

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

            // changed rows are all mods+adds
            WritableRowSet changed = upstream.added().union(upstream.modified());

            // need a writable rowset
            WritableRowSet tmpAffected;

            // compute the rows affected from these changes
            try (final WritableRowSet changedInverted = sourceRowSet.invert(changed)) {
                tmpAffected = computeAffectedRowsTicks(sourceRowSet, changed, changedInverted, prevUnits, fwdUnits);
            }

            // other rows can be affected by removes
            if (upstream.removed().isNonempty()) {
                try (final RowSet prev = sourceRowSet.copyPrev();
                        final RowSet removedPositions = prev.invert(upstream.removed());
                        final WritableRowSet affectedByRemoves =
                                computeAffectedRowsTicks(prev, upstream.removed(), removedPositions, prevUnits,
                                        fwdUnits)) {
                    // apply shifts to get back to pos-shift space
                    upstream.shifted().apply(affectedByRemoves);
                    // retain only the rows that still exist in the sourceRowSet
                    affectedByRemoves.retain(sourceRowSet);
                    tmpAffected.insert(affectedByRemoves);
                }
            }

            affectedRows = tmpAffected;

            // now get influencer rows for the affected rows
            // generate position data rowsets for efficiently computed position offsets
            affectedRowPositions = sourceRowSet.invert(affectedRows);

            influencerRows = computeInfluencerRowsTicks(sourceRowSet, affectedRows, affectedRowPositions, prevUnits,
                    fwdUnits);
            influencerPositions = sourceRowSet.invert(influencerRows);

            makeOperatorContexts();
            return true;
        }

        private void loadNextInfluencerValueChunks() {
            if (!influencerIt.hasMore()) {
                nextInfluencerPos = Integer.MAX_VALUE;
                nextInfluencerKey = Long.MAX_VALUE;
                return;
            }

            final RowSequence influencerRs = influencerIt.getNextRowSequenceWithLength(chunkSize);
            influencerKeyChunk = influencerRs.asRowKeyChunk();

            final RowSequence influencePosRs = influencerPosIt.getNextRowSequenceWithLength(chunkSize);
            influencerPosChunk = influencePosRs.asRowKeyChunk();

            Arrays.fill(inputSourceChunkPopulated, false);
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    final int srcIdx = operatorSourceSlots[opIdx];
                    prepareValuesChunkForSource(srcIdx, influencerRs);

                    opContext[opIdx].setValuesChunk(inputSourceChunks[srcIdx]);
                }
            }

            nextInfluencerIndex = 0;
            nextInfluencerPos = LongSizedDataStructure.intSize(
                    "updateBy window positions exceeded maximum size",
                    influencerPosChunk.get(nextInfluencerIndex));
            nextInfluencerKey = influencerKeyChunk.get(nextInfluencerIndex);
        }

        private void fillWindowTicks(long currentPos) {
            // compute the head and tail positions (inclusive)
            final long head = Math.max(0, currentPos - prevUnits + 1);
            final long tail = Math.min(sourceRowSet.size() - 1, currentPos + fwdUnits);

            // pop out all values from the current window that are not in the new window
            while (!currentWindowPositions.isEmpty() && currentWindowPositions.front() < head) {
                // operator pop
                for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                    if (opAffected[opIdx]) {
                        opContext[opIdx].pop();
                    }
                }
                currentWindowPositions.remove();
            }

            // if the window is empty, reset
            if (currentWindowPositions.isEmpty()) {
                for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                    if (opAffected[opIdx]) {
                        opContext[opIdx].reset();
                    }
                }
            }

            // skip values until they match the window (this can only happen on the initial addition of rows to the
            // table, because we short-circuit the precise building of the influencer rows for efficiency)
            while (nextInfluencerPos < head) {
                nextInfluencerIndex++;

                if (nextInfluencerIndex < influencerPosChunk.size()) {
                    nextInfluencerPos = LongSizedDataStructure.intSize(
                            "updateBy window positions exceeded maximum size",
                            influencerPosChunk.get(nextInfluencerIndex));
                    nextInfluencerKey = influencerKeyChunk.get(nextInfluencerIndex);
                } else {
                    // try to bring in new data
                    loadNextInfluencerValueChunks();
                }
            }

            // push matching values
            while (nextInfluencerPos <= tail) {
                // operator push
                for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                    if (opAffected[opIdx]) {
                        opContext[opIdx].push(nextInfluencerKey, nextInfluencerIndex);
                    }
                }
                currentWindowPositions.add(nextInfluencerPos);
                nextInfluencerIndex++;

                if (nextInfluencerIndex < influencerPosChunk.size()) {
                    nextInfluencerPos = LongSizedDataStructure.intSize(
                            "updateBy window positions exceeded maximum size",
                            influencerPosChunk.get(nextInfluencerIndex));
                    nextInfluencerKey = influencerKeyChunk.get(nextInfluencerIndex);
                } else {
                    // try to bring in new data
                    loadNextInfluencerValueChunks();
                }
            }
        }

        // this function process the affected rows chunkwise, but will call fillWindowTicks() for each
        // new row. fillWindowTicks() will advance the moving window (which is the same for all operators in this
        // collection) and will call push/pop for each operator as it advances the window.
        //
        // We track the minimum amount of data needed, only the window position data. The downstream operators
        // should manage local storage in a RingBuffer or other efficient structure
        @Override
        public void processRows() {
            if (trackModifications) {
                modifiedBuilder = RowSetFactory.builderSequential();
            }

            influencerIt = influencerRows.getRowSequenceIterator();
            influencerPosIt = influencerPositions.getRowSequenceIterator();

            try (final RowSequence.Iterator it = affectedRows.getRowSequenceIterator();
                    final RowSequence.Iterator posIt = affectedRowPositions.getRowSequenceIterator()) {

                // load the first chunk of influencer values (fillWindowTicks() will call in future)
                loadNextInfluencerValueChunks();

                while (it.hasMore()) {
                    final RowSequence rs = it.getNextRowSequenceWithLength(chunkSize);
                    final RowSequence posRs = posIt.getNextRowSequenceWithLength(chunkSize);

                    final LongChunk<OrderedRowKeys> posChunk = posRs.asRowKeyChunk();

                    // chunk processing
                    for (int ii = 0; ii < rs.size(); ii++) {
                        // read the current position
                        final long currentPos = posChunk.get(ii);

                        // fill the operator windows (calls push/pop/reset as appropriate)
                        fillWindowTicks(currentPos);

                        // now the operators have seen the correct window data, write to the output chunk
                        for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                            if (opAffected[opIdx]) {
                                opContext[opIdx].writeToOutputChunk(ii);
                            }
                        }
                    }

                    // chunk output to column
                    for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                        if (opAffected[opIdx]) {
                            opContext[opIdx].writeToOutputColumn(rs);
                        }
                    }

                    // all these rows were modified
                    if (modifiedBuilder != null) {
                        modifiedBuilder.appendRowSequence(rs);
                    }
                }
            }

            // call `finishUpdate()` function for each operator
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    operators[opIdx].finishUpdate(opContext[opIdx]);
                }
            }

            if (trackModifications) {
                newModified = modifiedBuilder.build();
            }
        }
    }

    public UpdateByWindowContext makeWindowContext(final TrackingRowSet sourceRowSet,
            final ColumnSource<?>[] inputSources,
            final ColumnSource<?> timestampColumnSource,
            final LongSegmentedSortedArray timestampSsa,
            final int chunkSize,
            final boolean isInitializeStep) {
        return new UpdateByWindowTicksContext(sourceRowSet, inputSources, timestampColumnSource, timestampSsa,
                chunkSize,
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

    UpdateByWindowTicks(UpdateByOperator[] operators, int[] operatorSourceSlots, long prevUnits, long fwdUnits) {
        super(operators, operatorSourceSlots, null);
        this.prevUnits = prevUnits;
        this.fwdUnits = fwdUnits;
    }

    @Override
    public int hashCode() {
        return hashCode(true,
                null,
                prevUnits,
                fwdUnits);
    }
}
