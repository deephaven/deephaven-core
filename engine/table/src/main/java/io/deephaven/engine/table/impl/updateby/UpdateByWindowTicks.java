package io.deephaven.engine.table.impl.updateby;

import io.deephaven.base.ringbuffer.IntRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.UpdateByWindowedOperator;
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
        private static final int WINDOW_CHUNK_SIZE = 4096;

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
        protected long influencerPosChunkSize;
        protected int currentGetContextSize;

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

        @Override
        protected void makeOperatorContexts() {
            // use this to determine which input sources are initialized
            Arrays.fill(inputSourceChunkPopulated, false);

            // create contexts for the affected operators
            currentGetContextSize = WINDOW_CHUNK_SIZE;

            // working chunk size need not be larger than affectedRows.size()
            workingChunkSize = Math.min(workingChunkSize, affectedRows.intSize());

            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    // create the fill contexts for the input sources
                    int sourceSlot = operatorSourceSlots[opIdx];
                    if (!inputSourceChunkPopulated[sourceSlot]) {
                        inputSourceGetContexts[sourceSlot] =
                                inputSources[sourceSlot].makeGetContext(currentGetContextSize);
                        inputSourceChunkPopulated[sourceSlot] = true;
                    }
                    opContext[opIdx] = operators[opIdx].makeUpdateContext(workingChunkSize, inputSources[sourceSlot]);
                }
            }
        }

        protected void ensureGetContextSize(long newSize) {
            if (currentGetContextSize < newSize) {
                long size = currentGetContextSize;
                while (size < newSize) {
                    size *= 2;
                }
                currentGetContextSize = LongSizedDataStructure.intSize(
                        "ensureGetContextSize exceeded Integer.MAX_VALUE",
                        size);

                // use this to determine which input sources are initialized
                Arrays.fill(inputSourceChunkPopulated, false);

                for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                    if (opAffected[opIdx]) {
                        int sourceSlot = operatorSourceSlots[opIdx];
                        if (!inputSourceChunkPopulated[sourceSlot]) {
                            // close the existing context
                            inputSourceGetContexts[sourceSlot].close();

                            // create a new context of the larger size
                            inputSourceGetContexts[sourceSlot] =
                                    inputSources[sourceSlot].makeGetContext(currentGetContextSize);
                            inputSourceChunkPopulated[sourceSlot] = true;
                        }
                    }
                }
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
                    upstream.removed().isNonempty();

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

        /***
         * This function takes care of loading/preparing the next set of influencer data, in this case we load the next
         * chunk of key and position data and reset the index
         */
        private void loadNextInfluencerChunks() {
            if (!influencerIt.hasMore()) {
                nextInfluencerPos = Integer.MAX_VALUE;
                nextInfluencerKey = Long.MAX_VALUE;
                return;
            }

            final RowSequence influencerRs = influencerIt.getNextRowSequenceWithLength(WINDOW_CHUNK_SIZE);
            influencerKeyChunk = influencerRs.asRowKeyChunk();

            final RowSequence influencePosRs = influencerPosIt.getNextRowSequenceWithLength(WINDOW_CHUNK_SIZE);
            influencerPosChunk = influencePosRs.asRowKeyChunk();

            Assert.eqTrue(influencePosRs.lastRowKey() < Integer.MAX_VALUE,
                    "updateBy window positions exceeded maximum size");

            influencerPosChunkSize = influencerPosChunk.size();

            nextInfluencerIndex = 0;
            nextInfluencerPos = LongSizedDataStructure.intSize(
                    "updateBy window positions exceeded maximum size",
                    influencerPosChunk.get(nextInfluencerIndex));
            nextInfluencerKey = influencerKeyChunk.get(nextInfluencerIndex);
        }

        /***
         * This function process the affected rows chunkwise, and will advance the moving window (which is the same for
         * all operators in this collection). For each row in the dataset the sliding window will adjust and
         * instructions for pushing/popping data will be created for the operators. For each chunk of `affected` rows,
         * we will identify exactly which `influencer` rows are needed and will provide those and the push/pop
         * instructions to the operators.
         *
         * Downstream operators should manage local storage in a RingBuffer or other efficient structure since our pop()
         * calls do not provide the popped data
         */
        public void processRows() {
            if (trackModifications) {
                modifiedBuilder = RowSetFactory.builderSequential();
            }

            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    UpdateByWindowedOperator winOp = (UpdateByWindowedOperator) operators[opIdx];
                    // call the specialized version of `intializeUpdate()` for these operators
                    winOp.initializeUpdate(opContext[opIdx]);
                }
            }

            influencerIt = influencerRows.getRowSequenceIterator();
            influencerPosIt = influencerPositions.getRowSequenceIterator();

            try (final RowSequence.Iterator it = affectedRows.getRowSequenceIterator();
                    final RowSequence.Iterator posIt = affectedRowPositions.getRowSequenceIterator();
                    final WritableIntChunk<? extends Values> pushChunk =
                            WritableIntChunk.makeWritableChunk(workingChunkSize);
                    final WritableIntChunk<? extends Values> popChunk =
                            WritableIntChunk.makeWritableChunk(workingChunkSize)) {

                // load the first chunk of influencer values (fillWindowTicks() will call in future)
                loadNextInfluencerChunks();

                final long sourceRowSetSize = sourceRowSet.size();

                while (it.hasMore()) {
                    final RowSequence chunkRs = it.getNextRowSequenceWithLength(workingChunkSize);
                    final RowSequence chunkPosRs = posIt.getNextRowSequenceWithLength(workingChunkSize);
                    final int chunkRsSize = chunkRs.intSize();

                    final LongChunk<OrderedRowKeys> posChunk = chunkPosRs.asRowKeyChunk();

                    // we are going to track all the influencer rows that affect this chunk of data
                    final RowSetBuilderSequential chunkInfluencerBuilder = RowSetFactory.builderSequential();

                    // chunk processing
                    for (int ii = 0; ii < chunkRsSize; ii++) {
                        // read the current position
                        final long currentPos = posChunk.get(ii);

                        // compute the head and tail positions (inclusive)
                        final long head = Math.max(0, currentPos - prevUnits + 1);
                        final long tail = Math.min(sourceRowSetSize - 1, currentPos + fwdUnits);

                        // pop out all values from the current window that are not in the new window
                        int popCount = 0;
                        while (!currentWindowPositions.isEmpty() && currentWindowPositions.front() < head) {
                            currentWindowPositions.remove();
                            popCount++;
                        }

                        // skip values until they match the window (this can only happen on initial addition of rows
                        // to the table, because we short-circuited the precise building of the influencer rows for
                        // efficiency)
                        while (nextInfluencerPos < head) {
                            nextInfluencerIndex++;

                            if (nextInfluencerIndex < influencerPosChunkSize) {
                                nextInfluencerPos = (int) influencerPosChunk.get(nextInfluencerIndex);
                                nextInfluencerKey = influencerKeyChunk.get(nextInfluencerIndex);
                            } else {
                                // try to bring in new data
                                loadNextInfluencerChunks();
                            }
                        }

                        // push matching values
                        int pushCount = 0;
                        while (nextInfluencerPos <= tail) {
                            currentWindowPositions.add(nextInfluencerPos);
                            pushCount++;
                            // add this key to the needed set for this chunk
                            chunkInfluencerBuilder.appendKey(nextInfluencerKey);
                            nextInfluencerIndex++;

                            if (nextInfluencerIndex < influencerPosChunkSize) {
                                nextInfluencerPos = (int) influencerPosChunk.get(nextInfluencerIndex);
                                nextInfluencerKey = influencerKeyChunk.get(nextInfluencerIndex);
                            } else {
                                // try to bring in new data
                                loadNextInfluencerChunks();
                            }
                        }

                        // write the push and pop counts to the chunks
                        popChunk.set(ii, popCount);
                        pushChunk.set(ii, pushCount);
                    }

                    // execute the operators
                    try (final RowSet chunkInfluencerRs = chunkInfluencerBuilder.build()) {
                        ensureGetContextSize(chunkInfluencerRs.size());

                        Arrays.fill(inputSourceChunkPopulated, false);
                        for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                            if (opAffected[opIdx]) {
                                final int srcIdx = operatorSourceSlots[opIdx];
                                prepareValuesChunkForSource(srcIdx, chunkInfluencerRs);

                                // make the specialized call for windowed operators
                                ((UpdateByWindowedOperator.Context) opContext[opIdx]).accumulate(
                                        chunkRs,
                                        inputSourceChunks[srcIdx],
                                        pushChunk,
                                        popChunk,
                                        chunkRsSize);
                            }
                        }
                    }

                    // all these rows were modified
                    if (modifiedBuilder != null) {
                        modifiedBuilder.appendRowSequence(chunkRs);
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
