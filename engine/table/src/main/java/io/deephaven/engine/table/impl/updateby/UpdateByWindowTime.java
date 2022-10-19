package io.deephaven.engine.table.impl.updateby;

import io.deephaven.base.ringbuffer.LongRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.UpdateByWindowedOperator;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_LONG;

// this class is currently too big, should specialize into CumWindow, TickWindow, TimeWindow to simplify implementation
public class UpdateByWindowTime extends UpdateByWindow {
    protected final long prevUnits;
    protected final long fwdUnits;

    public class UpdateByWindowTimeContext extends UpdateByWindowContext {
        private static final int WINDOW_CHUNK_SIZE = 4096;

        protected final ChunkSource.GetContext influencerTimestampContext;
        protected final LongRingBuffer currentWindowTimestamps;

        protected int nextInfluencerIndex;
        protected long nextInfluencerTimestamp;
        protected long nextInfluencerKey;

        protected RowSequence.Iterator influencerIt;
        protected LongChunk<OrderedRowKeys> influencerKeyChunk;
        protected LongChunk<? extends Values> influencerTimestampChunk;
        protected long influencerTimestampChunkSize;
        protected int currentGetContextSize;

        public UpdateByWindowTimeContext(final TrackingRowSet sourceRowSet, final ColumnSource<?>[] inputSources,
                @NotNull final ColumnSource<?> timestampColumnSource,
                @Nullable final LongSegmentedSortedArray timestampSsa, final int chunkSize, final boolean initialStep) {
            super(sourceRowSet, inputSources, timestampColumnSource, timestampSsa, chunkSize, initialStep);

            influencerTimestampContext = timestampColumnSource.makeGetContext(WINDOW_CHUNK_SIZE);
            currentWindowTimestamps = new LongRingBuffer(512, true);
        }

        @Override
        public void close() {
            super.close();
            try (final RowSequence.Iterator ignoreIt1 = influencerIt;
                    final ChunkSource.GetContext ignoreCtx1 = influencerTimestampContext) {
                // leveraging try with resources to auto-close
            }
        }

        @Override
        protected void makeOperatorContexts() {
            // use this to make which input sources are initialized
            Arrays.fill(inputSourceChunkPopulated, false);

            // create contexts for the affected operators
            currentGetContextSize = WINDOW_CHUNK_SIZE;

            // working chunk size need not be larger than affectedRows.size()
            workingChunkSize = Math.min(workingChunkSize, affectedRows.intSize());

            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    // create the fill contexts for the input sources
                    final int[] sourceIndices = operatorInputSourceSlots[opIdx];
                    final ColumnSource<?>[] inputSourceArr = new ColumnSource[sourceIndices.length];
                    for (int ii = 0; ii < sourceIndices.length; ii++) {
                        int sourceSlot = sourceIndices[ii];
                        if (!inputSourceChunkPopulated[sourceSlot]) {
                            inputSourceGetContexts[sourceSlot] =
                                    inputSources[sourceSlot].makeGetContext(currentGetContextSize);
                            inputSourceChunkPopulated[sourceSlot] = true;
                        }
                        inputSourceArr[ii] = inputSources[sourceSlot];
                    }
                    opContext[opIdx] = operators[opIdx].makeUpdateContext(workingChunkSize, inputSourceArr);
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
                        final int[] sourceIndices = operatorInputSourceSlots[opIdx];
                        for (int sourceSlot : sourceIndices) {
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
            WritableRowSet tmpAffected = computeAffectedRowsTime(sourceRowSet, changed, prevUnits, fwdUnits,
                    timestampColumnSource, timestampSsa, false);

            // other rows can be affected by removes
            if (upstream.removed().isNonempty()) {
                try (final RowSet prev = sourceRowSet.copyPrev();
                        final WritableRowSet affectedByRemoves =
                                computeAffectedRowsTime(prev, upstream.removed(), prevUnits, fwdUnits,
                                        timestampColumnSource, timestampSsa, true)) {
                    // we used the SSA (post-shift) to get these keys, no need to shift
                    // retain only the rows that still exist in the sourceRowSet
                    affectedByRemoves.retain(sourceRowSet);
                    tmpAffected.insert(affectedByRemoves);
                }
            }

            affectedRows = tmpAffected;

            // now get influencer rows for the affected rows
            influencerRows = computeInfluencerRowsTime(sourceRowSet, affectedRows, prevUnits, fwdUnits,
                    timestampColumnSource, timestampSsa, false);

            makeOperatorContexts();
            return true;
        }

        /***
         * This function takes care of loading/preparing the next set of influencer data, in this case we load the next
         * chunk of key and position data and reset the index
         */
        private void loadNextInfluencerChunks() {
            if (!influencerIt.hasMore()) {
                nextInfluencerTimestamp = Long.MAX_VALUE;
                nextInfluencerKey = Long.MAX_VALUE;
                return;
            }

            final RowSequence influencerRs = influencerIt.getNextRowSequenceWithLength(WINDOW_CHUNK_SIZE);
            influencerKeyChunk = influencerRs.asRowKeyChunk();
            influencerTimestampChunk =
                    timestampColumnSource.getChunk(influencerTimestampContext, influencerRs).asLongChunk();

            influencerTimestampChunkSize = influencerTimestampChunk.size();

            nextInfluencerIndex = 0;
            nextInfluencerTimestamp = influencerTimestampChunk.get(nextInfluencerIndex);
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
        @Override
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

            try (final RowSequence.Iterator it = affectedRows.getRowSequenceIterator();
                    final ChunkSource.GetContext localTimestampContext =
                            timestampColumnSource.makeGetContext(workingChunkSize);
                    final WritableIntChunk<? extends Values> pushChunk =
                            WritableIntChunk.makeWritableChunk(workingChunkSize);
                    final WritableIntChunk<? extends Values> popChunk =
                            WritableIntChunk.makeWritableChunk(workingChunkSize)) {

                // load the first chunk of influencer values (fillWindowTime() will call in future)
                loadNextInfluencerChunks();

                while (it.hasMore()) {
                    final RowSequence chunkRs = it.getNextRowSequenceWithLength(workingChunkSize);
                    final int chunkRsSize = chunkRs.intSize();

                    // NOTE: we did not put null values into our SSA and our influencer rowset is built using the
                    // SSA. there should be no null timestamps considered in the rolling windows
                    final LongChunk<? extends Values> timestampChunk =
                            timestampColumnSource.getChunk(localTimestampContext, chunkRs).asLongChunk();

                    // we are going to track all the influencer rows that affect this chunk of data
                    final RowSetBuilderSequential chunkInfluencerBuilder = RowSetFactory.builderSequential();

                    // chunk processing
                    for (int ii = 0; ii < chunkRsSize; ii++) {
                        // read the current position
                        final long currentTimestamp = timestampChunk.get(ii);

                        // compute the head and tail positions (inclusive)
                        final long head = currentTimestamp - prevUnits;
                        final long tail = currentTimestamp + fwdUnits;

                        // pop out all values from the current window that are not in the new window
                        int popCount = 0;
                        while (!currentWindowTimestamps.isEmpty() && currentWindowTimestamps.front() < head) {
                            currentWindowTimestamps.remove();
                            popCount++;
                        }


                        // skip values until they match the window (this can only happen on the initial addition of rows
                        // to the table, because we short-circuited the precise building of the influencer rows for
                        // efficiency)
                        while (nextInfluencerTimestamp < head) {
                            nextInfluencerIndex++;

                            if (nextInfluencerIndex < influencerTimestampChunkSize) {
                                nextInfluencerTimestamp = influencerTimestampChunk.get(nextInfluencerIndex);
                                nextInfluencerKey = influencerKeyChunk.get(nextInfluencerIndex);
                            } else {
                                // try to bring in new data
                                loadNextInfluencerChunks();
                            }
                        }

                        // push matching values
                        int pushCount = 0;
                        while (nextInfluencerTimestamp <= tail) {
                            currentWindowTimestamps.add(nextInfluencerTimestamp);
                            pushCount++;
                            // add this key to the needed set for this chunk
                            chunkInfluencerBuilder.appendKey(nextInfluencerKey);
                            nextInfluencerIndex++;

                            if (nextInfluencerIndex < influencerTimestampChunkSize) {
                                nextInfluencerTimestamp = influencerTimestampChunk.get(nextInfluencerIndex);
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
                                // prep the chunk array needed by the accumulate call
                                final int[] srcIndices = operatorInputSourceSlots[opIdx];
                                Chunk<? extends Values>[] chunkArr = new Chunk[srcIndices.length];
                                for (int ii = 0; ii < srcIndices.length; ii++) {
                                    int srcIdx = srcIndices[ii];
                                    // chunk prep
                                    prepareValuesChunkForSource(srcIdx, chunkInfluencerRs);
                                    chunkArr[ii] = inputSourceChunks[srcIdx];
                                }

                                // make the specialized call for windowed operators
                                ((UpdateByWindowedOperator.Context) opContext[opIdx]).accumulate(
                                        chunkRs,
                                        chunkArr,
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
        return new UpdateByWindowTimeContext(sourceRowSet, inputSources, timestampColumnSource, timestampSsa, chunkSize,
                isInitializeStep);
    }

    private static WritableRowSet computeAffectedRowsTime(final RowSet sourceSet, final RowSet subset, long revNanos,
            long fwdNanos, final ColumnSource<?> timestampColumnSource, final LongSegmentedSortedArray timestampSsa,
            boolean usePrev) {
        // swap fwd/rev to get the affected windows
        return computeInfluencerRowsTime(sourceSet, subset, fwdNanos, revNanos, timestampColumnSource, timestampSsa,
                usePrev);
    }

    private static WritableRowSet computeInfluencerRowsTime(final RowSet sourceSet, final RowSet subset, long revNanos,
            long fwdNanos, final ColumnSource<?> timestampColumnSource, final LongSegmentedSortedArray timestampSsa,
            boolean usePrev) {
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
                final int rsSize = rs.intSize();

                LongChunk<? extends Values> timestamps = usePrev
                        ? timestampColumnSource.getPrevChunk(context, rs).asLongChunk()
                        : timestampColumnSource.getChunk(context, rs).asLongChunk();

                for (int ii = 0; ii < rsSize; ii++) {
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

                        Assert.eqTrue(ssaIt.hasNext() && ssaIt.nextValue() >= head,
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

    UpdateByWindowTime(UpdateByOperator[] operators, int[][] operatorSourceSlots, @Nullable String timestampColumnName,
            long prevUnits, long fwdUnits) {
        super(operators, operatorSourceSlots, timestampColumnName);
        this.prevUnits = prevUnits;
        this.fwdUnits = fwdUnits;
    }
}
