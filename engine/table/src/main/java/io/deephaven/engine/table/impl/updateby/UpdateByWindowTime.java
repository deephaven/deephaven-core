package io.deephaven.engine.table.impl.updateby;

import io.deephaven.base.ringbuffer.LongRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_LONG;

// this class is currently too big, should specialize into CumWindow, TickWindow, TimeWindow to simplify implementation
public class UpdateByWindowTime extends UpdateByWindow {
    protected final long prevUnits;
    protected final long fwdUnits;

    public class UpdateByWindowTimeContext extends UpdateByWindowContext {
        protected final ChunkSource.GetContext influencerTimestampContext;
        protected final LongRingBuffer currentWindowTimestamps;

        protected int nextInfluencerIndex;
        protected long nextInfluencerTimestamp;
        protected long nextInfluencerKey;

        protected RowSequence.Iterator influencerIt;
        protected LongChunk<OrderedRowKeys> influencerKeyChunk;
        protected LongChunk<? extends Values> influencerTimestampChunk;

        public UpdateByWindowTimeContext(final TrackingRowSet sourceRowSet, final ColumnSource<?>[] inputSources,
                @NotNull final ColumnSource<?> timestampColumnSource,
                @Nullable final LongSegmentedSortedArray timestampSsa, final int chunkSize, final boolean initialStep) {
            super(sourceRowSet, inputSources, timestampColumnSource, timestampSsa, chunkSize, initialStep);

            influencerTimestampContext = timestampColumnSource.makeGetContext(chunkSize);
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
                    timestampColumnSource, timestampSsa);

            // other rows can be affected by removes
            if (upstream.removed().isNonempty()) {
                try (final RowSet prev = sourceRowSet.copyPrev();
                        final WritableRowSet affectedByRemoves =
                                computeAffectedRowsTime(prev, upstream.removed(), prevUnits, fwdUnits,
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
            influencerRows = computeInfluencerRowsTime(sourceRowSet, affectedRows, prevUnits, fwdUnits,
                    timestampColumnSource, timestampSsa);

            makeOperatorContexts();
            return true;
        }

        private void loadNextInfluencerValueChunks() {
            if (!influencerIt.hasMore()) {
                nextInfluencerTimestamp = Long.MAX_VALUE;
                nextInfluencerKey = Long.MAX_VALUE;
                return;
            }

            final RowSequence influencerRs = influencerIt.getNextRowSequenceWithLength(chunkSize);
            influencerKeyChunk = influencerRs.asRowKeyChunk();
            influencerTimestampChunk =
                    timestampColumnSource.getChunk(influencerTimestampContext, influencerRs).asLongChunk();

            Arrays.fill(inputSourceChunkPopulated, false);
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    final int srcIdx = operatorSourceSlots[opIdx];
                    prepareValuesChunkForSource(srcIdx, influencerRs);

                    opContext[opIdx].setValuesChunk(inputSourceChunks[srcIdx]);
                }
            }

            nextInfluencerIndex = 0;
            nextInfluencerTimestamp = influencerTimestampChunk.get(nextInfluencerIndex);
            nextInfluencerKey = influencerKeyChunk.get(nextInfluencerIndex);
        }

        public void fillWindowTime(long currentTimestamp) {
            // compute the head and tail positions (inclusive)
            final long head = currentTimestamp - prevUnits;
            final long tail = currentTimestamp + fwdUnits;

            // pop out all values from the current window that are not in the new window
            while (!currentWindowTimestamps.isEmpty() && currentWindowTimestamps.front() < head) {
                // operator pop
                for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                    if (opAffected[opIdx]) {
                        opContext[opIdx].pop();
                    }
                }
                currentWindowTimestamps.remove();
            }

            // if the window is empty, reset
            if (currentWindowTimestamps.isEmpty()) {
                for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                    if (opAffected[opIdx]) {
                        opContext[opIdx].reset();
                    }
                }
            }

            // skip values until they match the window (this can only happen on the initial addition of rows to the
            // table, because we short-circuit the precise building of the influencer rows for efficiency)
            while (nextInfluencerTimestamp < head) {
                nextInfluencerIndex++;

                if (nextInfluencerIndex < influencerTimestampChunk.size()) {
                    nextInfluencerTimestamp = influencerTimestampChunk.get(nextInfluencerIndex);
                    nextInfluencerKey = influencerKeyChunk.get(nextInfluencerIndex);
                } else {
                    // try to bring in new data
                    loadNextInfluencerValueChunks();
                }
            }

            // push matching values
            while (nextInfluencerTimestamp <= tail) {
                // operator push
                for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                    if (opAffected[opIdx]) {
                        opContext[opIdx].push(nextInfluencerKey, nextInfluencerIndex);
                    }
                }
                currentWindowTimestamps.add(nextInfluencerTimestamp);
                nextInfluencerIndex++;

                if (nextInfluencerIndex < influencerTimestampChunk.size()) {
                    nextInfluencerTimestamp = influencerTimestampChunk.get(nextInfluencerIndex);
                    nextInfluencerKey = influencerKeyChunk.get(nextInfluencerIndex);
                } else {
                    // try to bring in new data
                    loadNextInfluencerValueChunks();
                }
            }
        }

        // overview: this function process the affected rows chunkwise, but will call fillWindowTime() for each
        // new row. fillWindowTime() will advance the moving window (which is the same for all operators in this
        // collection) and will call push/pop for each operator as it advances the window.
        //
        // We track the minimum amount of data needed, only the window timestamp data. The downstream operators
        // should manage local storage in a RingBuffer or other efficient structure
        @Override
        public void processRows() {
            if (trackModifications) {
                modifiedBuilder = RowSetFactory.builderSequential();
            }

            influencerIt = influencerRows.getRowSequenceIterator();
            try (final RowSequence.Iterator it = affectedRows.getRowSequenceIterator();
                    final ChunkSource.GetContext localTimestampContext =
                            timestampColumnSource.makeGetContext(chunkSize)) {

                // load the first chunk of influencer values (fillWindowTime() will call in future)
                loadNextInfluencerValueChunks();

                while (it.hasMore()) {
                    final RowSequence rs = it.getNextRowSequenceWithLength(chunkSize);

                    // just a note, we did not put null values into our SSA and our influencer rowset is built using the
                    // SSA. there should be no null timestamps considered in the rolling windows
                    final LongChunk<? extends Values> timestampChunk =
                            timestampColumnSource.getChunk(localTimestampContext, rs).asLongChunk();

                    // chunk processing
                    for (int ii = 0; ii < rs.size(); ii++) {
                        // read the current position
                        final long ts = timestampChunk.get(ii);

                        // fill the operator windows (calls push/pop/reset as appropriate)
                        fillWindowTime(ts);

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
        return new UpdateByWindowTimeContext(sourceRowSet, inputSources, timestampColumnSource, timestampSsa, chunkSize,
                isInitializeStep);
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

    UpdateByWindowTime(UpdateByOperator[] operators, int[] operatorSourceSlots, @Nullable String timestampColumnName,
            long prevUnits, long fwdUnits) {
        super(operators, operatorSourceSlots, timestampColumnName);
        this.prevUnits = prevUnits;
        this.fwdUnits = fwdUnits;
    }

    @Override
    public int hashCode() {
        return hashCode(true,
                timestampColumnName,
                prevUnits,
                fwdUnits);
    }
}
