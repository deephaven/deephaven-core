package io.deephaven.engine.table.impl.updateby;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.hash.TIntHashSet;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.ChunkPoolConstants;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.stream.IntStream;

/**
 * This is the specialization of {@link UpdateByWindow} that handles tick based `windowed` operators. These operators
 * maintain a window of data based on row distance rather than timestamps. Window-based operators must maintain a buffer
 * of `influencer` values to add to the rolling window as the current row changes.
 */
class UpdateByWindowTicks extends UpdateByWindow {
    /** growth rate after the contexts have exceeded the poolable chunk size */
    public static final double CONTEXT_GROWTH_PERCENTAGE = 0.25;
    private static final int WINDOW_CHUNK_SIZE = 4096;
    private final long prevUnits;
    private final long fwdUnits;

    class UpdateByWindowTicksBucketContext extends UpdateByWindowBucketContext {
        private RowSet affectedRowPositions;
        private RowSet influencerPositions;
        private int currentGetContextSize;

        UpdateByWindowTicksBucketContext(final TrackingRowSet sourceRowSet,
                final int chunkSize, final boolean initialStep) {
            super(sourceRowSet, null, null, null, chunkSize, initialStep);
        }

        @Override
        public void close() {
            super.close();
            try (final SafeCloseable ignoredRs1 = affectedRowPositions;
                    final SafeCloseable ignoredRs2 =
                            influencerPositions == affectedRowPositions ? null : influencerPositions) {
            }
        }
    }

    UpdateByWindowTicks(UpdateByOperator[] operators, int[][] operatorSourceSlots, long prevUnits, long fwdUnits) {
        super(operators, operatorSourceSlots, null);
        this.prevUnits = prevUnits;
        this.fwdUnits = fwdUnits;

        // We would like to use jdk.internal.util.ArraysSupport.MAX_ARRAY_LENGTH, but it is not exported
        final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

        if (prevUnits + fwdUnits > MAX_ARRAY_SIZE) {
            throw (new IllegalArgumentException(
                    "UpdateBy window size may not exceed MAX_ARRAY_SIZE (" + MAX_ARRAY_SIZE + ")"));
        }
    }

    private void makeOperatorContexts(UpdateByWindowBucketContext context) {
        UpdateByWindowTicksBucketContext ctx = (UpdateByWindowTicksBucketContext) context;

        ctx.workingChunkSize = WINDOW_CHUNK_SIZE;
        ctx.currentGetContextSize = ctx.workingChunkSize;

        // create contexts for the affected operators
        for (int opIdx : context.dirtyOperatorIndices) {
            context.opContext[opIdx] = operators[opIdx].makeUpdateContext(context.workingChunkSize,
                    operatorInputSourceSlots[opIdx].length);
        }
    }

    @Override
    UpdateByWindowBucketContext makeWindowContext(final TrackingRowSet sourceRowSet,
            final ColumnSource<?> timestampColumnSource,
            final LongSegmentedSortedArray timestampSsa,
            final TrackingRowSet timestampValidRowSet,
            final int chunkSize,
            final boolean isInitializeStep) {
        return new UpdateByWindowTicksBucketContext(sourceRowSet, chunkSize, isInitializeStep);
    }

    private static WritableRowSet computeAffectedRowsTicks(final RowSet sourceSet, final RowSet invertedSubSet,
            long revTicks, long fwdTicks) {
        // adjust fwd/rev to get the affected windows

        // Potential cases and reasoning:
        // 1) rev 1, fwd 0 - this row influences, affected should also be 1, 0
        // 2) rev 2, fwd 0 - this row and previous 1 influences, affected should be 1, 1
        // 3) rev 10, fwd 0 - this row and previous 9 influences, affected should be 1, 9
        // 4) rev 0, fwd 10 - next 10 influences, affected should be 11, -1 (looks weird but that is how we would
        // exclude the current row)
        // 5) rev 10, fwd 50 - affected should be 51, 9

        return computeInfluencerRowsTicks(sourceSet, invertedSubSet, fwdTicks + 1, revTicks - 1);
    }

    private static WritableRowSet computeInfluencerRowsTicks(final RowSet sourceSet, final RowSet invertedSubSet,
            long revTicks, long fwdTicks) {
        long maxPos = sourceSet.size() - 1;

        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        final MutableLong minPos = new MutableLong(0L);

        invertedSubSet.forAllRowKeyRanges((s, e) -> {
            long head = s - revTicks + 1;
            long tail = e + fwdTicks;

            if (tail < minPos.longValue() || head > maxPos) {
                // ignore this range
                return;
            }
            head = Math.max(head, minPos.longValue());
            tail = Math.min(tail, maxPos);
            builder.appendRange(head, tail);
            minPos.setValue(tail + 1);
        });

        try (final RowSet positions = builder.build()) {
            return sourceSet.subSetForPositions(positions);
        }
    }

    private void ensureGetContextSize(UpdateByWindowTicksBucketContext ctx, long newSize) {
        if (ctx.currentGetContextSize < newSize) {
            long size = ctx.currentGetContextSize;
            while (size < newSize) {
                size *= 2;
            }

            // if size would no longer be poolable, use percentage growth for the new contexts
            ctx.currentGetContextSize = LongSizedDataStructure.intSize(
                    "ensureGetContextSize exceeded Integer.MAX_VALUE",
                    size >= ChunkPoolConstants.LARGEST_POOLED_CHUNK_CAPACITY
                            ? (long) (newSize * (1.0 + CONTEXT_GROWTH_PERCENTAGE))
                            : size);

            // use this to track which contexts have already resized
            final boolean[] resized = new boolean[ctx.inputSources.length];

            for (int opIdx : ctx.dirtyOperatorIndices) {
                final int[] sourceIndices = operatorInputSourceSlots[opIdx];
                for (int sourceSlot : sourceIndices) {
                    if (!resized[sourceSlot]) {
                        // close the existing context
                        ctx.inputSourceGetContexts[sourceSlot].close();

                        // create a new context of the larger size
                        ctx.inputSourceGetContexts[sourceSlot] =
                                ctx.inputSources[sourceSlot].makeGetContext(ctx.currentGetContextSize);
                        resized[sourceSlot] = true;
                    }
                }
            }
        }
    }

    /**
     * Finding the `affected` and `influencer` rowsets for a windowed operation is complex. We must identify modified
     * rows (including added rows) and deleted rows and determine which rows are `affected` by the change given the
     * window parameters. After these rows have been identified, must determine which rows will be needed to recompute
     * these values (i.e. that fall within the window and will `influence` this computation).
     */
    @Override
    void computeAffectedRowsAndOperators(UpdateByWindowBucketContext context, @NotNull TableUpdate upstream) {

        UpdateByWindowTicksBucketContext ctx = (UpdateByWindowTicksBucketContext) context;

        if (upstream.empty() || context.sourceRowSet.isEmpty()) {
            return;
        }

        // all rows are affected on the initial step
        if (ctx.initialStep) {
            ctx.affectedRows = ctx.sourceRowSet;
            // no need to invert, just create a flat rowset
            ctx.affectedRowPositions = RowSetFactory.flat(ctx.sourceRowSet.size());

            // quick test to see if we will need all rows
            if (prevUnits > 0 && fwdUnits >= 0) {
                // the current row influences itself, therefore all rows are needed
                ctx.influencerRows = ctx.affectedRows;
                ctx.influencerPositions = ctx.affectedRowPositions;
            } else {
                // some rows will be excluded, get the exact set of influencer rows
                final long size = ctx.affectedRows.size();
                final long startPos = Math.max(0, 1 - prevUnits);
                final long endPos = Math.min(size - 1, size + fwdUnits - 1);

                // subSetByPositionRange() endPos is exclusive
                ctx.influencerRows = ctx.affectedRows.subSetByPositionRange(startPos, endPos + 1);
                ctx.influencerPositions = ctx.affectedRowPositions.subSetByPositionRange(startPos, endPos + 1);
            }

            // mark all operators as affected by this update
            context.dirtyOperatorIndices = IntStream.range(0, operators.length).toArray();
            context.dirtySourceIndices = getUniqueSourceIndices();

            makeOperatorContexts(ctx);
            ctx.isDirty = true;
            return;
        }

        // determine which operators are affected by this update
        boolean allAffected = upstream.added().isNonempty() || upstream.removed().isNonempty();

        if (allAffected) {
            // mark all operators as affected by this update
            context.dirtyOperatorIndices = IntStream.range(0, operators.length).toArray();
            context.dirtySourceIndices = getUniqueSourceIndices();
            context.isDirty = true;
        } else {
            // determine which operators were affected by this update
            TIntArrayList dirtyOperatorList = new TIntArrayList(operators.length);
            TIntHashSet inputSourcesSet = new TIntHashSet(getUniqueSourceIndices().length);
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                UpdateByOperator op = operators[opIdx];
                if (upstream.modifiedColumnSet().nonempty() && (op.getInputModifiedColumnSet() == null
                        || upstream.modifiedColumnSet().containsAny(op.getInputModifiedColumnSet()))) {
                    dirtyOperatorList.add(opIdx);
                    inputSourcesSet.addAll(operatorInputSourceSlots[opIdx]);
                    context.isDirty = true;
                }
            }
            context.dirtyOperatorIndices = dirtyOperatorList.toArray();
            context.dirtySourceIndices = inputSourcesSet.toArray();
        }

        if (!ctx.isDirty) {
            return;
        }

        // need a writable rowset
        final WritableRowSet tmpAffected = RowSetFactory.empty();

        if (upstream.modified().isNonempty()) {
            // compute the rows affected from these changes
            try (final WritableRowSet modifiedInverted = ctx.sourceRowSet.invert(upstream.modified());
                    final RowSet modifiedAffected =
                            computeAffectedRowsTicks(ctx.sourceRowSet, modifiedInverted, prevUnits, fwdUnits)) {
                tmpAffected.insert(modifiedAffected);
            }
        }

        if (upstream.added().isNonempty()) {
            // add the new rows and any cascading changes from inserting rows
            final long prev = Math.max(1, prevUnits);
            final long fwd = Math.max(0, fwdUnits);

            try (final RowSet addedInverted = ctx.sourceRowSet.invert(upstream.added());
                    final RowSet addedAffected = computeAffectedRowsTicks(ctx.sourceRowSet, addedInverted, prev, fwd)) {
                tmpAffected.insert(addedAffected);
            }
        }

        if (upstream.removed().isNonempty()) {
            // add the cascading changes from removing rows
            final long prev = Math.max(0, prevUnits);
            final long fwd = Math.max(0, fwdUnits);

            try (final RowSet prevRows = ctx.sourceRowSet.copyPrev();
                    final RowSet removedInverted = prevRows.invert(upstream.removed());
                    final WritableRowSet removedAffected =
                            computeAffectedRowsTicks(prevRows, removedInverted, prev, fwd)) {
                // apply shifts to get back to pos-shift space
                upstream.shifted().apply(removedAffected);
                // retain only the rows that still exist in the sourceRowSet
                removedAffected.retain(ctx.sourceRowSet);
                tmpAffected.insert(removedAffected);
            }
        }

        ctx.affectedRows = tmpAffected;

        // now get influencer rows for the affected rows
        // generate position data rowsets for efficiently computed position offsets
        ctx.affectedRowPositions = ctx.sourceRowSet.invert(ctx.affectedRows);

        ctx.influencerRows = computeInfluencerRowsTicks(ctx.sourceRowSet, ctx.affectedRowPositions,
                prevUnits, fwdUnits);
        ctx.influencerPositions = ctx.sourceRowSet.invert(ctx.influencerRows);

        makeOperatorContexts(ctx);

    }

    @Override
    void processRows(UpdateByWindowBucketContext context, boolean initialStep) {
        UpdateByWindowTicksBucketContext ctx = (UpdateByWindowTicksBucketContext) context;

        Assert.neqNull(context.inputSources, "assignInputSources() must be called before processRow()");

        for (int opIdx : context.dirtyOperatorIndices) {
            UpdateByWindowedOperator winOp = (UpdateByWindowedOperator) operators[opIdx];
            // call the specialized version of `intializeUpdate()` for these operators
            winOp.initializeUpdate(ctx.opContext[opIdx]);
        }

        try (final RowSequence.Iterator it = ctx.affectedRows.getRowSequenceIterator();
                final RowSequence.Iterator posIt = ctx.affectedRowPositions.getRowSequenceIterator();
                final RowSequence.Iterator influencerPosHeadIt = ctx.influencerPositions.getRowSequenceIterator();
                final RowSequence.Iterator influencerPosTailIt = ctx.influencerPositions.getRowSequenceIterator();
                final RowSequence.Iterator influencerKeyIt = ctx.influencerRows.getRowSequenceIterator();
                final WritableIntChunk<? extends Values> pushChunk =
                        WritableIntChunk.makeWritableChunk(ctx.workingChunkSize);
                final WritableIntChunk<? extends Values> popChunk =
                        WritableIntChunk.makeWritableChunk(ctx.workingChunkSize)) {

            final long sourceRowSetSize = ctx.sourceRowSet.size();

            // chunk processing
            while (it.hasMore()) {
                final RowSequence chunkRs = it.getNextRowSequenceWithLength(ctx.workingChunkSize);
                final RowSequence chunkPosRs = posIt.getNextRowSequenceWithLength(ctx.workingChunkSize);
                final int chunkRsSize = chunkRs.intSize();

                final LongChunk<OrderedRowKeys> posChunk = chunkPosRs.asRowKeyChunk();

                // chunk processing
                long totalCount = 0;

                for (int ii = 0; ii < chunkRsSize; ii++) {
                    // read the current position
                    final long currentPos = posChunk.get(ii);

                    // compute the head and tail positions (inclusive)
                    final long head = Math.max(0, currentPos - prevUnits + 1);
                    final long tail = Math.min(sourceRowSetSize - 1, currentPos + fwdUnits);

                    // pop out all values from the current window that are not in the new window
                    long popCount = influencerPosHeadIt.advanceAndGetPositionDistance(head);

                    // push in all values that are in the new window (inclusive of tail)
                    long pushCount = influencerPosTailIt.advanceAndGetPositionDistance(tail + 1);

                    // write the push and pop counts to the chunks
                    popChunk.set(ii, Math.toIntExact(popCount));
                    pushChunk.set(ii, Math.toIntExact(pushCount));

                    totalCount += pushCount;
                }

                // execute the operators
                final RowSequence chunkInfluencerRs = influencerKeyIt.getNextRowSequenceWithLength(totalCount);
                ensureGetContextSize(ctx, chunkInfluencerRs.size());

                Arrays.fill(ctx.inputSourceChunks, null);
                for (int opIdx : context.dirtyOperatorIndices) {
                    UpdateByWindowedOperator.Context opCtx =
                            (UpdateByWindowedOperator.Context) context.opContext[opIdx];
                    // prep the chunk array needed by the accumulate call
                    final int[] srcIndices = operatorInputSourceSlots[opIdx];
                    for (int ii = 0; ii < srcIndices.length; ii++) {
                        int srcIdx = srcIndices[ii];
                        // chunk prep
                        prepareValuesChunkForSource(ctx, srcIdx, chunkInfluencerRs);
                        opCtx.chunkArr[ii] = ctx.inputSourceChunks[srcIdx];
                    }

                    // make the specialized call for windowed operators
                    ((UpdateByWindowedOperator.Context) ctx.opContext[opIdx]).accumulate(
                            chunkRs,
                            opCtx.chunkArr,
                            pushChunk,
                            popChunk,
                            chunkRsSize);
                }
            }
        }

        // call `finishUpdate()` function for each operator
        for (int opIdx : context.dirtyOperatorIndices) {
            operators[opIdx].finishUpdate(context.opContext[opIdx]);
        }
    }
}
