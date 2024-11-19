//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import java.util.BitSet;
import java.util.stream.IntStream;

/**
 * This is the specialization of {@link UpdateByWindow} that handles tick based `windowed` operators. These operators
 * maintain a window of data based on row distance rather than timestamps. Window-based operators must maintain a buffer
 * of `influencer` values to add to the rolling window as the current row changes.
 */
class UpdateByWindowRollingTicks extends UpdateByWindowRollingBase {
    static class UpdateByWindowTicksBucketContext extends UpdateByWindowRollingBucketContext {
        private RowSet affectedRowPositions;
        private RowSet influencerPositions;

        UpdateByWindowTicksBucketContext(
                final TrackingRowSet sourceRowSet,
                final int chunkSize,
                final boolean initialStep,
                final Object[] bucketKeyValues) {
            super(sourceRowSet, null, null, null, false, chunkSize, initialStep, bucketKeyValues);
        }

        @Override
        public void close() {
            super.close();
            Assert.eqNull(affectedRowPositions, "affectedRowPositions");
            Assert.eqNull(influencerPositions, "influencerPositions");
        }
    }

    UpdateByWindowRollingTicks(UpdateByOperator[] operators, int[][] operatorSourceSlots, long prevUnits,
            long fwdUnits) {
        super(operators, operatorSourceSlots, prevUnits, fwdUnits, null);

        // this is also checked at RollingSumSpec creation against a hard-coded value (Integer.MAX_VALUE - 8)
        if (prevUnits + fwdUnits > ArrayUtil.MAX_ARRAY_SIZE) {
            throw new IllegalArgumentException(
                    "UpdateBy window size may not exceed MAX_ARRAY_SIZE (" + ArrayUtil.MAX_ARRAY_SIZE + ")");
        }
    }

    @Override
    UpdateByWindow copy() {
        final UpdateByOperator[] copiedOperators = new UpdateByOperator[this.operators.length];
        for (int ii = 0; ii < copiedOperators.length; ii++) {
            copiedOperators[ii] = this.operators[ii].copy();
        }

        return new UpdateByWindowRollingTicks(copiedOperators, operatorInputSourceSlots, prevUnits, fwdUnits);
    }

    @Override
    void finalizeWindowBucket(UpdateByWindowBucketContext context) {
        UpdateByWindowTicksBucketContext ctx = (UpdateByWindowTicksBucketContext) context;
        try (final SafeCloseable ignoredRs1 = ctx.affectedRowPositions;
                final SafeCloseable ignoredRs2 =
                        ctx.influencerPositions == ctx.affectedRowPositions ? null : ctx.influencerPositions) {
            ctx.affectedRowPositions = null;
            ctx.influencerPositions = null;
        }
        super.finalizeWindowBucket(context);
    }

    @Override
    UpdateByWindowBucketContext makeWindowContext(
            final TrackingRowSet sourceRowSet,
            final ColumnSource<?> timestampColumnSource,
            final LongSegmentedSortedArray timestampSsa,
            final TrackingRowSet timestampValidRowSet,
            final boolean timestampsModified,
            final int chunkSize,
            final boolean isInitializeStep,
            final Object[] bucketKeyValues) {
        return new UpdateByWindowTicksBucketContext(sourceRowSet, chunkSize, isInitializeStep, bucketKeyValues);
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

            if (tail < minPos.get() || head > maxPos) {
                // ignore this range
                return;
            }
            head = Math.max(head, minPos.get());
            tail = Math.min(tail, maxPos);
            builder.appendRange(head, tail);
            minPos.set(tail + 1);
        });

        try (final RowSet positions = builder.build()) {
            return sourceSet.subSetForPositions(positions);
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
            // No further work will be done on this context
            finalizeWindowBucket(context);
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
            context.dirtyOperators = new BitSet(operators.length);
            context.dirtyOperators.set(0, operators.length);

            ctx.isDirty = true;
            return;
        }

        // determine which operators are affected by this update
        processUpdateForContext(context, upstream);

        if (!ctx.isDirty) {
            // No further work will be done on this context
            finalizeWindowBucket(context);
            return;
        }

        // need a writable rowset
        final WritableRowSet tmpAffected = RowSetFactory.empty();

        // consider the modifications only when input columns were modified
        if (upstream.modified().isNonempty() && ctx.inputModified) {
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

        if (ctx.affectedRows.isEmpty()) {
            // No further work will be done on this context
            finalizeWindowBucket(context);
            ctx.isDirty = false;
            return;
        }

        // now get influencer rows for the affected rows
        // generate position data rowsets for efficiently computed position offsets
        ctx.affectedRowPositions = ctx.sourceRowSet.invert(ctx.affectedRows);

        ctx.influencerRows = computeInfluencerRowsTicks(ctx.sourceRowSet, ctx.affectedRowPositions,
                prevUnits, fwdUnits);
        ctx.influencerPositions = ctx.sourceRowSet.invert(ctx.influencerRows);
    }

    void computeWindows(UpdateByWindowRollingBucketContext context) {
        UpdateByWindowTicksBucketContext ctx = (UpdateByWindowTicksBucketContext) context;

        try (final RowSequence.Iterator affectedPosIt = ctx.affectedRowPositions.getRowSequenceIterator();
                final RowSequence.Iterator influencerPosHeadIt = ctx.influencerPositions.getRowSequenceIterator();
                final RowSequence.Iterator influencerPosTailIt = ctx.influencerPositions.getRowSequenceIterator()) {

            final long sourceRowSetSize = ctx.sourceRowSet.size();
            int affectedChunkOffset = 0;

            while (affectedPosIt.hasMore()) {
                final RowSequence chunkPosRs = affectedPosIt.getNextRowSequenceWithLength(ctx.workingChunkSize);
                final int chunkSize = chunkPosRs.intSize();

                final LongChunk<OrderedRowKeys> posChunk = chunkPosRs.asRowKeyChunk();
                final WritableIntChunk<Values> pushChunk = ctx.pushChunks[affectedChunkOffset];
                final WritableIntChunk<Values> popChunk = ctx.popChunks[affectedChunkOffset];

                long totalPushCount = 0;

                for (int ii = 0; ii < chunkSize; ii++) {
                    // Read the current position.
                    final long currentPos = posChunk.get(ii);

                    // Compute the head and tail positions (inclusive).
                    final long head = Math.max(0, currentPos - prevUnits + 1);
                    final long tail = Math.min(sourceRowSetSize - 1, currentPos + fwdUnits);

                    // Pop out all values from the current window that are not in the new window.
                    long popCount = influencerPosHeadIt.advanceAndGetPositionDistance(head);

                    // Push in all values that are in the new window (inclusive of tail).
                    long pushCount = influencerPosTailIt.advanceAndGetPositionDistance(tail + 1);

                    pushChunk.set(ii, Math.toIntExact(pushCount));
                    popChunk.set(ii, Math.toIntExact(popCount));

                    totalPushCount += pushCount;
                }
                ctx.influencerCounts[affectedChunkOffset] = Math.toIntExact(totalPushCount);
                ctx.maxGetContextSize = Math.max(ctx.maxGetContextSize, Math.toIntExact(totalPushCount));

                affectedChunkOffset++;
            }
        }
    }
}
