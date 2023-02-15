package io.deephaven.engine.table.impl.updateby;

import io.deephaven.base.ringbuffer.LongRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.engine.table.iterators.LongColumnIterator;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.BitSet;
import java.util.stream.IntStream;

import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * This is the specialization of {@link UpdateByWindow} that handles time-based `windowed` operators. These operators
 * maintain a window of data based on a timestamp column rather than row distances. Window-based operators must maintain
 * a buffer of `influencer` values to add to the rolling window as the current row changes.
 */
class UpdateByWindowTime extends UpdateByWindow {
    private static final int RING_BUFFER_INITIAL_SIZE = 128;
    protected final long prevUnits;
    protected final long fwdUnits;

    public class UpdateByWindowTimeBucketContext extends UpdateByWindowBucketContext {
        ChunkSource.GetContext timestampColumnGetContext;
        protected int currentGetContextSize;
        private WritableIntChunk<Values>[] pushChunks;
        private WritableIntChunk<Values>[] popChunks;
        private int[] influencerCounts;

        public UpdateByWindowTimeBucketContext(final TrackingRowSet sourceRowSet,
                @NotNull final ColumnSource<?> timestampColumnSource,
                @Nullable final LongSegmentedSortedArray timestampSsa,
                final TrackingRowSet timestampValidRowSet,
                final boolean timestampsModified,
                final int chunkSize,
                final boolean initialStep) {
            super(sourceRowSet, timestampColumnSource, timestampSsa, timestampValidRowSet, timestampsModified,
                    chunkSize, initialStep);
            // This is needed by computeAffectedRowsAndOperators() before allocateResources() is called
            timestampColumnGetContext = timestampColumnSource.makeGetContext(workingChunkSize);
        }

        @Override
        public void close() {
            super.close();
            Assert.eqNull(timestampColumnGetContext, "timestampColumnGetContext");
            Assert.eqNull(pushChunks, "pushChunks");
            Assert.eqNull(popChunks, "popChunks");
        }
    }

    UpdateByWindowTime(UpdateByOperator[] operators, int[][] operatorSourceSlots, @Nullable String timestampColumnName,
            long prevUnits, long fwdUnits) {
        super(operators, operatorSourceSlots, timestampColumnName);
        this.prevUnits = prevUnits;
        this.fwdUnits = fwdUnits;
    }

    @Override
    void prepareWindowBucket(UpdateByWindowBucketContext context) {
        UpdateByWindowTimeBucketContext ctx = (UpdateByWindowTimeBucketContext) context;

        // working chunk size need not be larger than affectedRows.size()
        context.workingChunkSize = Math.toIntExact(Math.min(context.workingChunkSize, context.affectedRows.size()));
        ctx.currentGetContextSize = ctx.workingChunkSize;

        // create the array of push/pop chunks
        final long rowCount = ctx.affectedRows.size();
        final int chunkCount = Math.toIntExact((rowCount + ctx.workingChunkSize - 1) / ctx.workingChunkSize);

        ctx.pushChunks = new WritableIntChunk[chunkCount];
        ctx.popChunks = new WritableIntChunk[chunkCount];
        for (int ii = 0; ii < chunkCount; ii++) {
            ctx.pushChunks[ii] = WritableIntChunk.makeWritableChunk(ctx.workingChunkSize);
            ctx.popChunks[ii] = WritableIntChunk.makeWritableChunk(ctx.workingChunkSize);
        }
        ctx.influencerCounts = new int[chunkCount];

        computeWindows(ctx);
    }

    @Override
    void finalizeWindowBucket(UpdateByWindowBucketContext context) {
        UpdateByWindowTimeBucketContext ctx = (UpdateByWindowTimeBucketContext) context;
        ctx.timestampColumnGetContext.close();
        ctx.timestampColumnGetContext = null;
        if (ctx.pushChunks != null) {
            SafeCloseableArray.close(ctx.pushChunks);
            ctx.pushChunks = null;
        }
        if (ctx.popChunks != null) {
            SafeCloseableArray.close(ctx.popChunks);
            ctx.popChunks = null;
        }
        super.finalizeWindowBucket(context);
    }

    @Override
    public UpdateByWindowBucketContext makeWindowContext(final TrackingRowSet sourceRowSet,
            final ColumnSource<?> timestampColumnSource,
            final LongSegmentedSortedArray timestampSsa,
            final TrackingRowSet timestampValidRowSet,
            final boolean timestampsModified,
            final int chunkSize,
            final boolean isInitializeStep) {
        return new UpdateByWindowTimeBucketContext(sourceRowSet, timestampColumnSource, timestampSsa,
                timestampValidRowSet, timestampsModified, chunkSize, isInitializeStep);
    }

    /**
     * Finding the `affected` and `influencer` rowsets for a windowed operation is complex. We must identify modified
     * rows (including added rows) and deleted rows and determine which rows are `affected` by the change given the
     * window parameters. After these rows have been identified, must determine which rows will be needed to recompute
     * these values (i.e. that fall within the window and will `influence` this computation).
     */
    private static WritableRowSet computeAffectedRowsTime(final UpdateByWindowTimeBucketContext ctx,
            final RowSet subset, long revNanos, long fwdNanos, boolean usePrev) {
        // swap fwd/rev to get the affected windows
        return computeInfluencerRowsTime(ctx, subset, fwdNanos, revNanos, usePrev);
    }

    private static WritableRowSet computeInfluencerRowsTime(final UpdateByWindowTimeBucketContext ctx,
            final RowSet subset,
            long revNanos, long fwdNanos, boolean usePrev) {
        try (final RowSequence.Iterator it = subset.getRowSequenceIterator()) {
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            LongSegmentedSortedArray.Iterator ssaIt = ctx.timestampSsa.iterator(false, false);

            while (it.hasMore() && ssaIt.hasNext()) {
                final RowSequence rs = it.getNextRowSequenceWithLength(ctx.workingChunkSize);
                final int rsSize = rs.intSize();

                LongChunk<? extends Values> timestamps = usePrev
                        ? ctx.timestampColumnSource.getPrevChunk(ctx.timestampColumnGetContext, rs).asLongChunk()
                        : ctx.timestampColumnSource.getChunk(ctx.timestampColumnGetContext, rs).asLongChunk();

                for (int ii = 0; ii < rsSize; ii++) {
                    final long ts = timestamps.get(ii);
                    if (ts == NULL_LONG) {
                        // if the timestamp is null, the row won't belong to any set and we can pretend it doesn't exist
                        continue;
                    }
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

                    // step through the SSA and collect keys until outside the window
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
            return builder.build();
        }
    }

    /**
     * Windowed by time/ticks is complex to compute: must find all the changed rows and rows that would be affected by
     * the changes (includes newly added rows) and need to be recomputed. Then include all the rows that are affected by
     * deletions (if any). After the affected rows have been identified, determine which rows will be needed to compute
     * new values for the affected rows (influencer rows)
     */
    @Override
    public void computeAffectedRowsAndOperators(UpdateByWindowBucketContext context, @NotNull TableUpdate upstream) {
        UpdateByWindowTimeBucketContext ctx = (UpdateByWindowTimeBucketContext) context;

        if (upstream.empty() || ctx.sourceRowSet.isEmpty()) {
            // No further work will be done on this context
            finalizeWindowBucket(context);
            return;
        }

        // all rows are affected on the initial step
        if (ctx.initialStep) {
            ctx.affectedRows = ctx.sourceRowSet;

            // Get the exact set of rows needed to compute the initial row set
            ctx.influencerRows = computeInfluencerRowsTime(ctx, ctx.affectedRows, prevUnits, fwdUnits, false);

            // mark all operators as affected by this update
            context.dirtyOperatorIndices = IntStream.range(0, operators.length).toArray();
            context.dirtySourceIndices = getUniqueSourceIndices();
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

        final WritableRowSet tmpAffected = RowSetFactory.empty();

        // consider the modifications only when input or timestamp columns were modified
        if (upstream.modified().isNonempty() && (ctx.timestampsModified || ctx.inputModified)) {
            // recompute all windows that have the modified rows in their window
            try (final RowSet modifiedAffected =
                    computeAffectedRowsTime(ctx, upstream.modified(), prevUnits, fwdUnits, false)) {
                tmpAffected.insert(modifiedAffected);
            }

            if (ctx.timestampsModified) {
                // recompute all windows that previously contained the modified rows, they may not contain this value
                // after the timestamp modifications
                try (final WritableRowSet modifiedAffectedPrev =
                        computeAffectedRowsTime(ctx, upstream.getModifiedPreShift(), prevUnits, fwdUnits, true)) {
                    // we used the SSA (post-shift) to get these keys, no need to shift
                    // retain only the rows that still exist in the sourceRowSet
                    modifiedAffectedPrev.retain(ctx.timestampValidRowSet);
                    tmpAffected.insert(modifiedAffectedPrev);
                }

                // re-compute all modified rows, they have new windows after the timestamp modifications
                tmpAffected.insert(upstream.modified());
            }
        }

        if (upstream.added().isNonempty()) {
            try (final RowSet addedAffected =
                    computeAffectedRowsTime(ctx, upstream.added(), prevUnits, fwdUnits, false)) {
                tmpAffected.insert(addedAffected);
            }
            // compute all new rows
            tmpAffected.insert(upstream.added());
        }

        // other rows can be affected by removes
        if (upstream.removed().isNonempty()) {
            try (final WritableRowSet removedAffected =
                    computeAffectedRowsTime(ctx, upstream.removed(), prevUnits, fwdUnits, true)) {
                // we used the SSA (post-shift) to get these keys, no need to shift
                // retain only the rows that still exist in the sourceRowSet
                removedAffected.retain(ctx.timestampValidRowSet);

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
        ctx.influencerRows = computeInfluencerRowsTime(ctx, ctx.affectedRows, prevUnits, fwdUnits, false);
    }

    private long nextLongOrMax(LongColumnIterator it) {
        return it.hasNext() ? it.nextLong() : Long.MAX_VALUE;
    }

    public void computeWindows(UpdateByWindowTimeBucketContext ctx) {
        try (final RowSequence.Iterator affectedRowsIt = ctx.affectedRows.getRowSequenceIterator();
                final ChunkSource.GetContext affectedTsGetContext =
                        ctx.timestampColumnSource.makeGetContext(ctx.workingChunkSize);
                final LongColumnIterator influencerTsTailIt =
                        new LongColumnIterator(ctx.timestampColumnSource, ctx.influencerRows)) {

            final LongRingBuffer timestampWindowBuffer = new LongRingBuffer(RING_BUFFER_INITIAL_SIZE);

            int affectedChunkOffset = 0;

            long influencerTs = nextLongOrMax(influencerTsTailIt);

            while (affectedRowsIt.hasMore()) {
                // NOTE: We did not put null values into our SSA and our influencer rowset is built using the
                // SSA. There should be no null timestamps considered in the rolling windows
                final RowSequence affectedRs = affectedRowsIt.getNextRowSequenceWithLength(ctx.workingChunkSize);
                final long chunkSize = affectedRs.intSize();

                long totalPushCount = 0;

                final LongChunk affectedTsChunk =
                        ctx.timestampColumnSource.getChunk(affectedTsGetContext, affectedRs).asLongChunk();
                final WritableIntChunk<Values> pushChunk = ctx.pushChunks[affectedChunkOffset];
                final WritableIntChunk<Values> popChunk = ctx.popChunks[affectedChunkOffset];

                for (int ii = 0; ii < chunkSize; ii++) {
                    final long affectedTs = affectedTsChunk.get(ii);

                    if (affectedTs == NULL_LONG) {
                        // This signifies that does not belong to a time window.
                        popChunk.set(ii, NULL_INT);
                        pushChunk.set(ii, NULL_INT);
                        continue;
                    }

                    // Compute the head and tail timestamps (inclusive).
                    final long head = affectedTs - prevUnits;
                    final long tail = affectedTs + fwdUnits;

                    // Pop out all values from the current window that are not in the new window.
                    long popCount = 0;
                    while (!timestampWindowBuffer.isEmpty() && timestampWindowBuffer.front() < head) {
                        timestampWindowBuffer.remove();
                        popCount++;
                    }

                    // Push in all values that are in the new window (inclusive of tail).
                    long pushCount = 0;
                    while (influencerTs <= tail) {
                        // Add this value to the buffer before advancing.
                        timestampWindowBuffer.add(influencerTs);
                        influencerTs = nextLongOrMax(influencerTsTailIt);
                        pushCount++;
                    }

                    // Write the push and pop counts to the chunks.
                    popChunk.set(ii, Math.toIntExact(popCount));
                    pushChunk.set(ii, Math.toIntExact(pushCount));
                    totalPushCount += pushCount;
                }
                ctx.influencerCounts[affectedChunkOffset] = Math.toIntExact(totalPushCount);
                ctx.currentGetContextSize = Math.max(ctx.currentGetContextSize, Math.toIntExact(totalPushCount));

                affectedChunkOffset++;
            }
        }
    }

    @Override
    public void processBucketOperator(UpdateByWindowBucketContext context, int winOpIdx, boolean initialStep) {
        Assert.neqNull(context.inputSources, "assignInputSources() must be called before processRow()");

        UpdateByWindowTimeBucketContext ctx = (UpdateByWindowTimeBucketContext) context;

        final UpdateByOperator winOp = operators[winOpIdx];

        try (final UpdateByOperator.Context winOpCtx = winOp.makeUpdateContext(ctx.workingChunkSize);
                final RowSequence.Iterator affectedRowsIt = ctx.affectedRows.getRowSequenceIterator();
                final RowSequence.Iterator influencerRowsIt = ctx.influencerRows.getRowSequenceIterator()) {

            final int[] srcIndices = operatorInputSourceSlots[winOpIdx];

            // Call the specialized version of `intializeUpdate()` for this operator.
            winOp.initializeRolling(winOpCtx);

            final Chunk<? extends Values>[] chunkArr = new Chunk[srcIndices.length];
            final ChunkSource.GetContext[] chunkContexts = new ChunkSource.GetContext[srcIndices.length];
            for (int ii = 0; ii < srcIndices.length; ii++) {
                final int srcIdx = srcIndices[ii];
                chunkContexts[ii] = ctx.inputSources[srcIdx].makeGetContext(ctx.currentGetContextSize);
            }

            int affectedChunkOffset = 0;

            while (affectedRowsIt.hasMore()) {
                final int influencerCount = ctx.influencerCounts[affectedChunkOffset];

                final RowSequence affectedRs = affectedRowsIt.getNextRowSequenceWithLength(ctx.workingChunkSize);
                final RowSequence influencerRs = influencerRowsIt.getNextRowSequenceWithLength(influencerCount);

                final int affectedChunkSize = affectedRs.intSize();

                // Prep the chunk array needed by the accumulate call.
                for (int ii = 0; ii < srcIndices.length; ii++) {
                    int srcIdx = srcIndices[ii];
                    chunkArr[ii] = ctx.inputSources[srcIdx].getChunk(chunkContexts[ii], influencerRs);
                }

                // Make the specialized call for windowed operators.
                winOpCtx.accumulateRolling(
                        affectedRs,
                        chunkArr,
                        ctx.pushChunks[affectedChunkOffset],
                        ctx.popChunks[affectedChunkOffset],
                        affectedChunkSize);

                affectedChunkOffset++;
            }

            // Clean up the get contexts.
            SafeCloseableArray.close(chunkContexts);

            // Finalize the operator.
            winOp.finishUpdate(winOpCtx);
        }
    }
}
