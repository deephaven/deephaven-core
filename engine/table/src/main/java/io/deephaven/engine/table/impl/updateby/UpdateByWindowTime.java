package io.deephaven.engine.table.impl.updateby;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.hash.TIntHashSet;
import io.deephaven.base.ringbuffer.LongRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.ChunkPoolConstants;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.engine.table.iterators.LongColumnIterator;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.stream.IntStream;

import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * This is the specialization of {@link UpdateByWindow} that handles time-based `windowed` operators. These operators
 * maintain a window of data based on a timestamp column rather than row distances. Window-based operators must maintain
 * a buffer of `influencer` values to add to the rolling window as the current row changes.
 */
class UpdateByWindowTime extends UpdateByWindow {
    /** growth rate after the contexts have exceeded the poolable chunk size */
    private static final double CONTEXT_GROWTH_PERCENTAGE = 0.25;
    private static final int WINDOW_CHUNK_SIZE = 4096;
    private static final int RING_BUFFER_INITIAL_SIZE = 512;
    protected final long prevUnits;
    protected final long fwdUnits;

    public class UpdateByWindowTimeBucketContext extends UpdateByWindowBucketContext {
        protected final ChunkSource.GetContext influencerTimestampContext;
        final ChunkSource.GetContext timestampColumnGetContext;
        final LongRingBuffer timestampWindowBuffer;
        protected int currentGetContextSize;

        public UpdateByWindowTimeBucketContext(final TrackingRowSet sourceRowSet,
                @NotNull final ColumnSource<?> timestampColumnSource,
                @Nullable final LongSegmentedSortedArray timestampSsa,
                final TrackingRowSet timestampValidRowSet,
                final int chunkSize,
                final boolean initialStep) {
            super(sourceRowSet, timestampColumnSource, timestampSsa, timestampValidRowSet, chunkSize, initialStep);

            influencerTimestampContext = timestampColumnSource.makeGetContext(WINDOW_CHUNK_SIZE);
            timestampColumnGetContext = timestampColumnSource.makeGetContext(WINDOW_CHUNK_SIZE);
            timestampWindowBuffer = new LongRingBuffer(RING_BUFFER_INITIAL_SIZE, true);
        }

        @Override
        public void close() {
            super.close();
            try (final SafeCloseable ignoreCtx1 = influencerTimestampContext;
                    final SafeCloseable ignoreCtx2 = timestampColumnGetContext) {
                // leveraging try with resources to auto-close
            }
        }

    }

    UpdateByWindowTime(UpdateByOperator[] operators, int[][] operatorSourceSlots, @Nullable String timestampColumnName,
            long prevUnits, long fwdUnits) {
        super(operators, operatorSourceSlots, timestampColumnName);
        this.prevUnits = prevUnits;
        this.fwdUnits = fwdUnits;
    }

    protected void makeOperatorContexts(UpdateByWindowBucketContext context) {
        UpdateByWindowTimeBucketContext ctx = (UpdateByWindowTimeBucketContext) context;

        ctx.workingChunkSize = WINDOW_CHUNK_SIZE;
        ctx.currentGetContextSize = ctx.workingChunkSize;

        // create contexts for the affected operators
        for (int opIdx : context.dirtyOperatorIndices) {
            context.opContext[opIdx] = operators[opIdx].makeUpdateContext(context.workingChunkSize,
                    operatorInputSourceSlots[opIdx].length);
        }
    }

    @Override
    public UpdateByWindowBucketContext makeWindowContext(final TrackingRowSet sourceRowSet,
            final ColumnSource<?> timestampColumnSource,
            final LongSegmentedSortedArray timestampSsa,
            final TrackingRowSet timestampValidRowSet,
            final int chunkSize,
            final boolean isInitializeStep) {
        return new UpdateByWindowTimeBucketContext(sourceRowSet, timestampColumnSource, timestampSsa,
                timestampValidRowSet, chunkSize, isInitializeStep);
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
                final RowSequence rs = it.getNextRowSequenceWithLength(WINDOW_CHUNK_SIZE);
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

    private void ensureGetContextSize(UpdateByWindowTimeBucketContext ctx, long newSize) {
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
            boolean[] resized = new boolean[ctx.inputSources.length];

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
     * Windowed by time/ticks is complex to compute: must find all the changed rows and rows that would be affected by
     * the changes (includes newly added rows) and need to be recomputed. Then include all the rows that are affected by
     * deletions (if any). After the affected rows have been identified, determine which rows will be needed to compute
     * new values for the affected rows (influencer rows)
     */
    @Override
    public void computeAffectedRowsAndOperators(UpdateByWindowBucketContext context, @NotNull TableUpdate upstream) {
        UpdateByWindowTimeBucketContext ctx = (UpdateByWindowTimeBucketContext) context;

        if (upstream.empty() || ctx.sourceRowSet.isEmpty()) {
            return;
        }

        // all rows are affected on the initial step
        if (ctx.initialStep) {
            ctx.affectedRows = ctx.sourceRowSet;

            ctx.influencerRows = ctx.timestampValidRowSet;

            // mark all operators as affected by this update
            context.dirtyOperatorIndices = IntStream.range(0, operators.length).toArray();
            context.dirtySourceIndices = getUniqueSourceIndices();

            makeOperatorContexts(ctx);
            ctx.isDirty = true;
            return;
        }

        // determine which operators are affected by this update
        ctx.isDirty = false;
        boolean allAffected = upstream.added().isNonempty() ||
                upstream.removed().isNonempty();

        if (allAffected) {
            // mark all operators as affected by this update
            context.dirtyOperatorIndices = IntStream.range(0, operators.length).toArray();
            context.dirtySourceIndices = getUniqueSourceIndices();
            context.isDirty = true;
        } else {
            // determine which operators are affected by this update
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


        final WritableRowSet tmpAffected = RowSetFactory.empty();

        if (upstream.modified().isNonempty()) {
            // modified timestamps will affect the current and previous values
            try (final RowSet modifiedAffected =
                    computeAffectedRowsTime(ctx, upstream.modified(), prevUnits, fwdUnits, false)) {
                tmpAffected.insert(modifiedAffected);
            }
            try (final WritableRowSet modifiedAffectedPrev =
                    computeAffectedRowsTime(ctx, upstream.getModifiedPreShift(), prevUnits, fwdUnits, true)) {
                // we used the SSA (post-shift) to get these keys, no need to shift
                // retain only the rows that still exist in the sourceRowSet
                modifiedAffectedPrev.retain(ctx.timestampValidRowSet);
                tmpAffected.insert(modifiedAffectedPrev);
            }
            // compute all modified rows
            tmpAffected.insert(upstream.modified());
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

        // now get influencer rows for the affected rows
        ctx.influencerRows = computeInfluencerRowsTime(ctx, ctx.affectedRows, prevUnits, fwdUnits, false);

        makeOperatorContexts(ctx);
    }

    private long nextLongOrMax(LongColumnIterator it) {
        return it.hasNext() ? it.nextLong() : Long.MAX_VALUE;
    }

    /***
     * This function process the affected rows chunkwise, and will advance the moving window (which is the same for all
     * operators in this collection). For each row in the dataset the sliding window will adjust and instructions for
     * pushing/popping data will be created for the operators. For each chunk of `affected` rows, we will identify
     * exactly which `influencer` rows are needed and will provide those and the push/pop instructions to the operators.
     *
     * Downstream operators should manage local storage in a RingBuffer or other efficient structure since our pop()
     * calls do not provide the popped data
     */
    @Override
    public void processRows(UpdateByWindowBucketContext context, boolean initialStep) {
        UpdateByWindowTimeBucketContext ctx = (UpdateByWindowTimeBucketContext) context;

        for (int opIdx : context.dirtyOperatorIndices) {
            UpdateByWindowedOperator winOp = (UpdateByWindowedOperator) operators[opIdx];
            // call the specialized version of `intializeUpdate()` for these operators
            winOp.initializeUpdate(ctx.opContext[opIdx]);
        }

        try (final RowSequence.Iterator it = ctx.affectedRows.getRowSequenceIterator();
                final LongColumnIterator influencerTsTailIt =
                        new LongColumnIterator(context.timestampColumnSource, context.influencerRows);
                final RowSequence.Iterator influencerKeyIt = ctx.influencerRows.getRowSequenceIterator();
                final ChunkSource.GetContext localTimestampContext =
                        ctx.timestampColumnSource.makeGetContext(ctx.workingChunkSize);
                final WritableIntChunk<? extends Values> pushChunk =
                        WritableIntChunk.makeWritableChunk(ctx.workingChunkSize);
                final WritableIntChunk<? extends Values> popChunk =
                        WritableIntChunk.makeWritableChunk(ctx.workingChunkSize)) {

            long currentTailTs = nextLongOrMax(influencerTsTailIt);


            while (it.hasMore()) {
                final RowSequence chunkRs = it.getNextRowSequenceWithLength(ctx.workingChunkSize);
                final int chunkRsSize = chunkRs.intSize();

                // NOTE: we did not put null values into our SSA and our influencer rowset is built using the
                // SSA. there should be no null timestamps considered in the rolling windows
                final LongChunk<? extends Values> timestampChunk =
                        ctx.timestampColumnSource.getChunk(localTimestampContext, chunkRs).asLongChunk();

                // we need to use a builder because we are potentially skipping rows from the influencer rowset
                final RowSetBuilderSequential chunkBuilder = RowSetFactory.builderSequential();

                for (int ii = 0; ii < chunkRsSize; ii++) {
                    // read the current timestamp
                    final long currentTimestamp = timestampChunk.get(ii);
                    if (currentTimestamp == NULL_LONG) {
                        // this signifies that does not belong to a time window
                        popChunk.set(ii, NULL_INT);
                        pushChunk.set(ii, NULL_INT);
                        continue;
                    }

                    // compute the head and tail timestamps (inclusive)
                    final long head = currentTimestamp - prevUnits;
                    final long tail = currentTimestamp + fwdUnits;

                    // pop out all values from the current window that are not in the new window
                    long popCount = 0;
                    while (!ctx.timestampWindowBuffer.isEmpty() && ctx.timestampWindowBuffer.front() < head) {
                        ctx.timestampWindowBuffer.remove();
                        popCount++;
                    }

                    // advance the influencerKeyIt and timestamp iterators until we are within the window. This only
                    // happens when initialStep == true because we have not created the minimum set of rows but include
                    // all non-null timestamp rows in our influencer values
                    long skipCount = 0;
                    while (currentTailTs < head) {
                        Assert.eqTrue(initialStep, "initialStep when skipping rows");
                        currentTailTs = nextLongOrMax(influencerTsTailIt);
                        skipCount++;
                    }
                    influencerKeyIt.getNextRowSequenceWithLength(skipCount);

                    // push in all values that are in the new window (inclusive of tail)
                    long pushCount = 0;
                    while (currentTailTs <= tail) {
                        // add this value to the buffer before advancing
                        ctx.timestampWindowBuffer.add(currentTailTs);
                        currentTailTs = nextLongOrMax(influencerTsTailIt);
                        pushCount++;
                    }

                    // write the push and pop counts to the chunks
                    popChunk.set(ii, Math.toIntExact(popCount));
                    pushChunk.set(ii, Math.toIntExact(pushCount));

                    // store the influencer keys
                    chunkBuilder.appendRowSequence(influencerKeyIt.getNextRowSequenceWithLength(pushCount));
                }

                // execute the operators
                try (final RowSequence chunkInfluencerRs = chunkBuilder.build()) {
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
        }

        // call `finishUpdate()` function for each operator
        for (int opIdx : context.dirtyOperatorIndices) {
            operators[opIdx].finishUpdate(context.opContext[opIdx]);
        }
    }
}
