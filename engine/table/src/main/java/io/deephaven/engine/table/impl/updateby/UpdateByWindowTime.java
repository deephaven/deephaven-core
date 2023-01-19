package io.deephaven.engine.table.impl.updateby;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.hash.TIntHashSet;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
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
    public static final double CONTEXT_GROWTH_PERCENTAGE = 0.25;
    private static final int WINDOW_CHUNK_SIZE = 4096;
    protected final long prevUnits;
    protected final long fwdUnits;

    public class UpdateByWindowTimeBucketContext extends UpdateByWindowBucketContext {
        protected final ChunkSource.GetContext influencerTimestampContext;
        protected int currentGetContextSize;

        public UpdateByWindowTimeBucketContext(final TrackingRowSet sourceRowSet,
                @NotNull final ColumnSource<?> timestampColumnSource,
                @Nullable final LongSegmentedSortedArray timestampSsa,
                final TrackingRowSet timestampValidRowSet,
                final int chunkSize,
                final boolean initialStep) {
            super(sourceRowSet, timestampColumnSource, timestampSsa, timestampValidRowSet, chunkSize, initialStep);

            influencerTimestampContext = timestampColumnSource.makeGetContext(WINDOW_CHUNK_SIZE);
        }

        @Override
        public void close() {
            super.close();
            try (final SafeCloseable ignoreCtx1 = influencerTimestampContext) {
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
            context.opContext[opIdx] = operators[opIdx].makeUpdateContext(context.workingChunkSize);
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

    // windowed by time/ticks is more complex to compute: find all the changed rows and the rows that would
    // be affected by the changes (includes newly added rows) and need to be recomputed. Then include all
    // the rows that are affected by deletions (if any). After the affected rows have been identified,
    // determine which rows will be needed to compute new values for the affected rows (influencer rows)
    @Override
    public void computeAffectedRowsAndOperators(UpdateByWindowBucketContext context, @NotNull TableUpdate upstream) {
        UpdateByWindowTimeBucketContext ctx = (UpdateByWindowTimeBucketContext) context;

        if (upstream.empty() || ctx.sourceRowSet.isEmpty()) {
            return;
        }

        // all rows are affected on the initial step
        if (ctx.initialStep) {
            ctx.affectedRows = ctx.sourceRowSet;

            ctx.influencerRows = computeInfluencerRowsTime(ctx.timestampValidRowSet, ctx.affectedRows, prevUnits, fwdUnits,
                    ctx.timestampColumnSource, ctx.timestampSsa, false);

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

        try (final RowSet prevRows = upstream.modified().isNonempty() || upstream.removed().isNonempty() ?
                ctx.timestampValidRowSet.copyPrev() :
                null) {

            final WritableRowSet tmpAffected = RowSetFactory.empty();

            if (upstream.modified().isNonempty()) {
                // modified timestamps will affect the current and previous values
                try (final RowSet modifiedAffected = computeAffectedRowsTime(ctx.timestampValidRowSet, upstream.modified(), prevUnits, fwdUnits, ctx.timestampColumnSource, ctx.timestampSsa, false)) {
                    tmpAffected.insert(modifiedAffected);
                }
                try (final WritableRowSet modifiedAffectedPrev = computeAffectedRowsTime(prevRows, upstream.getModifiedPreShift(), prevUnits, fwdUnits, ctx.timestampColumnSource, ctx.timestampSsa, true)) {
                    // we used the SSA (post-shift) to get these keys, no need to shift
                    // retain only the rows that still exist in the sourceRowSet
                    modifiedAffectedPrev.retain(ctx.timestampValidRowSet);
                    tmpAffected.insert(modifiedAffectedPrev);
                }
                // naturally need to compute all modified rows
                tmpAffected.insert(upstream.modified());
            }

            if (upstream.added().isNonempty()) {
                try (final RowSet addedAffected = computeAffectedRowsTime(ctx.timestampValidRowSet, upstream.added(), prevUnits, fwdUnits, ctx.timestampColumnSource, ctx.timestampSsa, false)) {
                    tmpAffected.insert(addedAffected);
                }
                // naturally need to compute all new rows
                tmpAffected.insert(upstream.added());
            }

            // other rows can be affected by removes or mods
            if (upstream.removed().isNonempty()) {
                try (final WritableRowSet removedAffected = computeAffectedRowsTime(prevRows, upstream.removed(), prevUnits, fwdUnits, ctx.timestampColumnSource, ctx.timestampSsa, true)) {
                    // we used the SSA (post-shift) to get these keys, no need to shift
                    // retain only the rows that still exist in the sourceRowSet
                    removedAffected.retain(ctx.timestampValidRowSet);

                    tmpAffected.insert(removedAffected);
                }
            }

            ctx.affectedRows = tmpAffected;
        }

        // now get influencer rows for the affected rows
        ctx.influencerRows = computeInfluencerRowsTime(ctx.timestampValidRowSet, ctx.affectedRows, prevUnits, fwdUnits,
                ctx.timestampColumnSource, ctx.timestampSsa, false);

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
                final LongColumnIterator influencerTsHeadIt =
                        new LongColumnIterator(context.timestampColumnSource, context.influencerRows);
                final LongColumnIterator influencerTsTailIt =
                        new LongColumnIterator(context.timestampColumnSource, context.influencerRows);
                final RowSequence.Iterator influencerKeyIt = ctx.influencerRows.getRowSequenceIterator();
                final ChunkSource.GetContext localTimestampContext =
                        ctx.timestampColumnSource.makeGetContext(ctx.workingChunkSize);
                final WritableIntChunk<? extends Values> pushChunk =
                        WritableIntChunk.makeWritableChunk(ctx.workingChunkSize);
                final WritableIntChunk<? extends Values> popChunk =
                        WritableIntChunk.makeWritableChunk(ctx.workingChunkSize)) {

            long currentHeadTs = nextLongOrMax(influencerTsHeadIt);
            long currentTailTs = nextLongOrMax(influencerTsTailIt);

            while (it.hasMore()) {
                final RowSequence chunkRs = it.getNextRowSequenceWithLength(ctx.workingChunkSize);
                final int chunkRsSize = chunkRs.intSize();

                // NOTE: we did not put null values into our SSA and our influencer rowset is built using the
                // SSA. there should be no null timestamps considered in the rolling windows
                final LongChunk<? extends Values> timestampChunk =
                        ctx.timestampColumnSource.getChunk(localTimestampContext, chunkRs).asLongChunk();

                // chunk processing
                long totalCount = 0;

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
                    while (currentHeadTs < head) {
                        currentHeadTs = nextLongOrMax(influencerTsHeadIt);
                        popCount++;
                    }

                    // push in all values that are in the new window (inclusive of tail)
                    long pushCount = 0;
                    while (currentTailTs <= tail) {
                        currentTailTs = nextLongOrMax(influencerTsTailIt);
                        pushCount++;
                    }

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
                    // prep the chunk array needed by the accumulate call
                    final int[] srcIndices = operatorInputSourceSlots[opIdx];
                    Chunk<? extends Values>[] chunkArr = new Chunk[srcIndices.length];
                    for (int ii = 0; ii < srcIndices.length; ii++) {
                        int srcIdx = srcIndices[ii];
                        // chunk prep
                        prepareValuesChunkForSource(ctx, srcIdx, chunkInfluencerRs);
                        chunkArr[ii] = ctx.inputSourceChunks[srcIdx];
                    }

                    // make the specialized call for windowed operators
                    ((UpdateByWindowedOperator.Context) ctx.opContext[opIdx]).accumulate(
                            chunkRs,
                            chunkArr,
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
