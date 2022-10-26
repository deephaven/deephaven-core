package io.deephaven.engine.table.impl.updateby;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.hash.TIntHashSet;
import io.deephaven.base.ringbuffer.IntRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
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
import java.util.stream.IntStream;

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

        public UpdateByWindowTicksContext(final TrackingRowSet sourceRowSet,
                @Nullable final ColumnSource<?> timestampColumnSource,
                @Nullable final LongSegmentedSortedArray timestampSsa, final int chunkSize, final boolean initialStep) {
            super(sourceRowSet, null, null, chunkSize, initialStep);

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
    }

    UpdateByWindowTicks(UpdateByOperator[] operators, int[][] operatorSourceSlots, long prevUnits, long fwdUnits) {
        super(operators, operatorSourceSlots, null);
        this.prevUnits = prevUnits;
        this.fwdUnits = fwdUnits;
    }

    @Override
    protected void makeOperatorContexts(UpdateByWindowContext context) {
        UpdateByWindowTicksContext ctx = (UpdateByWindowTicksContext) context;

        ctx.workingChunkSize = UpdateByWindowTicksContext.WINDOW_CHUNK_SIZE;
        ctx.currentGetContextSize = ctx.workingChunkSize;

        // create contexts for the affected operators
        for (int opIdx : context.dirtyOperatorIndices) {
            context.opContext[opIdx] = operators[opIdx].makeUpdateContext(context.workingChunkSize);
        }
    }

    public UpdateByWindowContext makeWindowContext(final TrackingRowSet sourceRowSet,
            final ColumnSource<?> timestampColumnSource,
            final LongSegmentedSortedArray timestampSsa,
            final int chunkSize,
            final boolean isInitializeStep) {
        return new UpdateByWindowTicksContext(sourceRowSet, timestampColumnSource, timestampSsa,
                chunkSize,
                isInitializeStep);
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

    private void ensureGetContextSize(UpdateByWindowTicksContext ctx, long newSize) {
        if (ctx.currentGetContextSize < newSize) {
            long size = ctx.currentGetContextSize;
            while (size < newSize) {
                size *= 2;
            }
            ctx.currentGetContextSize = LongSizedDataStructure.intSize(
                    "ensureGetContextSize exceeded Integer.MAX_VALUE",
                    size);

            // use this to determine which input sources are initialized
            Arrays.fill(ctx.inputSourceChunkPopulated, false);

            for (int opIdx : ctx.dirtyOperatorIndices) {
                final int[] sourceIndices = operatorInputSourceSlots[opIdx];
                for (int sourceSlot : sourceIndices) {
                    if (!ctx.inputSourceChunkPopulated[sourceSlot]) {
                        // close the existing context
                        ctx.inputSourceGetContexts[sourceSlot].close();

                        // create a new context of the larger size
                        ctx.inputSourceGetContexts[sourceSlot] =
                                ctx.inputSources[sourceSlot].makeGetContext(ctx.currentGetContextSize);
                        ctx.inputSourceChunkPopulated[sourceSlot] = true;
                    }
                }
            }
        }
    }

    /***
     * This function takes care of loading/preparing the next set of influencer data, in this case we load the next
     * chunk of key and position data and reset the index
     */
    private void loadNextInfluencerChunks(UpdateByWindowTicksContext ctx) {
        if (!ctx.influencerIt.hasMore()) {
            ctx.nextInfluencerPos = Integer.MAX_VALUE;
            ctx.nextInfluencerKey = Long.MAX_VALUE;
            return;
        }

        final RowSequence influencerRs =
                ctx.influencerIt.getNextRowSequenceWithLength(UpdateByWindowTicksContext.WINDOW_CHUNK_SIZE);
        ctx.influencerKeyChunk = influencerRs.asRowKeyChunk();

        final RowSequence influencePosRs =
                ctx.influencerPosIt.getNextRowSequenceWithLength(UpdateByWindowTicksContext.WINDOW_CHUNK_SIZE);
        ctx.influencerPosChunk = influencePosRs.asRowKeyChunk();

        Assert.eqTrue(influencePosRs.lastRowKey() < Integer.MAX_VALUE,
                "updateBy window positions exceeded maximum size");

        ctx.influencerPosChunkSize = ctx.influencerPosChunk.size();

        ctx.nextInfluencerIndex = 0;
        ctx.nextInfluencerPos = LongSizedDataStructure.intSize(
                "updateBy window positions exceeded maximum size",
                ctx.influencerPosChunk.get(ctx.nextInfluencerIndex));
        ctx.nextInfluencerKey = ctx.influencerKeyChunk.get(ctx.nextInfluencerIndex);
    }

    // windowed by time/ticks is more complex to compute: find all the changed rows and the rows that would
    // be affected by the changes (includes newly added rows) and need to be recomputed. Then include all
    // the rows that are affected by deletions (if any). After the affected rows have been identified,
    // determine which rows will be needed to compute new values for the affected rows (influencer rows)
    @Override
    public void computeAffectedRowsAndOperators(UpdateByWindowContext context, @NotNull TableUpdate upstream) {

        UpdateByWindowTicksContext ctx = (UpdateByWindowTicksContext) context;

        // all rows are affected on the initial step
        if (ctx.initialStep) {
            ctx.affectedRows = ctx.sourceRowSet.copy();
            ctx.influencerRows = ctx.affectedRows;

            // no need to invert, just create a flat rowset
            ctx.affectedRowPositions = RowSetFactory.flat(ctx.sourceRowSet.size());
            ctx.influencerPositions = RowSetFactory.flat(ctx.sourceRowSet.size());

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
            // determine which operators wer affected by this update
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

        // changed rows are all mods+adds
        WritableRowSet changed = upstream.added().union(upstream.modified());

        // need a writable rowset
        WritableRowSet tmpAffected;

        // compute the rows affected from these changes
        try (final WritableRowSet changedInverted = ctx.sourceRowSet.invert(changed)) {
            tmpAffected = computeAffectedRowsTicks(ctx.sourceRowSet, changed, changedInverted, prevUnits, fwdUnits);
        }

        // other rows can be affected by removes
        if (upstream.removed().isNonempty()) {
            try (final RowSet prev = ctx.sourceRowSet.copyPrev();
                 final RowSet removedPositions = prev.invert(upstream.removed());
                 final WritableRowSet affectedByRemoves =
                         computeAffectedRowsTicks(prev, upstream.removed(), removedPositions, prevUnits,
                                 fwdUnits)) {
                // apply shifts to get back to pos-shift space
                upstream.shifted().apply(affectedByRemoves);
                // retain only the rows that still exist in the sourceRowSet
                affectedByRemoves.retain(ctx.sourceRowSet);
                tmpAffected.insert(affectedByRemoves);
            }
        }

        ctx.affectedRows = tmpAffected;

        // now get influencer rows for the affected rows
        // generate position data rowsets for efficiently computed position offsets
        ctx.affectedRowPositions = ctx.sourceRowSet.invert(ctx.affectedRows);

        ctx.influencerRows = computeInfluencerRowsTicks(ctx.sourceRowSet, ctx.affectedRows, ctx.affectedRowPositions,
                prevUnits, fwdUnits);
        ctx.influencerPositions = ctx.sourceRowSet.invert(ctx.influencerRows);

        makeOperatorContexts(ctx);

    }

    @Override
    public void processRows(UpdateByWindowContext context, boolean initialStep) {
        UpdateByWindowTicksContext ctx = (UpdateByWindowTicksContext) context;

        Assert.neqNull(context.inputSources, "assignInputSources() must be called before processRow()");

        for (int opIdx : context.dirtyOperatorIndices) {
            UpdateByWindowedOperator winOp = (UpdateByWindowedOperator) operators[opIdx];
            // call the specialized version of `intializeUpdate()` for these operators
            winOp.initializeUpdate(ctx.opContext[opIdx]);
        }

        ctx.influencerIt = ctx.influencerRows.getRowSequenceIterator();
        ctx.influencerPosIt = ctx.influencerPositions.getRowSequenceIterator();

        try (final RowSequence.Iterator it = ctx.affectedRows.getRowSequenceIterator();
                final RowSequence.Iterator posIt = ctx.affectedRowPositions.getRowSequenceIterator();
                final WritableIntChunk<? extends Values> pushChunk =
                        WritableIntChunk.makeWritableChunk(ctx.workingChunkSize);
                final WritableIntChunk<? extends Values> popChunk =
                        WritableIntChunk.makeWritableChunk(ctx.workingChunkSize)) {

            // load the first chunk of influencer values (fillWindowTicks() will call in future)
            loadNextInfluencerChunks(ctx);

            final long sourceRowSetSize = ctx.sourceRowSet.size();

            while (it.hasMore()) {
                final RowSequence chunkRs = it.getNextRowSequenceWithLength(ctx.workingChunkSize);
                final RowSequence chunkPosRs = posIt.getNextRowSequenceWithLength(ctx.workingChunkSize);
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
                    while (!ctx.currentWindowPositions.isEmpty() && ctx.currentWindowPositions.front() < head) {
                        ctx.currentWindowPositions.remove();
                        popCount++;
                    }

                    // skip values until they match the window (this can only happen on initial addition of rows
                    // to the table, because we short-circuited the precise building of the influencer rows for
                    // efficiency)
                    while (ctx.nextInfluencerPos < head) {
                        ctx.nextInfluencerIndex++;

                        if (ctx.nextInfluencerIndex < ctx.influencerPosChunkSize) {
                            ctx.nextInfluencerPos = (int) ctx.influencerPosChunk.get(ctx.nextInfluencerIndex);
                            ctx.nextInfluencerKey = ctx.influencerKeyChunk.get(ctx.nextInfluencerIndex);
                        } else {
                            // try to bring in new data
                            loadNextInfluencerChunks(ctx);
                        }
                    }

                    // push matching values
                    int pushCount = 0;
                    while (ctx.nextInfluencerPos <= tail) {
                        ctx.currentWindowPositions.add(ctx.nextInfluencerPos);
                        pushCount++;
                        // add this key to the needed set for this chunk
                        chunkInfluencerBuilder.appendKey(ctx.nextInfluencerKey);
                        ctx.nextInfluencerIndex++;

                        if (ctx.nextInfluencerIndex < ctx.influencerPosChunkSize) {
                            ctx.nextInfluencerPos = (int) ctx.influencerPosChunk.get(ctx.nextInfluencerIndex);
                            ctx.nextInfluencerKey = ctx.influencerKeyChunk.get(ctx.nextInfluencerIndex);
                        } else {
                            // try to bring in new data
                            loadNextInfluencerChunks(ctx);
                        }
                    }

                    // write the push and pop counts to the chunks
                    popChunk.set(ii, popCount);
                    pushChunk.set(ii, pushCount);
                }

                // execute the operators
                try (final RowSet chunkInfluencerRs = chunkInfluencerBuilder.build()) {
                    ensureGetContextSize(ctx, chunkInfluencerRs.size());

                    Arrays.fill(ctx.inputSourceChunkPopulated, false);
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
        }

        // call `finishUpdate()` function for each operator
        for (int opIdx : context.dirtyOperatorIndices) {
            operators[opIdx].finishUpdate(context.opContext[opIdx]);
        }
    }
}
