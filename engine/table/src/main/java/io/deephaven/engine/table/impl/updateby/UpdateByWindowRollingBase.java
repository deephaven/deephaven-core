package io.deephaven.engine.table.impl.updateby;

import gnu.trove.list.array.TIntArrayList;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.stream.IntStream;

/**
 * This is the base class of {@link UpdateByWindowRollingTicks} and {@link UpdateByWindowRollingTime}.
 */
abstract class UpdateByWindowRollingBase extends UpdateByWindow {
    final long prevUnits;
    final long fwdUnits;

    static class UpdateByWindowRollingBucketContext extends UpdateByWindowBucketContext {
        int maxGetContextSize;
        WritableIntChunk<Values>[] pushChunks;
        WritableIntChunk<Values>[] popChunks;
        int[] influencerCounts;

        UpdateByWindowRollingBucketContext(final TrackingRowSet sourceRowSet,
                @Nullable final ColumnSource<?> timestampColumnSource,
                @Nullable final LongSegmentedSortedArray timestampSsa,
                final TrackingRowSet timestampValidRowSet,
                final boolean timestampsModified,
                final int chunkSize,
                final boolean initialStep) {
            super(sourceRowSet,
                    timestampColumnSource,
                    timestampSsa,
                    timestampValidRowSet,
                    timestampsModified,
                    chunkSize,
                    initialStep);
        }

        @Override
        public void close() {
            super.close();
            Assert.eqNull(pushChunks, "pushChunks");
            Assert.eqNull(popChunks, "popChunks");
        }
    }

    UpdateByWindowRollingBase(@NotNull final UpdateByOperator[] operators,
            @NotNull final int[][] operatorSourceSlots,
            final long prevUnits,
            final long fwdUnits,
            @Nullable final String timestampColumnName) {
        super(operators, operatorSourceSlots, timestampColumnName);
        this.prevUnits = prevUnits;
        this.fwdUnits = fwdUnits;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    void prepareWindowBucket(UpdateByWindowBucketContext context) {
        UpdateByWindowRollingBucketContext ctx = (UpdateByWindowRollingBucketContext) context;

        // working chunk size need not be larger than affectedRows.size()
        ctx.workingChunkSize = Math.toIntExact(Math.min(ctx.workingChunkSize, ctx.affectedRows.size()));
        ctx.maxGetContextSize = ctx.workingChunkSize;

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
    @OverridingMethodsMustInvokeSuper
    void finalizeWindowBucket(UpdateByWindowBucketContext context) {
        UpdateByWindowRollingBucketContext ctx = (UpdateByWindowRollingBucketContext) context;
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

    abstract void computeWindows(UpdateByWindowRollingBucketContext ctx);


    @Override
    void processWindowBucketOperatorSet(final UpdateByWindowBucketContext context,
            final int[] opIndices,
            final int[] srcIndices,
            final UpdateByOperator.Context[] winOpContexts,
            final Chunk<? extends Values>[] chunkArr,
            final ChunkSource.GetContext[] chunkContexts,
            final boolean initialStep) {
        Assert.neqNull(context.inputSources, "assignInputSources() must be called before processRow()");

        final int[] dirtyOpIndices;
        if (initialStep) {
            dirtyOpIndices = IntStream.range(0, opIndices.length).toArray();
        } else {
            // Don't waste resources by considering the operators that are not dirty
            final TIntArrayList dirtyOpList = new TIntArrayList(opIndices.length);
            for (int ii = 0; ii < opIndices.length; ii++) {
                final int opIdx = opIndices[ii];
                if (context.dirtyOperators.get(opIdx)) {
                    // add the index of the dirty operator in opIndices
                    dirtyOpList.add(ii);
                }
            }
            dirtyOpIndices = dirtyOpList.toArray();
        }

        UpdateByWindowRollingBucketContext ctx = (UpdateByWindowRollingBucketContext) context;

        try (final RowSequence.Iterator affectedRowsIt = ctx.affectedRows.getRowSequenceIterator();
                final RowSequence.Iterator influencerRowsIt = ctx.influencerRows.getRowSequenceIterator()) {

            // Call the specialized version of `intializeUpdate()` for these operators.
            for (int ii : dirtyOpIndices) {
                UpdateByOperator rollingOp = operators[opIndices[ii]];
                rollingOp.initializeRolling(winOpContexts[ii]);
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
                for (int ii : dirtyOpIndices) {
                    winOpContexts[ii].accumulateRolling(
                            affectedRs,
                            chunkArr,
                            ctx.pushChunks[affectedChunkOffset],
                            ctx.popChunks[affectedChunkOffset],
                            affectedChunkSize);
                }

                affectedChunkOffset++;
            }

            // Finalize the operators.
            for (int ii : dirtyOpIndices) {
                UpdateByOperator rollingOp = operators[opIndices[ii]];
                rollingOp.finishUpdate(winOpContexts[ii]);
            }
        }
    }
}
