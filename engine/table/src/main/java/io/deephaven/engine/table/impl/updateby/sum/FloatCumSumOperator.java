package io.deephaven.engine.table.impl.updateby.sum;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.internal.BaseFloatUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class FloatCumSumOperator extends BaseFloatUpdateByOperator {

    public FloatCumSumOperator(@NotNull final MatchPair pair,
                               @Nullable final RowRedirection rowRedirection
                               // region extra-constructor-args
                               // endregion extra-constructor-args
    ) {
        super(pair, new String[] { pair.rightColumn }, rowRedirection);
        // region constructor
        // endregion constructor
    }

    @Override
    public void addChunk(@NotNull final UpdateContext context,
                         @NotNull final Chunk<Values> values,
                         @NotNull final LongChunk<? extends RowKeys> keyChunk,
                         @NotNull final IntChunk<RowKeys> bucketPositions,
                         @NotNull final IntChunk<ChunkPositions> startPositions,
                         @NotNull final IntChunk<ChunkLengths> runLengths) {
        final FloatChunk<Values> asFloats = values.asFloatChunk();
        final Context ctx = (Context) context;
        final WritableFloatChunk<Values> localOutputChunk = ctx.outputValues.get();
        for(int runIdx = 0; runIdx < startPositions.size(); runIdx++) {
            final int runStart = startPositions.get(runIdx);
            final int runLength = runLengths.get(runIdx);
            final int bucketPosition = bucketPositions.get(runStart);

            ctx.curVal = bucketLastVal.getFloat(bucketPosition);
            if(Float.isNaN(ctx.curVal)) {
                localOutputChunk.fillWithValue(runStart, runLength, ctx.curVal);
            } else {
                accumulate(asFloats, ctx, runStart, runLength);
                bucketLastVal.set(bucketPosition, ctx.curVal);
            }
        }

        //noinspection unchecked
        outputSource.fillFromChunkUnordered(ctx.fillContext.get(), localOutputChunk, (LongChunk<RowKeys>) keyChunk);
    }

    protected void doAddChunk(@NotNull final Context ctx,
                              @NotNull final RowSequence inputKeys,
                              @NotNull final Chunk<Values> workingChunk,
                              final long groupPosition) {
        ctx.curVal = groupPosition == singletonGroup ? singletonVal : NULL_FLOAT;
        if(ctx.lastGroupPosition != groupPosition) {
            ctx.lastGroupPosition = groupPosition;
            ctx.filledWithPermanentValue = false;
        }

        if(Float.isNaN(ctx.curVal)) {
            if(!ctx.filledWithPermanentValue) {
                ctx.filledWithPermanentValue = true;
                ctx.outputValues.get().fillWithValue(0, ctx.outputValues.get().capacity(), Float.NaN);
            }
        } else {
            accumulate(workingChunk.asFloatChunk(), ctx, 0, workingChunk.size());
        }

        singletonGroup = groupPosition;
        singletonVal = ctx.curVal;
        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }

    private void accumulate(@NotNull final FloatChunk<Values> asFloats,
                               @NotNull final Context ctx,
                               final int runStart,
                               final int runLength) {

        final WritableFloatChunk<Values> localOutputChunk = ctx.outputValues.get();
        for (int ii = runStart; ii < runStart + runLength; ii++) {
            final float currentVal = asFloats.get(ii);
            if (ctx.curVal == NULL_FLOAT) {
                ctx.curVal = currentVal;
            } else if (currentVal != NULL_FLOAT) {
                ctx.curVal += currentVal;
            }
            localOutputChunk.set(ii, ctx.curVal);
        }
    }
}
