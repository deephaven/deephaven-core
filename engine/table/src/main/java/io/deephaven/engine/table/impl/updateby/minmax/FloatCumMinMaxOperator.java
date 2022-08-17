package io.deephaven.engine.table.impl.updateby.minmax;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseFloatUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class FloatCumMinMaxOperator extends BaseFloatUpdateByOperator {
    private final boolean isMax;

    public FloatCumMinMaxOperator(@NotNull final MatchPair inputPair,
                                  final boolean isMax,
                                  @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                  // region extra-constructor-args
                                  // endregion extra-constructor-args
                                        ) {
        super(inputPair, new String[] { inputPair.rightColumn }, redirContext);
        this.isMax = isMax;
        // region constructor
        // endregion constructor
    }

    @Override
    protected void doAddChunk(@NotNull final Context ctx,
                              @NotNull final RowSequence inputKeys,
                              @NotNull final Chunk<Values> workingChunk) {
        if(Float.isNaN(ctx.curVal) || Float.isInfinite(ctx.curVal)) {
            if(!ctx.filledWithPermanentValue) {
                ctx.filledWithPermanentValue = true;
                ctx.outputValues.get().fillWithValue(0, ctx.outputValues.get().capacity(), ctx.curVal);
            }
        } else {
            accumulateMinMax(workingChunk.asFloatChunk(), ctx, 0, workingChunk.size());
        }
        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }

    private void accumulateMinMax(@NotNull final FloatChunk<Values> asFloats,
                                   @NotNull final Context ctx,
                                   final int runStart,
                                   final int runLength) {
        final WritableFloatChunk<Values> localOutputChunk = ctx.outputValues.get();
        for (int ii = runStart; ii < runStart + runLength; ii++) {
            final float currentVal = asFloats.get(ii);
            if (ctx.curVal == NULL_FLOAT) {
                ctx.curVal = currentVal;
            } else if (currentVal != NULL_FLOAT) {
                if ((isMax && currentVal > ctx.curVal) ||
                        (!isMax && currentVal < ctx.curVal)) {
                    ctx.curVal = currentVal;
                }
            }
            localOutputChunk.set(ii, ctx.curVal);
        }
    }
}
