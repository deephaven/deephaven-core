package io.deephaven.engine.table.impl.updateby.prod;

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

public class FloatCumProdOperator extends BaseFloatUpdateByOperator {

    public FloatCumProdOperator(@NotNull final MatchPair inputPair,
                                @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                // region extra-constructor-args
                                // endregion extra-constructor-args
    ) {
        super(inputPair, new String[]{inputPair.rightColumn}, redirContext);
        // region constructor
        // endregion constructor
    }

    protected void doProcessChunk(@NotNull final Context ctx,
                              @NotNull final RowSequence inputKeys,
                              @NotNull final Chunk<Values> workingChunk) {
        if(Float.isNaN(ctx.curVal)) {
            if(!ctx.filledWithPermanentValue) {
                ctx.filledWithPermanentValue = true;
                ctx.outputValues.get().fillWithValue(0, ctx.outputValues.get().capacity(), Float.NaN);
            }
        } else {
            accumulate(workingChunk.asFloatChunk(), ctx, 0, workingChunk.size());
        }

        outputSource.fillFromChunk(ctx.fillContext.get(), ctx.outputValues.get(), inputKeys);
    }

    private void accumulate(@NotNull final Chunk<Values> values,
                              @NotNull final Context ctx,
                              final int runStart,
                              final int runLength) {
        final FloatChunk<Values> asFloats = values.asFloatChunk();
        final WritableFloatChunk<Values> localOutputValues = ctx.outputValues.get();
        for (int ii = runStart; ii < runStart + runLength; ii++) {
            final float currentVal = asFloats.get(ii);
            if (ctx.curVal == NULL_FLOAT) {
                ctx.curVal = currentVal;
            } else if (currentVal != NULL_FLOAT) {
                ctx.curVal *= currentVal;
            }
            localOutputValues.set(ii, ctx.curVal);
        }
    }
}
